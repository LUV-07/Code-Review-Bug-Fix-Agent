"""
Phase 3 — Human Audit API
Routes: GET /reviews, GET /reviews/{pr}, POST /reviews/{pr}/decision
Secured with HTTP Bearer token.
Applies patches to GitHub on "accept" / "modify".
"""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import aiosqlite
import httpx
import structlog
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

load_dotenv()

log = structlog.get_logger(__name__)

GITHUB_TOKEN: str = os.environ["GITHUB_TOKEN"]
REVIEWER_TOKEN: str = os.environ["REVIEWER_TOKEN"]
DB_PATH: str = os.environ.get("DB_PATH", "./data/reviews.db")
GITHUB_API_BASE = "https://api.github.com"

# ─── Auth ─────────────────────────────────────────────────────────────────────

_bearer_scheme = HTTPBearer()


def require_auth(credentials: HTTPAuthorizationCredentials = Security(_bearer_scheme)):
    if credentials.credentials != REVIEWER_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing Bearer token",
        )
    return credentials.credentials


# ─── Pydantic v2 schemas ──────────────────────────────────────────────────────

class SeveritySummary(BaseModel):
    HIGH: int = 0
    MEDIUM: int = 0
    LOW: int = 0


class PRSummary(BaseModel):
    pr_number: int
    repo: str
    title: str
    author: str
    pr_url: str
    severity_summary: SeveritySummary
    patch_count: int
    created_at: str
    status: str


class DecisionRequest(BaseModel):
    action: Literal["accept", "modify", "reject"]
    modified_patch: Optional[str] = Field(default=None)
    reviewer_note: Optional[str] = Field(default=None)


class DecisionResponse(BaseModel):
    pr_number: int
    action: str
    github_result: Optional[str] = None
    message: str


# ─── DB helpers ───────────────────────────────────────────────────────────────

async def _list_awaiting_human() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT pr_number, repo, state_json, created_at FROM pr_reviews WHERE status = 'awaiting_human' ORDER BY created_at DESC"
        ) as cur:
            return [dict(row) for row in await cur.fetchall()]


async def _get_pr_state(pr_number: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT state_json FROM pr_reviews WHERE pr_number = ?", (pr_number,)
        ) as cur:
            row = await cur.fetchone()
    return json.loads(row[0]) if row else None


async def _mark_resolved(pr_number: int, state: Dict[str, Any]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE pr_reviews SET status='resolved', state_json=?, updated_at=? WHERE pr_number=?",
            (json.dumps(state), now, pr_number),
        )
        await db.commit()


# ─── GitHub helpers ───────────────────────────────────────────────────────────

def _gh_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def _post_pr_comment(
    client: httpx.AsyncClient,
    owner: str,
    repo: str,
    pr_number: int,
    body: str,
) -> None:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/issues/{pr_number}/comments"
    resp = await client.post(url, headers=_gh_headers(), json={"body": body})
    resp.raise_for_status()
    log.info("pr_comment_posted", pr_number=pr_number)


async def _get_file_sha(
    client: httpx.AsyncClient,
    owner: str,
    repo: str,
    path: str,
    ref: str,
) -> Optional[str]:
    """Get the blob SHA needed to update a file."""
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{path}"
    resp = await client.get(url, params={"ref": ref}, headers=_gh_headers())
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json().get("sha")


async def _apply_patch_via_contents_api(
    client: httpx.AsyncClient,
    owner: str,
    repo: str,
    branch: str,
    filename: str,
    original_content: str,
    patch_diff: str,
    commit_message: str,
) -> str:
    """
    Apply a unified diff patch by:
    1. Parsing the diff to produce new file content
    2. PUT /repos/{owner}/{repo}/contents/{path} to update the file
    Returns the new commit SHA.
    """
    # Apply the patch manually (simple line-based application)
    new_content = _apply_unified_diff(original_content, patch_diff)
    encoded = base64.b64encode(new_content.encode("utf-8")).decode()

    # Get current file blob SHA
    file_sha = await _get_file_sha(client, owner, repo, filename, branch)

    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{filename}"
    payload: Dict[str, Any] = {
        "message": commit_message,
        "content": encoded,
        "branch": branch,
    }
    if file_sha:
        payload["sha"] = file_sha

    resp = await client.put(url, headers=_gh_headers(), json=payload)
    resp.raise_for_status()
    commit_sha = resp.json().get("commit", {}).get("sha", "unknown")
    log.info("file_updated_via_api", filename=filename, commit=commit_sha[:8])
    return commit_sha


def _apply_unified_diff(original: str, patch: str) -> str:
    """
    Minimal unified diff applicator.
    Handles +/- lines in hunks. Good enough for single-file patches from LLMs.
    Falls back to appending a comment if parsing fails.
    """
    try:
        orig_lines = original.splitlines(keepends=True)
        result_lines = list(orig_lines)  # will be rebuilt
        output: List[str] = []
        in_hunk = False
        orig_pos = 0  # 0-indexed into result_lines

        for line in patch.splitlines(keepends=True):
            if line.startswith("---") or line.startswith("+++"):
                continue
            if line.startswith("@@"):
                # Parse @@ -start,count +start,count @@
                import re
                m = re.search(r"@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
                if m:
                    orig_start = int(m.group(1)) - 1   # 0-indexed
                    # Flush lines before this hunk
                    output.extend(result_lines[:orig_start])
                    orig_pos = orig_start
                in_hunk = True
                continue

            if not in_hunk:
                continue

            if line.startswith("-"):
                orig_pos += 1  # skip this original line
            elif line.startswith("+"):
                output.append(line[1:])
            else:
                # Context line
                if orig_pos < len(result_lines):
                    output.append(result_lines[orig_pos])
                orig_pos += 1

        # Append remaining original lines
        if orig_pos < len(result_lines):
            output.extend(result_lines[orig_pos:])

        return "".join(output)
    except Exception as exc:
        log.warning("diff_apply_fallback", error=str(exc))
        return original + f"\n# Auto-patch application failed: {exc}\n"


async def _apply_patches(state: Dict[str, Any], patch_diffs: List[str]) -> str:
    """Apply all patches to the PR branch via GitHub Contents API."""
    owner, repo = state["repo_full_name"].split("/", 1)
    branch = state["head_branch"]
    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, diff in enumerate(patch_diffs):
            # Extract filename from diff header
            import re
            m = re.search(r"^\+\+\+ b?/(.+)$", diff, re.MULTILINE)
            if not m:
                # Try to get file from patches list
                patches = state.get("patches", [])
                filename = patches[i]["file"] if i < len(patches) else "unknown.py"
            else:
                filename = m.group(1)

            # Get current file content
            url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{filename}"
            resp = await client.get(url, params={"ref": branch}, headers=_gh_headers())
            if resp.status_code == 404:
                log.warning("patch_file_not_found", filename=filename)
                continue
            resp.raise_for_status()
            current_b64 = resp.json().get("content", "")
            current_content = base64.b64decode(current_b64).decode("utf-8", errors="replace")

            try:
                sha = await _apply_patch_via_contents_api(
                    client, owner, repo, branch, filename, current_content, diff,
                    commit_message=f"fix: auto-patch issue #{i+1} in {filename} [bot]",
                )
                results.append(f"✓ {filename} → {sha[:8]}")
            except Exception as exc:
                log.error("patch_apply_error", filename=filename, error=str(exc))
                results.append(f"✗ {filename}: {exc}")

    return "; ".join(results) if results else "no patches applied"


# ─── FastAPI sub-application ──────────────────────────────────────────────────

audit_app = FastAPI(title="Code Review Audit API", version="1.0.0")


@audit_app.get(
    "/reviews",
    response_model=List[PRSummary],
    dependencies=[Depends(require_auth)],
)
async def list_reviews() -> List[PRSummary]:
    """List all PRs currently awaiting human review."""
    rows = await _list_awaiting_human()
    summaries: List[PRSummary] = []
    for row in rows:
        s = json.loads(row["state_json"])
        # Build severity summary
        diags = s.get("llm_diagnostics", [])
        sev = SeveritySummary(
            HIGH=sum(1 for d in diags if d.get("severity", "").upper() == "HIGH"),
            MEDIUM=sum(1 for d in diags if d.get("severity", "").upper() == "MEDIUM"),
            LOW=sum(1 for d in diags if d.get("severity", "").upper() == "LOW"),
        )
        summaries.append(PRSummary(
            pr_number=s["pr_number"],
            repo=s["repo_full_name"],
            title=s.get("pr_title", ""),
            author=s.get("pr_author", ""),
            pr_url=s.get("pr_url", ""),
            severity_summary=sev,
            patch_count=len(s.get("patches", [])),
            created_at=s.get("created_at", row["created_at"]),
            status="awaiting_human",
        ))
    return summaries


@audit_app.get(
    "/reviews/{pr_number}",
    dependencies=[Depends(require_auth)],
)
async def get_review(pr_number: int) -> Dict[str, Any]:
    """Return full PRState for a given PR."""
    state = await _get_pr_state(pr_number)
    if not state:
        raise HTTPException(status_code=404, detail=f"PR #{pr_number} not found")
    return state


@audit_app.post(
    "/reviews/{pr_number}/decision",
    response_model=DecisionResponse,
    dependencies=[Depends(require_auth)],
)
async def post_decision(pr_number: int, body: DecisionRequest) -> DecisionResponse:
    """
    Accept, modify, or reject a PR review.
    accept  → apply generated patches via GitHub API
    modify  → apply reviewer's modified_patch instead
    reject  → post a PR comment, no code changes
    """
    state = await _get_pr_state(pr_number)
    if not state:
        raise HTTPException(status_code=404, detail=f"PR #{pr_number} not found")

    owner, repo = state["repo_full_name"].split("/", 1)
    github_result: Optional[str] = None

    async with httpx.AsyncClient(timeout=60.0) as client:
        if body.action == "accept":
            patches = state.get("patches", [])
            if not patches:
                github_result = "no patches to apply"
            else:
                diffs = [p["diff"] for p in patches]
                github_result = await _apply_patches(state, diffs)
                await _post_pr_comment(
                    client, owner, repo, pr_number,
                    f"🤖 **Auto-patch applied** by Code Review Bot.\n\n"
                    f"Reviewer note: {body.reviewer_note or '—'}\n\n"
                    f"Result: {github_result}",
                )

        elif body.action == "modify":
            if not body.modified_patch:
                raise HTTPException(status_code=422, detail="modified_patch required for action='modify'")
            github_result = await _apply_patches(state, [body.modified_patch])
            await _post_pr_comment(
                client, owner, repo, pr_number,
                f"🤖 **Modified patch applied** by reviewer.\n\n"
                f"Reviewer note: {body.reviewer_note or '—'}\n\n"
                f"Result: {github_result}",
            )

        elif body.action == "reject":
            note = body.reviewer_note or "No additional notes."
            await _post_pr_comment(
                client, owner, repo, pr_number,
                f"🤖 **Auto-patch rejected** by reviewer.\n\n"
                f"Reason: {note}\n\n"
                "No automated changes were applied. Please address the issues manually.",
            )
            github_result = "comment posted, no code changes"

    # Update state with decision and mark resolved
    human_decision = {
        "action": body.action,
        "modified_patch": body.modified_patch,
        "reviewer_note": body.reviewer_note,
        "github_result": github_result,
        "decided_at": datetime.now(timezone.utc).isoformat(),
    }
    state["human_decision"] = human_decision
    state["awaiting_human"] = False
    await _mark_resolved(pr_number, state)

    # Resume LangGraph pipeline (fire-and-forget; won't block response)
    try:
        from pipeline import resume_pipeline
        import asyncio
        asyncio.create_task(resume_pipeline(pr_number, human_decision))
    except Exception as exc:
        log.warning("pipeline_resume_task_error", error=str(exc))

    log.info("decision_recorded", pr_number=pr_number, action=body.action)

    return DecisionResponse(
        pr_number=pr_number,
        action=body.action,
        github_result=github_result,
        message=f"Decision '{body.action}' recorded successfully.",
    )
