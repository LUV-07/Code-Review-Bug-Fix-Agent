from __future__ import annotations

import base64
import json
import os
import re
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

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger(__name__)

GITHUB_TOKEN: str = os.environ["GITHUB_TOKEN"]
REVIEWER_TOKEN: str = os.environ["REVIEWER_TOKEN"]
DB_PATH: str = os.environ.get("DB_PATH", "./data/reviews.db")
GITHUB_API_BASE = "https://api.github.com"

# Auth 

_bearer_scheme = HTTPBearer()


def require_auth(credentials: HTTPAuthorizationCredentials = Security(_bearer_scheme)):
    if credentials.credentials != REVIEWER_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing Bearer token",
        )
    return credentials.credentials


#  Pydantic v2 schemas 

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


# DB helpers 

async def _list_awaiting_human() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT pr_number, repo, state_json, created_at FROM pr_reviews "
            "WHERE status = 'awaiting_human' ORDER BY created_at DESC"
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


# GitHub helpers 

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


def _apply_unified_diff(original: str, patch: str) -> str:
    try:
        orig_lines = original.splitlines(keepends=True)
        output: List[str] = []
        orig_pos = 0

        for line in patch.splitlines(keepends=True):
            if line.startswith("---") or line.startswith("+++"):
                continue
            if line.startswith("@@"):
                m = re.search(r"@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
                if m:
                    orig_start = int(m.group(1)) - 1
                    output.extend(orig_lines[:orig_start])
                    orig_pos = orig_start
                continue
            if line.startswith("-"):
                orig_pos += 1
            elif line.startswith("+"):
                output.append(line[1:])
            else:
                if orig_pos < len(orig_lines):
                    output.append(orig_lines[orig_pos])
                orig_pos += 1

        if orig_pos < len(orig_lines):
            output.extend(orig_lines[orig_pos:])

        return "".join(output)
    except Exception as exc:
        log.warning("diff_apply_fallback", error=str(exc))
        return original + f"\n# Auto-patch application failed: {exc}\n"


async def _apply_patches(state: Dict[str, Any], patch_diffs: List[str]) -> str:
    owner, repo = state["repo_full_name"].split("/", 1)
    branch = state["head_branch"]
    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, diff in enumerate(patch_diffs):
            m = re.search(r"^\+\+\+ b?/(.+)$", diff, re.MULTILINE)
            if not m:
                patches = state.get("patches", [])
                filename = patches[i]["file"] if i < len(patches) else "unknown.py"
            else:
                filename = m.group(1)

            url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{filename}"
            resp = await client.get(url, params={"ref": branch}, headers=_gh_headers())
            if resp.status_code == 404:
                log.warning("patch_file_not_found", filename=filename)
                continue
            resp.raise_for_status()

            data = resp.json()
            file_sha = data.get("sha")
            current_b64 = data.get("content", "")
            current_content = base64.b64decode(current_b64).decode("utf-8", errors="replace")

            new_content = _apply_unified_diff(current_content, diff)
            encoded = base64.b64encode(new_content.encode("utf-8")).decode()

            payload: Dict[str, Any] = {
                "message": f"fix: auto-patch issue #{i+1} in {filename} [bot]",
                "content": encoded,
                "branch": branch,
            }
            if file_sha:
                payload["sha"] = file_sha

            try:
                put_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{filename}"
                put_resp = await client.put(put_url, headers=_gh_headers(), json=payload)
                put_resp.raise_for_status()
                commit_sha = put_resp.json().get("commit", {}).get("sha", "unknown")
                results.append(f"✓ {filename} → {commit_sha[:8]}")
            except Exception as exc:
                log.error("patch_apply_error", filename=filename, error=str(exc))
                results.append(f"✗ {filename}: {exc}")

    return "; ".join(results) if results else "no patches applied"


# FastAPI app

audit_app = FastAPI(title="Code Review Audit API", version="1.0.0")


@audit_app.get("/reviews", response_model=List[PRSummary], dependencies=[Depends(require_auth)])
async def list_reviews() -> List[PRSummary]:
    rows = await _list_awaiting_human()
    summaries: List[PRSummary] = []
    for row in rows:
        s = json.loads(row["state_json"])
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


@audit_app.get("/reviews/{pr_number}", dependencies=[Depends(require_auth)])
async def get_review(pr_number: int) -> Dict[str, Any]:
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
                    f"Reviewer note: {body.reviewer_note or '—'}\n\nResult: {github_result}",
                )

        elif body.action == "modify":
            if not body.modified_patch:
                raise HTTPException(status_code=422, detail="modified_patch required for action='modify'")
            github_result = await _apply_patches(state, [body.modified_patch])
            await _post_pr_comment(
                client, owner, repo, pr_number,
                f"🤖 **Modified patch applied** by reviewer.\n\n"
                f"Reviewer note: {body.reviewer_note or '—'}\n\nResult: {github_result}",
            )

        elif body.action == "reject":
            note = body.reviewer_note or "No additional notes."
            await _post_pr_comment(
                client, owner, repo, pr_number,
                f"🤖 **Auto-patch rejected** by reviewer.\n\nReason: {note}\n\n"
                "No automated changes were applied. Please address the issues manually.",
            )
            github_result = "comment posted, no code changes"

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