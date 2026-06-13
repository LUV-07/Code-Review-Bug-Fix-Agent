from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import aiosqlite
import httpx
import structlog
import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

load_dotenv()

# Logging 
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
#  Config 

GITHUB_TOKEN: str = os.environ["GITHUB_TOKEN"]
WEBHOOK_SECRET: str = os.environ["WEBHOOK_SECRET"]
REVIEWER_TOKEN: str = os.environ["REVIEWER_TOKEN"]
DB_PATH: str = os.environ.get("DB_PATH", "./data/reviews.db")
GITHUB_API_BASE = "https://api.github.com"

#  Shared queue 
pr_event_queue: asyncio.Queue = asyncio.Queue()


#  Data models 
@dataclass
class ChangedFile:
    filename: str
    status: str
    additions: int
    deletions: int
    patch: Optional[str] = None


@dataclass
class PREvent:
    repo_full_name: str
    pr_number: int
    pr_title: str
    pr_author: str
    head_sha: str
    base_sha: str
    head_branch: str
    base_branch: str
    pr_url: str
    changed_files: List[ChangedFile] = field(default_factory=list)


#  Pydantic schemas 
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


#  Auth 
_bearer_scheme = HTTPBearer()


def require_auth(credentials: HTTPAuthorizationCredentials = Security(_bearer_scheme)):
    if credentials.credentials != REVIEWER_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid Bearer token")
    return credentials.credentials


#  GitHub helpers 
def _gh_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _verify_signature(secret: str, payload: bytes, sig_header: Optional[str]) -> bool:
    if not sig_header or not sig_header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    received = sig_header[len("sha256="):]
    return hmac.compare_digest(expected, received)


async def _fetch_changed_files(client, owner, repo, pull_number) -> List[ChangedFile]:
    files = []
    page = 1
    while True:
        resp = await client.get(
            f"{GITHUB_API_BASE}/repos/{owner}/{repo}/pulls/{pull_number}/files",
            params={"per_page": 100, "page": page},
            headers=_gh_headers(),
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for f in batch:
            files.append(ChangedFile(
                filename=f["filename"], status=f["status"],
                additions=f.get("additions", 0), deletions=f.get("deletions", 0),
                patch=f.get("patch"),
            ))
        if len(batch) < 100:
            break
        page += 1
    return files


#  DB helpers 
async def _ensure_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pr_reviews (
                pr_number  INTEGER PRIMARY KEY,
                repo       TEXT NOT NULL,
                state_json TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'awaiting_human',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        await db.commit()
    log.info("db_initialized", path=DB_PATH)


async def _get_pr_state(pr_number: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT state_json FROM pr_reviews WHERE pr_number = ?", (pr_number,)
        ) as cur:
            row = await cur.fetchone()
    return json.loads(row[0]) if row else None


async def _list_awaiting() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT pr_number, repo, state_json, created_at FROM pr_reviews "
            "WHERE status='awaiting_human' ORDER BY created_at DESC"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def _mark_resolved(pr_number: int, state: Dict[str, Any]):
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE pr_reviews SET status='resolved', state_json=?, updated_at=? WHERE pr_number=?",
            (json.dumps(state), now, pr_number),
        )
        await db.commit()


#  App 
app = FastAPI(title="Code Review Pipeline", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _startup():
    await _ensure_db()
    asyncio.create_task(_pipeline_worker())
    log.info("app_started")


async def _pipeline_worker():
    from pipeline import run_pipeline
    log.info("pipeline_worker_started")
    while True:
        try:
            event = await pr_event_queue.get()
            log.info("worker_processing", pr_number=event.pr_number)
            await run_pipeline(event)
            pr_event_queue.task_done()
        except Exception as exc:
            log.error("worker_error", error=str(exc))
            await asyncio.sleep(1)


#  Routes 

@app.get("/health")
async def health():
    return {"status": "ok", "queue_size": pr_event_queue.qsize()}


@app.post("/webhook")
async def github_webhook(request: Request):
    raw_body = await request.body()
    sig_header = request.headers.get("X-Hub-Signature-256")

    if not _verify_signature(WEBHOOK_SECRET, raw_body, sig_header):
        log.warning("invalid_signature")
        return JSONResponse({"status": "ignored", "reason": "invalid_signature"})

    event_type = request.headers.get("X-GitHub-Event", "")
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        return JSONResponse({"status": "ignored", "reason": "bad_json"})

    log.info("webhook_received", event_type=event_type, action=payload.get("action"))

    if event_type != "pull_request":
        return JSONResponse({"status": "ignored", "reason": "not_pull_request"})

    action = payload.get("action", "")
    if action not in ("opened", "synchronize"):
        return JSONResponse({"status": "ignored", "reason": f"action_{action}_skipped"})

    pr = payload.get("pull_request", {})
    repo = payload.get("repository", {})
    repo_full_name = repo.get("full_name", "")
    owner, repo_name = repo_full_name.split("/", 1) if "/" in repo_full_name else ("", repo_full_name)

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            changed_files = await _fetch_changed_files(client, owner, repo_name, pr.get("number", 0))
        except Exception as exc:
            log.error("fetch_files_failed", error=str(exc))
            return JSONResponse({"status": "error", "reason": "github_api_failure"})

    event = PREvent(
        repo_full_name=repo_full_name,
        pr_number=pr.get("number", 0),
        pr_title=pr.get("title", ""),
        pr_author=pr.get("user", {}).get("login", ""),
        head_sha=pr.get("head", {}).get("sha", ""),
        base_sha=pr.get("base", {}).get("sha", ""),
        head_branch=pr.get("head", {}).get("ref", ""),
        base_branch=pr.get("base", {}).get("ref", ""),
        pr_url=pr.get("html_url", ""),
        changed_files=changed_files,
    )
    await pr_event_queue.put(event)
    log.info("pr_enqueued", pr_number=event.pr_number)
    return JSONResponse({"status": "accepted", "pr_number": event.pr_number})


@app.get("/api/reviews", response_model=List[PRSummary], dependencies=[Depends(require_auth)])
async def list_reviews():
    rows = await _list_awaiting()
    summaries = []
    for row in rows:
        s = json.loads(row["state_json"])
        diags = s.get("llm_diagnostics", [])
        sev = SeveritySummary(
            HIGH=sum(1 for d in diags if d.get("severity", "").upper() == "HIGH"),
            MEDIUM=sum(1 for d in diags if d.get("severity", "").upper() == "MEDIUM"),
            LOW=sum(1 for d in diags if d.get("severity", "").upper() == "LOW"),
        )
        summaries.append(PRSummary(
            pr_number=s["pr_number"], repo=s["repo_full_name"],
            title=s.get("pr_title", ""), author=s.get("pr_author", ""),
            pr_url=s.get("pr_url", ""), severity_summary=sev,
            patch_count=len(s.get("patches", [])),
            created_at=s.get("created_at", row["created_at"]),
            status="awaiting_human",
        ))
    return summaries


@app.get("/api/reviews/{pr_number}", dependencies=[Depends(require_auth)])
async def get_review(pr_number: int):
    state = await _get_pr_state(pr_number)
    if not state:
        raise HTTPException(status_code=404, detail=f"PR #{pr_number} not found")
    return state


@app.post("/api/reviews/{pr_number}/decision", response_model=DecisionResponse, dependencies=[Depends(require_auth)])
async def post_decision(pr_number: int, body: DecisionRequest):
    from audit_api import _apply_patches, _post_pr_comment

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
                github_result = await _apply_patches(state, [p["diff"] for p in patches])
                await _post_pr_comment(client, owner, repo, pr_number,
                    f"ðŸ¤– **Auto-patch applied**\n\nNote: {body.reviewer_note or 'â€”'}\nResult: {github_result}")
        elif body.action == "modify":
            if not body.modified_patch:
                raise HTTPException(status_code=422, detail="modified_patch required")
            github_result = await _apply_patches(state, [body.modified_patch])
            await _post_pr_comment(client, owner, repo, pr_number,
                f"ðŸ¤– **Modified patch applied**\n\nNote: {body.reviewer_note or 'â€”'}\nResult: {github_result}")
        elif body.action == "reject":
            await _post_pr_comment(client, owner, repo, pr_number,
                f"ðŸ¤– **Patch rejected**\n\nReason: {body.reviewer_note or 'No reason given.'}")
            github_result = "comment posted, no changes"

    human_decision = {
        "action": body.action, "modified_patch": body.modified_patch,
        "reviewer_note": body.reviewer_note, "github_result": github_result,
        "decided_at": datetime.now(timezone.utc).isoformat(),
    }
    state["human_decision"] = human_decision
    state["awaiting_human"] = False
    await _mark_resolved(pr_number, state)

    log.info("decision_recorded", pr_number=pr_number, action=body.action)
    return DecisionResponse(pr_number=pr_number, action=body.action,
                            github_result=github_result,
                            message=f"Decision '{body.action}' recorded.")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")
