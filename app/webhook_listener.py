"""
Phase 1 — GitHub Webhook Listener
Validates incoming GitHub webhooks, extracts PR metadata + changed files,
and enqueues PREvent dataclasses for the agent pipeline.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

import httpx
import structlog
from dotenv import load_dotenv
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

load_dotenv()

#  Structured logging 
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

#Config 
GITHUB_TOKEN: str = os.environ["GITHUB_TOKEN"]
WEBHOOK_SECRET: str = os.environ["WEBHOOK_SECRET"]
GITHUB_API_BASE = "https://api.github.com"

#  Shared queue consumed by the pipeline 
pr_event_queue: asyncio.Queue["PREvent"] = asyncio.Queue()


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


# Helpers 

def _verify_signature(secret: str, payload: bytes, sig_header: Optional[str]) -> bool:
    if not sig_header or not sig_header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    received = sig_header[len("sha256="):]
    return hmac.compare_digest(expected, received)


async def _fetch_changed_files(
    client: httpx.AsyncClient,
    owner: str,
    repo: str,
    pull_number: int,
) -> List[ChangedFile]:
    files: List[ChangedFile] = []
    page = 1
    while True:
        url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/pulls/{pull_number}/files"
        try:
            resp = await client.get(
                url,
                params={"per_page": 100, "page": page},
                headers={
                    "Authorization": f"Bearer {GITHUB_TOKEN}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            log.error(
                "github_files_fetch_error",
                status_code=exc.response.status_code,
                url=url,
                detail=exc.response.text[:300],
            )
            raise

        batch = resp.json()
        if not batch:
            break

        for f in batch:
            files.append(ChangedFile(
                filename=f["filename"],
                status=f["status"],
                additions=f.get("additions", 0),
                deletions=f.get("deletions", 0),
                patch=f.get("patch"),
            ))

        if len(batch) < 100:
            break
        page += 1

    log.info("files_fetched", count=len(files), pr_number=pull_number)
    return files


#  FastAPI app 
app = FastAPI(title="Code Review Webhook Listener", version="1.0.0")

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/webhook", status_code=status.HTTP_200_OK)
async def github_webhook(request: Request) -> JSONResponse:
    raw_body = await request.body()
    sig_header = request.headers.get("X-Hub-Signature-256")

    if not _verify_signature(WEBHOOK_SECRET, raw_body, sig_header):
        log.warning("invalid_webhook_signature")
        return JSONResponse({"status": "ignored", "reason": "invalid_signature"})

    event_type = request.headers.get("X-GitHub-Event", "")
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        log.error("webhook_json_parse_error", error=str(exc))
        return JSONResponse({"status": "ignored", "reason": "bad_json"})

    log.info("webhook_received", event=event_type, action=payload.get("action"))

    if event_type != "pull_request":
        return JSONResponse({"status": "ignored", "reason": "not_pull_request"})

    action = payload.get("action", "")
    if action not in ("opened", "synchronize"):
        return JSONResponse({"status": "ignored", "reason": f"action_{action}_skipped"})

    log.info("webhook_received", event=event_type, action=payload.get("action"))

    # 3. Filter events
    if event_type != "pull_request":
        return JSONResponse({"status": "ignored", "reason": "not_pull_request"})

    action = payload.get("action", "")
    if action not in ("opened", "synchronize"):
        return JSONResponse({"status": "ignored", "reason": f"action_{action}_skipped"})

    # 4. Extract metadata
    pr = payload.get("pull_request", {})
    repo = payload.get("repository", {})
    repo_full_name: str = repo.get("full_name", "")
    owner, repo_name = repo_full_name.split("/", 1) if "/" in repo_full_name else ("", repo_full_name)

    pr_number: int = pr.get("number", 0)
    head_sha: str = pr.get("head", {}).get("sha", "")
    base_sha: str = pr.get("base", {}).get("sha", "")
    head_branch: str = pr.get("head", {}).get("ref", "")
    base_branch: str = pr.get("base", {}).get("ref", "")
    pr_title: str = pr.get("title", "")
    pr_author: str = pr.get("user", {}).get("login", "")
    pr_url: str = pr.get("html_url", "")

    log.info("pr_event_processing", repo=repo_full_name, pr_number=pr_number, action=action)

    # 5. Fetch changed files
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            changed_files = await _fetch_changed_files(client, owner, repo_name, pr_number)
        except httpx.HTTPStatusError:
            log.error("failed_to_fetch_files", pr_number=pr_number)
            return JSONResponse({"status": "error", "reason": "github_api_failure"})

    # 6. Enqueue PREvent
    event = PREvent(
        repo_full_name=repo_full_name,
        pr_number=pr_number,
        pr_title=pr_title,
        pr_author=pr_author,
        head_sha=head_sha,
        base_sha=base_sha,
        head_branch=head_branch,
        base_branch=base_branch,
        pr_url=pr_url,
        changed_files=changed_files,
    )
    await pr_event_queue.put(event)
    log.info("pr_event_enqueued", pr_number=pr_number, files=len(changed_files))

    return JSONResponse({"status": "accepted", "pr_number": pr_number})