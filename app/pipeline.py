"""
Phase 2 — 5-Agent LangGraph Pipeline
Nodes: file_fetcher → ast_analyzer → llm_reviewer → patch_generator → human_gate
Uses LangGraph interrupt_before on human_gate for human-in-the-loop pausing.
All state is persisted to SQLite via aiosqlite.
"""

from __future__ import annotations

import ast
import asyncio
import json
import os
import re
import textwrap
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict

import aiosqlite
import httpx
import structlog
from dotenv import load_dotenv
from groq import Groq
from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph

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
GROQ_API_KEY: str = os.environ["GROQ_API_KEY"]
DB_PATH: str = os.environ.get("DB_PATH", "./data/reviews.db")
GITHUB_API_BASE = "https://api.github.com"
GROQ_MODEL = "llama-3.3-70b-versatile"

_groq_client = Groq(api_key=GROQ_API_KEY)


#  State TypedDict 

class ASTFinding(TypedDict):
    file: str
    kind: str
    line: Optional[int]
    detail: str


class LLMIssue(TypedDict):
    file: str
    line: Optional[int]
    severity: str
    description: str


class Patch(TypedDict):
    issue_index: int
    file: str
    severity: str
    diff: str


class PRState(TypedDict):
    pr_number: int
    repo_full_name: str
    pr_title: str
    pr_author: str
    head_sha: str
    base_sha: str
    head_branch: str
    base_branch: str
    pr_url: str
    changed_file_names: List[str]
    file_diffs: Dict[str, str]
    file_contents: Dict[str, str]
    ast_report: List[ASTFinding]
    llm_diagnostics: List[LLMIssue]
    patches: List[Patch]
    awaiting_human: bool
    human_decision: Optional[Dict[str, Any]]
    created_at: str
    error: Optional[str]


#  SQLite persistence 

async def _ensure_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pr_reviews (
                pr_number   INTEGER PRIMARY KEY,
                repo        TEXT NOT NULL,
                state_json  TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'awaiting_human',
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            )
        """)
        await db.commit()
    log.info("db_initialized", path=DB_PATH)


async def persist_state(state: PRState) -> None:
    status = "resolved" if state.get("human_decision") else "awaiting_human"
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO pr_reviews (pr_number, repo, state_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(pr_number) DO UPDATE SET
                state_json = excluded.state_json,
                status     = excluded.status,
                updated_at = excluded.updated_at
        """, (
            state["pr_number"],
            state["repo_full_name"],
            json.dumps(state),
            status,
            state.get("created_at", now),
            now,
        ))
        await db.commit()
    log.info("state_persisted", pr_number=state["pr_number"], status=status)


async def load_state(pr_number: int) -> Optional[PRState]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT state_json FROM pr_reviews WHERE pr_number = ?", (pr_number,)
        ) as cursor:
            row = await cursor.fetchone()
    if row:
        return json.loads(row[0])
    return None


#  GitHub helpers 

def _gh_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def _get_file_content(
    client: httpx.AsyncClient,
    owner: str,
    repo: str,
    path: str,
    ref: str,
) -> Optional[str]:
    import base64
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{path}"
    try:
        resp = await client.get(url, params={"ref": ref}, headers=_gh_headers())
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        if data.get("encoding") == "base64":
            return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
        return data.get("content", "")
    except Exception as exc:
        log.warning("file_content_fetch_failed", path=path, error=str(exc))
        return None


# Node 1: file_fetcher 

async def file_fetcher(state: PRState) -> PRState:
    log.info("node_file_fetcher_start", pr_number=state["pr_number"])

    owner, repo = state["repo_full_name"].split("/", 1)
    head_sha = state["head_sha"]
    filenames: List[str] = state["changed_file_names"]

    file_diffs: Dict[str, str] = {}
    file_contents: Dict[str, str] = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        pr_number = state["pr_number"]
        page = 1
        patch_map: Dict[str, str] = {}
        while True:
            resp = await client.get(
                f"{GITHUB_API_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/files",
                params={"per_page": 100, "page": page},
                headers=_gh_headers(),
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            for f in batch:
                if f.get("patch"):
                    patch_map[f["filename"]] = f["patch"]
            if len(batch) < 100:
                break
            page += 1

        py_files = [fn for fn in filenames if fn.endswith(".py")]
        tasks = [_get_file_content(client, owner, repo, fn, head_sha) for fn in py_files]
        contents = await asyncio.gather(*tasks, return_exceptions=True)
        for filename, content in zip(py_files, contents):
            if isinstance(content, str):
                file_contents[filename] = content

    for filename in filenames:
        file_diffs[filename] = patch_map.get(filename, "# No diff available")

    new_state = dict(state)
    new_state["file_diffs"] = file_diffs
    new_state["file_contents"] = file_contents
    log.info("node_file_fetcher_done", diffs=len(file_diffs), contents=len(file_contents))
    return new_state  # type: ignore[return-value]


#  Node 2: ast_analyzer 

def _extract_defined_names(tree: ast.AST) -> set:
    defined = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                defined.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                defined.add(alias.asname or alias.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    defined.add(target.id)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                defined.add(node.target.id)
        elif isinstance(node, ast.For):
            if isinstance(node.target, ast.Name):
                defined.add(node.target.id)
        elif isinstance(node, ast.arg):
            defined.add(node.arg)
    return defined


def _get_function_signatures(tree: ast.AST) -> Dict[str, str]:
    sigs = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            parts = [a.arg for a in args.args]
            if args.vararg:
                parts.append(f"*{args.vararg.arg}")
            if args.kwarg:
                parts.append(f"**{args.kwarg.arg}")
            sigs[node.name] = f"{node.name}({', '.join(parts)})"
    return sigs


async def ast_analyzer(state: PRState) -> PRState:
    log.info("node_ast_analyzer_start", pr_number=state["pr_number"])
    findings: List[ASTFinding] = []
    file_contents: Dict[str, str] = state.get("file_contents", {})

    _builtins = set(dir(__builtins__)) if isinstance(__builtins__, dict) else set(dir(__builtins__))
    _common = {"self", "cls", "True", "False", "None", "__name__", "__file__",
               "__doc__", "__all__", "TYPE_CHECKING", "annotations"}
    _excluded = _builtins | _common

    for filename, source in file_contents.items():
        try:
            tree = ast.parse(source, filename=filename)
        except SyntaxError as exc:
            findings.append(ASTFinding(
                file=filename, kind="syntax_error",
                line=exc.lineno, detail=str(exc.msg),
            ))
            continue

        defined = _extract_defined_names(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                if node.id not in defined and node.id not in _excluded:
                    findings.append(ASTFinding(
                        file=filename, kind="undefined_name",
                        line=getattr(node, "lineno", None),
                        detail=f"Name '{node.id}' may not be defined",
                    ))

        diff_text = state.get("file_diffs", {}).get(filename, "")
        head_sigs = _get_function_signatures(tree)
        added_lines = [l[1:] for l in diff_text.splitlines() if l.startswith("+") and not l.startswith("+++")]
        added_text = "\n".join(added_lines)
        for func_name, sig in head_sigs.items():
            if f"def {func_name}" in added_text:
                findings.append(ASTFinding(
                    file=filename, kind="signature_change",
                    line=None,
                    detail=f"Function '{func_name}' added or modified: {sig}",
                ))

    new_state = dict(state)
    new_state["ast_report"] = findings
    log.info("node_ast_analyzer_done", findings=len(findings))
    return new_state  # type: ignore[return-value]


#  Node 3: llm_reviewer

def _build_review_prompt(state: PRState) -> str:
    diffs_block = ""
    for filename, diff in state.get("file_diffs", {}).items():
        diffs_block += f"\n### {filename}\n```diff\n{diff[:3000]}\n```\n"

    ast_block = ""
    for f in state.get("ast_report", []):
        ast_block += f"- [{f['kind']}] {f['file']} L{f['line']}: {f['detail']}\n"

    return textwrap.dedent(f"""
        PR Title: {state['pr_title']}
        Repository: {state['repo_full_name']}
        Author: {state['pr_author']}

        ## Changed Files Diffs
        {diffs_block}

        ## Static Analysis (AST) Findings
        {ast_block or 'None'}

        Review all changed files. Identify bugs, regressions, security vulnerabilities,
        logic errors, and bad practices.
        Severity: HIGH (breaks functionality/security), MEDIUM (likely bug), LOW (minor).

        Output ONLY valid JSON, no markdown fences:
        {{"issues": [{{"file": "...", "line": <int or null>, "severity": "HIGH|MEDIUM|LOW", "description": "..."}}]}}
    """).strip()


async def llm_reviewer(state: PRState) -> PRState:
    log.info("node_llm_reviewer_start", pr_number=state["pr_number"])
    prompt = _build_review_prompt(state)
    loop = asyncio.get_event_loop()

    def _call_groq():
        return _groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a senior code reviewer. Identify bugs, regressions, and "
                        "security issues. Output ONLY valid JSON: "
                        "{\"issues\": [{\"file\": \"...\", \"line\": null, \"severity\": \"HIGH|MEDIUM|LOW\", \"description\": \"...\"}]}"
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=2048,
        )

    issues: List[LLMIssue] = []
    try:
        completion = await loop.run_in_executor(None, _call_groq)
        raw = completion.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)
        issues = parsed.get("issues", [])
    except json.JSONDecodeError as exc:
        log.error("llm_json_parse_error", error=str(exc))
    except Exception as exc:
        log.error("llm_reviewer_error", error=str(exc))

    new_state = dict(state)
    new_state["llm_diagnostics"] = issues
    log.info("node_llm_reviewer_done", issues=len(issues))
    return new_state  # type: ignore[return-value]


# Node 4: patch_generator 

async def patch_generator(state: PRState) -> PRState:
    log.info("node_patch_generator_start", pr_number=state["pr_number"])
    loop = asyncio.get_event_loop()
    patches: List[Patch] = []

    high_issues = [
        (i, issue)
        for i, issue in enumerate(state.get("llm_diagnostics", []))
        if issue.get("severity", "").upper() == "HIGH"
    ]

    for orig_idx, issue in high_issues:
        filename = issue.get("file", "unknown")
        diff_context = state.get("file_diffs", {}).get(filename, "# no diff")[:2000]
        prompt = (
            f"File: {filename}\n"
            f"Issue: {issue.get('description', '')}\n"
            f"Line: {issue.get('line', 'unknown')}\n\n"
            f"Existing diff context:\n```diff\n{diff_context}\n```\n\n"
            "Generate a minimal unified diff patch to fix this issue. "
            "Output ONLY the diff, no explanation, no markdown fences."
        )

        def _call():
            return _groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert Python engineer. Output ONLY a valid unified diff patch.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.05,
                max_tokens=512,
            )

        try:
            completion = await loop.run_in_executor(None, _call)
            diff_text = completion.choices[0].message.content.strip()
            diff_text = re.sub(r"^```(?:diff)?\s*", "", diff_text)
            diff_text = re.sub(r"\s*```$", "", diff_text)
        except Exception as exc:
            log.warning("patch_gen_error", issue_idx=orig_idx, error=str(exc))
            diff_text = f"# Patch generation failed: {exc}"

        patches.append(Patch(
            issue_index=orig_idx,
            file=filename,
            severity="HIGH",
            diff=diff_text,
        ))
        log.info("patch_generated", issue_idx=orig_idx, file=filename)

    new_state = dict(state)
    new_state["patches"] = patches
    new_state["awaiting_human"] = True
    log.info("node_patch_generator_done", patches=len(patches))
    await persist_state(new_state)
    return new_state


# Node 5: human_gate

async def human_gate(state: PRState) -> PRState:
    log.info("node_human_gate", pr_number=state["pr_number"])
    new_state = dict(state)
    new_state["awaiting_human"] = True
    await persist_state(new_state)  # type: ignore[arg-type]
    return new_state  # type: ignore[return-value]


#  Graph assembly 

def build_graph() -> CompiledGraph:
    graph = StateGraph(PRState)
    graph.add_node("file_fetcher", file_fetcher)
    graph.add_node("ast_analyzer", ast_analyzer)
    graph.add_node("llm_reviewer", llm_reviewer)
    graph.add_node("patch_generator", patch_generator)
    graph.add_node("human_gate", human_gate)

    graph.set_entry_point("file_fetcher")
    graph.add_edge("file_fetcher", "ast_analyzer")
    graph.add_edge("ast_analyzer", "llm_reviewer")
    graph.add_edge("llm_reviewer", "patch_generator")
    graph.add_edge("patch_generator", "human_gate")
    graph.add_edge("human_gate", END)

    return graph.compile(interrupt_before=["human_gate"])


_pipeline: Optional[CompiledGraph] = None


def get_pipeline() -> CompiledGraph:
    global _pipeline
    if _pipeline is None:
        _pipeline = build_graph()
    return _pipeline


async def run_pipeline(event) -> None:
    await _ensure_db()

    initial_state: PRState = {
        "pr_number": event.pr_number,
        "repo_full_name": event.repo_full_name,
        "pr_title": event.pr_title,
        "pr_author": event.pr_author,
        "head_sha": event.head_sha,
        "base_sha": event.base_sha,
        "head_branch": event.head_branch,
        "base_branch": event.base_branch,
        "pr_url": event.pr_url,
        "changed_file_names": [f.filename for f in event.changed_files],
        "file_diffs": {},
        "file_contents": {},
        "ast_report": [],
        "llm_diagnostics": [],
        "patches": [],
        "awaiting_human": False,
        "human_decision": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }

    pipeline = get_pipeline()
    config = {"configurable": {"thread_id": str(event.pr_number)}}

    log.info("pipeline_start", pr_number=event.pr_number)
    try:
        async for chunk in pipeline.astream(initial_state, config=config):
            node_name = list(chunk.keys())[0] if chunk else "unknown"
            log.info("pipeline_node_complete", node=node_name, pr_number=event.pr_number)
    except Exception as exc:
        log.error("pipeline_error", pr_number=event.pr_number, error=str(exc))
        initial_state["error"] = str(exc)
        await persist_state(initial_state)


async def resume_pipeline(pr_number: int, human_decision: Dict[str, Any]) -> Optional[PRState]:
    state = await load_state(pr_number)
    if not state:
        log.error("resume_state_not_found", pr_number=pr_number)
        return None

    state["human_decision"] = human_decision
    state["awaiting_human"] = False

    pipeline = get_pipeline()
    config = {"configurable": {"thread_id": str(pr_number)}}

    try:
        async for chunk in pipeline.astream(state, config=config):
            node_name = list(chunk.keys())[0] if chunk else "unknown"
            log.info("pipeline_resume_node", node=node_name, pr_number=pr_number)
    except Exception as exc:
        log.error("pipeline_resume_error", pr_number=pr_number, error=str(exc))
        state["error"] = str(exc)

    await persist_state(state)
    return state
