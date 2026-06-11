# Code Review & Bug-Fix Pipeline

A production-grade, multi-agent AI system that monitors GitHub pull requests,
runs LLM-powered code analysis, generates patches, and surfaces decisions to
a human reviewer — all fully containerised.

## Stack

<<Component  Technology>> 
 Language | Python 3.11 
 Orchestration | LangGraph 0.1 
 LLM | Groq API — LLaMA-3 70B (free tier) 
 Webhook / API | FastAPI + Uvicorn 
 Dashboard | Streamlit 
 GitHub calls | httpx.AsyncClient 
 State store | SQLite via aiosqlite 
 Logging | structlog (JSON) 
 Containers | Docker + Docker Compose 

---

## Quick Start

### 1. Clone & configure

```bash
git clone <your-repo>
cd code-review-pipeline

cp .env.example .env
# Edit .env — fill in all five values:
#   GITHUB_TOKEN       — fine-grained PAT with repo + PR write access
#   WEBHOOK_SECRET     — random string, set same in GitHub webhook settings
#   GROQ_API_KEY       — from console.groq.com (free)
#   REVIEWER_TOKEN     — random string for dashboard bearer auth
#   API_BASE_URL       — leave as http://localhost:8000 for local dev
```

### 2. Build & run

```bash
docker compose up --build
```

Services:
- **API + Pipeline** → http://localhost:8000
- **Dashboard** → http://localhost:8501
- **Health check** → http://localhost:8000/health

### 3. Register GitHub webhook

In your GitHub repo → Settings → Webhooks → Add webhook:

<<Field  Value>>  
 Payload URL | `https://your-domain.com/webhook` (use ngrok for local dev) |
 Content type | `application/json` |
 Secret | value of `WEBHOOK_SECRET` in your .env |
 Events | ✅ Pull requests |

For local development, expose port 8000 via ngrok:
```bash
ngrok http 8000
# Use the https URL as your webhook payload URL
```

---

## Project Structure

```
code-review-pipeline/
├── app/
│   ├── main.py              # FastAPI entry point + pipeline worker loop
│   ├── webhook_listener.py  # Phase 1: GitHub webhook receiver
│   ├── pipeline.py          # Phase 2: LangGraph 5-node pipeline
│   ├── audit_api.py         # Phase 3: Human audit REST API
│   └── dashboard.py         # Phase 4: Streamlit reviewer UI
├── Dockerfile.api
├── Dockerfile.dashboard
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Pipeline Nodes 

### 1. `file_fetcher`
Fetches raw unified diffs and full file content for every changed Python file
using the GitHub Contents API at `head_sha`.

### 2. `ast_analyzer`
Parses Python source with `ast.parse()` and detects:
- **Syntax errors** — caught via `SyntaxError`
- **Undefined names** — `ast.walk` looking for `ast.Name` loads not in scope
- **Signature changes** — function defs appearing in `+` lines of the diff

### 3. `llm_reviewer`
Sends a structured prompt (diffs + AST findings) to `llama3-70b-8192` via Groq.
Parses the JSON response into typed `LLMIssue` objects.
System prompt enforces `{"issues": [{file, line, severity, description}]}` output.

### 4. `patch_generator`
For each **HIGH** severity issue, calls Groq again asking for a minimal unified diff.
Stores patch objects (with issue linkage) in `state["patches"]`.

### 5. `human_gate`
Sets `awaiting_human = True`, persists full `PRState` to SQLite, then pauses via
LangGraph's `interrupt_before`. Resumed externally by `POST /api/reviews/{pr}/decision`.

---

## API Endpoints 

All `/api/reviews/*` routes require `Authorization: Bearer <REVIEWER_TOKEN>`.

| Method | Route | Description |
|--------|-------|-------------|
| `POST` | `/webhook` | GitHub webhook receiver (no auth — uses HMAC) |
| `GET` | `/health` | Service health check |
| `GET` | `/api/reviews` | List PRs awaiting human review |
| `GET` | `/api/reviews/{pr}` | Full PRState for a PR |
| `POST` | `/api/reviews/{pr}/decision` | Accept / modify / reject |

Decision payload:
```json
{
  "action": "accept",
  "modified_patch": null,
  "reviewer_note": "Looks good"
}
```

---

## Dashboard Features 

- **Sidebar** — pending PRs with severity colour badges (🔴 HIGH / 🟠 MED / 🟢 LOW)
- **Metadata card** — repo, author, branch, direct GitHub link
- **AST Findings** — expandable table of static analysis results
- **LLM Diagnostics** — structured table: file | line | severity | description
- **Diff viewer** — each proposed patch rendered as syntax-highlighted diff
- **Patch editor** — pre-filled `st.text_area` for inline modification
- **Action buttons** — ✅ Accept Patch · ✏️ Submit Modified · ❌ Reject
- **Auto-poll** — refreshes pending list every 30 s

---

## Real Bugs You Can Demonstrate

1. **httpx version conflict** — `httpx>=0.27` required for `AsyncClient` timeout kwarg format
2. **Groq model name** — `llama3-70b-8192` (not `llama-3-70b`) — confirmed active
3. **JSON parse guard** — LLM occasionally wraps output in ` ```json ``` ` fences; stripped by regex
4. **asyncio.Queue in async context** — worker started in `@app.on_event("startup")` via `create_task`
5. **HMAC compare_digest** — timing-safe comparison prevents timing attacks on webhook validation
6. **interrupt_before vs interrupt_after** — must use `interrupt_before=["human_gate"]` so state is persisted *before* the pause, enabling external resumption

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GITHUB_TOKEN` | GitHub PAT (repo + pull_requests scopes) |
| `WEBHOOK_SECRET` | Shared secret for HMAC-SHA256 validation |
| `GROQ_API_KEY` | Groq API key (free tier) |
| `REVIEWER_TOKEN` | Static bearer token for audit API |
| `API_BASE_URL` | Base URL dashboard uses to reach API |
| `DB_PATH` | SQLite file path (default: `./data/reviews.db`) |

---

## Local Development (without Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
mkdir -p data

# Terminal 1 — API
cd app && uvicorn main:app --reload --port 8000

# Terminal 2 — Dashboard
cd app && streamlit run dashboard.py
```
