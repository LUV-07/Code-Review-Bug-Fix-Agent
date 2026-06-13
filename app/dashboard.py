

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Dashboard runs in the browser, so always use localhost â€” never the Docker internal hostname
API_BASE = os.environ.get("API_BASE_URL", "http://api:8000")
REVIEWER_TOKEN = os.environ.get("REVIEWER_TOKEN", "")
POLL_INTERVAL = 30


def _headers() -> dict:
    return {"Authorization": f"Bearer {REVIEWER_TOKEN}"}


def fetch_pending_prs() -> List[Dict[str, Any]]:
    try:
        resp = requests.get(f"{API_BASE}/api/reviews", headers=_headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.sidebar.error(f"API error: {exc}")
        return []


def fetch_pr_detail(pr_number: int) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.get(f"{API_BASE}/api/reviews/{pr_number}", headers=_headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.error(f"Failed to load PR #{pr_number}: {exc}")
        return None


def post_decision(pr_number: int, action: str,
                  modified_patch: Optional[str] = None,
                  reviewer_note: Optional[str] = None) -> Optional[Dict[str, Any]]:
    payload: Dict[str, Any] = {"action": action}
    if modified_patch:
        payload["modified_patch"] = modified_patch
    if reviewer_note:
        payload["reviewer_note"] = reviewer_note
    try:
        resp = requests.post(
            f"{API_BASE}/api/reviews/{pr_number}/decision",
            headers=_headers(), json=payload, timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.error(f"Decision failed: {exc}")
        return None


def _severity_badge(summary: Dict[str, int]) -> str:
    parts = []
    if summary.get("HIGH", 0):
        parts.append(f"ðŸ”´ {summary['HIGH']} HIGH")
    if summary.get("MEDIUM", 0):
        parts.append(f"ðŸŸ  {summary['MEDIUM']} MED")
    if summary.get("LOW", 0):
        parts.append(f"ðŸŸ¢ {summary['LOW']} LOW")
    return " Â· ".join(parts) if parts else "âœ… Clean"


def _top_severity(summary: Dict[str, int]) -> str:
    if summary.get("HIGH", 0):
        return "HIGH"
    if summary.get("MEDIUM", 0):
        return "MEDIUM"
    return "LOW"


#  Page config 
st.set_page_config(
    page_title="Code Review Bot",
    page_icon="ðŸ¤–",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.badge-high   { background:#ff4b4b; color:white; padding:2px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }
.badge-medium { background:#ff8c00; color:white; padding:2px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }
.badge-low    { background:#21ba45; color:white; padding:2px 8px; border-radius:12px; font-size:0.75rem; font-weight:600; }
</style>
""", unsafe_allow_html=True)

#  Session state 
if "pending_prs"  not in st.session_state: st.session_state.pending_prs  = []
if "selected_pr"  not in st.session_state: st.session_state.selected_pr  = None
if "pr_detail"    not in st.session_state: st.session_state.pr_detail    = {}
if "last_poll"    not in st.session_state: st.session_state.last_poll    = 0
if "toast_msg"    not in st.session_state: st.session_state.toast_msg    = None

# Auto-poll 
now = time.time()
if now - st.session_state.last_poll > POLL_INTERVAL:
    st.session_state.pending_prs = fetch_pending_prs()
    st.session_state.last_poll = now

if st.session_state.toast_msg:
    st.toast(st.session_state.toast_msg, icon="âœ…")
    st.session_state.toast_msg = None

#  Sidebar 
with st.sidebar:
    st.title("ðŸ¤– Code Review Bot")
    st.caption("Human-in-the-Loop PR Reviewer")
    st.divider()

    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("Pending Reviews")
    with col2:
        if st.button("âŸ³", help="Refresh now"):
            st.session_state.pending_prs = fetch_pending_prs()
            st.session_state.last_poll = time.time()
            st.rerun()

    pending = st.session_state.pending_prs
    if not pending:
        st.info("No pending reviews.")
    else:
        for pr in pending:
            sev = _top_severity(pr.get("severity_summary", {}))
            badge_class = f"badge-{sev.lower()}"
            is_selected = st.session_state.selected_pr == pr["pr_number"]
            label = f"{'â–¶ ' if is_selected else ''}#{pr['pr_number']} â€” {pr['repo'].split('/')[-1]}"
            st.markdown(f"**{label}**")
            st.markdown(pr.get("title", "")[:50])
            st.markdown(f'<span class="{badge_class}">{sev}</span>', unsafe_allow_html=True)
            if st.button("View", key=f"select_{pr['pr_number']}", use_container_width=True):
                st.session_state.selected_pr = pr["pr_number"]
                st.session_state.pr_detail = {}
                st.rerun()
            st.divider()

    st.caption(f"Last refreshed: {time.strftime('%H:%M:%S', time.localtime(st.session_state.last_poll))}")

#  Main panel 
selected = st.session_state.selected_pr

if not selected:
    st.markdown("## Select a PR from the sidebar to begin review.")
    st.info("The pipeline will automatically run when a new PR is opened or updated on GitHub.")
    st.stop()

if selected not in st.session_state.pr_detail or not st.session_state.pr_detail.get(selected):
    detail = fetch_pr_detail(selected)
    if detail:
        st.session_state.pr_detail[selected] = detail

detail = st.session_state.pr_detail.get(selected)
if not detail:
    st.error("Could not load PR details.")
    st.stop()

# Metadata
sev_summary: Dict[str, int] = {}
for d in detail.get("llm_diagnostics", []):
    sev = d.get("severity", "LOW").upper()
    sev_summary[sev] = sev_summary.get(sev, 0) + 1

st.markdown(f"## PR #{detail['pr_number']}: {detail.get('pr_title', 'Untitled')}")
st.markdown(_severity_badge(sev_summary))
st.divider()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown("**Repository**")
    st.write(detail.get("repo_full_name", ""))
with col2:
    st.markdown("**Author**")
    st.write(f"@{detail.get('pr_author', '')}")
with col3:
    st.markdown("**Branch**")
    st.write(f"{detail.get('head_branch', '')} â†’ {detail.get('base_branch', '')}")
with col4:
    st.markdown("**Link**")
    st.markdown(f"[Open on GitHub]({detail.get('pr_url', '#')})")

st.divider()

# AST findings
ast_report = detail.get("ast_report", [])
with st.expander(f"ðŸ” AST Analysis ({len(ast_report)} findings)", expanded=len(ast_report) > 0):
    if not ast_report:
        st.success("No AST issues detected.")
    else:
        import pandas as pd
        st.dataframe(pd.DataFrame(ast_report), use_container_width=True, hide_index=True)

# LLM diagnostics
llm_diags = detail.get("llm_diagnostics", [])
with st.expander(f"ðŸ¤– LLM Review ({len(llm_diags)} issues)", expanded=len(llm_diags) > 0):
    if not llm_diags:
        st.success("No issues found by LLM reviewer.")
    else:
        import pandas as pd
        icons = {"HIGH": "ðŸ”´ HIGH", "MEDIUM": "ðŸŸ  MEDIUM", "LOW": "ðŸŸ¢ LOW"}
        rows = [{
            "File": d.get("file", ""), "Line": d.get("line"),
            "Severity": icons.get(d.get("severity", "LOW").upper(), d.get("severity", "")),
            "Description": d.get("description", ""),
        } for d in llm_diags]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# Patches
patches = detail.get("patches", [])
with st.expander(f"ðŸ“‹ Proposed Patches ({len(patches)})", expanded=len(patches) > 0):
    if not patches:
        st.info("No HIGH severity issues â€” no patches generated.")
    else:
        for i, patch in enumerate(patches):
            st.markdown(f"**Patch {i+1}** â€” `{patch.get('file', '?')}`")
            st.code(patch.get("diff", ""), language="diff")

st.divider()

# Patch editor + decision
st.subheader("âœï¸ Patch Editor")
default_patch = patches[0]["diff"] if patches else ""
reviewer_note = st.text_input("Reviewer note (optional)", key=f"note_{selected}")
modified_patch = st.text_area("Patch (editable)", value=default_patch, height=250, key=f"patch_{selected}")

st.divider()
st.subheader("Decision")
col_a, col_b, col_c = st.columns(3)

with col_a:
    if st.button("âœ… Accept Patch", use_container_width=True, type="primary"):
        result = post_decision(selected, "accept", reviewer_note=reviewer_note or None)
        if result:
            st.session_state.toast_msg = f"âœ… Accepted PR #{selected}"
            st.session_state.selected_pr = None
            st.session_state.pending_prs = fetch_pending_prs()
            st.rerun()

with col_b:
    if st.button("âœï¸ Submit Modified", use_container_width=True):
        if not modified_patch.strip():
            st.warning("Please enter a patch before submitting.")
        else:
            result = post_decision(selected, "modify", modified_patch=modified_patch,
                                   reviewer_note=reviewer_note or None)
            if result:
                st.session_state.toast_msg = f"âœï¸ Modified patch applied to PR #{selected}"
                st.session_state.selected_pr = None
                st.session_state.pending_prs = fetch_pending_prs()
                st.rerun()

with col_c:
    if st.button("âŒ Reject", use_container_width=True):
        result = post_decision(selected, "reject",
                               reviewer_note=reviewer_note or "Rejected by reviewer.")
        if result:
            st.session_state.toast_msg = f"âŒ PR #{selected} rejected"
            st.session_state.selected_pr = None
            st.session_state.pending_prs = fetch_pending_prs()
            st.rerun()

st.caption(f"Auto-refreshes every {POLL_INTERVAL}s Â· Last poll: {time.strftime('%H:%M:%S', time.localtime(st.session_state.last_poll))}")
if time.time() - st.session_state.last_poll > POLL_INTERVAL:
    st.rerun()
