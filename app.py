"""
RaptorDB Pro Readiness Analyzer
Connects to ServiceNow via REST API, collects performance-relevant data,
flags issues, and exports everything for RaptorDB Pro migration analysis.
"""

import streamlit as st
import pandas as pd
import os
import json
import tempfile
from datetime import datetime
from pathlib import Path

from sn_client import SNClient
from collector import collect_all
from analyzer import analyze_all, score_use_cases
from report_engine import generate_report, generate_claude_prompt, export_docx_report
from pov_selector import get_pov_shortlist, generate_pov_briefing
from pdf_report import generate_pdf_report

# =============================================================================
# Page Config
# =============================================================================
st.set_page_config(
    page_title="RaptorDB Pro Readiness Analyzer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# Styling
# =============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2rem; font-weight: 700; color: #1a1a2e; margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1rem; color: #6b7280; margin-bottom: 2rem;
    }
    .raptordb-tag {
        background: #10b981; color: white; padding: 2px 8px;
        border-radius: 4px; font-size: 0.8rem; font-weight: 600;
    }
    .conn-box {
        background: #f1f5f9; border: 1px solid #cbd5e1;
        border-radius: 8px; padding: 12px; margin-bottom: 12px;
    }
    .info-banner {
        background: linear-gradient(135deg, #e0f2fe 0%, #f0fdf4 100%);
        border: 1px solid #7dd3fc; border-radius: 10px;
        padding: 18px 22px; margin: 12px 0 20px 0;
    }
    .panel-container {
        background: #f8fafc; border: 1px solid #e2e8f0;
        border-radius: 10px; padding: 20px; margin-bottom: 20px;
    }
    .sidebar-panel-header {
        font-size: 1.1rem; font-weight: 700; color: #1a1a2e;
        border-bottom: 2px solid #10b981; padding-bottom: 6px; margin-bottom: 14px;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Session State
# =============================================================================
defaults = {
    "connected": False,
    "results": {},
    "issues": [],
    "collection_done": False,
    "conn_info": {},
    "export_path": "",
    "active_panel": None,   # "reports_pa" | "claude_export" | "pdf"
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# =============================================================================
# Export Function
# =============================================================================
def export_results(results: dict, issues: list, export_path: str, conn_info: dict):
    """Export all collected data to the specified folder."""
    os.makedirs(export_path, exist_ok=True)

    for key, df in results.items():
        if df is not None and not df.empty:
            safe_name = key.replace("/", "_").replace(" ", "_")
            df.to_csv(os.path.join(export_path, f"{safe_name}.csv"), index=False)

    issues_path = os.path.join(export_path, "flagged_issues.json")
    with open(issues_path, "w") as f:
        json.dump(issues, f, indent=2, default=str)

    if issues:
        pd.DataFrame(issues).to_csv(
            os.path.join(export_path, "flagged_issues.csv"), index=False
        )

    meta = {
        "collected_at": datetime.now().isoformat(),
        "instance": conn_info.get("instance_url", ""),
        "build": conn_info.get("build", ""),
        "datasets_collected": [k for k, v in results.items() if v is not None and not v.empty],
        "total_issues": len(issues),
        "issues_by_severity": {}
    }
    for issue in issues:
        sev = issue["severity"]
        meta["issues_by_severity"][sev] = meta["issues_by_severity"].get(sev, 0) + 1

    with open(os.path.join(export_path, "collection_metadata.json"), "w") as f:
        json.dump(meta, f, indent=2)

    lines = [
        "RaptorDB Pro Readiness Analyzer — Collection Summary",
        "=" * 55,
        f"Collected: {meta['collected_at']}",
        f"Instance: {meta['instance']}",
        f"Build: {meta['build']}",
        f"Datasets: {len(meta['datasets_collected'])}",
        f"Issues: {meta['total_issues']}",
        "",
        "Issues by Severity:",
    ]
    for sev, cnt in meta["issues_by_severity"].items():
        lines.append(f"  {sev}: {cnt}")

    lines += ["", "=" * 55, "FLAGGED ISSUES", "=" * 55]
    for i, issue in enumerate(issues, 1):
        lines.append(f"\n--- Issue #{i} ---")
        lines.append(f"[{issue['severity']}] {issue['title']}")
        lines.append(f"Category: {issue['category']}")
        lines.append(f"Detail: {issue['detail']}")
        if issue.get("raptordb_relevance"):
            lines.append(f"RaptorDB Pro: {issue['raptordb_relevance']}")

    lines += ["", "=" * 55, "DATASETS COLLECTED", "=" * 55]
    for ds in meta["datasets_collected"]:
        df = results[ds]
        lines.append(f"  {ds}.csv — {len(df)} rows, {len(df.columns)} columns")

    lines += [
        "",
        "NEXT STEP: Feed these CSVs + this summary to Claude for",
        "RaptorDB Pro use case generation and performance analysis.",
    ]

    with open(os.path.join(export_path, "SUMMARY.txt"), "w") as f:
        f.write("\n".join(lines))

    return export_path


# =============================================================================
# Panel toggle helper
# =============================================================================
def _toggle_panel(name: str):
    """Toggle a sidebar panel on/off."""
    if st.session_state["active_panel"] == name:
        st.session_state["active_panel"] = None
    else:
        st.session_state["active_panel"] = name


# =============================================================================
# SIDEBAR — Connection + Panel Buttons
# =============================================================================
with st.sidebar:
    st.markdown("### 🔌 ServiceNow Connection")

    instance_url = st.text_input(
        "Instance URL",
        value="https://your-instance.service-now.com",
        help="Full URL including https://"
    )
    username = st.text_input("Username", value="admin")
    password = st.text_input("Password", type="password")
    verify_ssl = st.checkbox("Verify SSL", value=True,
                             help="Uncheck for sub-prod with self-signed certs")

    st.markdown("---")

    _PERIOD_MAP = {
        "7 days": 7,
        "15 days": 15,
        "1 month": 30,
        "3 months": 90,
        "6 months": 180,
        "1 year": 365,
    }
    analysis_period = st.selectbox(
        "Analysis Period",
        options=list(_PERIOD_MAP.keys()),
        index=0,
        help="How far back to look for slow transaction data",
    )
    analysis_days = _PERIOD_MAP[analysis_period]

    st.markdown("---")
    export_dir = st.text_input(
        "Export Directory",
        value=f"./exports/raptordb_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        test_btn = st.button("🔗 Test", use_container_width=True)
    with col2:
        collect_btn = st.button("🚀 Collect", use_container_width=True, type="primary")

    if test_btn:
        if not password:
            st.error("Enter password.")
        else:
            with st.spinner("Testing..."):
                client = SNClient(instance_url, username, password, verify_ssl)
                result = client.test_connection()
            if result["success"]:
                st.success(result["message"])
            else:
                st.error(result["message"])

    if st.session_state.connected:
        st.success(f"✅ {st.session_state.conn_info.get('build', 'Connected')}")
    if st.session_state.collection_done:
        st.success("✅ Data collected")

    # ── Panel Buttons (always visible; disabled until data is collected) ────
    st.markdown("---")
    st.markdown("### 🛠 Tools")

    _active = st.session_state["active_panel"]
    _has_data = st.session_state.collection_done

    if st.button(
        "📊 Reports & PA",
        use_container_width=True,
        type="secondary" if _active != "reports_pa" else "primary",
        key="btn_reports_pa",
        disabled=not _has_data,
        help="Run a collection first" if not _has_data else "View reports and PA analytics",
    ):
        _toggle_panel("reports_pa")

    if st.button(
        "🤖 Claude Prompt & Export",
        use_container_width=True,
        type="secondary" if _active != "claude_export" else "primary",
        key="btn_claude",
        disabled=not _has_data,
        help="Run a collection first" if not _has_data else "Generate Claude prompt and export files",
    ):
        _toggle_panel("claude_export")

    if st.button(
        "📄 Generate PDF Report",
        use_container_width=True,
        type="secondary" if _active != "pdf" else "primary",
        key="btn_pdf",
        disabled=not _has_data,
        help="Run a collection first" if not _has_data else "Build and download a PDF report",
    ):
        _toggle_panel("pdf")


# =============================================================================
# MAIN
# =============================================================================
st.markdown('<p class="main-header">🔍 RaptorDB Pro Readiness Analyzer</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-header">'
    'Collect ServiceNow instance data via REST API and identify RaptorDB Pro opportunities'
    '</p>',
    unsafe_allow_html=True
)

# Handle collection
if collect_btn:
    if not password:
        st.error("Please enter a password.")
    else:
        client = SNClient(instance_url, username, password, verify_ssl)

        with st.spinner("Connecting..."):
            conn_result = client.test_connection()

        if not conn_result["success"]:
            st.error(conn_result["message"])
        else:
            st.session_state.connected = True
            st.session_state.conn_info = {
                "instance_url": instance_url,
                "build": conn_result.get("build", "")
            }

            st.info(f"📡 {conn_result['message']}")

            progress_bar = st.progress(0, text="Starting collection...")

            def update_progress(pct, msg):
                progress_bar.progress(pct, text=msg)

            results = collect_all(client, progress_callback=update_progress,
                                   days=analysis_days)

            with st.spinner("Analyzing collected data..."):
                issues = analyze_all(results)

            with st.spinner("Exporting..."):
                export_path = export_results(
                    results, issues, export_dir, st.session_state.conn_info
                )

            st.session_state.results = results
            st.session_state.issues = issues
            st.session_state.collection_done = True
            st.session_state.export_path = export_path
            # Invalidate cached computed values on re-collection
            for k in ("report_md", "claude_prompt", "docx_bytes",
                       "pov_docx_bytes", "pdf_bytes"):
                st.session_state.pop(k, None)

            st.success(f"✅ Done! {len(results)} datasets collected, "
                       f"{len(issues)} issues flagged. Exported to `{export_path}`")
            st.rerun()


# =============================================================================
# Results Display
# =============================================================================
if st.session_state.collection_done:
    results = st.session_state.results
    issues = st.session_state.issues

    # --- Info Banner ---
    st.markdown("""
<div class="info-banner">
<strong>📌 Two Ways to Use This Output</strong><br><br>
<strong>Option A — Use Directly:</strong> The tabs below contain scored use cases, a POV shortlist,
and a built-in readiness report — all ready to present to a customer or use for demo preparation.
Download a PDF or .docx from the sidebar tools.<br><br>
<strong>Option B — Upload to Claude:</strong> Click <em>"🤖 Claude Prompt & Export"</em> in the
sidebar to get a compressed, token-optimised prompt containing all collected data.
Paste it into <a href="https://claude.ai" target="_blank">claude.ai</a> or the Anthropic API
for a richer, AI-authored readiness narrative.
</div>
""", unsafe_allow_html=True)

    # ==========================================================================
    # SIDEBAR PANELS (rendered inline before tabs so they appear prominently)
    # ==========================================================================
    active_panel = st.session_state.get("active_panel")

    # ── PANEL: Reports & PA ────────────────────────────────────────────────────
    if active_panel == "reports_pa":
        with st.container():
            st.markdown('<div class="panel-container">', unsafe_allow_html=True)
            st.markdown('<p class="sidebar-panel-header">📊 Reports & Performance Analytics</p>',
                        unsafe_allow_html=True)

            col_r, col_p = st.columns(2)

            with col_r:
                st.markdown("#### Reports by Table")
                rpt_summary = results.get("report_table_summary")
                if rpt_summary is not None and not rpt_summary.empty:
                    st.bar_chart(
                        rpt_summary.head(15).set_index("table")["report_count"],
                        use_container_width=True
                    )
                    st.dataframe(rpt_summary, use_container_width=True, height=300)
                else:
                    st.info("No report data.")

            with col_p:
                st.markdown("#### PA Indicators by Table")
                pa_summary = results.get("pa_table_summary")
                if pa_summary is not None and not pa_summary.empty:
                    st.bar_chart(
                        pa_summary.head(15).set_index("table_name")["indicator_count"],
                        use_container_width=True
                    )
                    st.dataframe(pa_summary, use_container_width=True, height=300)
                else:
                    st.info("No PA data.")

            st.markdown("---")
            st.markdown("#### Slow Transaction Summary (Last 7 Days)")
            slow = results.get("slow_transaction_summary")
            if slow is not None and not slow.empty:
                st.dataframe(slow, use_container_width=True, height=300)
            else:
                st.info("No slow transactions captured.")

            st.markdown('</div>', unsafe_allow_html=True)

    # ── PANEL: Claude Prompt & Export ──────────────────────────────────────────
    elif active_panel == "claude_export":
        with st.container():
            st.markdown('<div class="panel-container">', unsafe_allow_html=True)
            st.markdown('<p class="sidebar-panel-header">🤖 Claude Prompt & Export</p>',
                        unsafe_allow_html=True)

            st.caption(
                "Generates an optimised prompt packaging all collected data for Claude. "
                "Large DataFrames are truncated to 50 rows and summaries inline."
            )

            if "claude_prompt" not in st.session_state:
                with st.spinner("Building prompt..."):
                    st.session_state["claude_prompt"] = generate_claude_prompt(results, issues)

            if st.button("🔄 Rebuild Prompt", key="rebuild_prompt_panel"):
                with st.spinner("Rebuilding..."):
                    st.session_state["claude_prompt"] = generate_claude_prompt(results, issues)

            prompt_text = st.session_state["claude_prompt"]

            word_count = len(prompt_text.split())
            char_count = len(prompt_text)
            pm1, pm2 = st.columns(2)
            pm1.metric("Approximate words", f"{word_count:,}")
            pm2.metric("Characters", f"{char_count:,}")

            st.text_area(
                label="Generated Claude Prompt (copy or download below)",
                value=prompt_text,
                height=500,
                key="prompt_display_panel",
            )

            ts = datetime.now().strftime("%Y%m%d_%H%M")
            st.download_button(
                label="⬇️ Download prompt as .txt",
                data=prompt_text.encode("utf-8"),
                file_name=f"RaptorDB_Claude_Prompt_{ts}.txt",
                mime="text/plain",
                key="dl_prompt_panel",
            )

            st.markdown("---")
            st.markdown("#### 📦 Export Files")
            export_path = st.session_state.get("export_path", "")
            st.markdown(f"**Export folder:** `{export_path}`")
            if export_path and os.path.exists(export_path):
                files = sorted(os.listdir(export_path))
                st.markdown(f"**{len(files)} files:**")
                for fn in files:
                    fpath = os.path.join(export_path, fn)
                    size_kb = os.path.getsize(fpath) / 1024
                    st.text(f"  📄 {fn}  ({size_kb:.1f} KB)")

            st.markdown("---")
            st.info(
                "**How to use:** Download the prompt above, open "
                "[claude.ai](https://claude.ai), and paste it in. "
                "Claude will return a richer, AI-authored readiness narrative."
            )

            st.markdown('</div>', unsafe_allow_html=True)

    # ── PANEL: PDF Report ──────────────────────────────────────────────────────
    elif active_panel == "pdf":
        with st.container():
            st.markdown('<div class="panel-container">', unsafe_allow_html=True)
            st.markdown('<p class="sidebar-panel-header">📄 Generate PDF Report</p>',
                        unsafe_allow_html=True)

            st.caption(
                "A professionally designed PDF starting with the top POV use cases, "
                "followed by scored candidate tables, executive summary, and key findings."
            )

            # Need the shortlist for the PDF
            if "pdf_shortlist" not in st.session_state:
                with st.spinner("Scoring POV candidates..."):
                    st.session_state["pdf_shortlist"] = get_pov_shortlist(results)

            shortlist = st.session_state["pdf_shortlist"]

            pdf_c1, pdf_c2 = st.columns([1, 2])
            with pdf_c1:
                if st.button("🖨 Build PDF", key="build_pdf", type="primary"):
                    with st.spinner("Generating PDF — this takes a few seconds..."):
                        try:
                            pdf_bytes = generate_pdf_report(
                                results,
                                issues,
                                shortlist,
                                st.session_state.get("conn_info", {}),
                            )
                            st.session_state["pdf_bytes"] = pdf_bytes
                            st.success("PDF ready — click Download below.")
                        except Exception as exc:
                            st.error(f"PDF generation failed: {exc}")

            if st.session_state.get("pdf_bytes"):
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                st.download_button(
                    label="⬇️ Download PDF",
                    data=st.session_state["pdf_bytes"],
                    file_name=f"RaptorDB_Pro_Readiness_{ts}.pdf",
                    mime="application/pdf",
                    key="dl_pdf",
                )

            st.markdown("---")
            st.markdown(
                "**PDF includes:** Cover page · Top targeted use cases · "
                "POV shortlist tables (reports / dashboards / slow queries) · "
                "Executive summary with metric tiles · Key findings · Next steps."
            )

            st.markdown('</div>', unsafe_allow_html=True)

    # ==========================================================================
    # MAIN TABS (4)
    # ==========================================================================
    st.markdown("---")
    tab_top, tab_pov, tab_report = st.tabs([
        "🏆 Top Use Cases",
        "🎯 POV Shortlist",
        "📝 Readiness Report",
    ])

    # ---- TOP USE CASES TAB ----
    with tab_top:
        st.markdown("#### Top 10 RaptorDB Pro Use Cases")
        st.caption(
            "Ranked by a composite score: table size · report load · "
            "PA indicator pressure · rotation · slow transactions."
        )

        top_df = score_use_cases(results)

        if top_df.empty:
            st.info(
                "Not enough data to rank use cases yet. "
                "Ensure the collection captured row counts, reports, and/or PA config."
            )
        else:
            def _score_style(val):
                if val >= 60:
                    return "background-color:#d1fae5; font-weight:600"
                if val >= 35:
                    return "background-color:#fef9c3"
                return ""

            styled = (
                top_df.style
                .applymap(_score_style, subset=["Score"])
                .set_properties(**{"text-align": "left"})
                .format({"Score": "{:.1f}"})
            )
            st.dataframe(styled, use_container_width=True, height=420)

            st.markdown("---")
            st.markdown(
                "**Score legend:** 🟢 ≥ 60 — strong candidate · "
                "🟡 35–59 — good candidate · ⚪ < 35 — viable candidate"
            )

            st.markdown("##### Score Distribution")
            chart_data = top_df[["Use Case", "Score"]].set_index("Use Case")
            st.bar_chart(chart_data, use_container_width=True)

    # ---- POV SHORTLIST TAB -------------------------------------------------------
    with tab_pov:
        st.markdown("#### 🎯 POV Candidate Shortlist")
        st.caption(
            "Exact reports, dashboards, and slow queries to benchmark in the "
            "RaptorDB Pro Proof of Value — scored and ranked from live instance data."
        )

        pov_c1, pov_c2, pov_c3 = st.columns(3)
        with pov_c1:
            n_reports = st.slider("Top N Reports",      min_value=1, max_value=10, value=5)
        with pov_c2:
            n_dashes  = st.slider("Top N Dashboards",   min_value=1, max_value=10, value=3)
        with pov_c3:
            n_slow    = st.slider("Top N Slow Queries", min_value=1, max_value=10, value=5)

        with st.spinner("Scoring candidates..."):
            shortlist = get_pov_shortlist(
                results,
                top_reports=n_reports,
                top_dashboards=n_dashes,
                top_slow_queries=n_slow,
            )
        # Also cache for PDF use
        st.session_state["pdf_shortlist"] = shortlist

        rep_df  = shortlist["reports"]
        dash_df = shortlist["dashboards"]
        slow_df = shortlist["slow_queries"]

        st.markdown("---")
        st.markdown(shortlist["summary"])
        st.markdown("---")

        def _pov_style(val):
            try:
                v = int(val)
                if v >= 30: return "background-color:#d1fae5;font-weight:600"
                if v >= 20: return "background-color:#fef9c3"
                if v >= 10: return "background-color:#ffedd5"
            except (TypeError, ValueError):
                pass
            return ""

        # ── REPORTS ──────────────────────────────────────────────────────────
        st.markdown("### 📊 Top Report Candidates")
        st.caption(
            "Scored by: source table size · aggregation presence · "
            "report type · filter complexity · recency.  "
            "Score guide: 🟢 ≥30 = critical · 🟡 ≥20 = strong · 🟠 ≥10 = good"
        )

        if rep_df.empty:
            st.info("No report data collected. Run a collection that includes sys_report.")
        else:
            summary_cols = ["Rank", "Name", "Table", "Rows", "Type",
                            "Aggregation", "Group By", "POV Score", "Effort"]
            available = [c for c in summary_cols if c in rep_df.columns]
            styled_rep = (
                rep_df[available].style
                .applymap(_pov_style, subset=["POV Score"])
                .set_properties(**{"text-align": "left"})
            )
            st.dataframe(styled_rep, use_container_width=True,
                         height=min(60 + len(rep_df) * 38, 350))

            st.markdown("##### Report Details")
            for _, r in rep_df.iterrows():
                label = (f"#{r['Rank']} — **{r['Name']}** "
                         f"on `{r['Table']}` ({r['Rows']} rows) — "
                         f"Score {r['POV Score']}")
                with st.expander(label, expanded=False):
                    d1, d2 = st.columns(2)
                    with d1:
                        st.markdown(f"**Type:** {r['Type']}")
                        st.markdown(f"**Aggregation:** {r['Aggregation']}")
                        st.markdown(f"**Group By:** {r['Group By']}")
                        st.markdown(f"**Filter Complexity:** {r['Filter Complexity']}")
                        st.markdown(f"**Last Updated:** {r['Last Updated']}")
                    with d2:
                        st.markdown(f"**POV Score:** {r['POV Score']}")
                        st.markdown(f"**Benchmark Effort:** {r['Effort']}")
                        st.markdown(f"**Expected Improvement:**")
                        st.info(r["Expected Improvement"])
                    st.markdown("**Why this is a PoV candidate:**")
                    st.warning(r["Why POV Candidate"])
                    st.markdown("**Benchmark Steps:**")
                    for step in str(r["Benchmark Steps"]).split("  "):
                        step = step.strip()
                        if step:
                            st.markdown(f"- {step}")

        st.markdown("---")

        # ── DASHBOARDS ───────────────────────────────────────────────────────
        st.markdown("### 📈 Top Dashboard Candidates")
        st.caption(
            "Scored by: widget density · PA indicator count · "
            "source table size · breakdown dimensions · indicator frequency."
        )

        if dash_df.empty:
            st.info(
                "No PA dashboard data collected, or pa_widgets/pa_indicators are empty. "
                "Ensure the collection captured PA config."
            )
        else:
            summary_cols = ["Rank", "Name", "Widgets", "PA Indicators",
                            "Source Tables", "Indicators w/ Breakdown",
                            "POV Score", "Effort"]
            available = [c for c in summary_cols if c in dash_df.columns]
            styled_dash = (
                dash_df[available].style
                .applymap(_pov_style, subset=["POV Score"])
                .set_properties(**{"text-align": "left"})
            )
            st.dataframe(styled_dash, use_container_width=True,
                         height=min(60 + len(dash_df) * 38, 300))

            st.markdown("##### Dashboard Details")
            for _, r in dash_df.iterrows():
                label = (f"#{r['Rank']} — **{r['Name']}** — "
                         f"{r['Widgets']} widgets, {r['PA Indicators']} indicators — "
                         f"Score {r['POV Score']}")
                with st.expander(label, expanded=False):
                    d1, d2 = st.columns(2)
                    with d1:
                        st.markdown(f"**Widgets:** {r['Widgets']}")
                        st.markdown(f"**PA Indicators:** {r['PA Indicators']}")
                        st.markdown(f"**Source Tables:** {r['Source Tables']}")
                        st.markdown(f"**Max Source Rows:** {r['Max Source Rows']}")
                    with d2:
                        st.markdown(f"**Indicators w/ Breakdown:** {r['Indicators w/ Breakdown']}")
                        st.markdown(f"**Daily Indicators:** {r['Daily Indicators']}")
                        st.markdown(f"**Aggregation Types:** {r['Aggregation Types']}")
                        st.markdown(f"**POV Score:** {r['POV Score']}")
                        st.markdown(f"**Benchmark Effort:** {r['Effort']}")
                        st.markdown("**Expected Improvement:**")
                        st.info(r["Expected Improvement"])
                    st.markdown("**Why this is a PoV candidate:**")
                    st.warning(r["Why POV Candidate"])
                    st.markdown("**Benchmark Steps:**")
                    for step in str(r["Benchmark Steps"]).split("  "):
                        step = step.strip()
                        if step:
                            st.markdown(f"- {step}")

        st.markdown("---")

        # ── SLOW QUERIES ─────────────────────────────────────────────────────
        st.markdown("### 🐢 Top Slow Query Candidates")
        st.caption(
            "Scored by: hit frequency · avg response time · URL type · "
            "inferred table size.  "
            "Impact Score = hits × avg_ms (higher = more urgent)."
        )

        if slow_df.empty:
            st.info(
                "No slow transaction data collected. "
                "Ensure syslog_transaction logging is enabled and the threshold is ≤5000 ms."
            )
        else:
            summary_cols = ["Rank", "URL Pattern", "URL Type", "Inferred Table",
                            "Table Rows", "Hits (7d)", "Avg (ms)",
                            "Impact Score", "POV Score", "Effort"]
            available = [c for c in summary_cols if c in slow_df.columns]
            styled_slow = (
                slow_df[available].style
                .applymap(_pov_style, subset=["POV Score"])
                .set_properties(**{"text-align": "left"})
            )
            st.dataframe(styled_slow, use_container_width=True,
                         height=min(60 + len(slow_df) * 38, 350))

            st.markdown("##### Slow Query Details")
            for _, r in slow_df.iterrows():
                url_short = str(r["URL Pattern"])[:80] + (
                    "…" if len(str(r["URL Pattern"])) > 80 else ""
                )
                label = (f"#{r['Rank']} — `{url_short}` — "
                         f"{r['Hits (7d)']} hits, {r['Avg (ms)']} ms avg — "
                         f"Score {r['POV Score']}")
                with st.expander(label, expanded=False):
                    d1, d2 = st.columns(2)
                    with d1:
                        st.markdown(f"**URL Type:** {r['URL Type']}")
                        st.markdown(f"**Inferred Table:** `{r['Inferred Table']}`")
                        st.markdown(f"**Table Rows:** {r['Table Rows']}")
                        st.markdown(f"**Hits (7d):** {r['Hits (7d)']}")
                    with d2:
                        st.markdown(f"**Avg Response:** {r['Avg (ms)']} ms")
                        st.markdown(f"**Max Response:** {r['Max (ms)']} ms")
                        st.markdown(f"**P90 Response:** {r['P90 (ms)']} ms")
                        st.markdown(f"**Impact Score:** {r['Impact Score']:,}")
                        st.markdown(f"**POV Score:** {r['POV Score']}")
                        st.markdown(f"**Benchmark Effort:** {r['Effort']}")
                        st.markdown("**Expected Improvement:**")
                        st.info(r["Expected Improvement"])
                    st.markdown("**Full URL Pattern:**")
                    st.code(r["URL Pattern"], language=None)
                    st.markdown("**Why this is a PoV candidate:**")
                    st.warning(r["Why POV Candidate"])
                    st.markdown("**Benchmark Steps:**")
                    for step in str(r["Benchmark Steps"]).split("  "):
                        step = step.strip()
                        if step:
                            st.markdown(f"- {step}")

        st.markdown("---")

        # ── DOWNLOAD POV BRIEFING ─────────────────────────────────────────────
        st.markdown("### 📄 Download PoV Briefing")
        st.caption(
            "Customer-shareable document with all shortlisted candidates, "
            "benchmark instructions, execution order, and success criteria."
        )

        dl_c1, dl_c2 = st.columns(2)

        with dl_c1:
            briefing_md = generate_pov_briefing(
                shortlist, st.session_state.get("conn_info", {})
            )
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            st.download_button(
                label="⬇️ Download as Markdown (.md)",
                data=briefing_md.encode("utf-8"),
                file_name=f"RaptorDB_PoV_Briefing_{ts}.md",
                mime="text/markdown",
                key="dl_pov_md",
            )

        with dl_c2:
            if st.button("Generate .docx", key="gen_pov_docx"):
                with st.spinner("Building Word document..."):
                    try:
                        with tempfile.NamedTemporaryFile(
                            suffix=".docx", delete=False
                        ) as tmp:
                            export_docx_report(briefing_md, tmp.name)
                            with open(tmp.name, "rb") as fh:
                                st.session_state["pov_docx_bytes"] = fh.read()
                        os.unlink(tmp.name)
                        st.success("Ready — click Download below.")
                    except Exception as exc:
                        st.error(f"Could not generate .docx: {exc}")

            if st.session_state.get("pov_docx_bytes"):
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                st.download_button(
                    label="⬇️ Download as .docx",
                    data=st.session_state["pov_docx_bytes"],
                    file_name=f"RaptorDB_PoV_Briefing_{ts}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key="dl_pov_docx",
                )

    # ---- READINESS REPORT TAB ----
    with tab_report:
        st.markdown("#### RaptorDB Pro Readiness Report")
        st.caption(
            "Rule-based analysis — no LLM required. "
            "Sections: Executive Summary · Top 10 Tables · Report Hotspots · "
            "Slow Transactions · CMDB · Indexes · Workload · Demo Scenarios · Next Steps."
        )

        if "report_md" not in st.session_state:
            with st.spinner("Generating report..."):
                st.session_state["report_md"] = generate_report(results, issues)
        report_md = st.session_state["report_md"]

        if st.button("🔄 Regenerate Report"):
            with st.spinner("Regenerating..."):
                st.session_state["report_md"] = generate_report(results, issues)
                report_md = st.session_state["report_md"]

        st.markdown("---")
        st.markdown(report_md, unsafe_allow_html=False)
        st.markdown("---")

        st.markdown("##### Download Report")
        col_dl1, col_dl2 = st.columns([1, 3])
        with col_dl1:
            if st.button("Generate .docx", key="gen_docx"):
                with st.spinner("Building Word document..."):
                    try:
                        with tempfile.NamedTemporaryFile(
                            suffix=".docx", delete=False
                        ) as tmp:
                            export_docx_report(report_md, tmp.name)
                            with open(tmp.name, "rb") as fh:
                                st.session_state["docx_bytes"] = fh.read()
                        os.unlink(tmp.name)
                        st.success("Ready — click Download below.")
                    except Exception as exc:
                        st.error(f"Could not generate .docx: {exc}")

        if st.session_state.get("docx_bytes"):
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            st.download_button(
                label="⬇️ Download as .docx",
                data=st.session_state["docx_bytes"],
                file_name=f"RaptorDB_Pro_Readiness_Report_{ts}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key="dl_docx",
            )

else:
    # ---- Landing State ----
    st.markdown("### Getting Started")
    st.markdown("""
1. Enter your ServiceNow instance URL and admin credentials in the sidebar
2. Click **Test** to verify connectivity
3. Click **Collect** to gather all data and run analysis
4. Review the Top Use Cases and POV Shortlist, or open the sidebar tools for PDF / Claude export
    """)

    st.markdown("---")
    st.markdown("### What Gets Collected (via REST API)")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("""
**Platform & Config**
- Build version, DB type, platform properties
- RaptorDB/WDF enablement status
- Slow query threshold, PA config
- Table rotation rules

**Tables & Indexes**
- Full table inventory (sys_db_object)
- Row counts for 40+ core SN tables
- Indexed fields and composite indexes
- Table hierarchy (parent/child extensions)
        """)
    with col_b:
        st.markdown("""
**Reports & Dashboards**
- All report definitions + table mapping
- Aggregation report identification
- PA indicators, dashboards, widgets
- PA source table summary

**Performance & Workload**
- Slow transactions (last 7 days, >5s)
- Slow URL pattern analysis
- Active scheduled jobs
- CMDB CI class distribution + relationships
        """)

    st.markdown("---")
    st.markdown(
        "⚡ **All data is collected read-only via REST API** — "
        "no writes, no scripts, no direct DB access required."
    )
