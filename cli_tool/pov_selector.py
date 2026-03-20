"""
RaptorDB Pro Readiness Analyzer — POV Candidate Selector

Scores individual reports, PA dashboards, and slow transaction patterns to
produce a concise shortlist of the exact artifacts to use in a Proof of Value.
The goal is precision over breadth: give the PoV team 3–5 specific things to
benchmark, not a long ranked table.

Scoring rubrics (all rule-based, no LLM):

  Report POV score  (max ~37 pts)
  ────────────────────────────────
  Source table row count tier  : >10M=10, >1M=7, >100K=4, else 1
  Aggregation field present    : 10 pts
  Group-by field present       :  5 pts
  Report type                  : trend/pivot=8, bar/pie/chart=4, list/other=2
  Filter complexity (length)   : >80 chars=3, >20 chars=1, else 0
  Recency                      : updated <90 days=4, <365 days=2, else 0

  Dashboard POV score  (max ~40 pts)
  ───────────────────────────────────
  Widget density               : >10=8, >5=5, >0=3
  PA indicator count           : >10=10, >5=7, >2=5, >0=2
  Max source table row count   : same tier as reports
  Indicators with breakdowns   : >3=8, >1=5, any=3
  Daily-frequency indicators   :  4 pts bonus

  Slow-query POV score  (max ~28 pts)
  ────────────────────────────────────
  Hit frequency (7 days)       : >100=10, >50=7, >10=4, else 1
  Avg response time            : >30s=10, >15s=7, >5s=4, else 1
  URL type bonus               : REST Table API=8, Report=7, Form=3, else 2
  Source table size            : same tier (added when table is identifiable)
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd


# =============================================================================
# Constants
# =============================================================================

_HEAVY_TYPES  = {"trend", "trendbox", "pivot", "calendar"}
_CHART_TYPES  = {"bar", "horizontal_bar", "pie", "donut", "dial", "area",
                 "spline", "column", "stacked_bar", "line"}
_LIST_TYPES   = {"list", "list_v2", "map"}

# Expected improvement ranges keyed by scenario type
_IMPROVEMENTS = {
    "trend_large":   "15–50×  faster with columnar time-series aggregation",
    "agg_large":     "10–30×  faster with columnar aggregation engine",
    "chart_medium":  "5–15×   faster per dashboard render",
    "list_large":    "3–10×   faster for filtered list scans",
    "pa_heavy":      "5–20×   faster dashboard load (columnar snapshot queries)",
    "slow_table":    "10–50×  faster with columnar index scan",
    "slow_report":   "5–20×   faster report execution",
    "default":       "5–10×   faster (baseline HTAP benefit)",
}

_90_DAYS  = timedelta(days=90)
_365_DAYS = timedelta(days=365)


# =============================================================================
# Shared helpers
# =============================================================================

def _tier(rows: int) -> int:
    """Row-count scoring tier shared by all scorers."""
    if rows > 10_000_000: return 10
    if rows > 1_000_000:  return 7
    if rows > 100_000:    return 4
    return 1


def _fmt(n) -> str:
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return "N/A"


def _nonempty(val) -> bool:
    return bool(val) and not pd.isna(val) and str(val).strip() not in ("", "None", "null")


def _parse_dt(val) -> Optional[datetime]:
    if not val or (isinstance(val, float) and pd.isna(val)):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(val)[:19], fmt)
        except ValueError:
            continue
    return None


def _effort(score: int) -> str:
    if score >= 30: return "🟢 Low  — high-value, easy to capture"
    if score >= 20: return "🟡 Low-Medium"
    if score >= 10: return "🟠 Medium"
    return "🔴 Medium-High"


def _get_row_counts(results: Dict) -> Dict[str, int]:
    rc_df = results.get("core_table_row_counts")
    if rc_df is None or rc_df.empty or "table_name" not in rc_df.columns:
        return {}
    return dict(zip(rc_df["table_name"], rc_df["row_count"]))


# =============================================================================
# Report scorer
# =============================================================================

def _score_one_report(row: pd.Series, row_counts: Dict[str, int],
                      now: datetime) -> Dict:
    """Score a single sys_report row; return an enriched dict."""
    table    = str(row.get("table",      "") or "").strip()
    title    = str(row.get("title",      "") or "Untitled Report").strip()
    rtype    = str(row.get("type",       "") or "").strip().lower()
    agg      = str(row.get("aggregate",  "") or "").strip()
    group_by = str(row.get("group_by",   "") or "").strip()
    filt     = str(row.get("filter",     "") or "").strip()
    chart_t  = str(row.get("chart_type", "") or "").strip().lower()
    updated  = _parse_dt(row.get("sys_updated_on", ""))
    rows     = row_counts.get(table, 0)

    # ── Score ──────────────────────────────────────────────────────────────
    s_rows    = _tier(rows)
    s_agg     = (10 if _nonempty(agg) else 0) + (5 if _nonempty(group_by) else 0)
    effective_type = rtype if rtype else chart_t
    s_type    = (8 if effective_type in _HEAVY_TYPES
                 else 4 if effective_type in _CHART_TYPES
                 else 2)
    s_filter  = 3 if len(filt) > 80 else (1 if len(filt) > 20 else 0)
    s_recency = 0
    if updated:
        age = now - updated
        s_recency = 4 if age <= _90_DAYS else (2 if age <= _365_DAYS else 0)

    score = s_rows + s_agg + s_type + s_filter + s_recency

    # ── Why / improvement ─────────────────────────────────────────────────
    reasons = []
    if rows > 0:
        reasons.append(f"`{table}` has {_fmt(rows)} rows")
    if _nonempty(agg):
        reasons.append(f"`{agg}` aggregation on every load")
    if _nonempty(group_by):
        reasons.append(f"GROUP BY `{group_by}` (table scan per group)")
    if effective_type in _HEAVY_TYPES:
        reasons.append(f"`{effective_type}` report executes a full scan per time-bucket")
    elif effective_type in _CHART_TYPES:
        reasons.append(f"`{effective_type}` chart aggregates on every render")
    if len(filt) > 80:
        reasons.append("complex multi-condition filter pushes full scan cost higher")
    if s_recency == 4:
        reasons.append("actively maintained (updated < 90 days ago)")

    if effective_type in _HEAVY_TYPES and rows > 1_000_000:
        imp_key = "trend_large"
    elif _nonempty(agg) and rows > 1_000_000:
        imp_key = "agg_large"
    elif effective_type in _CHART_TYPES:
        imp_key = "chart_medium"
    elif rows > 1_000_000:
        imp_key = "list_large"
    else:
        imp_key = "default"

    return {
        "Rank":               "—",
        "Name":               title,
        "Table":              table or "—",
        "Rows":               _fmt(rows),
        "Type":               effective_type or "—",
        "Aggregation":        agg or "—",
        "Group By":           group_by or "—",
        "Filter Complexity":  (f"{len(filt)} chars" if filt else "none"),
        "Last Updated":       str(row.get("sys_updated_on", ""))[:10] or "—",
        "POV Score":          score,
        "Why POV Candidate":  "; ".join(reasons) if reasons else "Baseline report",
        "Expected Improvement": _IMPROVEMENTS[imp_key],
        "Benchmark Steps": (
            f"1. Open '{title}' in ServiceNow browser  "
            f"2. Record load time from `syslog_transaction`  "
            f"3. Enable RaptorDB Pro on `{table}`  "
            f"4. Reload report and compare"
        ),
        "Effort": _effort(score),
        "_imp_key": imp_key,
        "_raw_rows": rows,
    }


def score_reports(results: Dict, top_n: int = 10) -> pd.DataFrame:
    """
    Score every report in the `reports` DataFrame.
    Returns a DataFrame of up to *top_n* entries ranked by POV Score descending.
    """
    reports_df = results.get("reports")
    if reports_df is None or reports_df.empty:
        return pd.DataFrame()

    row_counts = _get_row_counts(results)
    now        = datetime.now()

    scored = [_score_one_report(r, row_counts, now)
              for _, r in reports_df.iterrows()]
    if not scored:
        return pd.DataFrame()

    df = (pd.DataFrame(scored)
          .sort_values("POV Score", ascending=False)
          .drop_duplicates(subset=["Name"])
          .head(top_n)
          .reset_index(drop=True))
    df.index       = df.index + 1
    df.index.name  = "Rank"
    df["Rank"]     = df.index
    return df[[c for c in df.columns if not c.startswith("_")]]


# =============================================================================
# Dashboard scorer
# =============================================================================

def _build_dashboard_profiles(results: Dict) -> List[Dict]:
    """
    Join pa_dashboards → pa_widgets → pa_indicators → row_counts.
    Returns a list of per-dashboard enriched dicts.
    """
    dash_df   = results.get("pa_dashboards")
    widget_df = results.get("pa_widgets")
    ind_df    = results.get("pa_indicators")

    if dash_df is None or dash_df.empty:
        return []

    row_counts = _get_row_counts(results)

    # Indicator lookup by name
    ind_lookup: Dict[str, pd.Series] = {}
    if ind_df is not None and not ind_df.empty and "name" in ind_df.columns:
        for _, r in ind_df.iterrows():
            ind_lookup[str(r["name"]).strip()] = r

    # Widget lookup by dashboard display-name
    widget_lookup: Dict[str, List[pd.Series]] = {}
    if (widget_df is not None and not widget_df.empty
            and "dashboard" in widget_df.columns):
        for _, r in widget_df.iterrows():
            key = str(r.get("dashboard", "")).strip()
            widget_lookup.setdefault(key, []).append(r)

    profiles = []
    for _, dash_row in dash_df.iterrows():
        dash_name = str(dash_row.get("name", "")).strip()
        widgets   = widget_lookup.get(dash_name, [])

        # Resolve linked indicators
        linked: List[pd.Series] = []
        for w in widgets:
            ind_name = str(w.get("indicator", "")).strip()
            if ind_name in ind_lookup:
                linked.append(ind_lookup[ind_name])

        # Aggregate indicator properties
        source_tables = sorted(
            {str(ind.get("table_name", "")).strip()
             for ind in linked
             if _nonempty(ind.get("table_name", ""))},
            key=lambda t: row_counts.get(t, 0),
            reverse=True,
        )
        breakdowns  = [i for i in linked if _nonempty(i.get("breakdown_source", ""))]
        daily_inds  = [i for i in linked
                       if "daily" in str(i.get("frequency", "")).lower()]
        agg_types   = sorted({str(i.get("aggregate", "")).strip()
                               for i in linked
                               if _nonempty(i.get("aggregate", ""))})

        max_rows    = max((row_counts.get(t, 0) for t in source_tables), default=0)
        n_widgets   = len(widgets)
        n_inds      = len(linked)
        n_breaks    = len(breakdowns)
        n_daily     = len(daily_inds)

        # ── Score ──────────────────────────────────────────────────────────
        s_widgets = (8 if n_widgets > 10 else 5 if n_widgets > 5
                     else 3 if n_widgets > 0 else 0)
        s_inds    = (10 if n_inds > 10 else 7 if n_inds > 5
                     else 5 if n_inds > 2 else 2 if n_inds > 0 else 0)
        s_rows    = _tier(max_rows)
        s_breaks  = (8 if n_breaks > 3 else 5 if n_breaks > 1
                     else 3 if n_breaks > 0 else 0)
        s_daily   = 4 if n_daily > 0 else 0

        score = s_widgets + s_inds + s_rows + s_breaks + s_daily

        # ── Why / improvement ──────────────────────────────────────────────
        reasons = []
        if n_widgets > 0:
            reasons.append(f"{n_widgets} widgets")
        if n_inds > 0:
            reasons.append(f"{n_inds} PA indicators")
        if source_tables:
            top4 = source_tables[:4]
            reasons.append(
                "sources: " + ", ".join(
                    f"`{t}` ({_fmt(row_counts.get(t, 0))})"
                    for t in top4
                )
            )
        if n_breaks > 0:
            reasons.append(f"{n_breaks} indicators with breakdown dimensions (GROUP BY on load)")
        if agg_types:
            reasons.append("aggregations: " + ", ".join(agg_types))
        if n_daily > 0:
            reasons.append(f"{n_daily} daily-frequency indicators (run every day)")

        top_tables_str = ", ".join(
            f"{t} ({_fmt(row_counts.get(t, 0))})"
            for t in source_tables[:5]
        ) or "—"

        profiles.append({
            "Rank":                   "—",
            "Name":                   dash_name,
            "Widgets":                n_widgets,
            "PA Indicators":          n_inds,
            "Source Tables":          top_tables_str,
            "Max Source Rows":        _fmt(max_rows),
            "Indicators w/ Breakdown": n_breaks,
            "Daily Indicators":       n_daily,
            "Aggregation Types":      ", ".join(agg_types) or "—",
            "POV Score":              score,
            "Why POV Candidate":      "; ".join(reasons) if reasons else "Active PA dashboard",
            "Expected Improvement":   _IMPROVEMENTS["pa_heavy"],
            "Benchmark Steps": (
                f"1. Open dashboard '{dash_name}' in ServiceNow  "
                f"2. Record time-to-load (browser DevTools → Network → total transfer time)  "
                f"3. Enable RaptorDB Pro  "
                f"4. Reload and compare — repeat with 5 concurrent users for maximum impact"
            ),
            "Effort": _effort(score),
            "_max_rows": max_rows,
        })

    return profiles


def score_dashboards(results: Dict, top_n: int = 10) -> pd.DataFrame:
    """
    Score every PA dashboard.
    Returns a DataFrame of up to *top_n* entries ranked by POV Score descending.
    """
    profiles = _build_dashboard_profiles(results)
    if not profiles:
        return pd.DataFrame()

    df = (pd.DataFrame(profiles)
          .sort_values("POV Score", ascending=False)
          .head(top_n)
          .reset_index(drop=True))
    df.index      = df.index + 1
    df.index.name = "Rank"
    df["Rank"]    = df.index
    return df[[c for c in df.columns if not c.startswith("_")]]


# =============================================================================
# Slow query scorer
# =============================================================================

def _classify_url(url: str) -> tuple:
    """Return (label, score) for a slow transaction URL pattern."""
    u = url.lower()
    if "/table/" in u:
        return "REST Table API", 8
    if "/report/" in u or "do=report" in u or "report_viewer" in u:
        return "Report Viewer", 7
    if ".do?" in u or "sys_id=" in u:
        return "Form / Record", 3
    if "nav_to" in u or "home.do" in u:
        return "Navigation", 2
    return "Other", 2


def score_slow_queries(results: Dict, top_n: int = 10) -> pd.DataFrame:
    """
    Score each slow transaction URL pattern as a direct POV benchmarking target.
    Returns a DataFrame of up to *top_n* entries ranked by POV Score descending.
    """
    slow_df = results.get("slow_transaction_summary")
    if slow_df is None or slow_df.empty:
        return pd.DataFrame()

    row_counts = _get_row_counts(results)
    rows_list  = []

    for _, row in slow_df.iterrows():
        url    = str(row.get("url", "—"))
        count  = int(pd.to_numeric(row.get("count",           0), errors="coerce") or 0)
        avg_ms = float(pd.to_numeric(row.get("avg_response_ms", 0), errors="coerce") or 0)
        max_ms = float(pd.to_numeric(row.get("max_response_ms", 0), errors="coerce") or 0)
        p90_ms = float(pd.to_numeric(row.get("p90_response_ms", 0), errors="coerce") or 0)

        # Infer table from URL
        inferred = "—"
        if "/table/" in url:
            inferred = url.split("/table/")[-1].split("?")[0].split("/")[0]
        table_rows = row_counts.get(inferred, 0) if inferred != "—" else 0

        url_type, s_url = _classify_url(url)

        s_freq  = (10 if count > 100 else 7 if count > 50 else 4 if count > 10 else 1)
        s_rt    = (10 if avg_ms > 30_000 else 7 if avg_ms > 15_000
                   else 4 if avg_ms > 5_000 else 1)
        s_table = _tier(table_rows) if inferred != "—" else 0
        score   = s_freq + s_rt + s_url + s_table

        reasons = [
            f"{count} hits in the last 7 days",
            f"{avg_ms / 1000:.1f}s avg response ({max_ms / 1000:.1f}s max)",
        ]
        if inferred != "—":
            reasons.append(f"targets `{inferred}` ({_fmt(table_rows)} rows)")
        reasons.append(f"URL type: {url_type}")

        imp_key = ("slow_table" if url_type == "REST Table API" and table_rows > 1_000_000
                   else "slow_report" if url_type == "Report Viewer"
                   else "default")

        # Impact = count × avg_ms (prioritises frequent AND slow queries)
        impact = int(count * avg_ms)

        rows_list.append({
            "Rank":               "—",
            "URL Pattern":        url,
            "URL Type":           url_type,
            "Inferred Table":     inferred,
            "Table Rows":         _fmt(table_rows) if inferred != "—" else "—",
            "Hits (7d)":          count,
            "Avg (ms)":           int(avg_ms),
            "Max (ms)":           int(max_ms),
            "P90 (ms)":           int(p90_ms) if p90_ms else "—",
            "Impact Score":       impact,
            "POV Score":          score,
            "Why POV Candidate":  "; ".join(reasons),
            "Expected Improvement": _IMPROVEMENTS[imp_key],
            "Benchmark Steps": (
                f"1. Find SQL for this URL in `sys_db_slow_query` / `syslog_transaction`  "
                f"2. Record baseline execution plan + wall-clock time  "
                f"3. Enable RaptorDB Pro columnar index on `{inferred}`  "
                f"4. Re-execute identical query and compare"
            ),
            "Effort": _effort(score),
            "_impact": impact,
        })

    if not rows_list:
        return pd.DataFrame()

    df = (pd.DataFrame(rows_list)
          .sort_values(["POV Score", "_impact"], ascending=[False, False])
          .head(top_n)
          .reset_index(drop=True))
    df.index      = df.index + 1
    df.index.name = "Rank"
    df["Rank"]    = df.index
    return df[[c for c in df.columns if not c.startswith("_")]]


# =============================================================================
# Master shortlist
# =============================================================================

def get_pov_shortlist(
    results:          Dict,
    top_reports:      int = 5,
    top_dashboards:   int = 3,
    top_slow_queries: int = 5,
) -> Dict:
    """
    Build the complete POV shortlist from all three artifact types.

    Returns a dict with:
      'reports'      → pd.DataFrame  (top N reports, scored)
      'dashboards'   → pd.DataFrame  (top N dashboards, scored)
      'slow_queries' → pd.DataFrame  (top N slow queries, scored)
      'summary'      → str           (Markdown at-a-glance card)
    """
    reports_df    = score_reports(results,      top_n=top_reports)
    dashboards_df = score_dashboards(results,   top_n=top_dashboards)
    slow_df       = score_slow_queries(results, top_n=top_slow_queries)

    lines = ["### POV Shortlist — At a Glance", ""]

    if not reports_df.empty:
        b = reports_df.iloc[0]
        lines.append(
            f"**Best Report:** _{b['Name']}_ on `{b['Table']}` "
            f"({b['Rows']} rows, `{b['Aggregation']}` aggregation) — "
            f"Score **{b['POV Score']}** — {b['Expected Improvement']}"
        )
    if not dashboards_df.empty:
        b = dashboards_df.iloc[0]
        lines.append(
            f"**Best Dashboard:** _{b['Name']}_ — "
            f"{b['Widgets']} widgets, {b['PA Indicators']} indicators — "
            f"Score **{b['POV Score']}** — {b['Expected Improvement']}"
        )
    if not slow_df.empty:
        b = slow_df.iloc[0]
        url_short = str(b["URL Pattern"])[:70] + ("…" if len(str(b["URL Pattern"])) > 70 else "")
        lines.append(
            f"**Best Slow Query:** `{url_short}` — "
            f"{b['Hits (7d)']} hits, {b['Avg (ms)']} ms avg — "
            f"Score **{b['POV Score']}** — {b['Expected Improvement']}"
        )

    return {
        "reports":      reports_df,
        "dashboards":   dashboards_df,
        "slow_queries": slow_df,
        "summary":      "\n".join(lines),
    }


# =============================================================================
# POV Briefing document generator
# =============================================================================

def generate_pov_briefing(shortlist: Dict, conn_info: Dict = None) -> str:
    """
    Generate a concise, customer-shareable POV Briefing document in Markdown.
    Includes ranked candidates, full benchmark instructions, and a recommended
    execution order with success criteria.
    """
    now      = datetime.now().strftime("%Y-%m-%d %H:%M")
    instance = (conn_info or {}).get("instance_url", "Unknown Instance")
    build    = (conn_info or {}).get("build",        "Unknown Build")

    reports_df    = shortlist.get("reports",      pd.DataFrame())
    dashboards_df = shortlist.get("dashboards",   pd.DataFrame())
    slow_df       = shortlist.get("slow_queries", pd.DataFrame())

    def _md_table(df: pd.DataFrame, cols: List[str]) -> str:
        avail = [c for c in cols if c in df.columns]
        if df.empty or not avail:
            return "_No data._"
        sub    = df[avail]
        header = "| " + " | ".join(avail) + " |"
        sep    = "| " + " | ".join("---" for _ in avail) + " |"
        rows   = []
        for _, r in sub.iterrows():
            cells = " | ".join("" if pd.isna(v) else str(v) for v in r)
            rows.append(f"| {cells} |")
        return "\n".join([header, sep] + rows)

    def _detail_card(r: pd.Series, fields: List[tuple]) -> List[str]:
        """Render a detail card as a Markdown table + narrative."""
        out = ["| Field | Value |", "|---|---|"]
        for label, key in fields:
            if key in r.index:
                out.append(f"| {label} | {r[key]} |")
        out += [
            "",
            f"**Why this is a strong PoV candidate:** {r.get('Why POV Candidate', '—')}",
            "",
            f"**Expected improvement:** {r.get('Expected Improvement', '—')}",
            "",
            f"**Benchmark steps:**",
        ]
        for step in str(r.get("Benchmark Steps", "—")).split("  "):
            step = step.strip()
            if step:
                out.append(f"- {step}")
        out.append("")
        return out

    # ── Document ─────────────────────────────────────────────────────────────
    doc = [
        "# RaptorDB Pro — PoV Candidate Briefing",
        "",
        f"**Instance:** {instance}  ",
        f"**Build:** {build}  ",
        f"**Generated:** {now}",
        "",
        "> This briefing identifies the exact reports, dashboards, and slow queries",
        "> to use as before/after benchmarks in the RaptorDB Pro Proof of Value.",
        "> Each entry was scored and ranked automatically from live instance data.",
        "",
        "---",
        "",
        shortlist.get("summary", ""),
        "",
        "---",
        "",
        "## 1. Top Report Candidates",
        "",
        ("These reports run against large tables with aggregation or complex filtering — "
         "the exact workload profile that RaptorDB Pro's columnar engine targets."),
        "",
        _md_table(reports_df, ["Rank", "Name", "Table", "Rows", "Type",
                                "Aggregation", "Group By", "POV Score",
                                "Expected Improvement", "Effort"]),
        "",
    ]

    if not reports_df.empty:
        doc += ["### Detailed Cards", ""]
        for _, r in reports_df.iterrows():
            doc += [f"#### Report #{r['Rank']}: {r['Name']}", ""]
            doc += _detail_card(r, [
                ("Table",            "Table"),
                ("Row Count",        "Rows"),
                ("Report Type",      "Type"),
                ("Aggregation",      "Aggregation"),
                ("Group By",         "Group By"),
                ("Filter Complexity","Filter Complexity"),
                ("Last Updated",     "Last Updated"),
                ("POV Score",        "POV Score"),
                ("Benchmark Effort", "Effort"),
            ])

    doc += [
        "---",
        "",
        "## 2. Top Dashboard Candidates",
        "",
        ("PA dashboards with many widgets and indicators sourcing large tables are the most "
         "visible performance wins — load time improvement is immediately felt by end users."),
        "",
        _md_table(dashboards_df, ["Rank", "Name", "Widgets", "PA Indicators",
                                   "Source Tables", "Indicators w/ Breakdown",
                                   "POV Score", "Expected Improvement", "Effort"]),
        "",
    ]

    if not dashboards_df.empty:
        doc += ["### Detailed Cards", ""]
        for _, r in dashboards_df.iterrows():
            doc += [f"#### Dashboard #{r['Rank']}: {r['Name']}", ""]
            doc += _detail_card(r, [
                ("Widgets",                "Widgets"),
                ("PA Indicators",          "PA Indicators"),
                ("Source Tables",          "Source Tables"),
                ("Max Source Row Count",   "Max Source Rows"),
                ("Indicators w/ Breakdown","Indicators w/ Breakdown"),
                ("Daily Indicators",       "Daily Indicators"),
                ("Aggregation Types",      "Aggregation Types"),
                ("POV Score",              "POV Score"),
                ("Benchmark Effort",       "Effort"),
            ])

    doc += [
        "---",
        "",
        "## 3. Top Slow Query Candidates",
        "",
        ("These URL patterns generated the highest combined frequency × response-time impact. "
         "Each is a direct, reproducible before/after benchmark — "
         "no query authoring required; just replay what's already happening in production."),
        "",
        _md_table(slow_df, ["Rank", "URL Pattern", "URL Type", "Inferred Table",
                             "Table Rows", "Hits (7d)", "Avg (ms)",
                             "Impact Score", "POV Score", "Expected Improvement", "Effort"]),
        "",
    ]

    if not slow_df.empty:
        doc += ["### Detailed Cards", ""]
        for _, r in slow_df.iterrows():
            url_trunc = str(r["URL Pattern"])[:120] + ("…" if len(str(r["URL Pattern"])) > 120 else "")
            doc += [f"#### Slow Query #{r['Rank']}: `{url_trunc}`", ""]
            doc += _detail_card(r, [
                ("URL Type",        "URL Type"),
                ("Inferred Table",  "Inferred Table"),
                ("Table Rows",      "Table Rows"),
                ("Hits (7d)",       "Hits (7d)"),
                ("Avg Response",    "Avg (ms)"),
                ("Max Response",    "Max (ms)"),
                ("P90 Response",    "P90 (ms)"),
                ("Impact Score",    "Impact Score"),
                ("POV Score",       "POV Score"),
                ("Benchmark Effort","Effort"),
            ])

    # ── Recommended execution order ──────────────────────────────────────────
    doc += [
        "---",
        "",
        "## 4. Recommended PoV Execution Order",
        "",
        ("Execute benchmarks in this sequence for maximum customer impact. "
         "Start with quick wins to build confidence before the more complex tests."),
        "",
    ]

    step = 1
    if not slow_df.empty:
        b = slow_df.iloc[0]
        doc.append(
            f"{step}. **Day 1 — Quick Win:** Replay `{str(b['URL Pattern'])[:60]}…`  \n"
            f"   {b['Hits (7d)']} hits, {b['Avg (ms)']} ms avg → "
            f"expected **{b['Expected Improvement']}**.  \n"
            f"   Zero setup: just replay the URL and compare `syslog_transaction` times."
        )
        step += 1

    if not reports_df.empty:
        b = reports_df.iloc[0]
        doc.append(
            f"{step}. **Day 1–2 — Report Benchmark:** Run _{b['Name']}_ on "
            f"`{b['Table']}` ({b['Rows']} rows)  \n"
            f"   Expected **{b['Expected Improvement']}**.  \n"
            f"   Record time from `syslog_transaction` before and after enabling RaptorDB Pro."
        )
        step += 1

    if not dashboards_df.empty:
        b = dashboards_df.iloc[0]
        doc.append(
            f"{step}. **Day 2–3 — Dashboard Benchmark:** Load _{b['Name']}_ "
            f"({b['Widgets']} widgets, {b['PA Indicators']} indicators)  \n"
            f"   Expected **{b['Expected Improvement']}**.  \n"
            f"   Run with 5 concurrent users to demonstrate HTAP isolation."
        )
        step += 1

    doc += [
        "",
        "---",
        "",
        "## 5. Success Criteria",
        "",
        "| Benchmark | Target Improvement | Measurement Method |",
        "|---|---|---|",
    ]

    if not reports_df.empty:
        b = reports_df.iloc[0]
        doc.append(f"| Report: {b['Name'][:40]} | {b['Expected Improvement'].split(' ')[0]} faster | `syslog_transaction.response_time` |")
    if not dashboards_df.empty:
        b = dashboards_df.iloc[0]
        doc.append(f"| Dashboard: {b['Name'][:40]} | {b['Expected Improvement'].split(' ')[0]} faster | Browser DevTools Network tab |")
    if not slow_df.empty:
        b = slow_df.iloc[0]
        doc.append(f"| Slow query: {str(b['URL Pattern'])[:40]}… | {b['Expected Improvement'].split(' ')[0]} faster | `syslog_transaction.response_time` |")

    doc += [
        "",
        "---",
        "",
        f"_PoV Briefing generated by RaptorDB Pro Readiness Analyzer · {now}_",
    ]

    return "\n".join(doc)
