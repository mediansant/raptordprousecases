"""
RaptorDB Pro Readiness Analyzer — Issue Detection & Flagging
Analyzes REST API-collected data and flags issues relevant to RaptorDB Pro.
"""

import math
import pandas as pd
from typing import Dict, List


def analyze_all(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    """Run all analyzers and return list of flagged issues."""
    issues = []
    issues.extend(_check_properties(results))
    issues.extend(_check_table_sizes(results))
    issues.extend(_check_indexes(results))
    issues.extend(_check_reports(results))
    issues.extend(_check_pa(results))
    issues.extend(_check_cmdb(results))
    issues.extend(_check_slow_transactions(results))
    issues.extend(_check_workload_profile(results))
    return issues


def _flag(severity: str, category: str, title: str, detail: str,
          raptordb_relevance: str = "") -> Dict:
    return {
        "severity": severity,
        "category": category,
        "title": title,
        "detail": detail,
        "raptordb_relevance": raptordb_relevance
    }


# -------------------------------------------------------------------------
# System Properties
# -------------------------------------------------------------------------
def _check_properties(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []
    df = results.get("system_properties")
    if df is None or df.empty:
        return issues

    props = dict(zip(df["name"], df["value"])) if "name" in df.columns else {}

    # Check DB type
    db_type = props.get("glide.db.type", "") or props.get("glide.db.rdbms", "")
    if db_type:
        issues.append(_flag(
            "INFO", "Platform",
            f"Database type: {db_type}",
            "Current database engine identified.",
            "Baseline for RaptorDB Pro comparison."
        ))

    # Check if RaptorDB is already enabled
    rdb = props.get("glide.raptordb.enabled", "")
    rdb_pro = props.get("glide.raptordb.pro.enabled", "")
    if str(rdb).lower() == "true":
        issues.append(_flag(
            "INFO", "Platform",
            "RaptorDB (standard) is already enabled",
            "The instance has base RaptorDB active.",
            "Upgrade path to RaptorDB Pro for columnar indexing, HTAP, and full MVCC."
        ))
    if str(rdb_pro).lower() == "true":
        issues.append(_flag(
            "INFO", "Platform",
            "RaptorDB Pro is already enabled!",
            "This instance already runs RaptorDB Pro.",
            "Focus analysis on optimization rather than migration."
        ))

    # Slow query threshold
    slow_thresh = props.get("glide.db.slow_query_threshold", "")
    if slow_thresh:
        issues.append(_flag(
            "INFO", "Configuration",
            f"Slow query threshold: {slow_thresh}ms",
            "Queries exceeding this are logged as slow.",
            "Benchmark these slow queries against RaptorDB Pro for comparison."
        ))

    # PA enabled
    pa_enabled = props.get("glide.pa.enabled", "")
    if str(pa_enabled).lower() == "true":
        issues.append(_flag(
            "INFO", "Configuration",
            "Performance Analytics is enabled",
            "PA generates heavy aggregation queries on snapshot tables.",
            "PA aggregations are a primary RaptorDB Pro HTAP benefit area."
        ))

    # Data Fabric
    wdf = props.get("sn_data_fabric.enabled", "")
    if str(wdf).lower() == "true":
        issues.append(_flag(
            "INFO", "Platform",
            "Workflow Data Fabric is enabled",
            "WDF is active on this instance.",
            "RaptorDB Pro + WDF unlocks federated queries and cross-source analytics."
        ))

    return issues


# -------------------------------------------------------------------------
# Table Sizes (Row Counts)
# -------------------------------------------------------------------------
def _check_table_sizes(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []
    df = results.get("core_table_row_counts")
    if df is None or df.empty:
        return issues

    # Very large tables (>10M rows)
    huge = df[df["row_count"] > 10_000_000]
    if not huge.empty:
        tables = ", ".join(
            f"{r['table_name']} ({r['row_count']:,})"
            for _, r in huge.head(10).iterrows()
        )
        issues.append(_flag(
            "CRITICAL", "Table Analysis",
            f"{len(huge)} tables with >10M rows",
            tables,
            "Tables with 10M+ rows are prime RaptorDB Pro candidates — "
            "columnar indexing and HTAP architecture deliver the biggest gains at this scale."
        ))

    # Large tables (1M-10M)
    large = df[(df["row_count"] > 1_000_000) & (df["row_count"] <= 10_000_000)]
    if not large.empty:
        tables = ", ".join(
            f"{r['table_name']} ({r['row_count']:,})"
            for _, r in large.head(10).iterrows()
        )
        issues.append(_flag(
            "WARNING", "Table Analysis",
            f"{len(large)} tables with 1M-10M rows",
            tables,
            "Medium-large tables that will benefit from columnar indexing for reporting queries."
        ))

    # Audit/Journal specific
    audit_tables = df[df["table_name"].isin([
        "sys_audit", "sys_audit_delete", "sys_journal_field",
        "sys_history_line", "sys_history_set"
    ])]
    if not audit_tables.empty:
        total_audit = audit_tables["row_count"].sum()
        if total_audit > 5_000_000:
            issues.append(_flag(
                "CRITICAL", "ServiceNow",
                f"Audit/Journal tables: {total_audit:,} total rows",
                "These are the fastest-growing tables in ServiceNow.",
                "RaptorDB Pro compression + columnar storage can cut audit table footprint by 50-70%."
            ))

    # CMDB tables
    cmdb_tables = df[df["table_name"].str.startswith("cmdb")]
    if not cmdb_tables.empty:
        total_cmdb = cmdb_tables["row_count"].sum()
        if total_cmdb > 1_000_000:
            issues.append(_flag(
                "WARNING", "ServiceNow",
                f"CMDB tables: {total_cmdb:,} total rows across {len(cmdb_tables)} tables",
                "Large CMDB indicates complex discovery and dependency mapping.",
                "CMDB relationship queries benefit significantly from RaptorDB Pro columnar indexing."
            ))

    # Syslog
    syslog_rows = df[df["table_name"] == "syslog"]["row_count"].sum()
    if syslog_rows > 10_000_000:
        issues.append(_flag(
            "WARNING", "ServiceNow",
            f"Syslog table: {syslog_rows:,} rows",
            "High syslog volume indicates heavy platform activity.",
            "Log table queries are I/O intensive — RaptorDB Pro columnar scans are dramatically faster."
        ))

    return issues


# -------------------------------------------------------------------------
# Indexes
# -------------------------------------------------------------------------
def _check_indexes(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []

    df_idx = results.get("indexed_fields")
    if df_idx is not None and not df_idx.empty and "name" in df_idx.columns:
        # Count indexes per table
        idx_per_table = df_idx.groupby("name").size().reset_index(name="index_count")
        over_indexed = idx_per_table[idx_per_table["index_count"] > 15]
        if not over_indexed.empty:
            tables = ", ".join(
                f"{r['name']} ({r['index_count']})"
                for _, r in over_indexed.head(5).iterrows()
            )
            issues.append(_flag(
                "WARNING", "Index Analysis",
                f"{len(over_indexed)} tables with >15 indexed fields",
                tables,
                "Heavy indexing hurts write performance. "
                "RaptorDB Pro columnar indexing can replace multiple secondary indexes."
            ))

    df_comp = results.get("composite_indexes")
    if df_comp is not None and not df_comp.empty:
        issues.append(_flag(
            "INFO", "Index Analysis",
            f"{len(df_comp)} composite indexes found",
            "Composite indexes across the instance.",
            "Review which composite indexes can be replaced by RaptorDB Pro columnar indexes."
        ))

    return issues


# -------------------------------------------------------------------------
# Reports
# -------------------------------------------------------------------------
def _check_reports(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []

    df = results.get("reports")
    if df is None or df.empty:
        return issues

    issues.append(_flag(
        "INFO", "Reports",
        f"{len(df)} reports collected",
        "Report definitions that generate queries against the database.",
        "Each report is a potential RaptorDB Pro demo candidate."
    ))

    # Reports on large tables
    summary = results.get("report_table_summary")
    if summary is not None and not summary.empty:
        top_tables = summary.head(10)
        tables = ", ".join(
            f"{r['table']} ({r['report_count']} reports)"
            for _, r in top_tables.iterrows()
        )
        issues.append(_flag(
            "WARNING", "Reports",
            f"Top report-heavy tables",
            tables,
            "Tables with many reports = frequent query load. "
            "Cross-reference with row counts to identify best RaptorDB Pro demo scenarios."
        ))

    # Reports using aggregation
    if "aggregate" in df.columns:
        agg_reports = df[df["aggregate"].notna() & (df["aggregate"] != "")]
        if not agg_reports.empty:
            issues.append(_flag(
                "WARNING", "Reports",
                f"{len(agg_reports)} reports use aggregation",
                "Aggregate reports (COUNT, SUM, AVG) trigger table scans on large datasets.",
                "Aggregation queries are where RaptorDB Pro's columnar engine shines — "
                "often 10-50x faster."
            ))

    return issues


# -------------------------------------------------------------------------
# Performance Analytics
# -------------------------------------------------------------------------
def _check_pa(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []

    df_ind = results.get("pa_indicators")
    if df_ind is not None and not df_ind.empty:
        issues.append(_flag(
            "INFO", "Performance Analytics",
            f"{len(df_ind)} active PA indicators",
            "PA indicators drive periodic aggregation jobs.",
            "PA indicator collection and dashboard rendering are top RaptorDB Pro HTAP use cases."
        ))

    df_dash = results.get("pa_dashboards")
    if df_dash is not None and not df_dash.empty:
        issues.append(_flag(
            "INFO", "Performance Analytics",
            f"{len(df_dash)} active PA dashboards",
            "Dashboards query snapshot data on every load.",
            "Dashboard load times improve dramatically with RaptorDB Pro columnar queries."
        ))

    # PA indicators by table
    pa_summary = results.get("pa_table_summary")
    if pa_summary is not None and not pa_summary.empty:
        top = pa_summary.head(5)
        tables = ", ".join(
            f"{r['table_name']} ({r['indicator_count']} indicators)"
            for _, r in top.iterrows()
        )
        issues.append(_flag(
            "WARNING", "Performance Analytics",
            "PA-heavy source tables",
            tables,
            "These tables feed PA aggregations. "
            "High indicator count + large table = maximum RaptorDB Pro benefit."
        ))

    return issues


# -------------------------------------------------------------------------
# CMDB
# -------------------------------------------------------------------------
def _check_cmdb(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []

    summary = results.get("cmdb_summary")
    if summary is None or summary.empty:
        return issues

    total_cis = summary.iloc[0].get("total_cis", 0)
    ci_classes = summary.iloc[0].get("ci_classes", 0)
    total_rels = summary.iloc[0].get("total_relationships", 0)

    if total_cis > 500_000:
        issues.append(_flag(
            "CRITICAL", "CMDB",
            f"Large CMDB: {total_cis:,} CIs across {ci_classes} classes, {total_rels:,} relationships",
            "Large CMDB means complex graph traversals and dependency queries.",
            "CMDB graph queries and relationship lookups are significantly faster on RaptorDB Pro."
        ))
    elif total_cis > 100_000:
        issues.append(_flag(
            "WARNING", "CMDB",
            f"CMDB: {total_cis:,} CIs, {ci_classes} classes, {total_rels:,} relationships",
            "Medium CMDB footprint with active relationship data.",
            "RaptorDB Pro columnar indexing benefits CMDB queries at this scale."
        ))

    return issues


# -------------------------------------------------------------------------
# Slow Transactions
# -------------------------------------------------------------------------
def _check_slow_transactions(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []

    df = results.get("slow_transactions")
    if df is None or df.empty:
        issues.append(_flag(
            "INFO", "Performance",
            "No slow transactions found (last 7 days)",
            "Either the instance is performing well or syslog_transaction isn't logging.",
            "Check glide.db.slow_query_threshold property setting."
        ))
        return issues

    issues.append(_flag(
        "WARNING", "Performance",
        f"{len(df)} slow transactions in the last 7 days (>5s)",
        "Transactions exceeding 5 second response time.",
        "Each slow transaction is a candidate for RaptorDB Pro before/after benchmarking."
    ))

    # URL summary
    url_summary = results.get("slow_transaction_summary")
    if url_summary is not None and not url_summary.empty:
        top = url_summary.head(5)
        urls = "; ".join(
            f"{r['url']} (count={r['count']}, avg={r['avg_response_ms']:.0f}ms)"
            for _, r in top.iterrows()
        )
        issues.append(_flag(
            "CRITICAL", "Performance",
            "Top slow URL patterns",
            urls,
            "These URLs likely map to specific table queries/reports. "
            "Ideal candidates for RaptorDB Pro demo scenarios."
        ))

    return issues


# -------------------------------------------------------------------------
# Workload Profile
# -------------------------------------------------------------------------
def _check_workload_profile(results: Dict[str, pd.DataFrame]) -> List[Dict]:
    issues = []

    jobs = results.get("scheduled_jobs")
    if jobs is not None and not jobs.empty:
        job_count = len(jobs)
        issues.append(_flag(
            "INFO", "Workload",
            f"{job_count} active scheduled jobs",
            "Background jobs contribute to mixed OLTP/OLAP workload.",
            "Heavy scheduled job workload alongside user transactions = "
            "classic HTAP scenario where RaptorDB Pro excels."
        ))

    rotation = results.get("table_rotation")
    if rotation is not None and not rotation.empty:
        issues.append(_flag(
            "INFO", "Workload",
            f"{len(rotation)} table rotation rules active",
            "Table rotation indicates large, fast-growing tables.",
            "Tables under rotation are prime RaptorDB Pro candidates — "
            "columnar compression reduces the need for aggressive rotation."
        ))

    return issues


# =========================================================================
# Top Use Case Scoring
# =========================================================================

# Friendly display names for well-known ServiceNow tables
_TABLE_LABELS = {
    "task": "Task (base)",
    "incident": "Incident Management",
    "change_request": "Change Management",
    "problem": "Problem Management",
    "sc_request": "Service Catalog Request",
    "sc_req_item": "Requested Items",
    "sc_task": "Catalog Tasks",
    "kb_knowledge": "Knowledge Base",
    "sys_audit": "Audit Log",
    "sys_audit_delete": "Audit Delete Log",
    "sys_journal_field": "Journal / Field History",
    "sys_history_line": "History Lines",
    "sys_history_set": "History Sets",
    "cmdb_ci": "CMDB Configuration Items",
    "cmdb_rel_ci": "CMDB Relationships",
    "cmdb_ci_computer": "CMDB Computers",
    "cmdb_ci_server": "CMDB Servers",
    "cmdb_ci_service": "CMDB Services",
    "syslog": "System Log",
    "syslog_transaction": "Transaction Log",
    "pa_snapshots": "PA Snapshots",
    "pa_snapshot_daily": "PA Daily Snapshots",
    "wf_context": "Workflow Context",
    "wf_executing": "Workflow Executing",
    "wf_history": "Workflow History",
    "ecc_queue": "ECC Queue",
    "sysevent": "System Events",
    "alm_asset": "Asset Management",
    "alm_hardware": "Hardware Assets",
    "em_alert": "Event Management Alerts",
    "em_event": "Event Management Events",
    "hr_case": "HR Cases",
    "sn_hr_core_case": "HR Core Cases",
    "sn_customerservice_case": "Customer Service Cases",
}


def score_use_cases(results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Score and rank the top 10 RaptorDB Pro use cases from collected data.

    Scoring dimensions (100-point scale):
      - Table size (row count)   — up to 40 pts  (log scale)
      - Report query load        — up to 25 pts
      - PA indicator pressure    — up to 20 pts
      - Table rotation active    — 10 pts bonus
      - Slow transaction hits    —  5 pts bonus
    """
    # --- Build lookup dicts from collected DataFrames ---
    row_counts: Dict[str, int] = {}
    rc_df = results.get("core_table_row_counts")
    if rc_df is not None and not rc_df.empty and "table_name" in rc_df.columns:
        row_counts = dict(zip(rc_df["table_name"], rc_df["row_count"]))

    report_counts: Dict[str, int] = {}
    rpt_df = results.get("report_table_summary")
    if rpt_df is not None and not rpt_df.empty and "table" in rpt_df.columns:
        report_counts = dict(zip(rpt_df["table"], rpt_df["report_count"]))

    pa_counts: Dict[str, int] = {}
    pa_df = results.get("pa_table_summary")
    if pa_df is not None and not pa_df.empty and "table_name" in pa_df.columns:
        pa_counts = dict(zip(pa_df["table_name"], pa_df["indicator_count"]))

    rotation_tables: set = set()
    rot_df = results.get("table_rotation")
    if rot_df is not None and not rot_df.empty and "table" in rot_df.columns:
        rotation_tables = set(rot_df["table"].tolist())

    slow_tables: set = set()
    slow_df = results.get("slow_transaction_summary")
    if slow_df is not None and not slow_df.empty and "url" in slow_df.columns:
        for url in slow_df["url"]:
            if "/table/" in str(url):
                slug = str(url).split("/table/")[-1].split("?")[0].split("/")[0]
                if slug:
                    slow_tables.add(slug)

    candidates = []

    # --- Score every table that appears in at least one dimension ---
    all_tables = set(row_counts) | set(report_counts) | set(pa_counts)

    for table in all_tables:
        rows = row_counts.get(table, 0)
        reports = report_counts.get(table, 0)
        pa_inds = pa_counts.get(table, 0)
        in_rotation = table in rotation_tables
        in_slow = table in slow_tables

        score = 0.0
        score += min(40.0, math.log10(rows + 1) * 8) if rows > 0 else 0
        score += min(25.0, reports * 1.5)
        score += min(20.0, pa_inds * 4.0)
        score += 10.0 if in_rotation else 0
        score += 5.0 if in_slow else 0

        # Category
        if pa_inds > 0 and rows > 100_000:
            category = "HTAP / PA Aggregation"
        elif table in ("sys_audit", "sys_audit_delete", "sys_journal_field",
                       "sys_history_line", "sys_history_set"):
            category = "Audit / Log Analytics"
        elif table.startswith("cmdb"):
            category = "CMDB Graph Query"
        elif rows > 1_000_000:
            category = "Large Table Scan"
        elif reports >= 10:
            category = "Report-Heavy Query Load"
        else:
            category = "Mixed OLTP/OLAP"

        # Benefit blurb
        if pa_inds > 0:
            benefit = "Columnar engine accelerates PA aggregations 10–50×"
        elif rows > 10_000_000:
            benefit = "Columnar indexing eliminates full table scans at scale"
        elif reports >= 20:
            benefit = "Parallel columnar scans handle concurrent report load"
        elif in_rotation:
            benefit = "Columnar compression reduces aggressive rotation need"
        else:
            benefit = "HTAP isolation removes OLTP/reporting contention"

        # Evidence
        evidence_parts = []
        if rows:
            evidence_parts.append(f"{rows:,} rows")
        if reports:
            evidence_parts.append(f"{reports} reports")
        if pa_inds:
            evidence_parts.append(f"{pa_inds} PA indicators")
        if in_rotation:
            evidence_parts.append("rotation active")
        if in_slow:
            evidence_parts.append("appears in slow tx")

        label = _TABLE_LABELS.get(table, table)
        candidates.append({
            "Use Case": label,
            "Category": category,
            "Key Table(s)": table,
            "Evidence": ", ".join(evidence_parts),
            "RaptorDB Pro Benefit": benefit,
            "Score": round(score, 1),
        })

    # --- Non-table synthetic use cases ---

    # CMDB graph traversal (overall)
    cmdb_summary = results.get("cmdb_summary")
    if cmdb_summary is not None and not cmdb_summary.empty:
        total_cis = int(cmdb_summary.iloc[0].get("total_cis", 0))
        total_rels = int(cmdb_summary.iloc[0].get("total_relationships", 0))
        ci_classes = int(cmdb_summary.iloc[0].get("ci_classes", 0))
        if total_cis > 50_000:
            score = (min(40.0, math.log10(total_cis + 1) * 8)
                     + min(20.0, math.log10(total_rels + 1) * 5))
            candidates.append({
                "Use Case": "CMDB Dependency & Impact Analysis",
                "Category": "CMDB Graph Query",
                "Key Table(s)": "cmdb_ci, cmdb_rel_ci",
                "Evidence": f"{total_cis:,} CIs, {ci_classes} classes, {total_rels:,} relationships",
                "RaptorDB Pro Benefit": "Graph traversals & impact queries faster with columnar indexes",
                "Score": round(score, 1),
            })

    # Slow transaction benchmarking
    if slow_df is not None and not slow_df.empty:
        total_slow = int(slow_df["count"].sum()) if "count" in slow_df.columns else len(slow_df)
        avg_ms = float(slow_df["avg_response_ms"].mean()) if "avg_response_ms" in slow_df.columns else 0
        score = min(40.0, total_slow * 0.4) + min(25.0, avg_ms / 400)
        candidates.append({
            "Use Case": "Slow Query Optimization",
            "Category": "Performance Benchmark",
            "Key Table(s)": "Various (syslog_transaction)",
            "Evidence": f"{total_slow} slow tx patterns, avg {avg_ms:.0f} ms",
            "RaptorDB Pro Benefit": "Direct before/after benchmark — measurable, customer-visible ROI",
            "Score": round(score, 1),
        })

    # PA Dashboard rendering
    pa_dash_df = results.get("pa_dashboards")
    pa_ind_df = results.get("pa_indicators")
    if (pa_dash_df is not None and not pa_dash_df.empty
            and pa_ind_df is not None and not pa_ind_df.empty):
        score = min(25.0, len(pa_dash_df)) + min(40.0, len(pa_ind_df) * 0.2)
        candidates.append({
            "Use Case": "PA Dashboard Rendering",
            "Category": "HTAP / PA Aggregation",
            "Key Table(s)": "pa_snapshots, pa_snapshot_daily",
            "Evidence": f"{len(pa_dash_df)} dashboards, {len(pa_ind_df)} indicators",
            "RaptorDB Pro Benefit": "Dashboard load 5–20× faster with columnar snapshot queries",
            "Score": round(score, 1),
        })

    # Audit log search (combined across all audit tables)
    audit_table_names = ["sys_audit", "sys_audit_delete", "sys_journal_field",
                         "sys_history_line", "sys_history_set"]
    audit_rows = sum(row_counts.get(t, 0) for t in audit_table_names)
    if audit_rows > 500_000:
        score = min(70.0, math.log10(audit_rows + 1) * 10)
        candidates.append({
            "Use Case": "Audit Log Search & Compliance Reporting",
            "Category": "Audit / Log Analytics",
            "Key Table(s)": "sys_audit, sys_journal_field, sys_history_line",
            "Evidence": f"{audit_rows:,} combined audit/journal rows",
            "RaptorDB Pro Benefit": "Columnar compression cuts storage 50–70%; range scans 20–100× faster",
            "Score": round(score, 1),
        })

    if not candidates:
        return pd.DataFrame(columns=[
            "Use Case", "Category", "Key Table(s)", "Evidence",
            "RaptorDB Pro Benefit", "Score"
        ])

    df = pd.DataFrame(candidates)
    df = (df.sort_values("Score", ascending=False)
            .drop_duplicates(subset=["Use Case"])
            .reset_index(drop=True)
            .head(10))
    df.index = df.index + 1
    df.index.name = "Rank"
    return df
