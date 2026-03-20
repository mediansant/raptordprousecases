"""
RaptorDB Pro Readiness Analyzer — Data Collection via ServiceNow REST APIs
Collects table metadata, indexes, reports, PA config, CMDB stats,
system properties, and slow transaction data.
"""

import pandas as pd
import time
from typing import Dict, Callable
from sn_client import SNClient


# Key ServiceNow tables to profile (row counts + used in dashboards/reports)
SN_CORE_TABLES = [
    "task", "incident", "change_request", "problem", "sc_request",
    "sc_req_item", "sc_task", "kb_knowledge", "sys_user", "sys_user_group",
    "sys_audit", "sys_audit_delete", "sys_journal_field",
    "sys_history_line", "sys_history_set",
    "cmdb_ci", "cmdb_rel_ci", "cmdb_ci_computer", "cmdb_ci_server",
    "cmdb_ci_service", "cmdb_ci_vm_instance", "cmdb_ci_app_server",
    "syslog", "syslog_transaction", "sys_email",
    "pa_snapshots", "pa_snapshot_daily",
    "sys_trigger", "ecc_queue", "sysevent",
    "wf_context", "wf_executing", "wf_history",
    "alm_asset", "ast_contract", "alm_hardware",
    "sys_attachment", "sys_attachment_doc",
    "hr_case", "sn_hr_core_case",
    "sn_customerservice_case", "customer_service_case",
    "em_alert", "em_event",
    "cert_follow_on_task", "fm_expense_line",
    "dl_definition", "dl_matcher",
    "sys_report", "sys_report_map",
]

# System properties relevant to DB / performance / RaptorDB assessment
SN_PROPERTY_NAMES = [
    "glide.buildtag", "glide.buildname", "glide.builddate",
    "glide.db.type", "glide.db.name", "glide.db.rdbms",
    "glide.db.max_connections", "glide.db.pool_size",
    "glide.db.query.max_time", "glide.db.query.timeout",
    "glide.db.slow_query_threshold",
    "glide.sys.cache.max_size", "glide.cache.zone.enable",
    "glide.ts.enabled", "glide.ts.indexing.enabled",
    "glide.platform.is_paas",
    "glide.report.max_rows", "glide.report.timeout",
    "glide.pa.enabled", "glide.pa.snapshot.enabled",
    "com.snc.platform_analytics.enabled",
    "glide.cmdb.identification.enabled",
    "glide.db.impex.parallel.enabled",
    "glide.db.archiving.active",
    "glide.db.table_rotation.active",
    "sn_data_fabric.enabled",
    "glide.raptordb.enabled", "glide.raptordb.pro.enabled",
]


def collect_all(client: SNClient, progress_callback: Callable = None,
                days: int = 7) -> Dict[str, pd.DataFrame]:
    """
    Master collection function. Returns dict of category -> DataFrame.
    progress_callback(pct: float, msg: str) for UI updates.
    days: number of days of history to analyse for slow transactions.
    """
    results = {}
    steps = [
        ("System Properties", _collect_properties),
        ("Table Inventory (sys_db_object)", _collect_table_inventory),
        ("Core Table Row Counts", _collect_row_counts),
        ("Field & Index Definitions", _collect_dictionary),
        ("Report Inventory", _collect_reports),
        ("Performance Analytics Config", _collect_pa),
        ("CMDB Profile", _collect_cmdb_profile),
        ("Slow Transactions", lambda c: _collect_slow_transactions(c, days=days)),
        ("Scheduled Jobs", _collect_scheduled_jobs),
        ("Table Rotation Config", _collect_table_rotation),
    ]

    for i, (name, func) in enumerate(steps):
        if progress_callback:
            progress_callback((i) / len(steps), f"Collecting: {name}")
        try:
            result = func(client)
            results.update(result)
        except Exception as e:
            # Store error as a single-row DataFrame so the UI can show it
            results[name.lower().replace(" ", "_") + "_error"] = pd.DataFrame(
                [{"error": str(e)}]
            )

    if progress_callback:
        progress_callback(1.0, "Collection complete!")

    return results


# =========================================================================
# Individual Collectors
# =========================================================================

def _collect_properties(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Collect system properties relevant to DB and platform config."""
    records = client.get_properties(names=SN_PROPERTY_NAMES)
    # Also grab any glide.db.* and glide.raptordb.* we might have missed
    records += client.get_properties(prefix="glide.db.")
    records += client.get_properties(prefix="glide.raptordb")
    records += client.get_properties(prefix="sn_data_fabric")

    # Deduplicate by name
    seen = set()
    unique = []
    for r in records:
        name = r.get("name", "")
        if name not in seen:
            seen.add(name)
            unique.append(r)

    df = pd.DataFrame(unique) if unique else pd.DataFrame(columns=["name", "value", "description"])
    return {"system_properties": df}


def _collect_table_inventory(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Pull sys_db_object for the full table inventory."""
    records = client.get_table(
        "sys_db_object",
        fields="name,label,super_class,is_extendable,sys_class_name,number_ref,sys_created_on,sys_updated_on",
        limit=500,
        order_by="ORDERBYname",
        max_pages=40  # up to 20k tables
    )
    df = pd.DataFrame(records) if records else pd.DataFrame()
    return {"table_inventory": df}


def _collect_row_counts(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Get row counts for core ServiceNow tables using Stats API."""
    counts = []
    for table in SN_CORE_TABLES:
        count = client.get_row_count(table)
        counts.append({
            "table_name": table,
            "row_count": count
        })
        time.sleep(0.05)  # gentle on the instance

    df = pd.DataFrame(counts)
    df = df.sort_values("row_count", ascending=False).reset_index(drop=True)
    return {"core_table_row_counts": df}


def _collect_dictionary(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Pull sys_dictionary for field definitions and index markers."""
    # Indexed fields across all tables
    indexed = client.get_table(
        "sys_dictionary",
        query="index=true",
        fields="name,element,column_label,internal_type,max_length,index,active",
        limit=500,
        max_pages=10
    )

    # All fields for top task/cmdb tables
    key_table_fields = client.get_table(
        "sys_dictionary",
        query="nameINtask,incident,change_request,problem,cmdb_ci,cmdb_rel_ci,sys_audit,sys_journal_field",
        fields="name,element,column_label,internal_type,max_length,reference,index,active",
        limit=500,
        max_pages=10
    )

    # sys_index table for composite indexes
    indexes = client.get_table(
        "sys_index",
        fields="table,index_col_1,index_col_2,index_col_3,index_col_4,active,unique",
        limit=500,
        max_pages=5
    )

    return {
        "indexed_fields": pd.DataFrame(indexed) if indexed else pd.DataFrame(),
        "key_table_fields": pd.DataFrame(key_table_fields) if key_table_fields else pd.DataFrame(),
        "composite_indexes": pd.DataFrame(indexes) if indexes else pd.DataFrame(),
    }


def _collect_reports(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Collect report definitions — these map to dashboard queries."""
    reports = client.get_table(
        "sys_report",
        fields="title,table,field,type,chart_type,filter,group_by,aggregate,sys_created_on,sys_updated_on,user",
        limit=500,
        order_by="ORDERBYDESCsys_updated_on",
        display_value="true",
        max_pages=10
    )

    # Report usage stats — which tables are queried most by reports
    report_df = pd.DataFrame(reports) if reports else pd.DataFrame()

    # Summarize by table
    table_report_counts = pd.DataFrame()
    if not report_df.empty and "table" in report_df.columns:
        table_report_counts = (
            report_df.groupby("table")
            .size()
            .reset_index(name="report_count")
            .sort_values("report_count", ascending=False)
        )

    return {
        "reports": report_df,
        "report_table_summary": table_report_counts,
    }


def _collect_pa(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Collect Performance Analytics configuration."""
    # PA Indicators
    indicators = client.get_table(
        "pa_indicators",
        fields="name,frequency,aggregate,table_name,conditions,active,breakdown_source,sys_updated_on",
        query="active=true",
        limit=500,
        display_value="true",
        max_pages=5
    )

    # PA Dashboards
    dashboards = client.get_table(
        "pa_dashboards",
        fields="name,description,active,sys_created_on,sys_updated_on",
        query="active=true",
        limit=200,
        display_value="true",
        max_pages=3
    )

    # PA Widgets (what's on the dashboards)
    widgets = client.get_table(
        "pa_widgets",
        fields="name,indicator,dashboard,visualization,sys_updated_on",
        limit=500,
        display_value="true",
        max_pages=5
    )

    # Summarize PA indicators by source table
    ind_df = pd.DataFrame(indicators) if indicators else pd.DataFrame()
    pa_table_summary = pd.DataFrame()
    if not ind_df.empty and "table_name" in ind_df.columns:
        pa_table_summary = (
            ind_df.groupby("table_name")
            .size()
            .reset_index(name="indicator_count")
            .sort_values("indicator_count", ascending=False)
        )

    return {
        "pa_indicators": ind_df,
        "pa_dashboards": pd.DataFrame(dashboards) if dashboards else pd.DataFrame(),
        "pa_widgets": pd.DataFrame(widgets) if widgets else pd.DataFrame(),
        "pa_table_summary": pa_table_summary,
    }


def _collect_cmdb_profile(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Profile CMDB — CI classes and relationships."""
    # CI counts by class
    ci_stats = client.get_stats(
        "cmdb_ci",
        group_by="sys_class_name",
        count=True
    )

    ci_class_counts = []
    if isinstance(ci_stats, list):
        for item in ci_stats:
            ci_class_counts.append({
                "ci_class": item.get("groupby_fields", [{}])[0].get("value", ""),
                "ci_class_display": item.get("groupby_fields", [{}])[0].get("display_value", ""),
                "count": int(item.get("stats", {}).get("count", 0))
            })
    elif isinstance(ci_stats, dict) and "result" in ci_stats:
        for item in ci_stats.get("result", []):
            ci_class_counts.append({
                "ci_class": item.get("groupby_fields", [{}])[0].get("value", ""),
                "count": int(item.get("stats", {}).get("count", 0))
            })

    ci_df = pd.DataFrame(ci_class_counts) if ci_class_counts else pd.DataFrame()
    if not ci_df.empty:
        ci_df = ci_df.sort_values("count", ascending=False).reset_index(drop=True)

    # Total relationship count
    rel_count = client.get_row_count("cmdb_rel_ci")

    cmdb_summary = pd.DataFrame([{
        "total_cis": ci_df["count"].sum() if not ci_df.empty else 0,
        "ci_classes": len(ci_df),
        "total_relationships": rel_count
    }])

    return {
        "cmdb_ci_classes": ci_df,
        "cmdb_summary": cmdb_summary,
    }


def _collect_slow_transactions(client: SNClient, days: int = 7) -> Dict[str, pd.DataFrame]:
    """Collect slow transaction data from syslog_transaction."""
    slow = client.get_table(
        "syslog_transaction",
        query=(
            f"response_time>5000^sys_created_on"
            f"BETWEENjavascript:gs.daysAgoStart({days})"
            f"@javascript:gs.daysAgoEnd(0)"
        ),
        fields="url,response_time,sys_created_on,session,user",
        limit=500,
        order_by="ORDERBYDESCresponse_time",
        max_pages=4
    )

    slow_df = pd.DataFrame(slow) if slow else pd.DataFrame()

    # Summarize by URL pattern
    url_summary = pd.DataFrame()
    if not slow_df.empty and "url" in slow_df.columns:
        slow_df["response_time"] = pd.to_numeric(slow_df["response_time"], errors="coerce")
        url_summary = (
            slow_df.groupby("url")
            .agg(
                count=("url", "size"),
                avg_response_ms=("response_time", "mean"),
                max_response_ms=("response_time", "max"),
                p90_response_ms=("response_time", lambda x: x.quantile(0.9) if len(x) > 1 else x.max()),
            )
            .reset_index()
            .sort_values("count", ascending=False)
            .head(50)
        )

    return {
        "slow_transactions": slow_df,
        "slow_transaction_summary": url_summary,
    }


def _collect_scheduled_jobs(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Collect active scheduled jobs — background workload profile."""
    jobs = client.get_table(
        "sysauto",
        query="active=true",
        fields="name,run_type,run_period,run_time,run_dayofweek,sys_class_name,state",
        limit=500,
        display_value="true",
        max_pages=5
    )
    return {
        "scheduled_jobs": pd.DataFrame(jobs) if jobs else pd.DataFrame()
    }


def _collect_table_rotation(client: SNClient) -> Dict[str, pd.DataFrame]:
    """Collect table rotation/archiving configuration."""
    rotation = client.get_table(
        "sys_table_rotation_schedule",
        fields="table,max_table_size,active",
        limit=200,
        display_value="true",
        max_pages=2
    )
    return {
        "table_rotation": pd.DataFrame(rotation) if rotation else pd.DataFrame()
    }
