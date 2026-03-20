#!/usr/bin/env python3
"""
RaptorDB Pro Readiness Analyzer — CLI Report Generator

Run this script on any machine with Python 3.9+ and the required packages.
It connects to your ServiceNow instance, collects data, and generates a PDF report.
Optionally generates a Claude AI prompt file and CSV data export.

Usage:
    python generate_report.py [options]

    Or set environment variables:
        SN_INSTANCE_URL, SN_USERNAME, SN_PASSWORD

Examples:
    python generate_report.py --url https://myinstance.service-now.com \
                              --user admin --password mypass

    python generate_report.py  # will prompt for missing values interactively
"""

import argparse
import getpass
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependency check — give a clear message if packages are missing
# ---------------------------------------------------------------------------
_MISSING = []
try:
    import pandas as pd
except ImportError:
    _MISSING.append("pandas")
try:
    import requests  # noqa: F401
except ImportError:
    _MISSING.append("requests")
try:
    import reportlab  # noqa: F401
except ImportError:
    _MISSING.append("reportlab")

if _MISSING:
    print("ERROR: Missing required packages:", ", ".join(_MISSING))
    print("Install them with:  pip install -r requirements.txt")
    sys.exit(1)

from sn_client import SNClient
from collector import collect_all
from analyzer import analyze_all, score_use_cases
from pov_selector import get_pov_shortlist
from pdf_report import generate_pdf_report
from report_engine import generate_claude_prompt


# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="RaptorDB Pro Readiness Analyzer — CLI Report Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--url", "-u",
        default=os.environ.get("SN_INSTANCE_URL", ""),
        help="ServiceNow instance URL, e.g. https://myinstance.service-now.com",
    )
    parser.add_argument(
        "--user", "-U",
        default=os.environ.get("SN_USERNAME", ""),
        help="ServiceNow username (needs admin or rest_api_explorer role)",
    )
    parser.add_argument(
        "--password", "-p",
        default=os.environ.get("SN_PASSWORD", ""),
        help="ServiceNow password (omit to be prompted securely)",
    )
    parser.add_argument(
        "--no-ssl-verify",
        action="store_true",
        default=False,
        help="Disable SSL certificate verification (for sub-prod instances)",
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=7,
        help="Number of days of slow-transaction history to analyse (default: 7)",
    )
    parser.add_argument(
        "--output", "-o",
        default="",
        help="Output PDF file path (default: RaptorDB_Readiness_<timestamp>.pdf)",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        default=False,
        help="Also export raw collected data as CSV files alongside the PDF",
    )
    parser.add_argument(
        "--claude",
        action="store_true",
        default=False,
        help="Generate Claude AI prompt file (skips the interactive prompt)",
    )
    parser.add_argument(
        "--no-claude",
        action="store_true",
        default=False,
        help="Skip the Claude export prompt entirely",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP request timeout in seconds (default: 30)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _prompt(label: str, current: str, secret: bool = False) -> str:
    if current:
        return current
    if secret:
        return getpass.getpass(f"{label}: ")
    value = input(f"{label}: ").strip()
    if not value:
        print(f"ERROR: {label} is required.")
        sys.exit(1)
    return value


def _ask_yes_no(question: str) -> bool:
    """Ask a yes/no question and return True for yes."""
    while True:
        answer = input(f"{question} [y/n]: ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("  Please enter y or n.")


def _progress(pct: float, msg: str):
    bar_len = 30
    filled = int(bar_len * pct)
    bar = "#" * filled + "-" * (bar_len - filled)
    print(f"\r  [{bar}] {int(pct * 100):3d}%  {msg:<45}", end="", flush=True)
    if pct >= 1.0:
        print()  # newline when done


def _export_csv(results: dict, issues: list, export_dir: Path):
    export_dir.mkdir(parents=True, exist_ok=True)
    for key, df in results.items():
        if df is not None and not df.empty:
            safe_name = key.replace("/", "_").replace(" ", "_")
            df.to_csv(export_dir / f"{safe_name}.csv", index=False)
    with open(export_dir / "flagged_issues.json", "w") as f:
        json.dump(issues, f, indent=2, default=str)
    print(f"  CSV data exported to: {export_dir}")


def _export_claude_package(results: dict, issues: list,
                            export_dir: Path, timestamp: str):
    """
    Write the Claude prompt .txt file and all supporting CSV data into
    export_dir/claude_export_<timestamp>/.

    Returns the path to the prompt file.
    """
    claude_dir = export_dir / f"claude_export_{timestamp}"
    claude_dir.mkdir(parents=True, exist_ok=True)

    # ── Prompt file ───────────────────────────────────────────────
    prompt_text = generate_claude_prompt(results, issues)
    prompt_path = claude_dir / "claude_prompt.txt"
    prompt_path.write_text(prompt_text, encoding="utf-8")

    # ── Supporting CSV data ───────────────────────────────────────
    csv_datasets = [
        "system_properties",
        "core_table_row_counts",
        "report_table_summary",
        "pa_table_summary",
        "slow_transaction_summary",
        "cmdb_summary",
        "cmdb_ci_classes",
        "composite_indexes",
        "indexed_fields",
        "scheduled_jobs",
        "table_rotation",
    ]
    written = []
    for key in csv_datasets:
        df = results.get(key)
        if df is not None and not df.empty:
            out = claude_dir / f"{key}.csv"
            df.to_csv(out, index=False)
            written.append(f"{key}.csv  ({len(df)} rows)")

    # ── Instructions file ─────────────────────────────────────────
    instructions = [
        "HOW TO USE THIS CLAUDE EXPORT",
        "=" * 50,
        "",
        "1. Open claude.ai (or any Claude interface)",
        "2. Start a new conversation",
        "3. Upload ALL the CSV files in this folder as attachments",
        "4. Copy the full contents of claude_prompt.txt",
        "5. Paste it as your first message",
        "6. Claude will generate a rich, data-specific RaptorDB Pro",
        "   Readiness Report with exact table names and numbers.",
        "",
        "Files to upload:",
    ]
    for f in written:
        instructions.append(f"  - {f}")
    instructions += [
        "",
        "Generated: " + datetime.now().isoformat(timespec="seconds"),
    ]
    (claude_dir / "INSTRUCTIONS.txt").write_text(
        "\n".join(instructions), encoding="utf-8"
    )

    return prompt_path, claude_dir


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    print()
    print("=" * 60)
    print("  RaptorDB Pro Readiness Analyzer — Report Generator")
    print("=" * 60)
    print()

    # ── Gather credentials ────────────────────────────────────────
    instance_url = _prompt("ServiceNow instance URL", args.url)
    username     = _prompt("Username", args.user)
    password     = _prompt("Password", args.password, secret=True)

    # Normalise URL
    if not instance_url.startswith("http"):
        instance_url = "https://" + instance_url

    # ── Connect & test ────────────────────────────────────────────
    print()
    print("Connecting to ServiceNow...")
    client = SNClient(
        instance_url=instance_url,
        username=username,
        password=password,
        verify_ssl=not args.no_ssl_verify,
        timeout=args.timeout,
    )
    conn_result = client.test_connection()
    if not conn_result["success"]:
        print(f"\nERROR: {conn_result['message']}")
        sys.exit(1)

    print(f"  Connected — {conn_result['message']}")

    conn_info = {
        "instance_url": instance_url,
        "username":     username,
        "build":        conn_result.get("build", ""),
        "analysis_days": args.days,
        "collected_at": datetime.now().isoformat(timespec="seconds"),
    }

    # ── Collect data ──────────────────────────────────────────────
    print()
    print(f"Collecting data ({args.days}-day history window)...")
    results = collect_all(client, progress_callback=_progress, days=args.days)
    print()

    # ── Analyse ───────────────────────────────────────────────────
    print("Analysing collected data...")
    issues = analyze_all(results)
    top_df = score_use_cases(results)

    critical = sum(1 for i in issues if i.get("severity") == "CRITICAL")
    high     = sum(1 for i in issues if i.get("severity") == "HIGH")
    print(f"  Found {len(issues)} findings  ({critical} critical, {high} high)")

    # ── POV shortlist ─────────────────────────────────────────────
    print("Building POV shortlist...")
    shortlist = get_pov_shortlist(results)

    # ── Generate PDF ──────────────────────────────────────────────
    print("Generating PDF report...")
    pdf_bytes = generate_pdf_report(
        results=results,
        issues=issues,
        shortlist=shortlist,
        conn_info=conn_info,
        top_df=top_df,
        analysis_days=args.days,
    )

    # ── Save PDF ──────────────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else Path(f"RaptorDB_Readiness_{timestamp}.pdf")
    output_path.write_bytes(pdf_bytes)
    print(f"  PDF saved: {output_path.resolve()}")

    # ── Optional CSV export ───────────────────────────────────────
    if args.export_csv:
        csv_dir = output_path.parent / f"raptordb_export_{timestamp}"
        _export_csv(results, issues, csv_dir)

    # ── Claude export — ask interactively unless flag was passed ──
    generate_claude = False
    if args.claude:
        generate_claude = True
    elif not args.no_claude:
        print()
        print("─" * 60)
        print("  OPTIONAL: Claude AI Analysis")
        print("─" * 60)
        print("  Generate a prompt file + CSV data package that you can")
        print("  upload to claude.ai for a rich, AI-authored readiness")
        print("  report with specific recommendations.")
        print()
        generate_claude = _ask_yes_no("  Generate Claude export package?")

    if generate_claude:
        print("  Building Claude export package...")
        prompt_path, claude_dir = _export_claude_package(
            results, issues,
            export_dir=output_path.parent,
            timestamp=timestamp,
        )
        print(f"  Claude package saved to: {claude_dir.resolve()}")
        print()
        print("  Next steps:")
        print("  1. Open claude.ai and start a new conversation")
        print("  2. Upload all CSV files from the claude_export folder")
        print("  3. Paste the contents of claude_prompt.txt as your message")
        print("  (See INSTRUCTIONS.txt in the folder for full details)")

    # ── Summary ───────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  Report generation complete!")
    print(f"  PDF    : {output_path.resolve()}")
    if generate_claude:
        print(f"  Claude : {claude_dir.resolve()}")
    print(f"  Issues : {len(issues)} total  |  {critical} critical  |  {high} high")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
