"""
RaptorDB Pro Readiness Analyzer — PDF Report Generator

Produces a polished, customer-ready PDF using ReportLab Platypus.

Page layout:
  1. Cover  — plain navy, title + subtitle only
  2. Top Targeted Use Cases  — the POV shortlist ranked cards
  3. POV Shortlist tables  — reports / dashboards / slow queries
  4. Executive Summary  — metrics, platform table, largest tables
  5. Key Findings + Next Steps
"""

import io
import re
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    HRFlowable, KeepTogether, PageBreak,
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

# =============================================================================
# Brand palette
# =============================================================================
_NAVY  = colors.HexColor("#1a1a2e")
_TEAL  = colors.HexColor("#10b981")
_RED   = colors.HexColor("#ef4444")
_AMBER = colors.HexColor("#f59e0b")
_BLUE  = colors.HexColor("#3b82f6")
_LGRAY = colors.HexColor("#f8fafc")
_MGRAY = colors.HexColor("#e2e8f0")
_DGRAY = colors.HexColor("#64748b")

PW, PH  = A4                    # 595.27 x 841.89 pts
_MARGIN  = 1.8 * cm
_TOP_MARGIN    = 2.2 * cm       # Extra room below header bar
_BOTTOM_MARGIN = 2.0 * cm
_INNER_W = PW - 2 * _MARGIN    # usable text width ≈ 17.4 cm
_HEADER_H = 1.5 * cm           # height of navy header bar on content pages


# =============================================================================
# Helpers
# =============================================================================

def _pdf_safe(v, maxlen: int = 90) -> str:
    """Return a string that is safe for standard PDF fonts (Latin-1 range).
    Strips emoji and other non-Latin-1 characters.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    s = str(v)[:maxlen]
    # Replace known emoji effort labels with plain text equivalents
    replacements = [
        ("🟢", ""), ("🟡", ""), ("🟠", ""), ("🔴", ""),
        ("📊", ""), ("📈", ""), ("🐢", ""), ("✅", ""),
        ("⚡", ""), ("🔍", ""), ("🎯", ""), ("📋", ""),
    ]
    for old, new in replacements:
        s = s.replace(old, new)
    # Drop anything outside Latin-1
    s = s.encode("latin-1", errors="ignore").decode("latin-1")
    return s.strip()


# =============================================================================
# Paragraph styles
# =============================================================================

def _S() -> Dict[str, ParagraphStyle]:
    W = colors.white
    D = colors.HexColor("#374151")
    return {
        "section": ParagraphStyle(
            "section", fontName="Helvetica-Bold", fontSize=13,
            textColor=_NAVY, spaceBefore=12, spaceAfter=5),
        "subsection": ParagraphStyle(
            "subsection", fontName="Helvetica-Bold", fontSize=10,
            textColor=_NAVY, spaceBefore=8, spaceAfter=4),
        "body": ParagraphStyle(
            "body", fontName="Helvetica", fontSize=9,
            textColor=D, spaceAfter=5, leading=13),
        "improvement": ParagraphStyle(
            "improvement", fontName="Helvetica-Bold", fontSize=9,
            textColor=_TEAL, spaceAfter=3),
        "caption": ParagraphStyle(
            "caption", fontName="Helvetica-Oblique", fontSize=8,
            textColor=_DGRAY, spaceAfter=3),
        "metric_val": ParagraphStyle(
            "metric_val", fontName="Helvetica-Bold", fontSize=16,
            textColor=_NAVY, alignment=TA_CENTER, spaceAfter=2),
        "metric_lbl": ParagraphStyle(
            "metric_lbl", fontName="Helvetica", fontSize=7,
            textColor=_DGRAY, alignment=TA_CENTER),
        "bullet": ParagraphStyle(
            "bullet", fontName="Helvetica", fontSize=9,
            textColor=D, leftIndent=12, spaceAfter=4, leading=13),
        "th": ParagraphStyle(
            "th", fontName="Helvetica-Bold", fontSize=8,
            textColor=W, leading=11),
        "td": ParagraphStyle(
            "td", fontName="Helvetica", fontSize=8,
            textColor=D, leading=11),
        "td_teal": ParagraphStyle(
            "td_teal", fontName="Helvetica-Bold", fontSize=8,
            textColor=_TEAL, leading=11),
        "footer": ParagraphStyle(
            "footer", fontName="Helvetica-Oblique", fontSize=7,
            textColor=_DGRAY, alignment=TA_CENTER),
    }


# =============================================================================
# Canvas callbacks — page decoration
# =============================================================================

def _cover_canvas(canvas, doc):
    """Draw the entire cover page directly on canvas — no Platypus flowables."""
    canvas.saveState()

    # ── Background ────────────────────────────────────────────────────────
    canvas.setFillColor(_NAVY)
    canvas.rect(0, 0, PW, PH, fill=1, stroke=0)

    # ── Teal accent stripes ───────────────────────────────────────────────
    canvas.setFillColor(_TEAL)
    canvas.rect(0, PH - 10 * mm, PW, 10 * mm, fill=1, stroke=0)  # top
    canvas.rect(0, 0,            PW, 10 * mm, fill=1, stroke=0)  # bottom

    # ── Title — "RaptorDB Pro" ────────────────────────────────────────────
    y_title = PH * 0.50          # baseline at 50% up the page
    canvas.setFont("Helvetica-Bold", 38)
    canvas.setFillColor(colors.white)
    canvas.drawCentredString(PW / 2, y_title, "RaptorDB Pro")

    # ── Subtitle — below title by (font size × 1.4 + gap) ─────────────────
    y_sub = y_title - 54          # 38 pt cap + leading gap
    canvas.setFont("Helvetica", 22)
    canvas.setFillColor(_TEAL)
    canvas.drawCentredString(PW / 2, y_sub,
                             "Use Cases for POV and Readiness")

    # ── Thin separator line ───────────────────────────────────────────────
    y_hr = y_sub - 36
    canvas.setStrokeColor(colors.HexColor("#334155"))
    canvas.setLineWidth(0.8)
    canvas.line(PW * 0.33, y_hr, PW * 0.67, y_hr)

    # ── Metadata ──────────────────────────────────────────────────────────
    conn  = getattr(doc, "_conn_info", {}) or {}
    inst  = _pdf_safe(conn.get("instance_url", ""), 80)
    build = _pdf_safe(conn.get("build",        ""), 60)
    now   = datetime.now().strftime("%B %d, %Y")

    y_cur = y_hr - 32
    line_gap = 17

    canvas.setFont("Helvetica", 10)
    canvas.setFillColor(colors.HexColor("#94a3b8"))

    if inst and inst not in ("", "—"):
        canvas.drawCentredString(PW / 2, y_cur, inst)
        y_cur -= line_gap
    if build and build not in ("", "—"):
        canvas.drawCentredString(PW / 2, y_cur, f"Build: {build}")
        y_cur -= line_gap

    canvas.setFillColor(colors.HexColor("#cbd5e1"))
    canvas.drawCentredString(PW / 2, y_cur, now)
    y_cur -= 26

    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#475569"))
    canvas.drawCentredString(PW / 2, y_cur,
                             "RaptorDB Pro Readiness Analyzer")

    canvas.restoreState()


def _content_canvas(canvas, doc):
    """Navy header bar + teal bottom rule on every content page."""
    canvas.saveState()
    # Header bar
    canvas.setFillColor(_NAVY)
    canvas.rect(0, PH - _HEADER_H, PW, _HEADER_H, fill=1, stroke=0)
    canvas.setFillColor(colors.white)
    canvas.setFont("Helvetica", 7.5)
    canvas.drawString(_MARGIN, PH - _HEADER_H + 5 * mm,
                      "RaptorDB Pro  |  Readiness Report")
    canvas.drawRightString(PW - _MARGIN, PH - _HEADER_H + 5 * mm,
                           f"Page {doc.page}")
    # Bottom rule
    canvas.setStrokeColor(_TEAL)
    canvas.setLineWidth(1.5)
    canvas.line(_MARGIN, 1.3 * cm, PW - _MARGIN, 1.3 * cm)
    # Footer text
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(_DGRAY)
    canvas.drawString(_MARGIN, 0.7 * cm,
                      _pdf_safe(getattr(doc, "_instance", "")))
    canvas.drawRightString(PW - _MARGIN, 0.7 * cm,
                           datetime.now().strftime("%Y-%m-%d"))
    canvas.restoreState()


# =============================================================================
# Small reusable builders
# =============================================================================

def _section_head(text: str, S: Dict) -> List:
    """Teal left-border section heading."""
    inner = Table(
        [[Paragraph(_pdf_safe(text, 120), S["section"])]],
        colWidths=[_INNER_W],
        style=TableStyle([
            ("LINEBEFORE",    (0, 0), (-1, -1), 4, _TEAL),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]),
    )
    return [inner, Spacer(1, 4)]


def _metric_tiles(items: List[Tuple[str, str]], S: Dict) -> Table:
    """A row of metric tiles: (value, label) pairs."""
    n  = len(items)
    w  = _INNER_W / n
    r1 = [Paragraph(_pdf_safe(v, 20), S["metric_val"]) for v, _ in items]
    r2 = [Paragraph(_pdf_safe(l, 30), S["metric_lbl"]) for _, l in items]
    t  = Table([r1, r2], colWidths=[w] * n)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), _LGRAY),
        ("BOX",           (0, 0), (-1, -1), 0.4, _MGRAY),
        ("INNERGRID",     (0, 0), (-1, -1), 0.4, _MGRAY),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
    ]))
    return t


def _data_table(df: pd.DataFrame, cols: List[str],
                col_widths_cm: List[float], S: Dict) -> object:
    """Navy-header data table. Returns a Table or a caption Paragraph."""
    avail = [c for c in cols if c in df.columns]
    if df.empty or not avail:
        return Paragraph("No data available.", S["caption"])

    widths = [w * cm for w in col_widths_cm[: len(avail)]]
    data   = [[Paragraph(_pdf_safe(c, 40), S["th"]) for c in avail]]
    for _, row in df[avail].iterrows():
        data.append([
            Paragraph(_pdf_safe(v, 80), S["td"])
            for v in row
        ])
    t = Table(data, colWidths=widths, repeatRows=1, splitByRow=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",     (0, 0), (-1,  0), _NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _LGRAY]),
        ("GRID",           (0, 0), (-1, -1), 0.3, _MGRAY),
        ("TOPPADDING",     (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",  (0, 0), (-1, -1), 4),
        ("LEFTPADDING",    (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",   (0, 0), (-1, -1), 5),
        ("VALIGN",         (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def _use_case_card(rank: int, name: str, atype: str,
                   detail: str, why: str, improvement: str,
                   S: Dict) -> Table:
    """Ranked use-case card with teal badge."""
    rank_style = ParagraphStyle(
        "rk", fontName="Helvetica-Bold", fontSize=14,
        textColor=colors.white, alignment=TA_CENTER,
    )
    badge = Table(
        [[Paragraph(str(rank), rank_style)]],
        colWidths=[1.2 * cm], rowHeights=[1.2 * cm],
        style=TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), _TEAL),
            ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ]),
    )

    name_safe   = _pdf_safe(name,        80)
    detail_safe = _pdf_safe(detail,     100)
    why_safe    = _pdf_safe(why,        130)
    imp_safe    = _pdf_safe(improvement, 90)

    name_p = ParagraphStyle("cn", fontName="Helvetica-Bold", fontSize=10,
                            textColor=_NAVY, spaceAfter=3)
    det_p  = ParagraphStyle("cd", fontName="Helvetica",      fontSize=8,
                            textColor=_DGRAY, spaceAfter=3, leading=11)
    why_p  = ParagraphStyle("cw", fontName="Helvetica-Oblique", fontSize=8,
                            textColor=colors.HexColor("#374151"), spaceAfter=3, leading=11)
    imp_p  = ParagraphStyle("ci", fontName="Helvetica-Bold", fontSize=8,
                            textColor=_TEAL, leading=11)

    content_w = _INNER_W - 1.6 * cm
    content = [
        Paragraph(f"[{_pdf_safe(atype, 20)}]  {name_safe}", name_p),
        Paragraph(detail_safe, det_p),
        Paragraph(f"Why: {why_safe}", why_p),
        Paragraph(f"Expected: {imp_safe}", imp_p),
    ]

    card = Table(
        [[badge, content]],
        colWidths=[1.6 * cm, content_w],
        style=TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), _LGRAY),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING",    (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ("LEFTPADDING",   (1, 0), (1,  0),  10),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 7),
            ("BOX",           (0, 0), (-1, -1), 0.5, _MGRAY),
            ("LINEBEFORE",    (0, 0), (0, -1),  3,   _TEAL),
        ]),
    )
    return card




# =============================================================================
# Section builders
# =============================================================================

def _build_use_cases_page(shortlist: Dict, S: Dict) -> List:
    rep_df  = shortlist.get("reports",      pd.DataFrame())
    dash_df = shortlist.get("dashboards",   pd.DataFrame())
    slow_df = shortlist.get("slow_queries", pd.DataFrame())

    combined = []

    def _add(df, atype, name_col, detail_fn, why_col, imp_col, score_col):
        if df.empty:
            return
        for _, r in df.iterrows():
            combined.append({
                "score":  r.get(score_col, 0),
                "name":   str(r.get(name_col, "—")),
                "type":   atype,
                "detail": detail_fn(r),
                "why":    str(r.get(why_col, "")),
                "imp":    str(r.get(imp_col,  "")),
            })

    _add(rep_df, "Report",
         "Name",
         lambda r: (f"Table: {r.get('Table','—')} | Rows: {r.get('Rows','—')} | "
                    f"Type: {r.get('Type','—')} | Agg: {r.get('Aggregation','—')}"),
         "Why POV Candidate", "Expected Improvement", "POV Score")

    _add(dash_df, "PA Dashboard",
         "Name",
         lambda r: (f"Widgets: {r.get('Widgets','—')} | "
                    f"Indicators: {r.get('PA Indicators','—')} | "
                    f"Sources: {str(r.get('Source Tables','—'))[:50]}"),
         "Why POV Candidate", "Expected Improvement", "POV Score")

    _add(slow_df, "Slow Query",
         "URL Pattern",
         lambda r: (f"Table: {r.get('Inferred Table','—')} | "
                    f"Hits: {r.get('Hits (7d)','—')} | "
                    f"Avg: {r.get('Avg (ms)','—')} ms"),
         "Why POV Candidate", "Expected Improvement", "POV Score")

    combined.sort(key=lambda x: x["score"], reverse=True)
    top8 = combined[:8]

    story = _section_head("Top Targeted Use Cases for RaptorDB Pro PoV", S)
    story.append(Paragraph(
        "The following artifacts were identified from live instance data as the "
        "highest-value targets for the Proof of Value, ranked by composite score "
        "(table size, query complexity, aggregation pressure, slow-transaction frequency).",
        S["body"],
    ))
    story.append(Spacer(1, 0.3 * cm))

    if not top8:
        story.append(Paragraph(
            "No POV candidates scored. Ensure data was fully collected.", S["body"]))
        return story

    for i, c in enumerate(top8, 1):
        story.append(KeepTogether([
            _use_case_card(i, c["name"], c["type"],
                           c["detail"], c["why"], c["imp"], S),
            Spacer(1, 0.22 * cm),
        ]))
    return story


def _build_shortlist_tables(shortlist: Dict, S: Dict) -> List:
    rep_df  = shortlist.get("reports",      pd.DataFrame())
    dash_df = shortlist.get("dashboards",   pd.DataFrame())
    slow_df = shortlist.get("slow_queries", pd.DataFrame())

    story = _section_head("POV Shortlist — Detail Tables", S)

    if not rep_df.empty:
        story.append(Paragraph("Top Report Candidates", S["subsection"]))
        story.append(_data_table(
            rep_df,
            cols=["Rank", "Name", "Table", "Rows", "Type",
                  "Aggregation", "POV Score", "Effort"],
            col_widths_cm=[1.0, 4.0, 2.2, 1.8, 1.6, 2.2, 1.5, 3.1],
            S=S,
        ))
        story.append(Spacer(1, 0.4 * cm))

    if not dash_df.empty:
        story.append(Paragraph("Top Dashboard Candidates", S["subsection"]))
        story.append(_data_table(
            dash_df,
            cols=["Rank", "Name", "Widgets", "PA Indicators",
                  "Max Source Rows", "Indicators w/ Breakdown", "POV Score", "Effort"],
            col_widths_cm=[1.0, 3.6, 1.4, 1.8, 2.2, 2.7, 1.5, 3.2],
            S=S,
        ))
        story.append(Spacer(1, 0.4 * cm))

    if not slow_df.empty:
        story.append(Paragraph("Top Slow Query Candidates", S["subsection"]))
        story.append(_data_table(
            slow_df,
            cols=["Rank", "Inferred Table", "Table Rows",
                  "Hits (7d)", "Avg (ms)", "Impact Score", "POV Score", "Effort"],
            col_widths_cm=[1.0, 2.8, 2.0, 1.4, 1.6, 2.2, 1.5, 4.9],
            S=S,
        ))
    return story


def _build_executive_summary(results: Dict, issues: List[Dict],
                              props: Dict, S: Dict) -> List:
    def _fmt(n):
        try:    return f"{int(n):,}"
        except: return "N/A"

    rc_df   = results.get("core_table_row_counts")
    inv_df  = results.get("table_inventory")
    jobs_df = results.get("scheduled_jobs")
    pa_df   = results.get("pa_indicators")

    total_tables = len(inv_df)  if inv_df  is not None and not inv_df.empty  else 0
    total_rows   = (int(rc_df["row_count"].sum())
                    if rc_df is not None and not rc_df.empty
                       and "row_count" in rc_df.columns else 0)
    total_jobs   = len(jobs_df) if jobs_df is not None and not jobs_df.empty else 0
    total_pa     = len(pa_df)   if pa_df   is not None and not pa_df.empty   else 0

    story = _section_head("Executive Summary", S)
    story.append(_metric_tiles([
        (_fmt(total_tables), "Tables"),
        (_fmt(total_rows),   "Rows Sampled"),
        (_fmt(total_pa),     "PA Indicators"),
        (_fmt(total_jobs),   "Scheduled Jobs"),
        (str(len(issues)),   "Flagged Issues"),
    ], S))
    story.append(Spacer(1, 0.4 * cm))

    # Platform table
    story.append(Paragraph("Platform Details", S["subsection"]))
    build   = props.get("glide.buildtag", props.get("glide.buildname", "—"))
    db_type = props.get("glide.db.type", "") or props.get("glide.db.rdbms", "—")
    thr     = props.get("glide.db.slow_query_threshold", "—")
    plat_rows = [
        ["Build",                  _pdf_safe(build,    60)],
        ["Database Engine",        _pdf_safe(db_type,  40)],
        ["RaptorDB Pro enabled",   _pdf_safe(props.get("glide.raptordb.pro.enabled", "—"), 10)],
        ["RaptorDB (std) enabled", _pdf_safe(props.get("glide.raptordb.enabled",     "—"), 10)],
        ["Workflow Data Fabric",   _pdf_safe(props.get("sn_data_fabric.enabled",     "—"), 10)],
        ["Performance Analytics",  _pdf_safe(props.get("glide.pa.enabled",           "—"), 10)],
        ["Slow Query Threshold",   _pdf_safe(thr, 20) + " ms"],
    ]
    plat_data = [[Paragraph("Property", S["th"]), Paragraph("Value", S["th"])]]
    for k, v in plat_rows:
        plat_data.append([Paragraph(k, S["td"]), Paragraph(v, S["td"])])

    pt = Table(plat_data, colWidths=[5.5 * cm, _INNER_W - 5.5 * cm], splitByRow=1)
    pt.setStyle(TableStyle([
        ("BACKGROUND",     (0, 0), (-1,  0), _NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _LGRAY]),
        ("GRID",           (0, 0), (-1, -1), 0.3, _MGRAY),
        ("TOPPADDING",     (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",  (0, 0), (-1, -1), 5),
        ("LEFTPADDING",    (0, 0), (-1, -1), 7),
        ("VALIGN",         (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(pt)

    # Largest tables
    if rc_df is not None and not rc_df.empty:
        story.append(Spacer(1, 0.4 * cm))
        story.append(Paragraph("Largest Tables (by row count)", S["subsection"]))
        top = rc_df.head(10).copy()
        top["row_count"] = top["row_count"].apply(_fmt)
        story.append(_data_table(
            top,
            cols=["table_name", "row_count"],
            col_widths_cm=[9.5, 7.9],
            S=S,
        ))
    return story


def _build_findings_nextsteps(issues: List[Dict], props: Dict,
                               results: Dict, S: Dict) -> List:
    story = _section_head("Key Findings", S)
    story.append(Paragraph(
        "Critical and warning-level findings from the collected data. "
        "These are the highest-priority areas for RaptorDB Pro consideration.",
        S["body"],
    ))

    key = [i for i in issues if i["severity"] in ("CRITICAL", "WARNING")]
    if key:
        sev_styles = {
            "CRITICAL": ParagraphStyle("cs", fontName="Helvetica-Bold",
                                       fontSize=8, textColor=_RED),
            "WARNING":  ParagraphStyle("ws", fontName="Helvetica-Bold",
                                       fontSize=8, textColor=_AMBER),
        }
        rows = [[Paragraph(h, S["th"])
                 for h in ["Severity", "Category", "Finding"]]]
        for iss in key:
            detail = _pdf_safe(iss["detail"], 130)
            title  = _pdf_safe(iss["title"],  80)
            rdb    = _pdf_safe(iss.get("raptordb_relevance", ""), 80)
            body   = f"{title}. {detail}"
            if rdb:
                body += f" — RaptorDB Pro: {rdb}"
            rows.append([
                Paragraph(_pdf_safe(iss["severity"], 10),
                          sev_styles.get(iss["severity"], S["td"])),
                Paragraph(_pdf_safe(iss["category"], 30), S["td"]),
                Paragraph(body, S["td"]),
            ])
        t = Table(rows,
                  colWidths=[2.0 * cm, 3.0 * cm, _INNER_W - 5.0 * cm],
                  splitByRow=1)
        t.setStyle(TableStyle([
            ("BACKGROUND",     (0, 0), (-1,  0), _NAVY),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _LGRAY]),
            ("GRID",           (0, 0), (-1, -1), 0.3, _MGRAY),
            ("TOPPADDING",     (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING",  (0, 0), (-1, -1), 4),
            ("LEFTPADDING",    (0, 0), (-1, -1), 5),
            ("VALIGN",         (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(t)
    else:
        story.append(
            Paragraph("No critical or warning-level issues found.", S["body"]))

    story += [Spacer(1, 0.5 * cm)]
    story += _section_head("Next Steps", S)

    rdb_pro  = props.get("glide.raptordb.pro.enabled", "false")
    slow_thr = props.get("glide.db.slow_query_threshold", "")
    pa_on    = props.get("glide.pa.enabled", "false")

    steps = []
    if str(rdb_pro).lower() == "true":
        steps.append("RaptorDB Pro is already enabled — proceed directly to "
                     "benchmarking the top-scored artifacts in this report.")
    else:
        steps.append("Request RaptorDB Pro trial activation from your ServiceNow "
                     "account team, targeting the top-scored use cases in this report.")

    if not slow_thr or slow_thr in ("0", "false"):
        steps.append("Enable slow query logging: set glide.db.slow_query_threshold "
                     "to 2000 ms to capture more query patterns before the PoV.")
    else:
        steps.append(f"Current slow query threshold: {_pdf_safe(slow_thr, 10)} ms. "
                     "Consider lowering to 1000 ms to capture more patterns.")

    if str(pa_on).lower() == "true":
        steps.append("Identify the top 3-5 PA indicators on the largest source tables "
                     "as before/after test cases.")
    else:
        steps.append("Enable Performance Analytics and run an initial snapshot "
                     "to build a benchmarking baseline.")

    steps += [
        "Capture execution plans via sys_db_slow_query and syslog_transaction "
        "for the top 5 slow queries before the PoV.",
        "Schedule a PoV kick-off: share this report with the ServiceNow RaptorDB "
        "Pro team and agree on 3 benchmark scenarios and success criteria.",
        "Quantify business impact: estimate daily user count and query frequency "
        "per demo scenario to translate speedup into hours saved per day.",
    ]
    for step in steps:
        story.append(Paragraph(f"  -  {step}", S["bullet"]))

    story += [
        Spacer(1, 1.2 * cm),
        HRFlowable(width="100%", thickness=0.4, color=_MGRAY),
        Spacer(1, 0.3 * cm),
        Paragraph(
            f"Generated by RaptorDB Pro Readiness Analyzer  -  "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M')}",
            S["footer"],
        ),
    ]
    return story


# =============================================================================
# Public API
# =============================================================================

def generate_pdf_report(
    results:   Dict,
    issues:    List[Dict],
    shortlist: Dict,
    conn_info: Dict = None,
) -> bytes:
    """
    Build the full PDF report and return it as bytes.
    Suitable for a Streamlit st.download_button data= argument.
    """
    S = _S()

    # Platform properties
    props: Dict[str, str] = {}
    props_df = results.get("system_properties")
    if props_df is not None and not props_df.empty and "name" in props_df.columns:
        props = dict(zip(props_df["name"].astype(str),
                         props_df["value"].astype(str)))

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=_MARGIN,
        rightMargin=_MARGIN,
        topMargin=_TOP_MARGIN,
        bottomMargin=_BOTTOM_MARGIN,
        title="RaptorDB Pro — Use Cases for POV and Readiness",
        author="RaptorDB Pro Readiness Analyzer",
    )
    doc._instance  = _pdf_safe((conn_info or {}).get("instance_url", ""), 80)
    doc._conn_info = conn_info or {}

    story = []

    # ── Page 1: Cover — drawn entirely by _cover_canvas; only a token
    #    Spacer+PageBreak here so Platypus creates the page cleanly.
    story += [Spacer(1, 1), PageBreak()]

    # ── Page 2: Top Targeted Use Cases ────────────────────────────────────
    story += _build_use_cases_page(shortlist, S)
    story.append(PageBreak())

    # ── Page 3: POV Shortlist Tables ──────────────────────────────────────
    story += _build_shortlist_tables(shortlist, S)
    story.append(PageBreak())

    # ── Page 4: Executive Summary ─────────────────────────────────────────
    story += _build_executive_summary(results, issues, props, S)
    story.append(PageBreak())

    # ── Page 5: Key Findings + Next Steps ────────────────────────────────
    story += _build_findings_nextsteps(issues, props, results, S)

    doc.build(story, onFirstPage=_cover_canvas, onLaterPages=_content_canvas)
    buf.seek(0)
    return buf.getvalue()
