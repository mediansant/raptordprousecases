# RaptorDB Pro Readiness Analyzer

A Streamlit application that connects to a ServiceNow instance via REST API, collects performance-relevant data, scores and ranks RaptorDB Pro use cases, generates a POV shortlist, and produces customer-ready PDF and Word reports — with an optional Claude prompt for AI-assisted analysis.

---

## Quick Start

### Option A — Command Line (local Python)

**Prerequisites:** Python 3.9+

```bash
# Clone the repo
git clone <your-repo-url>
cd Ratordbanalysisdata

# Create a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py
```

Opens at **http://localhost:8501**

---

### Option B — Docker

**Prerequisites:** Docker 20+ and Docker Compose v2+

```bash
# Clone the repo
git clone <your-repo-url>
cd Ratordbanalysisdata

# Build and start
docker compose up -d --build

# View logs
docker compose logs -f

# Stop
docker compose down
```

Opens at **http://localhost:8510**

> Port 8510 is used by default to avoid clashing with a locally running Streamlit.
> To change it, edit the `ports` line in `docker-compose.yml`.

#### One-liner (no compose)

```bash
docker build -t raptordb-analyzer .
docker run -d -p 8510:8501 -v $(pwd)/exports:/app/exports raptordb-analyzer
```

#### Useful Docker commands

```bash
# Check container status
docker ps

# Tail logs
docker logs -f raptordb-analyzer

# Rebuild after code changes
docker compose up -d --build

# Remove container and image
docker compose down --rmi local
```

---

## How It Works

### 1. Connect via REST API

Enter your ServiceNow instance URL (e.g. `https://acme.service-now.com`), admin username, and password in the sidebar. The app uses the ServiceNow Table API and Stats API — no direct database access required.

**Uncheck "Verify SSL"** for sub-prod instances with self-signed certificates.

### 2. Choose Analysis Period

Select how far back to look for slow transaction data:

| Option | Days of history |
|---|---|
| 7 days | 7 |
| 15 days | 15 |
| 1 month | 30 |
| 3 months | 90 |
| 6 months | 180 |
| 1 year | 365 |

### 3. Data Collection (10 Categories)

| Category | API | What's Collected |
|---|---|---|
| System Properties | `sys_properties` | DB type, build, RaptorDB/WDF status, PA config |
| Table Inventory | `sys_db_object` | All tables, labels, hierarchy, extensions |
| Core Table Row Counts | Stats API (40+ tables) | Row counts for task, incident, CMDB, audit, PA, syslog etc. |
| Field & Index Definitions | `sys_dictionary`, `sys_index` | Indexed fields, composite indexes, field types |
| Report Inventory | `sys_report` | Report definitions, target tables, aggregation types |
| PA Configuration | `pa_indicators`, `pa_dashboards`, `pa_widgets` | Active indicators, dashboards, source tables |
| CMDB Profile | Stats API → `cmdb_ci` | CI class distribution, relationship counts |
| Slow Transactions | `syslog_transaction` | Slow queries (>5s) for the selected analysis period |
| Scheduled Jobs | `sysauto` | Active background jobs |
| Table Rotation | `sys_table_rotation_schedule` | Rotation and archiving rules |

### 4. Results Tabs

| Tab | Description |
|---|---|
| 🏆 Top Use Cases | Top 10 RaptorDB Pro candidates ranked by composite score |
| 🎯 POV Shortlist | Exact reports, dashboards, and slow queries for the Proof of Value |
| 📝 Readiness Report | Full rule-based readiness report (no LLM required) |

### 5. Sidebar Tools

| Button | What it does |
|---|---|
| 📊 Reports & PA | Report-by-table charts, PA indicator breakdown, slow transaction summary |
| 🤖 Claude Prompt & Export | Token-optimised prompt to paste into Claude + export file list |
| 📄 Generate PDF Report | Customer-ready 5-page PDF starting with the top POV use cases |

### 6. Exports

Every collection writes a timestamped folder (default: `./exports/raptordb_export_YYYYMMDD_HHMMSS/`):

```
exports/raptordb_export_YYYYMMDD_HHMMSS/
├── SUMMARY.txt                    ← Human-readable overview
├── collection_metadata.json       ← Run metadata
├── flagged_issues.json / .csv     ← All flagged issues
├── system_properties.csv
├── table_inventory.csv
├── core_table_row_counts.csv
├── indexed_fields.csv
├── composite_indexes.csv
├── reports.csv / report_table_summary.csv
├── pa_indicators.csv / pa_dashboards.csv / pa_table_summary.csv
├── cmdb_ci_classes.csv / cmdb_summary.csv
├── slow_transactions.csv / slow_transaction_summary.csv
├── scheduled_jobs.csv
└── table_rotation.csv
```

When running via Docker, exports are written to `./exports/` on the **host machine** (bind-mounted volume).

---

## Using the Output

### Option A — Use directly

The scored use cases, POV shortlist, readiness report, and PDF are ready to present to a customer or use for demo preparation without any additional tooling.

### Option B — Upload to Claude

1. Click **🤖 Claude Prompt & Export** in the sidebar
2. Download the generated prompt (`.txt`)
3. Open [claude.ai](https://claude.ai) and paste the prompt
4. Claude produces a richer, AI-authored readiness narrative with deeper recommendations

---

## Architecture

```
app.py            Streamlit UI — sidebar, tabs, sidebar panels
sn_client.py      ServiceNow REST client — auth, pagination, 429 handling
collector.py      Data collection — what to pull and how
analyzer.py       Issue detection + composite use-case scoring
report_engine.py  Built-in Markdown report generator + .docx export
pov_selector.py   POV candidate scoring — exact reports, dashboards, slow queries
pdf_report.py     ReportLab PDF generator — 5-page customer-ready report
```

---

## Requirements

| Requirement | Detail |
|---|---|
| Python | 3.9+ (3.11 recommended) |
| ServiceNow access | Admin credentials — read-only GET calls only |
| Network | Access to the instance REST API from the machine running the app |
| Docker (optional) | Docker 20+ and Docker Compose v2+ |

---

## Notes

- **Read-only:** All calls are GET requests via Table API / Stats API — no writes, no scripts, no side effects
- **Safe for production:** Can be run against a production instance without risk
- **Rate-limit aware:** Built-in delays and automatic 429 retry with back-off
- **Sub-prod friendly:** SSL verification toggle for self-signed certificates
- **Offline PDF:** PDF and .docx generation happens entirely locally — no external services
