# RaptorDB Pro Readiness Analyzer — CLI Tool

A standalone command-line tool that connects to a ServiceNow instance, collects
performance-relevant data, and generates a **RaptorDB Pro Readiness PDF report**.

No browser, no Streamlit, no UI required — runs entirely from the terminal and
can be executed on any customer machine.

---

## What the tool does

1. Connects to your ServiceNow instance using the REST API
2. Collects table inventory, row counts, index definitions, Performance Analytics
   configuration, CMDB profile, slow transaction history, reports, and system
   properties
3. Analyses the collected data and flags issues relevant to a RaptorDB Pro migration
4. Scores and ranks use-case candidates for a Proof of Value
5. Generates a formatted PDF report ready to share with stakeholders

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Python** | Version 3.9 or newer |
| **Network access** | Must be able to reach the ServiceNow instance URL |
| **ServiceNow role** | The user account needs **admin** or **rest\_api\_explorer** role |

---

## Installation — macOS

### Step 1 — Check Python

Open **Terminal** (Applications → Utilities → Terminal) and run:

```bash
python3 --version
```

You should see `Python 3.9.x` or higher. If not, install Python from
[https://www.python.org/downloads/](https://www.python.org/downloads/)
and re-open Terminal.

### Step 2 — Navigate to the tool folder

```bash
cd /path/to/cli_tool
```

Replace `/path/to/cli_tool` with the actual folder location, for example:

```bash
cd ~/Downloads/cli_tool
```

### Step 3 — Create a virtual environment

```bash
python3 -m venv .venv
```

### Step 4 — Activate the virtual environment

```bash
source .venv/bin/activate
```

Your prompt will change to show `(.venv)` — this means the environment is active.

### Step 5 — Install dependencies

```bash
pip install -r requirements.txt
```

Wait for all packages to download and install. This only needs to be done once.

### Step 6 — Run the tool

```bash
python generate_report.py
```

The tool will prompt you for the instance URL, username, and password.

---

## Installation — Windows

### Step 1 — Check Python

Open **Command Prompt** (press `Win + R`, type `cmd`, press Enter) and run:

```cmd
python --version
```

You should see `Python 3.9.x` or higher. If you see an error or an older version:

1. Go to [https://www.python.org/downloads/](https://www.python.org/downloads/)
2. Download the latest Python 3.x installer
3. Run the installer — **check the box "Add Python to PATH"** before clicking Install
4. Close and re-open Command Prompt

### Step 2 — Navigate to the tool folder

```cmd
cd C:\path\to\cli_tool
```

For example, if you extracted the tool to your Downloads folder:

```cmd
cd %USERPROFILE%\Downloads\cli_tool
```

### Step 3 — Create a virtual environment

```cmd
python -m venv .venv
```

### Step 4 — Activate the virtual environment

```cmd
.venv\Scripts\activate
```

Your prompt will change to show `(.venv)` — this means the environment is active.

> **Troubleshooting:** If you see a security error about running scripts, run this
> command first, then try activating again:
> ```cmd
> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
> ```
> (This only applies if you are using PowerShell instead of Command Prompt.)

### Step 5 — Install dependencies

```cmd
pip install -r requirements.txt
```

Wait for all packages to download and install. This only needs to be done once.

### Step 6 — Run the tool

```cmd
python generate_report.py
```

The tool will prompt you for the instance URL, username, and password.

---

## Usage

### Interactive mode (recommended)

Simply run the script and follow the prompts:

```bash
python generate_report.py
```

```
ServiceNow instance URL: https://mycompany.service-now.com
Username: admin
Password:
```

### Command-line arguments

Pass all values directly without prompts:

```bash
python generate_report.py \
  --url  https://mycompany.service-now.com \
  --user admin \
  --password mypassword
```

On Windows use `^` instead of `\` to continue a command on the next line,
or put everything on one line:

```cmd
python generate_report.py --url https://mycompany.service-now.com --user admin --password mypassword
```

### Environment variables (keeps credentials out of shell history)

**macOS / Linux:**
```bash
export SN_INSTANCE_URL=https://mycompany.service-now.com
export SN_USERNAME=admin
export SN_PASSWORD=mypassword
python generate_report.py
```

**Windows Command Prompt:**
```cmd
set SN_INSTANCE_URL=https://mycompany.service-now.com
set SN_USERNAME=admin
set SN_PASSWORD=mypassword
python generate_report.py
```

### All options

| Option | Short | Default | Description |
|---|---|---|---|
| `--url` | `-u` | prompt | ServiceNow instance URL |
| `--user` | `-U` | prompt | ServiceNow username |
| `--password` | `-p` | prompt (hidden) | ServiceNow password |
| `--days` | `-d` | `7` | Days of slow-transaction history to analyse |
| `--output` | `-o` | auto-named | Output PDF file path |
| `--export-csv` | | off | Also save raw data as CSV files |
| `--claude` | | off | Generate Claude AI prompt package (skips the prompt) |
| `--no-claude` | | off | Skip the Claude export prompt entirely |
| `--no-ssl-verify` | | off | Disable SSL check (sub-prod instances) |
| `--timeout` | | `30` | HTTP request timeout in seconds |

### Examples

```bash
# Analyse the last 14 days of slow transactions
python generate_report.py --url https://mycompany.service-now.com \
  --user admin --days 14

# Save the PDF to a specific location and also export raw CSV data
python generate_report.py --url https://mycompany.service-now.com \
  --user admin --output /tmp/customer_report.pdf --export-csv

# Sub-production instance with a self-signed certificate
python generate_report.py --url https://mycompany-dev.service-now.com \
  --user admin --no-ssl-verify

# Increase timeout for slow instances
python generate_report.py --url https://mycompany.service-now.com \
  --user admin --timeout 60
```

---

## Claude AI Export (optional)

After the PDF is generated, the tool asks:

```
Generate Claude export package? [y/n]:
```

If you answer **y**, it creates a `claude_export_<timestamp>/` folder containing:

- **`claude_prompt.txt`** — a fully structured prompt with all instance data embedded
- **CSV files** — the key data sets Claude needs to reference specific numbers
- **`INSTRUCTIONS.txt`** — step-by-step guide on how to use the files

**How to use it with Claude:**
1. Go to [claude.ai](https://claude.ai) and start a new conversation
2. Upload all the CSV files from the `claude_export_` folder as attachments
3. Copy and paste the full contents of `claude_prompt.txt` as your message
4. Claude generates a rich, data-specific readiness report referencing actual table names, row counts, and query patterns from your instance

You can also trigger this without the prompt using `--claude`, or skip it entirely with `--no-claude`.

---

## Output

| File / Folder | Description |
|---|---|
| `RaptorDB_Readiness_<timestamp>.pdf` | Full readiness report (always generated) |
| `raptordb_export_<timestamp>/` | Raw CSV data + `flagged_issues.json` (with `--export-csv`) |
| `claude_export_<timestamp>/` | Claude prompt + supporting CSVs + instructions (optional) |

The PDF is saved in the **same folder where you run the script** unless you
specify a path with `--output`.

---

## Deactivating the virtual environment

When you are finished, you can deactivate the environment:

```bash
deactivate
```

To run the tool again in a future session, just re-activate the environment
(Steps 4 and 6 above) — you do not need to reinstall dependencies.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `python3: command not found` (macOS) | Install Python from python.org |
| `'python' is not recognized` (Windows) | Reinstall Python and check "Add to PATH" |
| `pip install` fails with SSL error | Try `pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt` |
| `Authentication failed` | Verify the username and password; ensure the user has admin or rest\_api\_explorer role |
| `Cannot reach instance` | Check VPN, firewall, or proxy settings; verify the instance URL is correct |
| `Access denied (403)` | The user account lacks the required ServiceNow role |
| PDF is blank or incomplete | Try `--timeout 60` — some large instances are slow to respond |
