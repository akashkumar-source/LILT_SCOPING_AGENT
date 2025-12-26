# Lilt Scoping Agent

## Status
Live (Deployment Ready)

## Purpose
Automated scoping engine that analyzes source files to estimate translation project complexity, turnaround time (TAT), and resource requirements.

## What It Does
- Ingests documents from Google Cloud Storage (GCS)
- Extracts text and metadata from various formats (Parquet, DOCX, PPTX, XLSX, PDF)
- Queries BigQuery for historical Lilt project data and SLAs
- Runs AI Parsing (GPT-4o) to determine complexity, domain, and linguistic requirements
- Generates Reports:
  - Detailed Project Summary (CSV)
  - PM Planning Summary (CSV)
  - Full Analysis JSON
- Logs Activity to Google Sheets

## Entry Point
- API Endpoint: POST /scoping/run (Cloud Run)
- Frontend: Google Apps Script Web UI (Integration Pending)

## Processing Layer
Google Cloud Run (Service: scoping-agent)

## Outputs
- Google Sheets: Centralized Job Status Log
- GCS Artifacts:
  - detailed_project_summary.csv
  - pm_planning_summary.csv
  - document_analysis_output.json
- Email Notifications (If configured)

## Integrations
- Google Cloud Storage (GCS): File Inputs/Outputs
- BigQuery: Historical Data & SLA Lookup
- OpenAI (GPT-4o): Complexity Analysis
- Google Sheets: Logging
- Google Cloud Run: Serverless Execution Environment

## Repo
https://github.com/lilt/lilt-scoping-agent (Placeholder)

## Docs
(Link to Notion documentation)

## Owner
AGNT & DAT Team

---

# Technical Implementation

## Deployment Model
- Runtime: Google Cloud Run (Python / FastAPI)
- Authentication: Workload Identity Federation (Keyless)
- CI/CD: GitHub Actions (Manual Trigger via workflow_dispatch)

## GitHub Secrets Setup
To deploy, the following secrets must be configured in the repo:
1. OPENAI_API_KEY: Your OpenAI API key.
2. INPUT_BUCKET: GCS bucket for inputs (e.g., agent-input-files).
3. OUTPUT_BUCKET: GCS bucket for results (e.g., agent-output-files).
4. LOG_SHEET_ID: (Optional) ID of the Google Sheet for logging.

## API Usage
**Endpoint**: POST /scoping/run

**Payload Example**:
```json
{
  "job_ids": "1446777",
  "gcs_input_path": "gs://agent-input-files/job_123/",
  "instructions": "Focus on marketing tone",
  "translator_pct": 0.6,
  "reviewer_pct": 0.3,
  "pm_pct": 0.1
}
```

## Local Development
To run locally:
1. pip install -r requirements.txt
2. python app.py (Runs on localhost:8000)
