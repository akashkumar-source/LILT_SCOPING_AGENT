Lilt Scoping Agent - Production Deployment

This service runs the scoping analysis engine on Google Cloud Run. it processes documents, queries BigQuery, and uses GPT-4o to estimate project complexity and requirements.

Deployment Process

The repository is configured to deploy via GitHub Actions. **You must manually trigger the workflow** from the "Actions" tab (select "Deploy to Cloud Run" -> "Run workflow") to start the deployment.

GitHub Secrets Setup
For the deployment to work, you must add the following secrets in the repository settings (Settings > Secrets and variables > Actions):

1. OPENAI_API_KEY: Your OpenAI API key for processing.
2. INPUT_BUCKET: The name of the GCS bucket where input files are stored (e.g., scoping-input-dev).
3. OUTPUT_BUCKET: The name of the GCS bucket where reports will be saved (e.g., scoping-output-dev).
4. LOG_SHEET_ID: (Optional) The ID of the Google Sheet for logging.

API Integration

The service provides a POST endpoint at /scoping/run for the Apps Script UI to call.

Input Payload Format:
{
  "job_ids": "1446777",
  "gcs_input_path": "gs://your-input-bucket/pm-uploads/job_abc/",
  "instructions": "Any specific project notes",
  "translator_pct": 0.6,
  "reviewer_pct": 0.3,
  "pm_pct": 0.1
}

The service will process the files and return signed URLs for the generated JSON and CSV reports.

Local Development

To run the app locally for testing, make sure you have the required packages installed and your environment variables set in a .env file.

Commands:
pip install -r requirements.txt
python app.py
