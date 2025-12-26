import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# ==================== CREDENTIALS & SECRETS ====================
# OpenAI API Key - Must be set via environment variable for security
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# GCP Project ID for BigQuery
PROJECT_ID = os.getenv("PROJECT_ID", "arched-champion-847")
# Path to service account JSON (optional, for local Docker testing)
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

# ==================== GCS CONFIGURATION ====================
INPUT_BUCKET = os.getenv("INPUT_BUCKET", "agent-input-files")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "agent-output-files")
GCP_SERVICE_ACCOUNT_EMAIL = os.getenv("GCP_SERVICE_ACCOUNT_EMAIL", "")

# Production Logging (Aditya's requirement)
LOG_SHEET_ID = os.getenv("LOG_SHEET_ID", "1_Fm0-jS8i9bK-unrTsIvXVEagMvn6K8EnAqo9AoFtbY") 
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL", "")

# ==================== APPLICATION CONSTANTS ====================
NON_TRANSLATABLE_PATTERNS = [
    r"do not translate",
    r"do not locali[sz]e",
    r"not for localization",
    r"not for localisation",
    r"not for translation"
]

BENCHMARK_FILE_ID = "1PytrQkMHYCLcnLn0w8DH0A79Ymy3CyIj"
BENCHMARK_FILENAME = "benchmark_df.parquet"

# ==================== PATH CONFIGURATION ====================
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

# Shared directory for persistent data (logs/outputs)
DATA_BASE_DIR = os.getenv("DATA_DIR", BASE_DIR)
LOG_DIR = os.getenv("LOG_DIR", os.path.join(DATA_BASE_DIR, "logs"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(DATA_BASE_DIR, "outputs"))

BENCHMARK_LOCAL_PATH = os.getenv("BENCHMARK_PATH", os.path.join(DATA_BASE_DIR, BENCHMARK_FILENAME))
FALLBACK_SLA_PATH = os.path.join(BASE_DIR, "resources", "assignment_turn_around_times.json")

# ==================== BUSINESS LOGIC DEFAULTS ====================
DEFAULT_FALLBACK_TAT_RULES = [
    {"wordVolumeMin": 0, "wordVolumeMax": 500, "hoursUntilDue": 24},
    {"wordVolumeMin": 501, "wordVolumeMax": 2500, "hoursUntilDue": 48},
    {"wordVolumeMin": 2501, "wordVolumeMax": 5000, "hoursUntilDue": 72},
    {"wordVolumeMin": 5001, "wordVolumeMax": 10000, "hoursUntilDue": 96},
    {"wordVolumeMin": 10001, "wordVolumeMax": -1, "hoursUntilDue": 120},
]
