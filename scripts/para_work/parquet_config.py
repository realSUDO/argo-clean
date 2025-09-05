import datetime
import json
import os
import threading

# -------------------------------
# CONFIG PATHS (relative to root directory)
# -------------------------------
CONFIG_FILE = "scripts/config.json"
NC_DIR = "argo_nc_files_requests"
OUTPUT_DIR = "argo_parquet"
LOG_DIR = "logs"
MAX_WORKERS = 200

# Ensure we're in root directory before creating directories
current_dir = os.getcwd()
if os.path.basename(current_dir) == "scripts":
    os.chdir("..")

# Create directories in root
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Load variable config
with open(CONFIG_FILE, "r") as f:
    VAR_CONFIG = json.load(f)

ESSENTIAL_KEYS = VAR_CONFIG.get("essential", [])
RESEARCH_KEYS = VAR_CONFIG.get("research", [])

# Per-run log file
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"run_{timestamp}.txt")

# Thread-safe counters
counter_lock = threading.Lock()
success, warnings, fails = 0, 0, 0
processed_files = []
warning_files = []
