import pandas as pd
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import os

# Config (relative to root directory)
CSV_FILE = "global_csvs/argo_index_last3yrs.csv"
DOWNLOAD_DIR = Path("argo_nc_files_requests")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Auto-detect optimal workers
MAX_WORKERS = min(1000, (os.cpu_count() or 1) * 50)  # 50x CPU cores, capped at 1000
print(f"Using {MAX_WORKERS} workers (CPU cores: {os.cpu_count()})")

# Thread-safe counter
counter_lock = threading.Lock()
downloaded_count = 0
total_files = 0

def download_file(row_data):
    global downloaded_count
    idx, file_path = row_data
    url = f"http://data-argo.ifremer.fr/dac/{file_path}"
    local_file = DOWNLOAD_DIR / Path(file_path).name

    if local_file.exists():
        with counter_lock:
            print(f"✅ Already exists: {local_file}")
        return True

    try:
        with requests.get(url, stream=True, allow_redirects=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        with counter_lock:
            downloaded_count += 1
            print(f"✅ Downloaded ({downloaded_count}/{total_files}): {file_path}")
        return True
    except Exception as e:
        with counter_lock:
            print(f"❌ Failed: {file_path} : {e}")
        return False

# Load CSV and prepare data
df = pd.read_csv(CSV_FILE)
total_files = len(df)
print(f"Starting concurrent download of {total_files} files with {MAX_WORKERS} workers...")

# Create list of (index, file_path) tuples
download_tasks = [(idx, row["file"]) for idx, row in df.iterrows()]

# Download with ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    results = list(executor.map(download_file, download_tasks))

print(f"Download complete! Success: {sum(results)}/{total_files}")

