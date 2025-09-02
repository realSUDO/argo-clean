import pandas as pd
import requests
from pathlib import Path

# Config
CSV_FILE = "global_csvs/argo_index_last3yrs.csv"  # Change to your CSV file
DOWNLOAD_DIR = Path("argo_nc_files_requests")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Load CSV
df = pd.read_csv(CSV_FILE)
print(f"Total files to download: {len(df)}")

# Download loop
for idx, row in df.iterrows():
    file_path = row["file"]
    url = f"http://data-argo.ifremer.fr/dac/{file_path}"
    local_file = DOWNLOAD_DIR / Path(file_path).name

    if local_file.exists():
        print(f"✅ Already exists: {local_file}")
        continue

    print(f"Downloading ({idx+1}/{len(df)}): {file_path}")
    try:
        with requests.get(url, stream=True, allow_redirects=True, timeout=60) as r:
            r.raise_for_status()  # Raise error for HTTP issues
            with open(local_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"✅ Downloaded: {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to download: {file_path} : {e}")

