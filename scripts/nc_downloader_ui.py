import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import calendar

# -------------------------------
# CONFIG
# -------------------------------
CSV_FILE = "global_csvs/argo_index_last3yrs.csv"
BASE_DOWNLOAD_DIR = Path("argo_nc_files_requests")
BASE_DOWNLOAD_DIR.mkdir(exist_ok=True)

# -------------------------------
# Ask user for year/month range safely
# -------------------------------
def get_input(prompt, min_val, max_val):
    while True:
        try:
            val = int(input(prompt))
            if val < min_val or val > max_val:
                print(f"Enter a value between {min_val} and {max_val}.")
                continue
            return val
        except ValueError:
            print("Invalid input. Enter an integer.")

start_year = get_input("Enter start year (2020-2025): ", 2020, 2025)
end_year = get_input("Enter end year (2020-2025): ", 2020, 2025)
if end_year < start_year:
    start_year, end_year = end_year, start_year

start_month = get_input("Enter start month (1-12): ", 1, 12)
end_month = get_input("Enter end month (1-12): ", 1, 12)
if end_month < start_month:
    start_month, end_month = end_month, start_month

# -------------------------------
# Load CSV
# -------------------------------
try:
    df = pd.read_csv(CSV_FILE)
    # Use date_update instead of date
    df["date_update"] = pd.to_datetime(df["date_update"], format="%Y%m%d%H%M%S", errors="coerce")
except Exception as e:
    print(f"❌ Failed to read CSV file: {e}")
    exit(1)

# -------------------------------
# Filter by year/month
# -------------------------------
filtered_df = df[
    (df["date_update"].dt.year >= start_year) &
    (df["date_update"].dt.year <= end_year) &
    (df["date_update"].dt.month >= start_month) &
    (df["date_update"].dt.month <= end_month)
]

if filtered_df.empty:
    print("⚠️ No data available for the selected year/month range. Try changing CSV file.")
else:
    print(f"Total files to download in range: {len(filtered_df)}")

# -------------------------------
# Download loop with folders: year/month/day
# -------------------------------
for idx, row in filtered_df.iterrows():
    file_path = row["file"]
    url = f"http://data-argo.ifremer.fr/dac/{file_path}"
    
    try:
        file_date = row["date_update"]
        year_folder = BASE_DOWNLOAD_DIR / str(file_date.year)
        month_name = calendar.month_name[file_date.month]
        month_folder = year_folder / month_name
        day_folder = month_folder / f"{file_date.day:02d}"
        day_folder.mkdir(parents=True, exist_ok=True)
        
        local_file = day_folder / Path(file_path).name
        if local_file.exists():
            print(f"✅ Already exists: {local_file}")
            continue

        print(f"Downloading ({idx+1}/{len(filtered_df)}): {file_path}")
        try:
            with requests.get(url, stream=True, allow_redirects=True, timeout=60) as r:
                r.raise_for_status()
                with open(local_file, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            print(f"✅ Downloaded: {local_file}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to download: {file_path} : {e}")

    except Exception as e:
        print(f"⚠️ Skipping file {file_path} due to unexpected error: {e}")

