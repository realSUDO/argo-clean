import datetime
import json
import os
import uuid
from glob import glob

import numpy as np
import pandas as pd
from netCDF4 import Dataset

# -------------------------------
# CONFIG PATH
# -------------------------------
CONFIG_FILE = "scripts/config.json"
NC_DIR = os.path.join(os.getcwd(), "argo_nc_files_requests")
OUTPUT_DIR = os.path.join(os.getcwd(), "argo_parquet")
LOG_DIR = os.path.join(os.getcwd(), "logs")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# -------------------------------
# Load variable config
# -------------------------------
with open(CONFIG_FILE, "r") as f:
    VAR_CONFIG = json.load(f)

ESSENTIAL_KEYS = VAR_CONFIG.get("essential", [])
RESEARCH_KEYS = VAR_CONFIG.get("research", [])
ALL_KEYS = ESSENTIAL_KEYS + RESEARCH_KEYS

# -------------------------------
# Per-run log file
# -------------------------------
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"run_{timestamp}.txt")


def log_message(level, msg):
    """Write structured log message to file"""
    line = f"[{datetime.datetime.now()}] [{level}] {msg}\n"
    with open(LOG_FILE, "a") as f:
        f.write(line)


# -------------------------------
# Utility: safe normalization
# -------------------------------
def normalize_value(val):
    if isinstance(val, (bytes, np.bytes_)):
        try:
            return val.decode("utf-8").strip()
        except:
            return str(val)
    return val


def safe_array(var, default=np.array([np.nan])):
    """
    Safe wrapper for NetCDF variables.
    - Scalars promoted to 1D
    - Masked arrays filled with NaN
    - Normalization applied element-wise
    """
    if var is None:
        return np.atleast_1d(default)

    if hasattr(var, "filled"):
        arr = var.filled(np.nan)
    elif hasattr(var, "mask"):  # masked_array
        arr = np.array(var[:]).filled(np.nan)
    else:
        arr = np.array(var)

    arr = np.atleast_1d(arr)
    return np.array([normalize_value(x) for x in arr])


# -------------------------------
# Shape normalizer
# -------------------------------
def align_shapes(arrays):
    """Pad arrays with NaN so they all match the same length (per profile)."""
    max_len = max(a.shape[0] for a in arrays if a.ndim > 0)
    padded = []
    for a in arrays:
        if a.ndim == 0:
            padded.append(np.full(max_len, np.nan))
        elif a.shape[0] < max_len:
            pad_width = ((0, max_len - a.shape[0]),) + ((0, 0),) * (a.ndim - 1)
            padded.append(np.pad(a, pad_width, constant_values=np.nan))
        else:
            padded.append(a)
    return padded


# -------------------------------
# Safe getter
# -------------------------------
def get_var(ds, key, missing_vars):
    if key in ds.variables:
        try:
            arr = ds.variables[key][:]
            return safe_array(arr)
        except Exception as e:
            missing_vars.append(f"{key} (error: {e})")
            return np.array([np.nan])
    else:
        missing_vars.append(key)
        return np.array([np.nan])


# -------------------------------
# Convert one .nc → DataFrame
# -------------------------------
def nc_to_parquet(nc_file, out_dir=OUTPUT_DIR, verbose=True):
    ds = Dataset(nc_file)
    file_id = str(uuid.uuid4())[:8]
    missing_vars = []

    # Core essential vars
    core_data = {key.lower(): get_var(ds, key, missing_vars) for key in ESSENTIAL_KEYS}

    # Extra research vars
    research_data = {
        key.lower(): get_var(ds, key, missing_vars) for key in RESEARCH_KEYS
    }

    # Float ID
    float_id = "UNKNOWN"
    if "PLATFORM_NUMBER" in ds.variables:
        raw = ds.variables["PLATFORM_NUMBER"][:]
        try:
            float_id = (
                "".join(map(chr, raw[:, 0])).strip() if raw.ndim == 2 else str(raw)
            )
        except Exception:
            float_id = str(normalize_value(raw))

    pres = core_data.get("pres", np.array([np.nan]))
    temp = core_data.get("temp", np.array([np.nan]))
    psal = core_data.get("psal", np.array([np.nan]))
    lat = core_data.get("latitude", np.array([np.nan]))
    lon = core_data.get("longitude", np.array([np.nan]))
    time = core_data.get("juld", np.array([np.nan]))

    def safe_slice(arr, i):
        """Safely extract column i, padding with NaN if needed"""
        if arr.ndim == 1:
            return arr
        if i >= arr.shape[1]:
            return np.full(arr.shape[0], np.nan)
        return arr[:, i]

    def pad_array(arr, length):
        if arr is None:
            return np.full(length, np.nan)
        arr = np.array(arr)
        if arr.ndim == 0:
            arr = np.array([arr])
        if len(arr) < length:
            pad = np.full(length, np.nan)
            pad[:len(arr)] = arr
            return pad
        return arr[:length]

    df_list = []
    if pres.ndim == 2:
        n_levels, n_profiles = pres.shape
        for i in range(n_profiles):
            # Safe indexing to prevent out-of-bounds
            depth, t, s = align_shapes([
                safe_slice(pres, i),
                safe_slice(temp, i) if temp.ndim > 1 else temp,
                safe_slice(psal, i) if psal.ndim > 1 else psal,
            ])

            data = {
                "depth": depth,
                "temp": t,
                "sal": s,
                "lat": lat[i] if lat.size > i else (lat[0] if lat.size > 0 else np.nan),
                "lon": lon[i] if lon.size > i else (lon[0] if lon.size > 0 else np.nan),
                "time": time[i] if time.size > i else (time[0] if time.size > 0 else np.nan),
                "float_id": float_id,
            }
            
            for k, v in research_data.items():
                if v.ndim > 1:
                    data[k] = safe_slice(v, i)
                elif v.ndim == 1:
                    data[k] = v
                else:
                    data[k] = np.nan

            max_len = max(len(np.atleast_1d(v)) for v in data.values() if v is not None)
            
            for k in list(data.keys()):
                if k in ["lat", "lon", "time", "float_id"]:
                    data[k] = np.full(max_len, data[k])
                else:
                    data[k] = pad_array(data[k], max_len)

            df_list.append(pd.DataFrame(data))
        df = pd.concat(df_list, ignore_index=True)

    else:
        data = {
            "depth": pres,
            "temp": temp,
            "sal": psal,
            "lat": lat[0] if lat.size > 0 else np.nan,
            "lon": lon[0] if lon.size > 0 else np.nan,
            "time": time[0] if time.size > 0 else np.nan,
            "float_id": float_id,
        }
        
        for k, v in research_data.items():
            data[k] = v if v.ndim == 1 else np.nan

        max_len = max(len(np.atleast_1d(v)) for v in data.values() if v is not None)
        
        for k in list(data.keys()):
            if k in ["lat", "lon", "time", "float_id"]:
                data[k] = np.full(max_len, data[k])
            else:
                data[k] = pad_array(data[k], max_len)

        df = pd.DataFrame(data)

    df.dropna(how="all", inplace=True)

    base_name = os.path.splitext(os.path.basename(nc_file))[0]
    csv_file = os.path.join(out_dir, f"{base_name}.csv")
    parquet_file = os.path.join(out_dir, f"{base_name}.parquet")
    df.to_csv(csv_file, index=False)
    df.to_parquet(parquet_file, index=False)

    if missing_vars:
        log_message(
            "WARN",
            f"[{file_id}] {os.path.basename(nc_file)} missing: {', '.join(missing_vars)}",
        )
        print(
            f"⚠️ {os.path.basename(nc_file)}: Missing some variables (see {LOG_FILE}, ID={file_id})"
        )

    log_message(
        "INFO", f"[{file_id}] Processed {os.path.basename(nc_file)} | Shape={df.shape}"
    )

    if verbose:
        print(f"✅ Saved {csv_file} & {parquet_file} | Shape={df.shape}")

    return df


# -------------------------------
# LOOP & SUMMARY
# -------------------------------
nc_files = glob(os.path.join(NC_DIR, "*.nc"))
success, warnings, fails = 0, 0, 0

for nc_file in nc_files:
    print(f"\nProcessing: {nc_file}")
    try:
        df = nc_to_parquet(nc_file)
        success += 1
    except Exception as e:
        file_id = str(uuid.uuid4())[:8]
        print(f"❌ Failed {nc_file} (ID={file_id}): {e}")
        log_message("ERROR", f"[{file_id}] {os.path.basename(nc_file)}: {e}")
        fails += 1

with open(LOG_FILE, "a") as f:
    f.write("\n--- SUMMARY ---\n")
    f.write(f"✅ Success: {success}\n")
    f.write(f"⚠️ Warnings: {warnings} (check logs)\n")
    f.write(f"❌ Failed: {fails}\n")

print("\n=== FINAL SUMMARY ===")
print(f"✅ Success: {success}")
print(f"⚠️ Warnings: {warnings} (see {LOG_FILE})")
print(f"❌ Failed: {fails}")
