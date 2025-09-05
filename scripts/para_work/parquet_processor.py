import os
import uuid
import pandas as pd
import numpy as np
from netCDF4 import Dataset
from .parquet_config import (
    counter_lock, success, fails, processed_files, warning_files,
    ESSENTIAL_KEYS, RESEARCH_KEYS, OUTPUT_DIR
)
from .parquet_utils import (
    log_message, get_var, normalize_value, align_shapes, pad_array
)

def process_nc_file(nc_file_data):
    """Process a single NetCDF file"""
    nc_file, selected_files_len = nc_file_data
    global success, fails, processed_files, warning_files
    
    try:
        # Check if file exists and has content
        if not os.path.exists(nc_file) or os.path.getsize(nc_file) == 0:
            with counter_lock:
                fails += 1
                print(f"❌ [{fails} failed] {os.path.basename(nc_file)}: File empty or missing")
            return False
            
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
        csv_file = os.path.join(OUTPUT_DIR, f"{base_name}.csv")
        parquet_file = os.path.join(OUTPUT_DIR, f"{base_name}.parquet")
        df.to_csv(csv_file, index=False)
        df.to_parquet(parquet_file, index=False)

        if missing_vars:
            log_message(
                "WARN",
                f"[{file_id}] {os.path.basename(nc_file)} missing: {', '.join(missing_vars)}",
            )
            with counter_lock:
                warning_files.append(f"{os.path.basename(nc_file)} (missing vars)")

        log_message(
            "INFO", f"[{file_id}] ({success+1}/{selected_files_len}) Processed {os.path.basename(nc_file)} | Shape={df.shape}"
        )

        with counter_lock:
            success += 1
            print(f"✅ [{success}/{selected_files_len}] {os.path.basename(nc_file)} | Shape={df.shape}")
            processed_files.append(f"({success}/{selected_files_len}) {os.path.basename(nc_file)} | Shape={df.shape}")

        return True

    except Exception as e:
        file_id = str(uuid.uuid4())[:8]
        with counter_lock:
            fails += 1
            print(f"❌ [{fails} failed] {os.path.basename(nc_file)}: {str(e)[:50]}...")
        log_message("ERROR", f"[{file_id}] Failed {os.path.basename(nc_file)}: {e}")
        log_message("ERROR", f"[{file_id}] Full path: {nc_file}")
        return False
