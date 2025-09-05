import numpy as np
import datetime
from .parquet_config import counter_lock, LOG_FILE

def log_message(level, msg):
    """Write structured log message to file"""
    line = f"[{datetime.datetime.now()}] [{level}] {msg}\n"
    with counter_lock:
        with open(LOG_FILE, "a") as f:
            f.write(line)

def normalize_value(val):
    if isinstance(val, (bytes, np.bytes_)):
        try:
            return val.decode("utf-8").strip()
        except:
            return str(val)
    return val

def safe_array(var, default=np.array([np.nan])):
    """Safe wrapper for NetCDF variables"""
    if var is None:
        return np.atleast_1d(default)

    if hasattr(var, "filled"):
        arr = var.filled(np.nan)
    elif hasattr(var, "mask"):
        arr = np.array(var[:]).filled(np.nan)
    else:
        arr = np.array(var)

    arr = np.atleast_1d(arr)
    return np.array([normalize_value(x) for x in arr])

def align_shapes(arrays):
    """Pad arrays with NaN so they all match the same length"""
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
