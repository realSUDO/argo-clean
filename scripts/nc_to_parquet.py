from netCDF4 import Dataset
import pandas as pd
import numpy as np
import os
from glob import glob

# -------------------------------
# CONFIG: Local directory containing .nc files
# -------------------------------
NC_DIR = os.path.join(os.getcwd(), "argo_nc_files_requests")  # change if needed
OUTPUT_DIR = os.path.join(os.getcwd(), "argo_parquet")        # directory to save CSV + Parquet

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------
# Function to convert single .nc to CSV + Parquet
# -------------------------------
def nc_to_parquet(nc_file, out_dir=OUTPUT_DIR, verbose=True):
    ds = Dataset(nc_file)
    
    # Pull variables
    pres = ds.variables["PRES"][:]
    temp = ds.variables["TEMP"][:]
    psal = ds.variables["PSAL"][:]
    lat = ds.variables["LATITUDE"][:]
    lon = ds.variables["LONGITUDE"][:]
    time = ds.variables["JULD"][:]
    
    # Fill masked arrays
    def fill_var(v):
        return v.filled(np.nan) if hasattr(v, "filled") else v
    
    pres, temp, psal = fill_var(pres), fill_var(temp), fill_var(psal)
    
    # Handle multi-profile
    if pres.ndim == 2:
        n_levels, n_profiles = pres.shape
        df_list = []
        for i in range(n_profiles):
            df_temp = pd.DataFrame({
                "depth": pres[:, i],
                "temp": temp[:, i] if temp.ndim > 1 else temp,
                "sal": psal[:, i] if psal.ndim > 1 else psal,
                "lat": lat[i] if lat.size > 1 else lat[0],
                "lon": lon[i] if lon.size > 1 else lon[0],
                "time": time[i] if time.size > 1 else time[0]
            })
            df_list.append(df_temp)
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = pd.DataFrame({
            "depth": pres,
            "temp": temp,
            "sal": psal,
            "lat": lat[0] if lat.size == 1 else lat,
            "lon": lon[0] if lon.size == 1 else lon,
            "time": time[0] if time.size == 1 else time
        })
    
    # Generate output filenames
    base_name = os.path.splitext(os.path.basename(nc_file))[0]
    csv_file = os.path.join(out_dir, f"{base_name}.csv")
    parquet_file = os.path.join(out_dir, f"{base_name}.parquet")
    
    # Save
    df.to_csv(csv_file, index=False)
    df.to_parquet(parquet_file, index=False)
    
    if verbose:
        print(f"Saved CSV: {csv_file}")
        print(f"Saved Parquet: {parquet_file}")
        print("Shape:", df.shape)
    
    return df

# -------------------------------
# LOOP over all .nc files in NC_DIR
# -------------------------------
nc_files = glob(os.path.join(NC_DIR, "*.nc"))

for nc_file in nc_files:
    print(f"\nProcessing: {nc_file}")
    df = nc_to_parquet(nc_file)

