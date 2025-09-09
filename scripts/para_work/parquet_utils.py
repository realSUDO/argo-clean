import netCDF4 as nc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from netCDF4 import num2date
import numpy as np
import datetime

def log_message(level, message):
    """Log message with timestamp"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")

def get_var(ds, var_name, missing_vars):
    """Extract variable from NetCDF dataset with proper handling"""
    if var_name in ds.variables:
        var_data = ds.variables[var_name][:]
        # Handle masked arrays
        if hasattr(var_data, 'mask'):
            var_data = np.ma.filled(var_data, np.nan)
        return var_data
    else:
        missing_vars.append(var_name)
        return np.array([np.nan])

def normalize_value(value):
    """Normalize a value to a standard format"""
    if hasattr(value, 'mask'):
        value = np.ma.filled(value, np.nan)
    if isinstance(value, np.ndarray):
        if value.size == 1:
            return float(value.item()) if not np.isnan(value.item()) else np.nan
        return value
    return value

def align_shapes(arrays):
    """Align array shapes to the same length"""
    max_len = max(len(np.atleast_1d(arr)) for arr in arrays if arr is not None)
    aligned = []
    for arr in arrays:
        if arr is None:
            aligned.append(np.full(max_len, np.nan))
        else:
            arr = np.atleast_1d(arr)
            if len(arr) < max_len:
                padded = np.full(max_len, np.nan)
                padded[:len(arr)] = arr
                aligned.append(padded)
            else:
                aligned.append(arr[:max_len])
    return aligned

def pad_array(arr, target_len):
    """Pad array to target length with NaN"""
    if arr is None:
        return np.full(target_len, np.nan)
    arr = np.atleast_1d(arr)
    if len(arr) >= target_len:
        return arr[:target_len]
    padded = np.full(target_len, np.nan)
    padded[:len(arr)] = arr
    return padded

def convert_juld_to_datetime(juld_var):
    """Convert JULD variable to proper datetime using num2date"""
    if juld_var is None:
        return np.array([np.nan])
    
    juld_data = juld_var[:]
    
    # Handle masked arrays
    if hasattr(juld_data, 'mask'):
        juld_data = np.ma.filled(juld_data, np.nan)
    
    # Check if variable has units attribute for proper conversion
    if hasattr(juld_var, 'units'):
        try:
            # Convert using num2date
            datetime_data = num2date(juld_data, juld_var.units)
            
            # Convert to pandas Timestamps
            timestamps = []
            if np.isscalar(datetime_data):
                datetime_data = [datetime_data]
            
            for dt in datetime_data:
                if hasattr(dt, 'year'):  # Valid datetime object
                    timestamp = pd.Timestamp(year=dt.year, month=dt.month, day=dt.day,
                                           hour=dt.hour, minute=dt.minute, second=dt.second)
                    timestamps.append(timestamp)
                else:
                    timestamps.append(pd.NaT)
            
            return np.array(timestamps)
        except Exception as e:
            log_message("WARN", f"Failed to convert JULD time: {e}")
            return juld_data
    else:
        log_message("WARN", "JULD variable missing units attribute")
        return juld_data

def convert_nc_to_parquet(nc_file_path, output_path):
    """Convert NetCDF file to Parquet with proper time conversion"""
    
    print(f"Processing: {os.path.basename(nc_file_path)}")
    
    try:
        nc_dataset = nc.Dataset(nc_file_path, 'r')
        df = pd.DataFrame()
        
        # Get depth data
        pres_var = nc_dataset.variables.get('PRES')
        if pres_var is not None:
            if len(pres_var.shape) == 2:
                df['depth'] = pres_var[0, :]  # First profile's depth levels
            else:
                df['depth'] = pres_var[:]
        
        # Get temperature data
        temp_var = nc_dataset.variables.get('TEMP')
        if temp_var is not None:
            if len(temp_var.shape) == 2:
                df['temp'] = temp_var[0, :]
            else:
                df['temp'] = temp_var[:]
        
        # Get salinity data
        psal_var = nc_dataset.variables.get('PSAL')
        if psal_var is not None:
            if len(psal_var.shape) == 2:
                df['sal'] = psal_var[0, :]
            else:
                df['sal'] = psal_var[:]
        
        # Convert JULD time to proper datetime - THIS IS THE KEY PART!
        juld_var = nc_dataset.variables.get('JULD')
        if juld_var is not None and hasattr(juld_var, 'units'):
            juld_data = juld_var[:]
            datetime_data = num2date(juld_data, juld_var.units)
            if len(datetime_data) > 0:
                dt = datetime_data[0]
                timestamp = pd.Timestamp(year=dt.year, month=dt.month, day=dt.day,
                                       hour=dt.hour, minute=dt.minute, second=dt.second)
                df['time'] = timestamp
                print(f"✅ Measurement time: {timestamp}")
        
        # Get location data
        lat_var = nc_dataset.variables.get('LATITUDE')
        lon_var = nc_dataset.variables.get('LONGITUDE')
        if lat_var is not None and len(lat_var) > 0:
            df['lat'] = lat_var[0]
        if lon_var is not None and len(lon_var) > 0:
            df['lon'] = lon_var[0]
        
        # Add metadata
        df['source_file'] = os.path.basename(nc_file_path)
        
        # SIMPLIFIED float ID extraction - skip complex handling
        df['float_id_clean'] = os.path.basename(nc_file_path).split('_')[0]
        print(f"✅ Float ID: {df['float_id_clean'].iloc[0]}")
        
        nc_dataset.close()
        
        # Save as parquet
        table = pa.Table.from_pandas(df)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        pq.write_table(table, output_path)
        
        print(f"✅ Created: {output_path}")
        print(f"📊 Rows: {len(df)}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_file = "argo_nc_files_requests/D2902120_260.nc"
    output_path = "scripts/sampleparquet.parquet"
    
    result = convert_nc_to_parquet(test_file, output_path)
    
    if result is not None:
        print(f"\n🎉 Success! File saved to: {os.path.abspath(output_path)}")
        print("\nFirst few rows:")
        print(result[['time', 'depth', 'temp', 'sal']].head() if 'time' in result.columns else result.head())
