import netCDF4 as nc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from netCDF4 import num2date
import numpy as np

def test_time_conversion(nc_file_path):
    """Test time conversion on a single NetCDF file"""
    
    print(f"🔍 Testing: {os.path.basename(nc_file_path)}")
    
    try:
        # Open NetCDF file
        nc_dataset = nc.Dataset(nc_file_path, 'r')
        
        # Check what time variables are available
        print("Available variables:", list(nc_dataset.variables.keys()))
        
        # Check for JULD variable (Argo standard time variable)
        juld_var = nc_dataset.variables.get('JULD')
        if juld_var is not None:
            print(f"📊 JULD variable info:")
            print(f"   Shape: {juld_var.shape}")
            print(f"   Units: {getattr(juld_var, 'units', 'No units attribute')}")
            
            # Get raw JULD data
            juld_data = juld_var[:]
            print(f"   Raw JULD values: {juld_data}")
            
            # Convert to proper datetime
            if hasattr(juld_var, 'units'):
                print("🔄 Converting JULD using num2date...")
                datetime_data = num2date(juld_data, juld_var.units)
                print(f"   Converted dates: {datetime_data}")
                
                # Convert cftime to pandas Timestamp
                timestamps = []
                for dt in datetime_data:
                    # Convert cftime to regular datetime
                    regular_dt = pd.Timestamp(year=dt.year, month=dt.month, day=dt.day, 
                                            hour=dt.hour, minute=dt.minute, second=dt.second)
                    timestamps.append(regular_dt)
                print(f"   Pandas timestamps: {timestamps}")
                
            else:
                print("⚠️ No units attribute for JULD")
                
        else:
            print("❌ No JULD variable found either")
            
        nc_dataset.close()
        
    except Exception as e:
        print(f"❌ Error processing {nc_file_path}: {e}")
        import traceback
        traceback.print_exc()

def fix_and_convert_single_file(nc_file_path, output_path):
    """Fix time conversion and create corrected parquet file"""
    
    print(f"\n🛠️ Fixing: {os.path.basename(nc_file_path)}")
    
    try:
        nc_dataset = nc.Dataset(nc_file_path, 'r')
        
        # Create DataFrame from NetCDF data
        df = pd.DataFrame()
        
        # Get PRES (pressure) which determines the profile depth levels
        pres_var = nc_dataset.variables.get('PRES')
        if pres_var is not None:
            print(f"📏 PRES shape: {pres_var.shape}")
            # Take the first profile if it's 2D (N_PROF, N_LEVELS)
            if len(pres_var.shape) == 2:
                df['depth'] = pres_var[0, :]  # First profile's depth levels
            else:
                df['depth'] = pres_var[:]  # 1D array
        
        # Get temperature data
        temp_var = nc_dataset.variables.get('TEMP')
        if temp_var is not None:
            print(f"🌡️ TEMP shape: {temp_var.shape}")
            if len(temp_var.shape) == 2:
                df['temp'] = temp_var[0, :]  # First profile
            else:
                df['temp'] = temp_var[:]
        
        # Get salinity data
        psal_var = nc_dataset.variables.get('PSAL')
        if psal_var is not None:
            print(f"🧂 PSAL shape: {psal_var.shape}")
            if len(psal_var.shape) == 2:
                df['sal'] = psal_var[0, :]  # First profile
            else:
                df['sal'] = psal_var[:]
        
        # Handle JULD time conversion properly
        juld_var = nc_dataset.variables.get('JULD')
        if juld_var is not None:
            juld_data = juld_var[:]
            
            if hasattr(juld_var, 'units'):
                datetime_data = num2date(juld_data, juld_var.units)
                # Convert cftime to pandas Timestamp
                if len(datetime_data) > 0:
                    dt = datetime_data[0]
                    timestamp = pd.Timestamp(year=dt.year, month=dt.month, day=dt.day,
                                           hour=dt.hour, minute=dt.minute, second=dt.second)
                    df['time'] = timestamp
                    print(f"📅 Measurement time: {timestamp}")
        
        # Handle latitude and longitude (single values per profile)
        lat_var = nc_dataset.variables.get('LATITUDE')
        lon_var = nc_dataset.variables.get('LONGITUDE')
        if lat_var is not None and len(lat_var) > 0:
            df['lat'] = lat_var[0]  # Single latitude for the profile
            print(f"📍 Latitude: {lat_var[0]}")
        if lon_var is not None and len(lon_var) > 0:
            df['lon'] = lon_var[0]  # Single longitude for the profile
            print(f"📍 Longitude: {lon_var[0]}")
        
        # Add source file info and float ID
        df['source_file'] = os.path.basename(nc_file_path)
        
        # Extract float ID from platform number - FIXED FOR BYTES
        platform_var = nc_dataset.variables.get('PLATFORM_NUMBER')
        if platform_var is not None:
            # Handle platform number stored as bytes/strings
            platform_data = platform_var[:]
            
            if platform_data.dtype.kind in ['S', 'U']:  # String or unicode
                # Convert bytes to string
                if platform_data.dtype.kind == 'S':
                    platform_str = platform_data[0].decode('utf-8').strip() if len(platform_data.shape) == 2 else platform_data.decode('utf-8').strip()
                else:
                    platform_str = platform_data[0].strip() if len(platform_data.shape) == 2 else platform_data.strip()
            else:
                # Handle numeric array
                if len(platform_data.shape) == 2:
                    platform_str = ''.join([chr(int(x)) for x in platform_data[0] if x != 0])
                else:
                    platform_str = ''.join([chr(int(x)) for x in platform_data if x != 0])
            
            df['float_id_clean'] = platform_str
            print(f"🆔 Float ID: '{platform_str}'")
        else:
            # Fallback: use filename
            float_id = os.path.basename(nc_file_path).split('_')[0]
            df['float_id_clean'] = float_id
            print(f"🆔 Float ID (from filename): {float_id}")
        
        nc_dataset.close()
        
        print(f"📊 Final DataFrame shape: {df.shape}")
        print("First few rows:")
        print(df.head())
        
        # Convert to parquet
        table = pa.Table.from_pandas(df)
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        pq.write_table(table, output_path)
        
        print(f"✅ Created corrected parquet: {output_path}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error fixing {nc_file_path}: {e}")
        import traceback
        traceback.print_exc()
        return None

# Usage example
if __name__ == "__main__":
    # Test on a single file
    test_file = "argo_nc_files_requests/D2902120_260.nc"  # Use your actual file path
    
    # First test the time conversion
    test_time_conversion(test_file)
    
    # Then fix and create corrected parquet with relative path
    output_path = "scripts/sampleparquet.parquet"  # Save in scripts directory
    
    fixed_df = fix_and_convert_single_file(test_file, output_path)
    
    if fixed_df is not None:
        print("\n🎉 Test completed successfully!")
        
        # Show the absolute path for clarity
        abs_output_path = os.path.abspath(output_path)
        print(f"\n📁 Output file saved at: {abs_output_path}")
