import psycopg2
import time

# Connect to your PostgreSQL database
print("🔌 Connecting to PostgreSQL database...")
conn = psycopg2.connect(
    host="localhost",
    database="ocean",
    user="multiply",
    password="unlocked",
    port="5432"
)
cursor = conn.cursor()
print("✅ Connected to database successfully!")

# Get ALL unique float_id_precise values from the table
print("\n📋 Fetching ALL unique float IDs from argo_meta table...")
cursor.execute("SELECT DISTINCT float_id_precise FROM argo_meta WHERE float_id_precise IS NOT NULL ORDER BY float_id_precise")
all_floats = [row[0] for row in cursor.fetchall()]

print(f"📊 Found {len(all_floats)} unique float IDs to process")

# Drop table if it exists and create sample table with only summary column
print("\n🗑️  Dropping existing argo_summary_big table if it exists...")
cursor.execute("DROP TABLE IF EXISTS argo_summary_big")

print("📦 Creating new argo_summary_big table...")
cursor.execute("""
CREATE TABLE argo_summary_big (
    float_id_precise TEXT PRIMARY KEY,
    summary TEXT
)
""")
print("✅ Table argo_summary_big created successfully!")

# Counter for progress tracking
processed_count = 0
success_count = 0
error_count = 0
start_time_total = time.time()

print(f"\n🔄 Starting processing of {len(all_floats)} floats...")
print("=" * 80)

# Process each float
for fid in all_floats:
    processed_count += 1
    if processed_count % 100 == 0:  # Show progress every 100 floats
        print(f"\n[{processed_count}/{len(all_floats)}] Processing float: {fid}")
    
    try:
        # Query to get metadata for this float
        query = """
        SELECT 
            MIN(time) as start_time,
            MAX(time) as end_time,
            MIN(depth) as min_depth,
            MAX(depth) as max_depth,
            AVG(temp) as avg_temp,
            AVG(sal) as avg_sal,
            AVG(lat) as avg_lat,
            AVG(lon) as avg_lon
        FROM argo_meta
        WHERE float_id_precise = %s
        """
        
        cursor.execute(query, (fid,))
        result = cursor.fetchone()
        
        if not result or all(x is None for x in result):
            if processed_count % 100 == 0:
                print(f"   ⚠️  No data found for Float {fid}")
            error_count += 1
            continue
            
        # Unpack the result
        (start_time, end_time, min_depth, max_depth, avg_temp, avg_sal, avg_lat, avg_lon) = result
        
        # Format values for summary
        start_time_str = start_time.strftime("%Y-%m-%d") if start_time else "unknown"
        end_time_str = end_time.strftime("%Y-%m-%d") if end_time else "unknown"
        min_depth_str = f"{min_depth:.1f}" if min_depth is not None else "unknown"
        max_depth_str = f"{max_depth:.1f}" if max_depth is not None else "unknown"
        avg_temp_str = f"{avg_temp:.2f}" if avg_temp is not None else "unknown"
        avg_sal_str = f"{avg_sal:.2f}" if avg_sal is not None else "unknown"
        avg_lat_str = f"{avg_lat:.2f}" if avg_lat is not None else "unknown"
        avg_lon_str = f"{avg_lon:.2f}" if avg_lon is not None else "unknown"
        
        # Create summary text
        summary = (
            f"Float {fid} operated from {start_time_str} to {end_time_str} "
            f"near ({avg_lat_str}°N, {avg_lon_str}°E). "
            f"Profiles range {min_depth_str}–{max_depth_str} m, "
            f"with mean temperature {avg_temp_str} °C and "
            f"mean salinity {avg_sal_str} PSU."
        )

        # Insert into sample table (only float_id_precise and summary)
        cursor.execute(
            "INSERT INTO argo_summary_big (float_id_precise, summary) VALUES (%s, %s)", 
            (fid, summary)
        )
        
        success_count += 1
        
        # Show detailed info only occasionally to avoid too much output
        if processed_count % 100 == 0:
            print(f"   ✅ Successfully processed float {fid}")
            print(f"   📍 Location: ({avg_lat_str}°N, {avg_lon_str}°E)")
            print(f"   📅 Period: {start_time_str} to {end_time_str}")
        
        # Commit every 1000 records to avoid large transactions
        if processed_count % 1000 == 0:
            conn.commit()
            print(f"   💾 Committed transaction after {processed_count} floats")
            
    except Exception as e:
        error_count += 1
        if processed_count % 100 == 0:
            print(f"   ❌ Error processing float {fid}: {str(e)}")

# Final commit
print("\n💾 Final commit...")
conn.commit()

# Calculate total time
end_time_total = time.time()
total_time = end_time_total - start_time_total

print("\n" + "=" * 80)
print("📊 PROCESSING COMPLETE - SUMMARY")
print("=" * 80)
print(f"Total floats found: {len(all_floats)}")
print(f"Successfully processed: {success_count}")
print(f"Errors: {error_count}")
print(f"Total time: {total_time:.2f} seconds")
print(f"Average time per float: {total_time/len(all_floats):.3f} seconds")

# Final verification
print("\n🔍 Verifying results...")
cursor.execute("SELECT COUNT(*) FROM argo_summary_big")
sample_count = cursor.fetchone()[0]
print(f"Entries in argo_summary_big table: {sample_count}")

# Show sample results
cursor.execute("SELECT float_id_precise, summary FROM argo_summary_big LIMIT 5")
sample_results = cursor.fetchall()

print("\n📋 Sample entries:")
print("float_id_precise | summary")
print("------------------+-----------------------------------------------------------------------------")
for fid, summary in sample_results:
    print(f"{fid:<16} | {summary}")

# Close connection
conn.close()
print("\n✅ Script completed successfully!")
print(f"🎉 argo_summary_big table created with {sample_count} floats!")
