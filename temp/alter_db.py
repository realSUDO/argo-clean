import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# Connect to your SQLite database
conn = sqlite3.connect("SQL-DB/argo_profile.db")
cursor = conn.cursor()

# # Test different time conversions for one float
# test_query = """
# SELECT 
#     time,
#     time / 86400.0 as days_value,
#     datetime(time, 'unixepoch') as unix_epoch,
#     datetime(time + 978307200, 'unixepoch') as since_2001,  -- Mac OS X reference
#     datetime(time + 631152000, 'unixepoch') as since_1990   -- Common scientific reference
# FROM argo_measurements 
# WHERE float_id_clean = 'D2902120' 
# LIMIT 5
# """
#
# test_df = pd.read_sql(test_query, conn)
# print("Testing time conversions for D2902120:")
# print(test_df)
# print("\n")
#
# # Let's also check what the actual time range looks like
# range_query = """
# SELECT 
#     MIN(time) as min_time,
#     MAX(time) as max_time,
#     MAX(time) - MIN(time) as time_span
# FROM argo_measurements 
# WHERE float_id_clean = 'D2902120'
# """
#
# range_df = pd.read_sql(range_query, conn)
# print("Time range for D2902120:")
# print(range_df)
#
# Check if time values might be seconds since 1970 but with different scaling
check_query = """
SELECT 
    time,
    datetime(time, 'unixepoch') as direct_unix,
    datetime(time / 1000, 'unixepoch') as milliseconds,
    datetime(time / 1000000, 'unixepoch') as microseconds
FROM argo_measurements 
WHERE float_id_clean = 'D2902120'
LIMIT 5
"""

check_df = pd.read_sql(check_query, conn)
print("Testing different Unix epoch scalings:")
print(check_df)
