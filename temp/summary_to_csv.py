import psycopg2

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="ocean",
    user="multiply",
    password="unlocked",
    port="5432"
)
cursor = conn.cursor()

# Export to CSV using COPY command
print("📤 Exporting argo_summary_big table to CSV using COPY...")
copy_sql = """
COPY argo_summary_big TO STDOUT WITH CSV HEADER
"""
with open('argo_summary_export.csv', 'w', encoding='utf-8') as f:
    cursor.copy_expert(copy_sql, f)

print("✅ Exported argo_summary table to argo_summary_big_export.csv")

# Close connection
conn.close()
