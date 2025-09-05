import sqlite3
import pandas as pd
from pathlib import Path
import argparse


def load_and_analyze(db_path: Path):
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)

    # Database structure
    print("📊 Database Structure:")
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table'", conn
    )
    print(f"Tables: {tables['name'].tolist()}")

    for table in tables["name"]:
        info = pd.read_sql_query(f"PRAGMA table_info({table})", conn)
        print(f"\n{table} columns: {info['name'].tolist()}")

    # Load and analyze data
    df = pd.read_sql_query(f"SELECT * FROM {tables['name'][0]}", conn)
    conn.close()

    print(f"\n🌊 {tables['name'][0]} Summary")
    print("=" * 40)
    print(f"Total records: {len(df):,}")

    # Clean float_id if it exists
    if "float_id" in df.columns and df["float_id"].dtype == "object":
        df["float_id"] = df["float_id"].astype(str).str.replace(r"[\[\]b\'\s-]", "", regex=True)

    # Dynamic range printing
    for col, name in [("depth", "Depth (m)"), ("temp", "Temperature (°C)"), ("sal", "Salinity"), 
                      ("lat", "Latitude"), ("lon", "Longitude")]:
        if col in df.columns:
            print(f"{name}: {df[col].min():.2f} - {df[col].max():.2f}")

    if "float_id" in df.columns:
        print(f"Unique floats: {df['float_id'].nunique()}")

    # Data completeness dynamically
    print(f"\n📊 Data Completeness:")
    for col in df.columns:
        non_null = df[col].notna().sum()
        pct = (non_null / len(df)) * 100
        print(f"{col:20}: {pct:5.1f}% ({non_null}/{len(df)})")

    # Show sample data dynamically
    sample_cols = [c for c in ["depth", "temp", "sal", "lat", "lon", "float_id"] if c in df.columns]
    print(f"\n🔬 Sample Data:")
    print(df[sample_cols].head(3).to_string(index=False))

    # Float distribution if float_id exists
    if "float_id" in df.columns:
        print(f"\n🏊 Float Distribution:")
        for float_id, count in df["float_id"].value_counts().head().items():
            print(f"Float {float_id}: {count} profiles")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize and analyze ARGO SQLite database"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parent / "argo_profile.db",
        help="Path to the SQLite database (default: SQL-DB/argo_profile.db)",
    )
    args = parser.parse_args()

    load_and_analyze(args.db)


if __name__ == "__main__":
    main()

