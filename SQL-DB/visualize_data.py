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
    df = pd.read_sql_query("SELECT * FROM profiles", conn)
    conn.close()

    print(f"\n🌊 ARGO Profiles Summary")
    print("=" * 40)
    print(f"Total records: {len(df):,}")

    # Clean float_id
    if df["float_id"].dtype == "object":
        df["float_id"] = (
            df["float_id"]
            .astype(str)
            .str.replace(r"[\[\]b\'\s-]", "", regex=True)
        )

    print(f"\n📈 Data Ranges:")
    print(f"Depth: {df['depth'].min():.1f} - {df['depth'].max():.1f} m")
    print(f"Temperature: {df['temp'].min():.2f} - {df['temp'].max():.2f} °C")
    print(f"Salinity: {df['sal'].min():.2f} - {df['sal'].max():.2f}")
    print(f"Latitude: {df['lat'].min():.2f}° - {df['lat'].max():.2f}°")
    print(f"Longitude: {df['lon'].min():.2f}° - {df['lon'].max():.2f}°")
    print(f"Unique floats: {df['float_id'].nunique()}")

    print(f"\n📊 Data Completeness:")
    for col in df.columns:
        non_null = df[col].notna().sum()
        pct = (non_null / len(df)) * 100
        print(f"{col:15}: {pct:5.1f}% ({non_null}/{len(df)})")

    print(f"\n🔬 Sample Data:")
    sample = df.head(3)[["depth", "temp", "sal", "lat", "lon", "float_id"]]
    print(sample.to_string(index=False))

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
        default=Path(__file__).resolve().parent.parent / "SQL-DB" / "argo_profiles.db",
        help="Path to the SQLite database (default: repo_root/SQL-DB/argo_profiles.db)",
    )
    args = parser.parse_args()

    load_and_analyze(args.db)


if __name__ == "__main__":
    main()
