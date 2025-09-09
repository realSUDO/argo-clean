import pandas as pd

# Load the index file properly
df = pd.read_csv(
    "ar_index_global_prof.txt",
    comment="#",
    header=0,  # because file,date,latitude,... are real headers
    dtype={
        "file": str,
        "date": str,
        "latitude": float,
        "longitude": float,
        "ocean": str,
        "profiler_type": str,
        "institution": str,
        "date_update": str,
    },
    low_memory=False
)

print(f"Total profiles in global index: {len(df)}")

# ✅ Step 1: Define bounding box for Indian Ocean
latmin, latmax = -20, 30    # Roughly covers India region
lonmin, lonmax = 40, 100    # Arabian Sea + Bay of Bengal

# ✅ Step 2: Filter using bounding box
regional_df = df[
    (df["latitude"] >= latmin) & (df["latitude"] <= latmax) &
    (df["longitude"] >= lonmin) & (df["longitude"] <= lonmax)
]

print(f"Profiles in Indian Ocean region: {len(regional_df)}")

# ✅ Step 3: Save filtered list for future downloading
regional_df.to_csv("argo_index_india.csv", index=False)
print("Saved filtered index to argo_index_india.csv")

