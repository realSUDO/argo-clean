import pandas as pd

# Read the combined CSV file
combined_df = pd.read_csv('argo_summaries_hierarchical.csv')

# Modify the summary text based on hierarchy level
def enhance_summary_text(row):
    summary = row['summary']
    if row['hierarchy_level'] == 'float_aggregate':
        # Add "on average" for aggregate data
        if "on average" not in summary.lower():
            summary = summary.replace("Float", "On average, float")
    return summary

# Apply the text enhancement
combined_df['summary'] = combined_df.apply(enhance_summary_text, axis=1)

# Select only the columns we want to keep
final_df = combined_df[['float_id', 'summary', 'hierarchy_level', 'retrieval_priority']]

# Save the cleaned version
final_df.to_csv('argo_summaries_clean.csv', index=False)

print(f"✅ Cleaned file created with {len(final_df)} rows")
print("Final columns:", final_df.columns.tolist())
print("\nSample of cleaned data:")
print(final_df.head(3).to_string(index=False))
