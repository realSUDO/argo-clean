import pandas as pd
from datetime import datetime

def main():
    # Read the CSV
    df = pd.read_csv('/home/just_multiply/sih/ocean/batch_1_files.csv')
    
    # Convert date column to readable format
    df['date'] = df['date'].apply(lambda x: datetime.strptime(str(int(x)), '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S'))
    
    # Sort by date
    df = df.sort_values('date')
    
    # Save back to CSV
    df.to_csv('/home/just_multiply/sih/ocean/batch_1_files.csv', index=False)
    
    print(f"Converted dates and sorted {len(df)} rows by date in batch_1_files.csv")

if __name__ == "__main__":
    main()
