import pandas as pd
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(__file__))
from db import create_table, save_to_postgres

def load_historical_csv(filepath):
    filename = os.path.basename(filepath)
    date_str = filename.replace(".csv", "").replace("_", "-")
    
    try:
        trading_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Could not parse date from filename: {filename}")
    
    df = pd.read_csv(filepath)
    
    # handle old schema where LTP column doesn't exist
    # use Close as LTP since they're equivalent at end of day
    if "LTP" not in df.columns:
        df["LTP"] = df["Close"]
    
    # add scraped_date from filename
    df["scraped_date"] = trading_date
    df = df.drop_duplicates()
    
    return df


def bulk_load(folder_path):
    files = sorted([
        f for f in os.listdir(folder_path)
        if f.endswith(".csv")
    ])
    
    print(f"Found {len(files)} CSV files in {folder_path}")
    
    create_table()
    
    success = 0
    skipped = 0
    failed = 0
    
    for filename in files:
        filepath = os.path.join(folder_path, filename)
        
        try:
            df = load_historical_csv(filepath)
            save_to_postgres(df)
            print(f"  Loaded {filename} — {len(df)} rows for {df['scraped_date'].iloc[0]}")
            success += 1
            
        except Exception as e:
            print(f"  ERROR loading {filename}: {e}")
            failed += 1
            continue
    
    print(f"\nBulk load complete:")
    print(f"  Success:  {success}")
    print(f"  Failed:   {failed}")


if __name__ == "__main__":
    folder = "data/historical/all_data"
    bulk_load(folder)