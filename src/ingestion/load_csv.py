import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(__file__))
from db import create_table, save_to_postgres

def load_csv_to_postgres(filepath: str):
    print(f"Reading {filepath}...")
    df = pd.read_csv(filepath)

    print(f"Loaded {len(df)} rows from CSV")
    print(df.head())

    create_table()
    save_to_postgres(df)

if __name__ == "__main__":
    filepath = "data/raw/nepse_2026-03-28.csv"
    load_csv_to_postgres(filepath)