# This module handles all database interactions, including creating tables and saving data to PostgreSQL.
# bridge between python and postgres
import psycopg2
# execute_values is a helper function from psycopg2 
# that allows us to efficiently insert multiple rows into the database in a single query, 
# which is much faster than inserting rows one by one.
from psycopg2.extras import execute_values
import pandas as pd
# dotenv is a library that allows us to load environment variables from a .env file,
# which is a common practice for managing sensitive information like database credentials.
from dotenv import load_dotenv
# os is a built-in library that provides a way to interact with the operating system,
# in this case, we use it to access environment variables that we loaded with dotenv.
import os

# load environment variables from .env file
load_dotenv()

# connectionn to the postgres database
def get_connection():
    # reads credentials from .env file, never hardcoded
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn

# This function creates the table in the database.
def create_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nepse_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20),
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            ltp NUMERIC,
            volume VARCHAR(30),
            turnover VARCHAR(30),
            scraped_date DATE,
            UNIQUE(symbol, scraped_date)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Table created successfully")


def save_to_postgres(df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()

    df_clean = df[[
        "Symbol", "Open", "High", "Low",
        "Close", "LTP", "Vol", "Turnover", "scraped_date"
    ]].copy()

    # remove commas from numeric columns so postgres can parse them
    numeric_cols = ["Open", "High", "Low", "Close", "LTP"]
    for col in numeric_cols:
        df_clean[col] = df_clean[col].astype(str).str.replace(",", "", regex=False)
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

    # replace empty strings with None
    df_clean = df_clean.replace("", None)

    rows = [tuple(row) for row in df_clean.itertuples(index=False)]

    insert_query = """
        INSERT INTO nepse_prices 
            (symbol, open, high, low, close, ltp, volume, turnover, scraped_date)
        VALUES %s
        ON CONFLICT (symbol, scraped_date) DO NOTHING
    """

    execute_values(cursor, insert_query, rows)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Saved {len(rows)} rows to PostgreSQL")


if __name__ == "__main__":
    create_table()
    print("Database setup complete")