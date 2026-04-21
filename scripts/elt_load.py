import pandas as pd
import sqlite3

def run_elt_load():
    # 1. Extract
    df = pd.read_csv('data/raw_clicks.csv')
    
    # 2. Load (Raw)
    conn = sqlite3.connect('data/ad_clicks.db')
    # We load it into a staging table without any cleaning
    df.to_sql('raw_clicks_staging', conn, if_exists='replace', index=False)
    conn.close()
    print("ELT Step 1: Raw data dumped into 'raw_clicks_staging'.")

if __name__ == "__main__":
    run_elt_load()
