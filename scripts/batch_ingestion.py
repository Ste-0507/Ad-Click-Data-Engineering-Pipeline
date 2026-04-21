import pandas as pd
import sqlite3
import os

def run_batch_ingestion():
    input_file = 'data/raw_clicks.csv'
    db_path = 'data/ad_clicks.db'
    
    if not os.path.exists(input_file):
        print("No batch file found to ingest.")
        return

    # 1. Read the batch
    df = pd.read_csv(input_file)

    # 2. Connect to DB
    conn = sqlite3.connect(db_path)
    
    # 3. Batch Ingestion (using 'append' for real batch flow)
    # We use a temporary staging table to avoid direct corruption
    df.to_sql('batch_staging', conn, if_exists='replace', index=False)
    
    # 4. Atomic move to production table
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO clicks (click_id, ad_id, user_id, timestamp, ip_address, cost_per_click)
        SELECT click_id, ad_id, user_id, timestamp, ip_address, cost_per_click 
        FROM batch_staging
        WHERE click_id NOT IN (SELECT click_id FROM clicks);
    """)
    
    conn.commit()
    conn.close()
    print(f"Task 11: Batch Ingestion complete. Deduplicated records added to 'clicks' table.")

if __name__ == "__main__":
    run_batch_ingestion()
