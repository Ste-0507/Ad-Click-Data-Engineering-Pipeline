import pandas as pd
import sqlite3

def simulate_bigquery_load(csv_file, table_name):
    """Simulate loading data to BigQuery"""
    print(f"[BigQuery] Queuing load job for {csv_file}")
    
    # Read CSV
    df = pd.read_csv(csv_file)
    
    # Save to "data warehouse"
    conn = sqlite3.connect("data_warehouse/warehouse.db")
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    
    print(f"[BigQuery] ✓ Loaded {len(df)} rows into {table_name}")

def simulate_bigquery_query(query, table_name):
    """Simulate querying BigQuery"""
    print(f"[BigQuery] Running query on {table_name}...")
    
    conn = sqlite3.connect("data_warehouse/warehouse.db")
    result = pd.read_sql_query(
        f"SELECT COUNT(*) as total_rows FROM {table_name}",
        conn
    )
    
    print(f"[BigQuery] Result: {result['total_rows'][0]} rows")
    conn.close()

if __name__ == "__main__":
    simulate_bigquery_load("data/cleaned_clicks.csv", "ad_clicks")
    simulate_bigquery_query("SELECT * FROM ad_clicks", "ad_clicks")
    print("\n✓ Task 27 Complete: Data Warehouse Simulation")
