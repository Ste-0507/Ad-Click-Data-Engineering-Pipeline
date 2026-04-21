import sqlite3
import pandas as pd

def run_queries():
    conn = sqlite3.connect('data/ad_clicks.db')
    
    # Task 6 Requirement: SELECT, WHERE, GROUP BY, ORDER BY
    query = """
    SELECT 
        ad_id, 
        COUNT(click_id) as total_clicks,
        SUM(cost_per_click) as total_spend
    FROM clicks
    WHERE cost_per_click > 0.50
    GROUP BY ad_id
    ORDER BY total_spend DESC;
    """
    
    results = pd.read_sql_query(query, conn)
    print("\n--- Ad Performance Report (High Value Clicks > $0.50) ---")
    print(results)
    conn.close()

if __name__ == "__main__":
    run_queries()
