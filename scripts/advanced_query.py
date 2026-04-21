import sqlite3
import pandas as pd

def run_advanced_queries():
    conn = sqlite3.connect('data/ad_clicks.db')
    
    # Task 7 Requirement: JOINs and Window Functions
    query = """
    WITH AdMetrics AS (
        SELECT 
            c.ad_id,
            cam.campaign_name,
            cam.category,
            SUM(c.cost_per_click) as total_spend
        FROM clicks c
        JOIN campaigns cam ON c.ad_id = cam.ad_id
        GROUP BY c.ad_id
    )
    SELECT 
        campaign_name,
        ad_id,
        category,
        total_spend,
        RANK() OVER (PARTITION BY category ORDER BY total_spend DESC) as rank_in_category
    FROM AdMetrics;
    """
    
    results = pd.read_sql_query(query, conn)
    print("\n--- Advanced Reporting: Joins & Window Function Ranking ---")
    print(results)
    conn.close()

if __name__ == "__main__":
    run_advanced_queries()
