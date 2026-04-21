import sqlite3
import pandas as pd

def create_olap_view():
    conn = sqlite3.connect('data/ad_clicks.db')
    cursor = conn.cursor()
    
    # Creating a summary table (common in OLAP/Data Warehousing)
    cursor.execute("DROP TABLE IF EXISTS monthly_ad_summary")
    cursor.execute("""
        CREATE TABLE monthly_ad_summary AS
        SELECT ad_id, COUNT(*) as click_count, SUM(cost_per_click) as total_spend
        FROM clicks
        GROUP BY ad_id
    """)
    
    print("OLAP: Summary table 'monthly_ad_summary' created for fast reporting.")
    conn.close()

if __name__ == "__main__":
    create_olap_view()
