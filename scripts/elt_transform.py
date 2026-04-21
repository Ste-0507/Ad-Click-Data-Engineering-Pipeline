import sqlite3

def run_elt_transform():
    conn = sqlite3.connect('data/ad_clicks.db')
    cursor = conn.cursor()
    
    # 3. Transform (SQL logic replaces Pandas logic)
    cursor.execute("DROP TABLE IF EXISTS final_clicks_elt")
    cursor.execute("""
        CREATE TABLE final_clicks_elt AS
        SELECT 
            click_id, 
            UPPER(ad_id) as ad_id, 
            COALESCE(cost_per_click, 0.0) as cost_per_click,
            'PROCESSED_VIA_ELT' as metadata
        FROM raw_clicks_staging
        WHERE click_id IS NOT NULL;
    """)
    
    conn.commit()
    conn.close()
    print("ELT Step 2: Data transformed inside the DB using SQL.")

if __name__ == "__main__":
    run_elt_transform()
