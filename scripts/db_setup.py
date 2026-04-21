import sqlite3
import os

def init_db():
    # Ensure the data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Connect to (or create) the SQLite database
    conn = sqlite3.connect('data/ad_clicks.db')
    cursor = conn.cursor()
    
    print("Initializing tables...")

    # 1. Fact Table: ad_clicks (Task 6 & 9)
    # Stores the quantitative data (the 'events')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clicks (
            click_id TEXT PRIMARY KEY,
            ad_id TEXT,
            user_id TEXT,
            timestamp DATETIME,
            ip_address TEXT,
            cost_per_click REAL
        )
    ''')

    # 2. Dimension Table: campaigns (Task 7 & 9)
    # Stores descriptive data (the 'context')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS campaigns (
            ad_id TEXT PRIMARY KEY,
            campaign_name TEXT,
            category TEXT
        )
    ''')

    # 3. Seed Data for Campaigns
    # We populate this so we have something to JOIN against in Task 7
    campaign_data = [
        ('AD_100', 'Summer_Sale', 'Electronics'),
        ('AD_101', 'Summer_Sale', 'Electronics'),
        ('AD_102', 'Back_To_School', 'Stationery'),
        ('AD_103', 'Back_To_School', 'Stationery'),
        ('AD_104', 'Holiday_Special', 'Fashion'),
        ('AD_105', 'Holiday_Special', 'Fashion')
    ]
    
    # Use INSERT OR REPLACE to avoid errors if you run this script multiple times
    cursor.executemany('''
        INSERT OR REPLACE INTO campaigns (ad_id, campaign_name, category) 
        VALUES (?, ?, ?)
    ''', campaign_data)

    conn.commit()
    conn.close()
    print("Database successfully initialized at data/ad_clicks.db")

if __name__ == "__main__":
    init_db()
