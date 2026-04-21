import pandas as pd
import sqlite3

def load_data():
    # 1. Read the cleaned data
    df = pd.read_csv('data/cleaned_clicks.csv')
    
    # 2. Connect to SQLite
    conn = sqlite3.connect('data/ad_clicks.db')
    
    # 3. Load the dataframe into the 'clicks' table
    # if_exists='replace' ensures we don't get duplicate errors while testing
    df.to_sql('clicks', conn, if_exists='replace', index=False)
    
    conn.close()
    print("Task 6: Data successfully loaded from CSV to SQL Table 'clicks'.")

if __name__ == "__main__":
    load_data()
