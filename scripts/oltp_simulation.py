import sqlite3
from datetime import datetime
import uuid

def simulate_oltp_write():
    conn = sqlite3.connect('data/ad_clicks.db')
    cursor = conn.cursor()
    
    # We add a 7th value (0 for is_fraud) to match your 7-column schema
    click_event = (
        str(uuid.uuid4()), 
        'AD_101', 
        'USR_999', 
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        '127.0.0.1', 
        1.50,
        0  # The 7th column
    )
    
    # Notice we now use 7 question marks
    cursor.execute("INSERT INTO clicks VALUES (?,?,?,?,?,?,?)", click_event)
    conn.commit()
    conn.close()
    print("OLTP: Single transaction committed successfully.")

if __name__ == "__main__":
    simulate_oltp_write()
