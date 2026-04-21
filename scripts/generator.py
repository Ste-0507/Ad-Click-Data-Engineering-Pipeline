import pandas as pd
import uuid
import random
from datetime import datetime

def generate_data(n=100):
    data = []
    # Generate 100 fake ad clicks
    for _ in range(n):
        data.append({
            "click_id": str(uuid.uuid4()),
            "ad_id": f"AD_{random.randint(100, 105)}",
            "user_id": f"USER_{random.randint(1000, 5000)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ip_address": f"192.168.1.{random.randint(1, 255)}",
            "cost_per_click": round(random.uniform(0.10, 2.50), 2)
        })
    
    # Save to the data folder
    df = pd.DataFrame(data)
    df.to_csv('data/raw_clicks.csv', index=False)
    print("Success: 100 clicks generated in data/raw_clicks.csv")

if __name__ == "__main__":
    generate_data()
