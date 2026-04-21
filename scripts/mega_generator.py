import pandas as pd
import numpy as np
import uuid
import time

def generate_mega_data(rows=1000000):
    print(f"Generating {rows} rows... this might take a second.")
    start_time = time.time()
    
    data = {
        "click_id": [str(uuid.uuid4()) for _ in range(rows)],
        "ad_id": np.random.choice([f"AD_{i}" for i in range(100, 200)], rows),
        "user_id": np.random.randint(1000, 99999, rows),
        "cost_per_click": np.random.uniform(0.10, 5.0, rows).astype(np.float32) # Task 5: Use float32 for memory optimization
    }
    
    df = pd.DataFrame(data)
    df.to_csv('data/mega_clicks.csv', index=False)
    
    end_time = time.time()
    print(f"Created 1M rows in {round(end_time - start_time, 2)} seconds.")

if __name__ == "__main__":
    generate_mega_data()
