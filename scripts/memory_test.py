import pandas as pd
import time

def analyze_performance():
    # 1. Standard Loading
    start = time.time()
    df = pd.read_csv('data/mega_clicks.csv')
    mem_standard = df.memory_usage(deep=True).sum() / (1024**2) # Convert to MB
    print(f"Standard Load Memory: {round(mem_standard, 2)} MB")
    print(f"Standard Load Time: {round(time.time() - start, 2)}s")

    # 2. Optimized Loading (Task 5: Selecting only needed columns and downcasting)
    start = time.time()
    df_opt = pd.read_csv('data/mega_clicks.csv', usecols=['ad_id', 'cost_per_click'])
    mem_opt = df_opt.memory_usage(deep=True).sum() / (1024**2)
    print(f"Optimized Load Memory: {round(mem_opt, 2)} MB")
    print(f"Optimized Load Time: {round(time.time() - start, 2)}s")

if __name__ == "__main__":
    analyze_performance()
