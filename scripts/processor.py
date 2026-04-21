import pandas as pd
from utils import normalize_costs, validate_data # Importing our custom module

def run_advanced_processing():
    df = pd.read_csv('data/raw_clicks.csv')
    
    # 1. Validation (from our module)
    cols_to_check = ['click_id', 'ad_id', 'cost_per_click']
    if validate_data(df, cols_to_check):
        print("Data Validation Passed.")

    # 2. Normalization (from our module)
    df = normalize_costs(df, 'cost_per_click')

    # 3. Save
    df.to_csv('data/cleaned_clicks.csv', index=False)
    print("Task 4 Complete: Reusable modules used for Normalization and Validation.")

if __name__ == "__main__":
    run_advanced_processing()
