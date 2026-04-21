import pandas as pd

def normalize_costs(df, column_name):
    """Example of a Transformation Function: Scales costs between 0 and 1."""
    max_val = df[column_name].max()
    min_val = df[column_name].min()
    df[f'normalized_{column_name}'] = (df[column_name] - min_val) / (max_val - min_val)
    return df

def validate_data(df, required_columns):
    """Example of a Validation Function: Checks if all columns exist."""
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    return True
