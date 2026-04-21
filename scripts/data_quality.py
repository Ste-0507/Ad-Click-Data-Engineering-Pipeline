import pandas as pd
import numpy as np

class DataQualityFramework:
    def __init__(self, df):
        self.df = df
        self.issues = []
    
    def check_completeness(self, column):
        """Check for missing values"""
        missing = self.df[column].isnull().sum()
        if missing > 0:
            self.issues.append(f"⚠ {column}: {missing} missing values")
        else:
            print(f"✓ {column}: Complete (0 missing)")
    
    def check_uniqueness(self, column):
        """Check for duplicates"""
        dupes = self.df[column].duplicated().sum()
        if dupes > 0:
            self.issues.append(f"⚠ {column}: {dupes} duplicates")
        else:
            print(f"✓ {column}: All unique")
    
    def check_validity(self, column, min_val, max_val):
        """Check for out-of-range values"""
        invalid = ((self.df[column] < min_val) | (self.df[column] > max_val)).sum()
        if invalid > 0:
            self.issues.append(f"⚠ {column}: {invalid} out-of-range values")
        else:
            print(f"✓ {column}: All values in range [{min_val}, {max_val}]")
    
    def detect_anomalies(self, column):
        """Detect 3-sigma outliers"""
        mean = self.df[column].mean()
        std = self.df[column].std()
        outliers = ((self.df[column] < mean - 3*std) | (self.df[column] > mean + 3*std)).sum()
        
        if outliers > 0:
            self.issues.append(f"⚠ {column}: {outliers} anomalies detected (3-sigma)")
        else:
            print(f"✓ {column}: No anomalies detected")
    
    def report(self):
        """Print quality report"""
        print("\n" + "="*50)
        print("DATA QUALITY REPORT")
        print("="*50)
        if self.issues:
            for issue in self.issues:
                print(issue)
        else:
            print("✓ All checks passed!")
        print("="*50)

if __name__ == "__main__":
    # Load data
    df = pd.read_csv("data/cleaned_clicks.csv")
    
    # Run quality checks
    qf = DataQualityFramework(df)
    qf.check_completeness("click_id")
    qf.check_uniqueness("click_id")
    qf.check_validity("cost_per_click", 0, 10)
    qf.detect_anomalies("cost_per_click")
    qf.report()
    
    print("\n✓ Task 29 Complete: Data Quality Framework")
