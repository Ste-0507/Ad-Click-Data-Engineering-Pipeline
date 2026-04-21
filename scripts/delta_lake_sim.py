import pandas as pd
import json
import os
from datetime import datetime

class DeltaLakeSimulator:
    def __init__(self, path="delta_lake"):
        self.path = path
        self.versions = {}
        os.makedirs(path, exist_ok=True)
    
    def write(self, table_name, df, version=None):
        """Write data with versioning"""
        table_path = f"{self.path}/{table_name}"
        os.makedirs(table_path, exist_ok=True)
        
        version = version or datetime.now().isoformat()
        df.to_parquet(f"{table_path}/v{version}.parquet")
        
        print(f"[Delta] ✓ Wrote {table_name} version {version}")
        return version
    
    def read(self, table_name, version=None):
        """Read data (optionally from specific version)"""
        table_path = f"{self.path}/{table_name}"
        
        if version:
            df = pd.read_parquet(f"{table_path}/v{version}.parquet")
        else:
            # Get latest version
            files = sorted(os.listdir(table_path))[-1]
            df = pd.read_parquet(f"{table_path}/{files}")
        
        print(f"[Delta] ✓ Read {table_name} ({len(df)} rows)")
        return df
    
    def time_travel(self, table_name, version):
        """Read from past version"""
        df = self.read(table_name, version)
        print(f"[Delta] ✓ Time-traveled to version {version}")
        return df

if __name__ == "__main__":
    delta = DeltaLakeSimulator()
    
    # Write v1
    df1 = pd.DataFrame({"ad_id": ["AD_101", "AD_102"], "cost": [1.5, 2.0]})
    v1 = delta.write("ads", df1, "2026-04-21-v1")
    
    # Write v2 (update)
    df2 = pd.DataFrame({"ad_id": ["AD_101", "AD_102", "AD_103"], "cost": [1.5, 2.0, 2.5]})
    v2 = delta.write("ads", df2, "2026-04-21-v2")
    
    # Read latest
    latest = delta.read("ads")
    
    # Time travel to v1
    old = delta.time_travel("ads", "2026-04-21-v1")
    
    print("\n✓ Task 28 Complete: Lakehouse with versioning")
