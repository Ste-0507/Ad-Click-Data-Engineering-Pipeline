import subprocess
import pandas as pd
import sqlite3
from datetime import datetime

class FinalPipeline:
    def __init__(self):
        self.start_time = datetime.now()
        self.log = []
    
    def step(self, name, command):
        """Execute a pipeline step"""
        print(f"\n{'='*60}")
        print(f"STEP: {name}")
        print(f"{'='*60}")
        
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ {name} completed")
            self.log.append(f"✓ {name}")
        else:
            print(f"✗ {name} failed")
            self.log.append(f"✗ {name}")
    
    def generate_dashboard(self):
        """Create a simple HTML dashboard"""
        conn = sqlite3.connect("data/ad_clicks.db")
        
        # Get metrics
        total_clicks = pd.read_sql("SELECT COUNT(*) as count FROM clicks", conn)['count'][0]
        avg_cost = pd.read_sql("SELECT AVG(cost_per_click) as avg FROM clicks", conn)['avg'][0]
        top_ads = pd.read_sql("SELECT ad_id, COUNT(*) as clicks FROM clicks GROUP BY ad_id ORDER BY clicks DESC LIMIT 5", conn)
        
        conn.close()
        
        # Create HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Ad Click Monitoring Dashboard</title>
            <style>
                body {{ font-family: Arial; background: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 50px auto; background: white; padding: 20px; border-radius: 8px; }}
                .metric {{ display: inline-block; margin: 20px; padding: 20px; background: #e3f2fd; border-radius: 5px; }}
                .metric h3 {{ margin: 0; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background: #1976d2; color: white; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Ad Click Monitoring System</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="metric">
                    <h3>Total Clicks</h3>
                    <h2>{total_clicks:,}</h2>
                </div>
                
                <div class="metric">
                    <h3>Avg Cost</h3>
                    <h2>${avg_cost:.2f}</h2>
                </div>
                
                <h3>Top 5 Ads</h3>
                <table>
                    <tr><th>Ad ID</th><th>Clicks</th></tr>
                    {''.join([f"<tr><td>{row[0]}</td><td>{row[1]}</td></tr>" for row in top_ads.itertuples(index=False)])}
                </table>
            </div>
        </body>
        </html>
        """
        
        with open("data/dashboard.html", "w") as f:
            f.write(html)
        
        print("✓ Dashboard generated: data/dashboard.html")
    
    def run_full_pipeline(self):
        """Execute the complete pipeline"""
        print("\n" + "🚀 "*20)
        print("STARTING FINAL END-TO-END PIPELINE")
        print("🚀 "*20)
        
        # Step 1: Generate data
        self.step("Data Generation", "python3 scripts/mega_generator.py")
        
        # Step 2: Clean data
        self.step("Data Cleaning", "python3 scripts/processor.py")
        
        # Step 3: Load to SQL
        self.step("Load to Database", "python3 scripts/load_to_sql.py")
        
        # Step 4: Quality check
        self.step("Quality Validation", "python3 scripts/data_quality.py")
        
        # Step 5: Cloud simulation
        self.step("Cloud Storage Upload", "python3 scripts/cloud_storage_sim.py")
        
        # Step 6: Data warehouse
        self.step("Data Warehouse Load", "python3 scripts/data_warehouse_sim.py")
        
        # Step 7: Lakehouse versioning
        self.step("Delta Lake Versioning", "python3 scripts/delta_lake_sim.py")
        
        # Generate dashboard
        self.generate_dashboard()
        
        # Summary
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"\n{'='*60}")
        print(f"PIPELINE COMPLETE in {elapsed:.1f} seconds")
        print(f"{'='*60}")
        for log in self.log:
            print(log)
        print(f"\n📊 Open: data/dashboard.html in your browser")

if __name__ == "__main__":
    pipeline = FinalPipeline()
    pipeline.run_full_pipeline()
