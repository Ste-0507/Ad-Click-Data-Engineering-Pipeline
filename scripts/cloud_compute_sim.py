import subprocess
import time

def simulate_ec2_job(script_name, instance_type="t2.micro"):
    """Simulate running a script on an EC2 instance"""
    print(f"[EC2] Launching instance: {instance_type}")
    time.sleep(1)
    
    print(f"[EC2] Running: {script_name}")
    result = subprocess.run(
        f"python3 scripts/{script_name}",
        shell=True,
        capture_output=True,
        text=True
    )
    
    print(f"[EC2] Output:\n{result.stdout}")
    print(f"[EC2] Instance terminated (cost: $0.02)")

if __name__ == "__main__":
    simulate_ec2_job("spark_processor.py", "t2.micro")
    print("\n✓ Task 26 Complete: Cloud Compute Simulation")
