from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Step 1: Initialize Spark Session
# 'local[*]' means use all available CPU cores on your laptop
spark = SparkSession.builder \
    .appName("AdClickAnalysis") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session Started. Loading 1M rows...")

# Step 2: Load the Mega Dataset
df = spark.read.csv("data/mega_clicks.csv", header=True, inferSchema=True)

# Step 3: Transformation (Task 14 requirement)
# Group by Ad_ID and calculate average cost
analysis_df = df.groupBy("ad_id") \
    .agg(avg("cost_per_click").alias("avg_cpc")) \
    .orderBy(col("avg_cpc").desc())

# Step 4: Show Results
print("Top Performing Ads by Average Cost (Processed via Spark):")
analysis_df.show(10)

# Step 5: Stop Spark
spark.stop()
