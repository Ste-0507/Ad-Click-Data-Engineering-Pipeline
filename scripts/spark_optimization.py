from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time

spark = SparkSession.builder.appName("SparkOptimization").master("local[*]").getOrCreate()

# 1. Load the Big Data (1M rows)
big_df = spark.read.csv("data/mega_clicks.csv", header=True, inferSchema=True)

# 2. Create a Small Dataframe (Campaigns)
# In reality, this would be your 'campaigns' table from Task 7
campaign_data = [("AD_101", "Electronics"), ("AD_102", "Stationery"), ("AD_103", "Stationery")]
small_df = spark.createDataFrame(campaign_data, ["ad_id", "category"])

# 3. PERFORMANCE TEST: Regular Join
start = time.time()
regular_join = big_df.join(small_df, "ad_id")
regular_join.collect()
print(f"Regular Join Time: {round(time.time() - start, 2)}s")

# 4. PERFORMANCE TEST: Broadcast Join (Optimization)
start = time.time()
# We tell Spark: "This table is tiny, send it to everyone!"
optimized_join = big_df.join(broadcast(small_df), "ad_id")
optimized_join.collect()
print(f"Broadcast Join Time: {round(time.time() - start, 2)}s")

# 5. CACHING (Optimization)
# If you plan to use big_df 5 more times in this script, cache it in RAM
big_df.cache()
big_df.count() # Materialize the cache

spark.stop()
