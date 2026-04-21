from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("RDDvsDF").master("local[*]").getOrCreate()
sc = spark.sparkContext # Get the Spark Context for RDDs

# 1. DATAFRAME APPROACH (Task 14 style)
start_df = time.time()
df = spark.read.csv("data/mega_clicks.csv", header=True, inferSchema=True)
df_result = df.groupBy("ad_id").count()
df_result.collect() # Force execution
print(f"DataFrame Time: {round(time.time() - start_df, 2)}s")

# 2. RDD APPROACH (The low-level way)
start_rdd = time.time()
# Load as text, split by comma, and map to (ad_id, 1)
rdd = sc.textFile("data/mega_clicks.csv")
header = rdd.first()
rdd_result = rdd.filter(lambda line: line != header) \
    .map(lambda line: (line.split(",")[1], 1)) \
    .reduceByKey(lambda a, b: a + b)
rdd_result.collect() # Force execution
print(f"RDD Time: {round(time.time() - start_rdd, 2)}s")

spark.stop()
