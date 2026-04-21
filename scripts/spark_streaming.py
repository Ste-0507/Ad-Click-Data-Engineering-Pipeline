from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Initialize Spark with Kafka connector
# We use the specific version for Spark 4.1.1 and Scala 2.13
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to WARN so the console isn't flooded with INFO logs
spark.sparkContext.setLogLevel("WARN")

# 2. Define the Schema (matches what you type in the producer)
schema = StructType() \
    .add("ad_id", StringType()) \
    .add("cost", DoubleType())

# 3. Read from Kafka
print("Spark Streaming: Connecting to Kafka topic 'ad-clicks'...")
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ad-clicks") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transform Binary Kafka data to JSON
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Transformation: Running count of clicks per ad
result_df = json_df.groupBy("ad_id").count()

# 6. Output to Console (updates every time you type in the producer)
query = result_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print("Streaming started. Waiting for data from Producer...")
query.awaitTermination()
