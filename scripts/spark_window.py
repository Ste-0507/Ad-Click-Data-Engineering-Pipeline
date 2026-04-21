from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLWindow").master("local[*]").getOrCreate()

# 1. Load Data
df = spark.read.csv("data/mega_clicks.csv", header=True, inferSchema=True)

# 2. Register as a Table (Temp View)
df.createOrReplaceTempView("mega_clicks")

# 3. Task 17: Spark SQL + Window Function
# We use ROW_NUMBER() to rank clicks by cost for each ad
query = """
SELECT ad_id, click_id, cost_per_click, rank
FROM (
    SELECT 
        ad_id, 
        click_id, 
        cost_per_click,
        ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY cost_per_click DESC) as rank
    FROM mega_clicks
) 
WHERE rank <= 3
ORDER BY ad_id, rank
"""

print("Running Spark SQL Window Function...")
result = spark.sql(query)
result.show(15)

spark.stop()
