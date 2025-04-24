import findspark
findspark.init()
from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder \
        .appName("HelloSpark") \
        .master("spark://spark:7077") \
        .getOrCreate()
    # Simple check: print the master URL to confirm connectivity
    print(f"Connected to Spark Master: {spark.sparkContext.master}")
except Exception as e:
    print(f"Failed to connect to Spark Master: {e}")
    raise
df = spark.range(1, 6)
df.show()
spark.stop()
