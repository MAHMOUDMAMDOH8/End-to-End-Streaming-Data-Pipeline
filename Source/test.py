from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('spark_test')

def create_spark_session():
    spark = SparkSession.builder \
        .appName("spark_test") \
        .master("spark://172.21.0.8:7077") \
        .getOrCreate()

    logger.info(f"Connected to Spark Master: {spark.sparkContext.master}")

    # Sample Spark job: create a DataFrame and show its content
    df = spark.range(0, 5)
    df.show()

    logger.info("DataFrame created and displayed successfully.")

if __name__ == "__main__":
    create_spark_session()
    logger.info("Spark job completed successfully.")