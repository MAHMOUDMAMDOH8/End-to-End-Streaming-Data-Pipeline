from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import csv
import logging
import pyarrow as pa
import pyarrow.csv as pv
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('spark_cleaning_job')

# — your HDFS RPC endpoint for data I/O —
hdfs_rpc_prefix = "hdfs://172.27.0.2:8020"
hdfs_bronze_path    = "/bronze"
hdfs_silver_path    = "/silver"




spark = SparkSession.builder \
    .appName("bronze_to_silver_cleaning_job") \
    .master("spark://172.27.0.8:7077") \
    .getOrCreate()

logger.info(f"Connected to Spark Master: {spark.sparkContext.master}")


csv_path = f"{hdfs_rpc_prefix}{hdfs_bronze_path}/new_json_files.csv"
csv_df = spark.read.csv(csv_path, header=True)
file_name_list = csv_df.select("file_name").rdd.flatMap(lambda x: x).collect()

row_count = 0
result_df = []
for file_name in file_name_list:
    file_path = f"{hdfs_rpc_prefix}{hdfs_bronze_path}/{file_name}"
    logger.info(f"Processing file: {file_path}")
    try:
        df = spark.read.json(file_path)
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        continue

    time_series_fields = [field.name for field in df.schema["data"].dataType.fields if "Time Series" in field.name]

    time_series_field= time_series_fields[0]
    
    field_names = df.select(f"data.`{time_series_field}`").schema[0].dataType.names

    time_series_map = F.map_from_arrays(
        F.array([F.lit(f) for f in field_names]), 
        F.array([F.col(f"data.`{time_series_field}`.`{f}`") for f in field_names])
    )
    df_map = df.withColumn("time_series_map", time_series_map)
    exploded_df = df_map.select(
        "symbol", 
        "timestamp",
        F.explode_outer("time_series_map").alias("time_series","event_values")
    ) 
    cleaned_df = exploded_df.select(
        "symbol",
        "timestamp",
        F.col("event_values.`1. open`").alias("open"),
        F.col("event_values.`2. high`").alias("high"),
        F.col("event_values.`3. low`").alias("low"),
        F.col("event_values.`4. close`").alias("close"),
        F.col("event_values.`5. volume`").alias("volume"),
    )
    final_df = cleaned_df.dropna()
    logger.info(f"Cleaned data from {file_path}, {final_df.count()} records")
    row_count += final_df.count()
    result_df.append(final_df)
    logger.info(f"Saved cleaned data to {file_path}")


if result_df:

    final_result_df = result_df[0]
    for df in result_df[1:]:
        final_result_df = final_result_df.union(df)
        logger.info(f"total records: {final_result_df.count()}")

output_path = f"{hdfs_rpc_prefix}{hdfs_silver_path}/stock-prices"
final_result_df.coalesce(1).write.mode("overwrite").parquet(output_path)
logger.info(f"Total records processed: {row_count}")



