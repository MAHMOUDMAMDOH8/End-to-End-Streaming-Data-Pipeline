from hdfs import InsecureClient
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def upload_to_hdfs():
    logger.info("Uploading to HDFS")
    HDFS_URI = "http://namenode:9870"  
    LOCAL_FILE = "/opt/airflow/includes/local warehose/bronze/stock-prices.jsonl"
    HDFS_DEST = "/bronze/stock-prices.jsonl"
    client = InsecureClient(HDFS_URI, user='hadoop')  
    client.upload(hdfs_path=HDFS_DEST, local_path=LOCAL_FILE, overwrite=True)
    logger.info(f"Uploaded {LOCAL_FILE} to {HDFS_DEST}")