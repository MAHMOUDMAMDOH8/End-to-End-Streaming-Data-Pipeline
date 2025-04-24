from hdfs import InsecureClient
import os
import logging

# --- Configuration ---
HDFS_URI = "http://namenode:9870"  # Change if your HDFS Namenode uses a different hostname or port
LOCAL_FILE = "/opt/airflow/includes/local warehose/bronze/stock-prices.jsonl"  # Full path inside Airflow container
HDFS_DEST = "/bronze/stock-prices.jsonl"  # Destination path in HDFS
HDFS_USER = "hadoop"  # Change if your HDFS user is different

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_to_hdfs():
    if not os.path.exists(LOCAL_FILE):
        logger.error(f"❌ File does not exist: {LOCAL_FILE}")
        return

    try:
        logger.info(f"📡 Connecting to HDFS at {HDFS_URI} as user '{HDFS_USER}'")
        client = InsecureClient(HDFS_URI, user=HDFS_USER)

        logger.info(f"🚀 Uploading {LOCAL_FILE} → HDFS:{HDFS_DEST}")
        client.upload(hdfs_path=HDFS_DEST, local_path=LOCAL_FILE, overwrite=True)

        logger.info(f"✅ Successfully uploaded to HDFS at {HDFS_DEST}")

    except Exception as e:
        logger.error(f"❌ Failed to upload to HDFS: {e}")

if __name__ == "__main__":
    upload_to_hdfs()
