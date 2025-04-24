from kafka import KafkaConsumer, TopicPartition
import json
import os
import time
import logging
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("STOCK_KAFKA_TOPIC", "stock-prices")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "stock-consumer-group")
BRONZE_DIR = os.getenv("BRONZE_DIR", "/opt/airflow/includes/local warehose/bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)
single_file_path = os.path.join(BRONZE_DIR, f"{TOPIC}.jsonl")

try:
    # Clear previous output file for fresh run
    with open(single_file_path, "w"):
        pass
    logger.info(f"Cleared existing output file {single_file_path}")
except Exception as e:
    logger.error(f"Failed to clear file {single_file_path}: {e}")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

logger.info(f"Listening to Kafka topic '{TOPIC}' and writing to {BRONZE_DIR}")

# Poll messages until no new messages for a while
no_message_timeout = 5  # seconds
max_empty_polls = 3
empty_polls = 0

while empty_polls < max_empty_polls:
    msg_pack = consumer.poll(timeout_ms=no_message_timeout * 1000)
    
    if not msg_pack:
        empty_polls += 1
        logger.info(f"No messages received (#{empty_polls})...")
        continue

    empty_polls = 0  # Reset counter on receiving messages

    for tp, messages in msg_pack.items():
        for message in messages:
            logger.info(f"Received message: {message}")
            data = message.value
            try:
                with open(single_file_path, "a") as f:
                    f.write(json.dumps(data) + "\n")
                logger.info(f"Appended message to {single_file_path}")
            except Exception as e:
                logger.error(f"Failed to append to {single_file_path}: {e}")


