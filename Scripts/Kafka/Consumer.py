import os
import json
import logging
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kafka_consumer')


BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC = os.getenv('STOCK_KAFKA_TOPIC')
CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP')
BRONZE_DIR =  '/opt/airflow/includes/local_warehose/bronze'
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP")




# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

logger.info(f"Listening on topic(s): {TOPIC}")

no_message_timeout = 5  
max_empty_polls = 3
empty_polls = 0

while empty_polls < max_empty_polls:
    msg_pack = consumer.poll(timeout_ms=no_message_timeout * 1000)
    
    if not msg_pack:
        empty_polls += 1
        logger.info(f"No messages received (#{empty_polls})...")
        continue

    empty_polls = 0  

    for tp, messages in msg_pack.items():
        for message in messages:
            logger.info("Received message")
            data = message.value
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            try:
                file_path = os.path.join(BRONZE_DIR, f"{data['symbol']}_{timestamp}.json")
                with open(file_path, "a") as f:
                    f.write(json.dumps(data) + "\n")
                logger.info(f"Appended message to {file_path}")
            except Exception as e:
                logger.error(f"Failed to append to {file_path}: {e}")
