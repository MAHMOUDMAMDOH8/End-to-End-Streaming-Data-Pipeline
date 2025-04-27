from kafka import KafkaProducer
import os
import time
import json
import requests
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('producer')

API_KEY = os.getenv("API_KEY")
SYMBOLS = os.getenv("STOCK_SYMBOLS").split(",")
TOPIC = os.getenv("STOCK_KAFKA_TOPIC")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_stock_data(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()

        if "Error Message" in data:
            logger.warning(f"Error fetching data for {symbol}: {data['Error Message']}")
            return 
        
        message = {
            "symbol": symbol,
            "timestamp": time.strftime("%d-%m-%Y %H:%M"),
            "data": data
        }
        producer.send(TOPIC, value=message)
        producer.flush()
        logger.info(f"Sent data for {symbol} to Kafka topic {TOPIC}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {symbol}: {e}")


if __name__ == "__main__":
    for symbol in SYMBOLS:
        fetch_stock_data(symbol)
        time.sleep(10)

    logger.info("producer completed.")
    producer.close()
    logger.info("Producer closed.")

