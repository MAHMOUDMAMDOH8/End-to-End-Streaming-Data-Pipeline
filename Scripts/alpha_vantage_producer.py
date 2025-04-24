import os
import time
import json
import requests
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from environment
API_KEY = "02LUMITX3ON1TILA"
SYMBOLS = os.getenv("STOCK_SYMBOLS", "IBM,AAPL,MSFT,TSLA").split(",")
TOPIC = os.getenv("STOCK_KAFKA_TOPIC", "stock-prices")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_publish(symbol: str):
    """Fetch intraday data for a symbol and send to Kafka."""
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_INTRADAY"
        f"&symbol={symbol}"
        f"&interval=5min"
        f"&apikey={API_KEY}"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Handle API errors or limits
        if "Error Message" in data or "Note" in data:
            logger.warning(f"Issue with API response for {symbol}: {data.get('Error Message') or data.get('Note')}")
            return

        message = {
            "symbol": symbol,
            "timestamp": time.time(),
            "data": data
        }

        producer.send(TOPIC, value=message)
        producer.flush()
        logger.info(f"Published data for {symbol} to topic '{TOPIC}'")

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")

if __name__ == "__main__":
    logger.info(f"Alpha Vantage producer started for symbols: {SYMBOLS}")
    for sym in SYMBOLS:
        fetch_and_publish(sym)
        time.sleep(12)  # Respect API rate limit (5 req/min)
    logger.info("Alpha Vantage producer completed.")
