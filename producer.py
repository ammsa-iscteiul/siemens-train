import json
import time
import requests
import logging
from confluent_kafka import Producer

# >>> CONFIGURATION <<<
API_URL = 'https://rata.digitraffic.fi/api/v1/live-trains/'
KAFKA_TOPIC = 'live_trains'
POLL_INTERVAL = 10  # seconds between each API call

# >>> KAFKA PRODUCER CONFIG <<<
producer_config = {
    'bootstrap.servers': 'kafka:9092',
    # In librdkafka, use 'message.max.bytes' to increase the maximum message size
    'message.max.bytes': 50000000  # 50 MB
}

producer = Producer(producer_config)

def fetch_data():
    """
    Makes a request to the Digitraffic API and returns the data in JSON format.
    """
    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        logging.info("Successfully obtained data from the Digitraffic API.")
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching data from the API: {e}")
        return None

def produce_data():
    """
    Fetches data from the API and sends it to the Kafka topic.
    """
    data = fetch_data()
    if data:
        try:
            # Send the JSON list (which can be large) to Kafka
            producer.produce(KAFKA_TOPIC, json.dumps(data).encode('utf-8'))
            producer.flush()
            logging.info("Data successfully sent to Kafka.")
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")

def main():
    """
    Main function that continuously fetches and sends data to Kafka 
    at regular intervals defined by POLL_INTERVAL.
    """
    logging.basicConfig(level=logging.INFO)
    while True:
        produce_data()
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
