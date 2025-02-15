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

def fetch_data_with_retry(max_retries=3, backoff_factor=2):
    """
    Makes a request to the Digitraffic API with a retry logic in case of temporary failures.
    - max_retries: maximum number of attempts
    - backoff_factor: multiplier to increase wait time after each failure
    Returns the JSON data or None if all retries fail.
    """
    attempt = 1
    wait_time = 2  # initial wait time (in seconds) before retry
    while attempt <= max_retries:
        try:
            response = requests.get(API_URL, timeout=15)
            response.raise_for_status()
            logging.info(f"Attempt {attempt}: Successfully obtained data from the Digitraffic API.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempt} failed: {e}")
            if attempt == max_retries:
                logging.error("Max retries reached. Giving up on API call.")
                return None
            time.sleep(wait_time)
            wait_time *= backoff_factor
            attempt += 1

def produce_data():
    """
    Fetches data from the API (with retry) and sends it to the Kafka topic.
    """
    data = fetch_data_with_retry()
    if data:
        try:
            # Send the JSON list (which can be large) to Kafka
            producer.produce(KAFKA_TOPIC, json.dumps(data).encode('utf-8'))
            producer.flush()
            logging.info("Data successfully sent to Kafka.")
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
    else:
        logging.error("Could not obtain data from the API after multiple retries.")

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
