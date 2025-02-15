import json
import time
import logging
import os
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer

# >>> OUTPUT DIRECTORIES <<<
OUTPUT_DIR = 'output'
REPORT_DIR = os.path.join(OUTPUT_DIR, 'reports')
CLEANED_DIR = os.path.join(OUTPUT_DIR, 'cleaned')

# Create directories if they don't exist
os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)

# >>> CONSUMER CONFIGURATIONS <<<
consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'data_quality_streaming_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['live_trains'])

# Buffer to accumulate messages
messages_buffer = []

def generate_report_chart(report_csv_filename):
    """
    Reads the data quality report CSV and creates a bar chart 
    showing the key metrics (e.g., total_records, missing fields, duplicates, anomalies).
    """
    if not os.path.exists(report_csv_filename):
        print(f"Report file '{report_csv_filename}' not found. Cannot generate chart.")
        return

    df = pd.read_csv(report_csv_filename)
    if df.empty:
        print(f"Report file '{report_csv_filename}' is empty. Cannot generate chart.")
        return

    # We assume there is only one row in the report CSV
    row = df.iloc[0]

    # Metrics we want to plot as bars (adjust if your report columns differ)
    metrics = [
        'total_records',
        'missing_trainNumber',
        'missing_departureTime',
        'missing_arrivalTime',
        'duplicates',
        'anomaly_departureTime',
        'anomaly_arrivalTime'
    ]
    # Filter only the columns that actually exist
    metrics = [m for m in metrics if m in df.columns]

    # Extract values from the single row
    values = [row[m] for m in metrics]

    plt.figure(figsize=(10, 6))
    plt.bar(metrics, values)
    plt.title("Data Quality Report Metrics")
    plt.xlabel("Metrics")
    plt.ylabel("Values")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the chart as a PNG file in the same folder as the report CSV
    chart_filename = report_csv_filename.replace('.csv', '_chart.png')
    plt.savefig(chart_filename)
    plt.close()
    print(f"Chart saved to '{chart_filename}'.")

def process_buffer(buffer):
    """
    Processes the accumulated messages, generates a data quality report,
    standardizes/sanitizes the data, and saves the results.
    """
    if not buffer:
        logging.info("No messages to process in this interval.")
        return

    # Flatten the messages if each API call returns a list
    flattened = []
    for item in buffer:
        if isinstance(item, list):
            flattened.extend(item)
        else:
            flattened.append(item)

    df = pd.DataFrame(flattened)

    # >>> DATA QUALITY REPORT <<<
    # Always store numeric values so we can plot them without type errors
    report = {}
    report['total_records'] = len(df)

    critical_fields = ['trainNumber', 'departureTime', 'arrivalTime']
    missing_values = {}

    # Count missing values for each critical field; if the field doesn't exist, use 0
    for field in critical_fields:
        if field in df.columns:
            missing_values[field] = int(df[field].isnull().sum())
        else:
            missing_values[field] = 0  # numeric placeholder for "field not found"

    # Check for duplicates based on 'trainNumber' + 'timestamp'; if columns don't exist, use 0
    if 'trainNumber' in df.columns and 'timestamp' in df.columns:
        duplicates = int(df.duplicated(subset=['trainNumber', 'timestamp']).sum())
    else:
        duplicates = 0

    # Check for anomalies in date fields; if columns don't exist, use 0
    date_fields = ['departureTime', 'arrivalTime']
    anomalies = {}
    for field in date_fields:
        if field in df.columns:
            parsed_dates = pd.to_datetime(df[field], errors='coerce', format='%Y-%m-%d')
            anomalies[field] = int(parsed_dates.isnull().sum())
        else:
            anomalies[field] = 0

    # Build a DataFrame for the report
    report_data = {
        'total_records': [report['total_records']],
        'missing_trainNumber': [missing_values.get('trainNumber', 0)],
        'missing_departureTime': [missing_values.get('departureTime', 0)],
        'missing_arrivalTime': [missing_values.get('arrivalTime', 0)],
        'duplicates': [duplicates],
        'anomaly_departureTime': [anomalies.get('departureTime', 0)],
        'anomaly_arrivalTime': [anomalies.get('arrivalTime', 0)]
    }
    report_df = pd.DataFrame(report_data)

    # Generate a string with current date/time for the filenames
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save the report CSV in the reports subdirectory
    report_csv_filename = os.path.join(REPORT_DIR, f"data_quality_report_{now_str}.csv")
    report_df.to_csv(report_csv_filename, index=False)
    logging.info(f"Data quality report saved to '{report_csv_filename}'.")

    # >>> GENERATE THE CHART FROM THE REPORT <<<
    generate_report_chart(report_csv_filename)

    # >>> DATA STANDARDIZATION AND SANITIZATION <<<
    # Convert date fields to YYYY-MM-DD
    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce').dt.strftime('%Y-%m-%d')

    # Remove duplicates
    if 'trainNumber' in df.columns and 'timestamp' in df.columns:
        df = df.drop_duplicates(subset=['trainNumber', 'timestamp'])

    # Remove extra spaces from string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Save cleaned data in the cleaned subdirectory
    cleaned_csv_filename = os.path.join(CLEANED_DIR, f"cleaned_data_{now_str}.csv")
    cleaned_json_filename = os.path.join(CLEANED_DIR, f"cleaned_data_{now_str}.json")
    df.to_csv(cleaned_csv_filename, index=False)
    df.to_json(cleaned_json_filename, orient='records', lines=True)
    logging.info(f"Cleaned data saved to '{cleaned_csv_filename}' and '{cleaned_json_filename}'.")

def main():
    """
    Main function that polls messages from Kafka, accumulates them in a buffer,
    and processes them every 60 seconds to generate the data quality report
    and cleaned data. The chart is generated from the data quality report.
    """
    logging.basicConfig(level=logging.INFO)
    processing_interval = 60  # 1 minute
    last_process_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Error consuming message: {msg.error()}")
                continue

            # Decode the message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                messages_buffer.append(data)
            except Exception as e:
                logging.error(f"Error decoding message: {e}")

            # Check if 1 minute has passed since the last processing
            current_time = time.time()
            if current_time - last_process_time >= processing_interval:
                logging.info("Processing accumulated messages...")
                process_buffer(messages_buffer)
                messages_buffer.clear()
                last_process_time = current_time

    except KeyboardInterrupt:
        logging.info("Processing interrupted by user.")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
