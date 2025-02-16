import json
import time
import logging
import os
import matplotlib.pyplot as plt
import pandas as pd
import unicodedata
from datetime import datetime
from confluent_kafka import Consumer

# OUTPUT DIRECTORIES
OUTPUT_DIR = 'output'
REPORT_DIR = os.path.join(OUTPUT_DIR, 'reports')
CLEANED_DIR = os.path.join(OUTPUT_DIR, 'cleaned')

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)

# CONSUMER CONFIGURATIONS
consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'data_quality_streaming_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['live_trains'])

# Buffer to accumulate messages
messages_buffer = []

# Reads the data quality report CSV and creates a bar chart
def generate_report_chart(report_csv_filename):
    if not os.path.exists(report_csv_filename):
        print(f"Report file '{report_csv_filename}' not found. Cannot generate chart.")
        return

    df = pd.read_csv(report_csv_filename)
    if df.empty:
        print(f"Report file '{report_csv_filename}' is empty. Cannot generate chart.")
        return

    row = df.iloc[0]
    metrics = df.columns.tolist()
    values = [row[m] for m in metrics]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values)
    plt.title("Data Quality Report Metrics")
    plt.xlabel("Metrics")
    plt.ylabel("Values")
    plt.xticks(rotation=45)
    plt.tight_layout()

    for bar in bars:
        height = bar.get_height()
        plt.annotate(
            f'{int(height)}',
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),
            textcoords="offset points",
            ha='center',
            va='bottom'
        )

    chart_filename = report_csv_filename.replace('.csv', '_chart.png')
    plt.savefig(chart_filename)
    plt.close()
    print(f"Chart saved to '{chart_filename}'.")

# Normalize and remove unexpected Unicode encoding issues
def normalize_unicode(text):
    if isinstance(text, str):
        return unicodedata.normalize("NFKC", text).strip()
    return text

#     Transforms a train record into a structured format.
def flatten_train_record(train):
    
    flat = {
        "trainNumber": train.get("trainNumber"),
        "departureDate": train.get("departureDate"),
        "operatorShortCode": train.get("operatorShortCode"),
        "trainType": train.get("trainType"),
        "trainCategory": train.get("trainCategory"),
        "cancelled": train.get("cancelled"),
    }

    timeTableRows = train.get("timeTableRows", [])
    departure_time = None
    arrival_time = None
    station_timetable = []

    for row in timeTableRows:
        station_timetable.append({
            "stationShortCode": normalize_unicode(row["stationShortCode"]) if "stationShortCode" in row else None,
            "stationUICCode": row.get("stationUICCode"),
            "type": row.get("type"),
            "trainStopping": row.get("trainStopping"),
            "scheduledTime": row.get("scheduledTime"),
            "actualTime": row.get("actualTime"),
            "differenceInMinutes": row.get("differenceInMinutes"),
            "commercialStop": row.get("commercialStop", False),
            "cancelled": row.get("cancelled"),
            "causes": row.get("causes", [])
        })

    # Sort station timetable by scheduledTime
    station_timetable = sorted(station_timetable, key=lambda x: x["scheduledTime"] if x["scheduledTime"] else "")

    # Get first scheduledTime for DEPARTURE
    for row in timeTableRows:
        if row.get("type") == "DEPARTURE" and row.get("scheduledTime"):
            departure_time = row["scheduledTime"]
            break

    # Get last scheduledTime for ARRIVAL
    for row in reversed(timeTableRows):
        if row.get("type") == "ARRIVAL" and row.get("scheduledTime"):
            arrival_time = row["scheduledTime"]
            break

    flat["departureTime"] = departure_time
    flat["arrivalTime"] = arrival_time
    flat["stationTimetable"] = station_timetable

    return flat

# Processes accumulated messages, generates a data quality report, standardizes data
def process_buffer(buffer):
    
    if not buffer:
        logging.info("No messages to process in this interval.")
        return

    flattened = []
    for item in buffer:
        if isinstance(item, list):
            for record in item:
                if isinstance(record, dict):
                    flattened.append(flatten_train_record(record))
        elif isinstance(item, dict):
            flattened.append(flatten_train_record(item))
        else:
            logging.warning(f"Unexpected data format: {type(item)} - {item}")

    df = pd.DataFrame(flattened)

    # Trim all string columns to remove extra spaces
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # DATA QUALITY REPORT
    report = {
        "total_records": len(df),
        "missing_trainNumber": df["trainNumber"].isnull().sum() if "trainNumber" in df.columns else 0,
        "missing_departureTime": df["departureTime"].isnull().sum() if "departureTime" in df.columns else 0,
        "missing_arrivalTime": df["arrivalTime"].isnull().sum() if "arrivalTime" in df.columns else 0,
        "duplicates": df.duplicated(subset=["trainNumber", "departureTime"]).sum() if "trainNumber" in df.columns and "departureTime" in df.columns else 0,
        "invalid_date_format": df["departureTime"].apply(lambda x: 0 if pd.to_datetime(x, errors="coerce") is not pd.NaT else 1).sum() if "departureTime" in df.columns else 0,
        "unexpected_data_types": sum(df["trainNumber"].apply(lambda x: 1 if not isinstance(x, (int, float)) else 0)) if "trainNumber" in df.columns else 0,
    }

    report_df = pd.DataFrame([report])

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_csv_filename = os.path.join(REPORT_DIR, f"data_quality_report_{now_str}.csv")
    report_df.to_csv(report_csv_filename, index=False)
    generate_report_chart(report_csv_filename)
    logging.info(f"Data quality report saved to '{report_csv_filename}'.")

    cleaned_json_filename = os.path.join(CLEANED_DIR, f"cleaned_data_{now_str}.json")
    with open(cleaned_json_filename, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, indent=4, ensure_ascii=False)

    logging.info(f"Cleaned data saved to '{cleaned_json_filename}'.")

    # DATA STANDARDIZATION
    date_fields = ["departureTime", "arrivalTime"]
    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if "trainNumber" in df.columns and "departureTime" in df.columns:
        df = df.drop_duplicates(subset=["trainNumber", "departureTime"])

    cleaned_csv_filename = os.path.join(CLEANED_DIR, f"cleaned_data_{now_str}.csv")
    cleaned_json_filename = os.path.join(CLEANED_DIR, f"cleaned_data_{now_str}.json")
    df.to_csv(cleaned_csv_filename, index=False)
    
    with open(cleaned_json_filename, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, indent=4, ensure_ascii=False)

    logging.info(f"Cleaned data saved to '{cleaned_csv_filename}' and '{cleaned_json_filename}'.")

def main():
    logging.basicConfig(level=logging.INFO)
    processing_interval = 60 # Polls messages from Kafka and processes them every 60 seconds
    last_process_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Error consuming message: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                messages_buffer.append(data)
            except Exception as e:
                logging.error(f"Error decoding message: {e}")

            current_time = time.time()
            if current_time - last_process_time >= processing_interval:
                process_buffer(messages_buffer)
                messages_buffer.clear()
                last_process_time = current_time

    except KeyboardInterrupt:
        logging.info("Processing interrupted by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
