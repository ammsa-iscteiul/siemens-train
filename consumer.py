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
all_processed_data = pd.DataFrame()

# Reads the data quality report CSV and creates a bar chart
def generate_report_chart(report_csv_filename):
    """Lê o relatório CSV e gera um gráfico de barras."""
    
    if not os.path.exists(report_csv_filename):
        logging.error(f"Report file '{report_csv_filename}' not found. Cannot generate chart.")
        return

    df = pd.read_csv(report_csv_filename)

    if df.empty:
        logging.error(f"Report file '{report_csv_filename}' is empty. Cannot generate chart.")
        return

    try:
        # Convertendo os valores corretamente
        report_data = df.iloc[0].to_dict()
        metrics = list(report_data.keys())
        values = list(report_data.values())

        # Criar o gráfico
        plt.figure(figsize=(10, 6))
        bars = plt.bar(metrics, values, color="blue")
        plt.title("Data Quality Report Metrics")
        plt.xlabel("Metrics")
        plt.ylabel("Values")
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Adicionar os valores no topo das barras
        for bar in bars:
            height = bar.get_height()
            plt.annotate(f'{int(height)}', 
                         xy=(bar.get_x() + bar.get_width() / 2, height),
                         xytext=(0, 5),
                         textcoords="offset points",
                         ha='center', va='bottom')

        # Definir o nome correto do ficheiro do gráfico
        chart_filename = report_csv_filename.replace('.csv', '_chart.png')
        plt.savefig(chart_filename)
        plt.close()

        logging.info(f"Chart successfully saved to '{chart_filename}'")

    except Exception as e:
        logging.error(f"Failed to generate chart: {e}")


# Normalize and remove unexpected Unicode encoding issues
def normalize_unicode(text):
    if isinstance(text, str):
        return unicodedata.normalize("NFKC", text).strip()
    return text

# Cleans the raw train record while maintaining the original structure
def clean_train_record(train):
    if not isinstance(train, dict):
        return {}

    cleaned_train = {}
    for key, value in train.items():
        if isinstance(value, str):
            cleaned_train[key] = normalize_unicode(value)
        elif isinstance(value, list):
            cleaned_train[key] = [clean_train_record(item) for item in value]
        elif isinstance(value, dict):
            cleaned_train[key] = clean_train_record(value)
        else:
            cleaned_train[key] = value

    if "timeTableRows" in cleaned_train and not isinstance(cleaned_train["timeTableRows"], list):
        cleaned_train["timeTableRows"] = []

    return cleaned_train

# Processes accumulated messages, generates a data quality report, standardizes data
def process_buffer(buffer):
    
    logging.info(f"Received {len(buffer)} messages in buffer")
    if len(buffer) == 0:
        logging.warning("Buffer is empty, skipping processing.")
        return

    cleaned_data = [clean_train_record(record) for record in buffer if isinstance(record, dict)]
    for item in buffer:
        if isinstance(item, list):
            for record in item:
                if isinstance(record, dict):
                    cleaned_data.append(clean_train_record(record))
        elif isinstance(item, dict):
            cleaned_data.append(clean_train_record(item))
        else:
            logging.warning(f"Unexpected data format: {type(item)} - {item}")

    df = pd.DataFrame(cleaned_data)

    # Explode timeTableRows to ensure scheduledTime is accessible at train level
    if "timeTableRows" in df.columns:
        logging.info(f"DataFrame size before exploding timeTableRows: {df.shape}")
        df = df.explode("timeTableRows")
        df = df[df["timeTableRows"].notna()]
        df = pd.concat([df.drop(columns=["timeTableRows"]), df["timeTableRows"].apply(pd.Series)], axis=1)
        logging.info(f"DataFrame size after exploding timeTableRows: {df.shape}")
        
    # Standardize date format for scheduledTime
    if "scheduledTime" in df.columns:
        df["scheduledTime"] = pd.to_datetime(df["scheduledTime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # Count duplicates correctly
    duplicate_count = df.duplicated(subset=["trainNumber", "scheduledTime"]).sum()

    # Build Data Quality Report
    report = {
        "total_records": len(df),
        "missing_trainNumber": df["trainNumber"].isnull().sum() if "trainNumber" in df.columns else 0,
        "missing_departureDate": df["departureDate"].isnull().sum() if "departureDate" in df.columns else 0,
        "missing_arrivalDate": df["arrivalDate"].isnull().sum() if "arrivalDate" in df.columns else 0,
        "duplicates": duplicate_count,
    }

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_csv_filename = os.path.join(REPORT_DIR, f"data_quality_report_{now_str}.csv")

    try:
        report_df = pd.DataFrame([report])
        report_df.to_csv(report_csv_filename, index=False)
        generate_report_chart(report_csv_filename)
        logging.info(f"Data quality report successfully saved to '{report_csv_filename}'")
    except Exception as e:
        logging.error(f"Failed to save data quality report: {e}")

    if duplicate_count > 0:
        try:
            duplicates_filename = os.path.join(REPORT_DIR, f"duplicate_entries_{now_str}.csv")
            duplicates_df = df[df.duplicated(subset=["trainNumber", "scheduledTime"], keep=False)]
            duplicates_df.to_csv(duplicates_filename, index=False)
            logging.info(f"Duplicate records successfully saved to '{duplicates_filename}'")
        except Exception as e:
            logging.error(f"Failed to save duplicate entries: {e}")

    df = df.drop_duplicates(subset=["trainNumber", "scheduledTime"])

    cleaned_csv_filename = os.path.join(CLEANED_DIR, f"cleaned_data_{now_str}.csv")
    cleaned_json_filename = os.path.join(CLEANED_DIR, f"cleaned_data_{now_str}.json")
    try:
        with open(cleaned_json_filename, "w", encoding="utf-8") as f:
            json.dump(df.to_dict(orient="records"), f, indent=4, ensure_ascii=False)
        df.to_csv(cleaned_csv_filename, index=False)
        logging.info(f"Cleaned data saved to '{cleaned_csv_filename}' and '{cleaned_json_filename}'.")    
    except Exception as e:
        logging.error(f"Failed to save cleaned CSV: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    processing_interval = 60
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
