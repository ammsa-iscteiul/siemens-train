Siemens Data Engineer Project
Overview

This project is a containerized application that:

    Fetches real-time train data from the Digitraffic API.
    Ingests this data into a Kafka topic (Producer).
    Processes the messages (Consumer) to generate:
        A data quality report (number of records, missing fields, duplicates, anomalies).
        Standardized data (dates in YYYY-MM-DD format, duplicate removal, trimming whitespace).
        A bar chart with the report metrics.

Project Structure

siemens-data-engineer/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── producer.py
├── consumer.py
├── output/
│   ├── reports/
│   └── cleaned/
└── README.md

    Dockerfile: Configures the Python environment (including required libraries) for both Producer and Consumer.
    docker-compose.yml: Launches Zookeeper, Kafka, Producer, and Consumer containers.
    requirements.txt: Lists Python dependencies (e.g., confluent-kafka, requests, pandas, matplotlib).
    producer.py: Script that fetches data from the API and sends it to Kafka.
    consumer.py: Script that consumes data from Kafka, generates the quality report, and produces standardized data.
    output/: Main folder for generated files.
        reports/: Where the data quality reports (CSV) and corresponding charts (PNG) are saved.
        cleaned/: Where the cleaned CSV and JSON files are saved.
        Note: These subfolders are automatically created if they do not exist.

How to Build and Run with Docker

    Clone this repository (or download the files).

    Navigate to the project directory:

cd siemens-train

Start the containers using Docker Compose:

docker-compose up --build

    This will:
        Build the Docker image defined by the Dockerfile.
        Start Zookeeper and Kafka.
        Start the producer and consumer services.

Check the logs (Optional):

    In the same terminal, you will see logs for both Producer (sending data to Kafka) and Consumer (generating reports and standardized data).

    To specifically follow producer or consumer logs, open another terminal and run:

    docker-compose logs -f producer
    docker-compose logs -f consumer

Stop the application:

    Press CTRL+C in the terminal running docker-compose up.

    Or run:

        docker-compose down

How to Run the Ingestion Script (Producer) and Generate the Report (Consumer)

    Producer (producer.py):
        Periodically (every X seconds) calls the Digitraffic API and sends data to the live_trains Kafka topic.

    Consumer (consumer.py):
        Consumes messages from the live_trains topic.
        Every 1 minute (by default), it processes the buffered messages and generates:
            A Data Quality Report in output/reports/ (e.g., data_quality_report_YYYYMMDD_HHMMSS.csv) which includes total records, missing fields, duplicates, and date anomalies.
            A Chart (e.g., data_quality_report_YYYYMMDD_HHMMSS_chart.png) in the same reports folder.
            Standardized Data in output/cleaned/ (e.g., cleaned_data_YYYYMMDD_HHMMSS.csv and cleaned_data_YYYYMMDD_HHMMSS.json).

Example Outputs
1. Data Quality Report

output/reports/data_quality_report_YYYYMMDD_HHMMSS.csv:

total_records,missing_trainNumber,missing_departureTime,missing_arrivalTime,duplicates,invalid_date_format,unexpected_data_types
100,0,50,50,3,50,2

Interpretation:

    total_records = 100 → A total of 100 records were processed.
    missing_trainNumber = 0 → No records were missing the train number.
    missing_departureTime = 50 and missing_arrivalTime = 50 → 50 records had missing departure or arrival times.
    duplicates = 3 → 3 duplicate records were found based on trainNumber and departureTime.
    invalid_date_format = 50 → 50 records had incorrectly formatted or missing dates.
    unexpected_data_types = 2 → 2 records contained unexpected data types (e.g., non-numeric values in numerical fields).

2. Chart

output/reports/data_quality_report_YYYYMMDD_HHMMSS.png:

A bar chart visually representing the key metrics from the data quality report, including:

    Total records processed
    Missing values (e.g., train number, departure time, arrival time)
    Duplicate records
    Invalid date formats
    Unexpected data types

3. Standardized Data

output/cleaned/cleaned_data_YYYYMMDD_HHMMSS.csv (sample lines):

trainNumber,departureTime,arrivalTime,operatorShortCode,trainType,trainCategory,cancelled
123,2025-02-15T08:30:00.000Z,2025-02-15T12:45:00.000Z,VR,IC,Long-distance,False
124,2025-02-15T09:00:00.000Z,2025-02-15T13:15:00.000Z,VR,IC,Long-distance,False
...

  Data cleaning and standardization applied:

    Date standardization: departureTime and arrivalTime converted to ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ).
    Duplicates removed: Identical records based on trainNumber and departureTime were eliminated.
    Whitespace trimmed: Leading/trailing spaces removed from all string fields to ensure consistency.

