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

Check the logs:

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

total_records,missing_trainNumber,missing_departureTime,missing_arrivalTime,duplicates,anomaly_departureTime,anomaly_arrivalTime
100,0,50,50,3,50,50

Interpretation:

    total_records = 100 → 100 records processed.
    missing_trainNumber = 0 → No records missing the train number.
    missing_departureTime = 50 and missing_arrivalTime = 50 → Half the records had no departure/arrival times.
    duplicates = 3 → Found 3 duplicates based on trainNumber + timestamp.
    anomaly_departureTime = 50 and anomaly_arrivalTime = 50 → Those 50 records had invalid/missing date fields.

2. Chart

output/reports/data_quality_report_YYYYMMDD_HHMMSS.png:

A bar chart illustrating total_records, missing_trainNumber, missing_departureTime, etc.
3. Standardized Data

output/cleaned/cleaned_data_YYYYMMDD_HHMMSS.csv (sample lines):

trainNumber,departureTime,arrivalTime,operator,...
123,2025-02-15,2025-02-15,VR,...
123,2025-02-15,2025-02-15,VR,...
...

    departureTime and arrivalTime columns converted to YYYY-MM-DD.
    Duplicates removed.
    Whitespace trimmed from string fields.

