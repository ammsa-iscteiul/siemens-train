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

Sample Report:
total_records	missing_trainNumber	missing_departureDate	missing_arrivalDate	duplicates	invalid_trainNumber	invalid_departureDate	invalid_arrivalDate
3192	0	0	0	0	0	0	0
2. Interpretation:

    total_records = 3192 → A total of 3192 train records were processed.
    missing_trainNumber = 0 → No records had a missing trainNumber.
    missing_departureDate = 0 → No records had a missing departureDate.
    missing_arrivalDate = 0 → No records had a missing arrivalDate.
    duplicates = 0 → No duplicate records were found based on trainNumber and scheduledTime.
    invalid_trainNumber = 0 → No records had an invalid or incorrectly formatted trainNumber.
    invalid_departureDate = 0 → No records had an invalid departureDate.
    invalid_arrivalDate = 0 → No records had an invalid arrivalDate.

2. Chart

output/reports/data_quality_report_YYYYMMDD_HHMMSS.png:

A bar chart visually representing the key metrics from the data quality report, including:

    Total records processed
    Missing values (train number, departure date, arrival date)
    Duplicate records
    Invalid date formats
    Unexpected data types (if applicable)
    
3. Standardized Data

output/cleaned/cleaned_data_YYYYMMDD_HHMMSS.csv (sample lines):

Sample Data:
trainNumber	departureDate	arrivalDate	operatorShortCode	trainType	trainCategory	cancelled
123	2025-02-15	2025-02-15	VR	IC	Long-distance	False
124	2025-02-15	2025-02-15	VR	IC	Long-distance	False
5. Data Cleaning & Standardization:

    Date standardization: departureDate and arrivalDate are converted to YYYY-MM-DD format.
    Duplicate removal: Identical records based on trainNumber and scheduledTime were eliminated.
    Whitespace trimming: Leading/trailing spaces were removed from all string fields.

