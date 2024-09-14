Fintech Data Pipeline Project

Overview

The Fintech Data Pipeline project is designed to handle multi-format data (CSV, JSON, TXT, and Parquet) for a financial dataset. It extracts, transforms, and loads the data into a PostgreSQL database using PySpark, dbt for SQL transformations, and custom schema migrations. The project includes advanced logging, schema versioning, and unit testing to ensure data integrity and proper operation.

Project Structure

graphql

fintech_data_pipeline/
│
├── config/
│   └── db_config.py        # Database connection parameters
│
├── dags/
│   └── run_etl.py          # Script to trigger the entire ETL process
│
├── data/
│   └── generate_data.py     # Script to generate sample CSV, TXT, Parquet, and JSON data
│
├── etl/
│   ├── extract/
│   │   ├── extract_csv.py   # Extract data from CSV
│   │   ├── extract_json.py  # Extract data from JSON
│   │   ├── extract_parquet.py  # Extract data from Parquet
│   │   └── extract_txt.py   # Extract data from TXT
│   │
│   ├── transform/
│   │   ├── custom_transform.py  # Custom transformations using PySpark
│   │   └── dbt_transform.sql    # SQL-based transformations using dbt
│   │
│   ├── schema/
│   │   └── migration.py     # Schema migrations for PostgreSQL
│   │
│   └── load/
│       └── load_postgres.py  # Load data into PostgreSQL
│
├── logs/
│   └── etl_log.txt          # Log file for the ETL process
│
├── tests/
│   ├── test_extract_csv.py   # Unit test for CSV extraction
│   ├── test_extract_json.py  # Unit test for JSON extraction
│   ├── test_extract_parquet.py  # Unit test for Parquet extraction
│   ├── test_extract_txt.py   # Unit test for TXT extraction
│   ├── test_transform.py     # Unit test for PySpark and dbt transformations
│   ├── test_load.py          # Unit test for PostgreSQL loading
│   └── test_migration.py     # Unit test for schema migrations
│
├── utils/
│   └── logger.py             # Logging utility for the project
│
└── README.md                 # Project documentation (this file)

Features

Data Extraction: Extract data from multiple formats (CSV, JSON, TXT, Parquet) using PySpark.

Data Transformation: Perform data transformations using both PySpark and dbt SQL-based transformations.

Schema Management: Includes schema migrations for PostgreSQL to ensure the table structure evolves without data loss.

Data Loading: Load processed data into a PostgreSQL database with support for automatic table creation.

Logging: A logging utility that writes logs to both a log file and the console for easy debugging.

Testing: Comprehensive unit tests for each stage of the ETL pipeline.

Setup Instructions

Prerequisites
Python 3.8+: Ensure Python is installed.
PostgreSQL: Install PostgreSQL and ensure it is running on your system.
PySpark: Install PySpark for distributed data processing.
dbt: Install dbt for SQL-based transformations.
psycopg2 & SQLAlchemy: Python libraries for connecting and interacting with PostgreSQL.

Installation
Clone the repository:

bash

git clone https://github.com/your-repo/fintech_data_pipeline.git

cd fintech_data_pipeline

Install the required dependencies:

pip install -r requirements.txt
Configure the database connection by updating the config/db_config.py file with your PostgreSQL credentials.

Running the Pipeline

Generate Sample Data: Run the generate_data.py script to create sample data files (CSV, JSON, TXT, Parquet):

python data/generate_data.py --rows 1000
Trigger the ETL Process: Run the run_etl.py script to start the entire ETL pipeline:

python dags/run_etl.py

Logging

All logs are saved to logs/etl_log.txt and also printed to the console. You can modify the log file path in utils/logger.py.

Unit Testing
Run the unit tests to ensure each component is working as expected:

pytest tests/

ETL Process

Extraction: Data is extracted from various file formats (CSV, JSON, TXT, Parquet) using the respective extract scripts. Each extraction script returns a PySpark DataFrame for further processing.

Transformation:

PySpark Custom Transform: Data undergoes cleansing and enrichment using custom transformation logic (e.g., handling missing values, outliers).

dbt Transformations: SQL-based transformations are handled by dbt, allowing advanced calculations such as ranking.

Schema Migration: PostgreSQL schema migrations are applied, ensuring the database structure is updated based on the incoming data.

Load: The transformed data is loaded into PostgreSQL using load_postgres.py, creating the table if it doesn’t already exist.

Utils: Logging

The utils/logger.py script sets up logging for the project. It writes logs to both a file and the console with detailed timestamped messages.

# Example usage:
from utils.logger import setup_logging

logger = setup_logging('/path/to/logfile.txt')
logger.info('This is an info log message.')