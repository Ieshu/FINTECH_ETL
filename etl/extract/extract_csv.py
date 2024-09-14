import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import setup_logging

# Set up the log file path
log_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'etl_log.txt')
log_file_path = os.path.abspath(log_file_path)

# Set up logging (log file path and optional log level)
logger = setup_logging(log_file_path, log_level=logging.INFO)

def extract_csv_data(file_path, spark):
    """Extract data from CSV file and return as a PySpark DataFrame."""
    try:
        logger.info(f"Extracting data from CSV file: {file_path}")
        
        # Read CSV file into PySpark DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        # Parse dates if necessary
        if 'timestamp' in df.columns:
            df = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
        
        logger.info(f"Successfully extracted {df.count()} records from CSV.")
        return df
    except Exception as e:
        logger.error(f"Failed to extract CSV data: {e}")
        raise
