import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import setup_logging

# Get absolute path for logs directory
log_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'etl_log.txt')
log_file_path = os.path.abspath(log_file_path)

logger = setup_logging(log_file_path)

def extract_json_data(file_path, spark):
    """Extract data from JSON file and return as a PySpark DataFrame."""
    try:
        logger.info(f"Extracting data from JSON file: {file_path}")
        df = spark.read.json(file_path)
        
        # Normalize JSON if necessary
        if df.columns:
            df = df.select([col(c) for c in df.columns])
        
        logger.info(f"Successfully extracted {df.count()} records from JSON.")
        return df
    except Exception as e:
        logger.error(f"Failed to extract JSON data: {e}")
        raise
