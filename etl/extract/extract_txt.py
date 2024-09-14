import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import setup_logging

# Get absolute path for logs directory
log_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'etl_log.txt')
log_file_path = os.path.abspath(log_file_path)

logger = setup_logging(log_file_path)

def extract_txt_data(file_path, spark):
    """Extract data from a TXT file with comma delimiter handling and return as a PySpark DataFrame."""
    try:
        logger.info(f"Extracting data from TXT file: {file_path}")
        
        # Read the TXT file into a PySpark DataFrame
        df = spark.read.option("header", "true").option("sep", ",").csv("data/transactions.txt")
        
        # Parse dates if necessary
        if 'timestamp' in df.columns:
            df = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
        
        logger.info(f"Successfully extracted {df.count()} records from TXT file.")
        return df
    except Exception as e:
        logger.error(f"Failed to extract TXT data: {e}")
        raise
