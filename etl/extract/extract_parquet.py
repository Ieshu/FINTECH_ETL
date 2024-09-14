import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.logger import setup_logging

# Get absolute path for logs directory
log_file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'etl_log.txt')
log_file_path = os.path.abspath(log_file_path)

logger = setup_logging(log_file_path)

def extract_parquet_data(file_path, spark):
    """Extract data from Parquet file and return as a PySpark DataFrame."""
    try:
        logger.info(f"Extracting data from Parquet file: {file_path}")
        df = spark.read.parquet(file_path)
        
        # Check if 'timestamp' column exists before conversion
        if 'timestamp' in df.columns:
            df = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
        else:
            logger.warning("The 'timestamp' column is missing from the Parquet file.")
        
        logger.info(f"Successfully extracted {df.count()} records from Parquet.")
        return df
    except Exception as e:
        logger.error(f"Failed to extract Parquet data: {e}")
        raise
