import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the parent directory of 'etl' to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.extract.extract_csv import extract_csv_data
from etl.extract.extract_json import extract_json_data
from etl.extract.extract_txt import extract_txt_data
from etl.extract.extract_parquet import extract_parquet_data
from etl.transform.custom_transform import custom_transform
from etl.load.load_postgres import load_data_to_postgres
from etl.schema.migration import apply_schema_migration
from data.generate_data import generate_data
from config.db_config import DB_CONN_PARAMS

def run_etl_pipeline():
    """Run the complete ETL pipeline."""
    
    # Generate synthetic data
    generate_data(1000)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Pipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    
    # Extract data
    csv_data = extract_csv_data('data/transactions.csv', spark)
    json_data = extract_json_data('data/transactions.json', spark)
    txt_data = extract_txt_data('data/transactions.txt', spark)
    parquet_data = extract_parquet_data('data/transactions.parquet', spark)
    
    # Check for and filter out corrupt records
    if "_corrupt_record" in json_data.columns:
        json_data = json_data.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    
    if "_corrupt_record" in txt_data.columns:
        txt_data = txt_data.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    
    # Combine data from all sources
    combined_df = csv_data.unionByName(json_data, allowMissingColumns=True) \
                          .unionByName(txt_data, allowMissingColumns=True) \
                          .unionByName(parquet_data, allowMissingColumns=True)
    
    # Rename duplicate columns to avoid conflicts
    combined_df = rename_duplicate_columns(combined_df)
    
    # Handle NaN values
    combined_df = handle_nan_values(combined_df)
    
    print("************** Combined DataFrame **********")
    combined_df.show()
    
    # Transform the combined DataFrame using custom logic
    final_df = custom_transform(combined_df)
    
    # Schema migration before loading
    print("Initiating schema migration process")
    apply_schema_migration(DB_CONN_PARAMS)
    
    # Load the transformed data into PostgreSQL
    print("****************")
    print("Loading data into PostgreSQL")
    print("+++++++++++++++++++")
    load_data_to_postgres(final_df, 'transactions', DB_CONN_PARAMS)

    # Stop the Spark session
    spark.stop()

def rename_duplicate_columns(df):
    """
    Rename duplicate columns in a DataFrame by appending a suffix if they are duplicated.
    """
    cols = df.columns
    seen = set()
    new_cols = []
    for col in cols:
        new_col = col
        count = 1
        while new_col in seen:
            new_col = f"{col}_{count}"
            count += 1
        seen.add(new_col)
        new_cols.append(new_col)
    return df.toDF(*new_cols)

def handle_nan_values(df):
    """
    Handle NaN values in the DataFrame by filling or dropping them.
    """
    # Drop rows where all columns are NaN
    df = df.na.drop(how='all')
    
    # Fill NaN values for specific columns with a default value
    df = df.na.fill({
        'transaction_id': 'unknown',  # Example for string column
        'amount': 0,                  # Example for numerical column
        'category': 'uncategorized'    # Example for string column
    })
    
    return df

if __name__ == '__main__':
    run_etl_pipeline()
