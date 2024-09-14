import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from utils.logger import setup_logging
from urllib.parse import quote_plus

logger = setup_logging('/path/to/logs/etl_log.txt')

def load_data_to_postgres(df, table_name, conn_params):
    """Load data to PostgreSQL with advanced settings."""
    try:
        logger.info(f"Loading data into PostgreSQL table: {table_name}")

        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()

        # URL-encode the password to handle special characters
        password_encoded = quote_plus(conn_params['password'])

        # Establish PostgreSQL connection
        conn = psycopg2.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            dbname=conn_params['dbname'],
            user=conn_params['user'],
            password=conn_params['password']
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            transaction_id INTEGER PRIMARY KEY,
            amount FLOAT,
            timestamp TIMESTAMP,
            category VARCHAR(255),
            status VARCHAR(50),
            discount FLOAT,
            discounted_amount FLOAT,
            year INTEGER,
            month INTEGER,
            rank INTEGER
        )
        """).format(table=sql.Identifier(table_name))
        cursor.execute(create_table_query)

        # Use SQLAlchemy to create the engine with URL-encoded password
        engine = create_engine(
            f"postgresql+psycopg2://{conn_params['user']}:{password_encoded}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
        )

        # Insert data into PostgreSQL
        pandas_df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Data loaded successfully into PostgreSQL.")
    except Exception as e:
        logger.error(f"Failed to load data into PostgreSQL: {e}")
        raise
