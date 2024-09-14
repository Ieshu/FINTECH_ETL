import psycopg2
from psycopg2 import sql
from utils.logger import setup_logging

logger = setup_logging('/path/to/logs/etl_log.txt')

def apply_schema_migration(conn_params):
    """Apply schema migration to PostgreSQL."""
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # First, ensure the table exists by creating it if not
        create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY,
            amount NUMERIC,
            timestamp TIMESTAMP,
            category VARCHAR(255),
            status VARCHAR(50),
            discount NUMERIC,
            discounted_amount NUMERIC,
            year INTEGER,
            month INTEGER,
            rank INTEGER
        );
        """)
        cursor.execute(create_table_query)

        # Now, apply the schema migration to add new columns
        migration_query = sql.SQL("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name='transactions' AND column_name='rank'
            ) THEN
                ALTER TABLE transactions ADD COLUMN rank INTEGER;
            END IF;
        END
        $$;
        """)
        cursor.execute(migration_query)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Schema migration applied successfully.")
    except Exception as e:
        logger.error(f"Schema migration failed: {e}")
        raise
