from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

def custom_transform(df):
    """Custom transformation logic with additional validations for PySpark DataFrame."""
    try:
        # Apply transformations using PySpark SQL functions
        df = df.withColumn('amount', F.when(F.col('amount') >= 0, F.col('amount')).otherwise(None))
        df = df.withColumn('discount', F.coalesce(F.col('discount'), F.lit(0)))
        df = df.withColumn('timestamp', F.to_timestamp(F.col('timestamp'), 'yyyy-MM-dd'))

        # Handling missing values and outliers
        df = df.dropna()
        df = df.filter(F.col('amount') < 10000)  # Example outlier filtering

        return df

    except Exception as e:
        raise ValueError(f"Custom transformation failed: {e}")
