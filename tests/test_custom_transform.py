import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from etl.transform.custom_transform import custom_transform

@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("Test").getOrCreate()

@pytest.fixture
def sample_dataframe(spark):
    data = [
        (1, 100.0, '2023-09-01', 'A', 'completed', 0.1, 90.0, 2023, 9, 1),
        (2, 200.0, '2023-09-02', 'B', 'failed', 0.2, 160.0, 2023, 9, 2)
    ]
    columns = ['transaction_id', 'amount', 'timestamp', 'category', 'status', 'discount', 'discounted_amount', 'year', 'month', 'rank']
    return spark.createDataFrame(data, columns)

def test_custom_transform(spark, sample_dataframe):
    try:
        transformed_df = custom_transform(sample_dataframe)
        assert transformed_df.count() == 2
        assert 'amount' in transformed_df.columns
        assert 'discount' in transformed_df.columns
    except Exception as e:
        pytest.fail(f"Exception raised: {e}")
