import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from etl.extract.extract_json import extract_json_data

@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("Test").getOrCreate()

@patch('extract_json.logger')
@patch('extract_json.SparkSession')
def test_extract_json_data(mock_spark_session, mock_logger, spark):
    mock_spark_session.read.json.return_value = spark.createDataFrame([(1, "test")], ["id", "value"])
    
    df = extract_json_data('data/transactions.json', spark)
    assert df.count() == 1
    assert 'id' in df.columns
    assert 'value' in df.columns

    # Check log messages
    mock_logger.info.assert_called_with("Successfully extracted 1 records from JSON.")
