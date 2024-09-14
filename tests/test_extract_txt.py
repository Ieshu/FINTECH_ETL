import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from etl.extract.extract_txt import extract_txt_data

@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("Test").getOrCreate()

@patch('extract_txt.logger')
@patch('extract_txt.SparkSession')
def test_extract_txt_data(mock_spark_session, mock_logger, spark):
    mock_spark_session.read.option.return_value.csv.return_value = spark.createDataFrame([(1, "test", "2023-09-01")], ["id", "value", "timestamp"])
    
    df = extract_txt_data('data/transactions.txt', spark)
    assert df.count() == 1
    assert 'id' in df.columns
    assert 'timestamp' in df.columns

    # Check log messages
    mock_logger.info.assert_called_with("Successfully extracted 1 records from TXT file.")
