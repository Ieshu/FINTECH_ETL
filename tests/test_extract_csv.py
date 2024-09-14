import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from etl.extract.extract_csv import extract_csv_data

@patch('extract_csv.pd.read_csv')
def test_extract_from_csv(mock_read_csv):
    # Mock DataFrame returned by read_csv
    mock_df = MagicMock()
    mock_df.to_dict.return_value = {
        'transaction_id': [1, 2],
        'amount': [100.0, 200.0],
        'timestamp': ['2023-09-01', '2023-09-02'],
        'category': ['A', 'B'],
        'status': ['completed', 'failed'],
        'discount': [0.1, 0.2],
        'discounted_amount': [90.0, 160.0],
        'year': [2023, 2023],
        'month': [9, 9],
        'rank': [1, 2]
    }
    mock_read_csv.return_value = mock_df
    
    file_path = 'data/transactions.csv'
    data = extract_csv_data(file_path)
    
    assert len(data) == 2
    assert data[0]['transaction_id'] == 1
    assert data[1]['amount'] == 200.0

