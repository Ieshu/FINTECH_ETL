import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from etl.load.load_postgres import load_data_to_postgres

@pytest.fixture
def sample_dataframe():
    data = {
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
    return pd.DataFrame(data)

@patch('load_postgres.psycopg2.connect')
@patch('load_postgres.create_engine')
def test_load_data_to_postgres(mock_create_engine, mock_psycopg2_connect, sample_dataframe):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    
    conn_params = {
        'dbname': 'stock_warehouse',
        'user': 'postgres',
        'password': 'ies123@IES123',
        'host': 'localhost',
        'port': '5432'
}

    try:
        load_data_to_postgres(sample_dataframe, 'test_transactions', conn_params)
        mock_psycopg2_connect.assert_called_once_with(
            host='localhost',
            port=5432,
            dbname='stock_warehouse',
            user='postgres',
            password='ies123@IES123'
        )
        mock_create_engine.assert_called_once()
        mock_engine.connect().execute.assert_called()  # Ensure to check calls to engine
        assert True  # Placeholder assertion
    except Exception as e:
        pytest.fail(f"Exception raised: {e}")
