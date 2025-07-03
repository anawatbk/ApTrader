"""
Simplified test suite for DuckDB Stock Data Client
"""

from unittest.mock import Mock, patch
import pandas as pd
import pytest

from clients.duckdb_stock_client import DuckDBStockClient


class TestDuckDBStockClient:
    """Test cases for simplified DuckDBStockClient class"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.test_bucket = "test-bucket"
        
    @patch('duckdb.connect')
    def test_init_success(self, mock_connect):
        """Test successful client initialization"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        
        assert client.bucket == self.test_bucket
        mock_connect.assert_called_once_with()
        
        # Verify DuckDB setup calls
        execute_calls = mock_conn.execute.call_args_list
        call_commands = [call[0][0] for call in execute_calls]
        
        assert "INSTALL httpfs; LOAD httpfs;" in call_commands
        assert any("SET s3_region=" in cmd for cmd in call_commands)
        
    @patch('duckdb.connect')
    def test_init_with_custom_region(self, mock_connect):
        """Test initialization with custom region"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        client = DuckDBStockClient(
            bucket=self.test_bucket,
            region="us-east-1"
        )
        
        execute_calls = mock_conn.execute.call_args_list
        call_commands = [call[0][0] for call in execute_calls]
        
        assert any("SET s3_region='us-east-1'" in cmd for cmd in call_commands)
        
    @patch('duckdb.connect')
    def test_get_data_basic(self, mock_connect):
        """Test basic get_data functionality"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        # Create sample DataFrame
        test_data = pd.DataFrame({
            'ticker': ['AAPL', 'AAPL'],
            'window_start_et': ['2024-01-01', '2024-01-02'],
            'close': [150.0, 155.0],
            'year': [2024, 2024]
        })
        
        mock_conn.execute.return_value.df.return_value = test_data
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        
        # Execute query
        result = client.get_data(
            tickers=['AAPL'],
            years=[2024],
            start_date='2024-01-01',
            end_date='2024-01-02'
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'ticker' in result.columns
        
        # Verify SQL query was built correctly
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "ticker IN ('AAPL')" in executed_query
        assert "year IN (2024)" in executed_query
        assert "window_start_et::date BETWEEN '2024-01-01'::date AND '2024-01-02'::date" in executed_query
        
    @patch('duckdb.connect')
    def test_get_data_single_ticker_string(self, mock_connect):
        """Test get_data with single ticker as string"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        client.get_data(tickers='AAPL')
        
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "ticker IN ('AAPL')" in executed_query
        
    @patch('duckdb.connect')
    def test_get_data_multiple_tickers(self, mock_connect):
        """Test get_data with multiple tickers"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        client.get_data(tickers=['AAPL', 'msft', 'googl'])
        
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "ticker IN ('AAPL','MSFT','GOOGL')" in executed_query
        
    @patch('duckdb.connect')
    def test_get_data_single_year(self, mock_connect):
        """Test get_data with single year as integer"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        client.get_data(years=2024)
        
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "year IN (2024)" in executed_query
        
    @patch('duckdb.connect')
    def test_get_data_multiple_years(self, mock_connect):
        """Test get_data with multiple years"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        client.get_data(years=[2023, 2024])
        
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "year IN (2023,2024)" in executed_query
        
    @patch('duckdb.connect')
    def test_get_data_specific_columns(self, mock_connect):
        """Test get_data with specific columns"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        client.get_data(columns=['ticker', 'close', 'volume'])
        
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "SELECT ticker, close, volume" in executed_query
        
    @patch('duckdb.connect')
    def test_get_data_no_filters(self, mock_connect):
        """Test get_data with no filters"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        client.get_data()
        
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "SELECT *" in executed_query
        assert "WHERE 1=1" in executed_query
        assert f"s3://{self.test_bucket}/parquet/minute_aggs/year=*/ticker=*/*.parquet" in executed_query
        
    @patch('duckdb.connect')
    def test_hive_partitioning_enabled(self, mock_connect):
        """Test that hive partitioning is enabled in queries"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()
        
        client = DuckDBStockClient(bucket=self.test_bucket)
        client.get_data()
        
        executed_query = mock_conn.execute.call_args_list[-1][0][0]
        assert "hive_partitioning = 1" in executed_query