"""
Comprehensive test suite for S3 Stock Data Client
"""

from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from clients.exceptions import (
    DataNotFoundError,
    S3ConnectionError,
    DataValidationError
)
from clients.query_builder import QueryBuilder
from clients.s3_stock_client import S3StockDataClient


class TestQueryBuilder:
    """Test cases for QueryBuilder class"""
    
    def test_with_tickers_single(self):
        """Test adding single ticker"""
        query = QueryBuilder().with_tickers('AAPL')
        assert query.tickers == ['AAPL']
        
    def test_with_tickers_multiple(self):
        """Test adding multiple tickers"""
        query = QueryBuilder().with_tickers(['AAPL', 'msft', 'googl'])
        assert query.tickers == ['AAPL', 'MSFT', 'GOOGL']
        
    def test_with_years_single(self):
        """Test adding single year"""
        query = QueryBuilder().with_years(2024)
        assert query.years == [2024]
        
    def test_with_years_multiple(self):
        """Test adding multiple years"""
        query = QueryBuilder().with_years([2023, 2024])
        assert query.years == [2023, 2024]
        
    def test_with_date_range(self):
        """Test date range filtering"""
        query = QueryBuilder().with_date_range('2024-01-01', '2024-12-31')
        assert query.start_date == date(2024, 1, 1)
        assert query.end_date == date(2024, 12, 31)
        assert query.years == [2024]
        
    def test_with_date_range_multi_year(self):
        """Test date range spanning multiple years"""
        query = QueryBuilder().with_date_range('2023-06-01', '2024-06-01')
        assert query.start_date == date(2023, 6, 1)
        assert query.end_date == date(2024, 6, 1)
        assert query.years == [2023, 2024]
        
    def test_invalid_date_range(self):
        """Test invalid date range raises error"""
        with pytest.raises(DataValidationError):
            QueryBuilder().with_date_range('2024-12-31', '2024-01-01')
            
    def test_with_columns(self):
        """Test column selection"""
        query = QueryBuilder().with_columns(['open', 'high', 'low', 'close'])
        assert query.columns == ['open', 'high', 'low', 'close']
        
    def test_get_partition_paths_basic(self):
        """Test partition path generation"""
        query = QueryBuilder().with_tickers(['AAPL']).with_years([2024])
        paths = query.get_partition_paths('s3://bucket/data')
        assert paths == ['s3://bucket/data/year=2024/ticker=AAPL']
        
    def test_get_partition_paths_multiple(self):
        """Test partition path generation with multiple tickers and years"""
        query = QueryBuilder().with_tickers(['AAPL', 'MSFT']).with_years([2023, 2024])
        paths = query.get_partition_paths('s3://bucket/data')
        expected = [
            's3://bucket/data/year=2023/ticker=AAPL',
            's3://bucket/data/year=2023/ticker=MSFT',
            's3://bucket/data/year=2024/ticker=AAPL',
            's3://bucket/data/year=2024/ticker=MSFT'
        ]
        assert sorted(paths) == sorted(expected)
        
    def test_apply_filters_date_range(self):
        """Test applying date range filters to DataFrame"""
        df = pd.DataFrame({
            'window_start_et': pd.to_datetime(['2024-01-01', '2024-06-01', '2024-12-31']),
            'ticker': ['AAPL', 'AAPL', 'AAPL'],
            'close': [100, 110, 120]
        })
        
        query = QueryBuilder().with_date_range('2024-01-01', '2024-06-30')
        filtered_df = query.apply_filters(df)
        
        assert len(filtered_df) == 2
        assert filtered_df['window_start_et'].dt.date.max() <= date(2024, 6, 30)
        
    def test_apply_filters_columns(self):
        """Test applying column selection filters"""
        df = pd.DataFrame({
            'window_start_et': pd.to_datetime(['2024-01-01']),
            'ticker': ['AAPL'],
            'open': [95],
            'close': [100],
            'volume': [1000]
        })
        
        query = QueryBuilder().with_columns(['ticker', 'close'])
        filtered_df = query.apply_filters(df)
        
        assert list(filtered_df.columns) == ['ticker', 'close']
        
    def test_apply_filters_missing_columns(self):
        """Test error when requested columns don't exist"""
        df = pd.DataFrame({
            'ticker': ['AAPL'],
            'close': [100]
        })
        
        query = QueryBuilder().with_columns(['ticker', 'nonexistent'])
        with pytest.raises(DataValidationError):
            query.apply_filters(df)
            
    def test_get_query_summary(self):
        """Test query summary generation"""
        query = (QueryBuilder()
                .with_tickers(['AAPL'])
                .with_date_range('2024-01-01', '2024-12-31')
                .with_columns(['close']))
        
        summary = query.get_query_summary()
        
        assert summary['tickers'] == ['AAPL']
        assert summary['start_date'] == '2024-01-01'
        assert summary['end_date'] == '2024-12-31'
        assert summary['columns'] == ['close']


class TestS3StockDataClient:
    """Test cases for S3StockDataClient class"""
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing"""
        return {
            'bucket': 'test-bucket',
            'base_prefix': 'test-data',
            'aws_profile': 'test',
            'aws_region': 'us-east-1',
            'cache_enabled': True
        }
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_init_with_parameters(self, mock_s3fs, sample_config):
        """Test client initialization with parameters"""
        client = S3StockDataClient(**sample_config)
        
        assert client.config['aws_region'] == 'us-east-1'
        assert client.config['aws_profile'] == 'test'
        assert client.config['bucket'] == 'test-bucket'
        assert client.config['base_prefix'] == 'test-data'
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_init_with_defaults(self, mock_s3fs):
        """Test client initialization with default values"""
        client = S3StockDataClient(bucket='test-bucket')
        
        assert client.config['bucket'] == 'test-bucket'
        assert client.config['base_prefix'] == 'parquet'
        assert client.config['aws_profile'] == 'default'
        assert client.config['cache_enabled'] is False
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_init_s3_connection_error(self, mock_s3fs):
        """Test S3 connection error handling"""
        mock_s3fs.side_effect = Exception("S3 connection failed")
        
        with pytest.raises(S3ConnectionError):
            S3StockDataClient(bucket='test-bucket')
            
    def test_config_missing_bucket(self):
        """Test error when bucket is not provided"""
        with patch('clients.s3_stock_client.s3fs.S3FileSystem'):
            with pytest.raises(TypeError):  # Missing required argument
                S3StockDataClient()
                
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_get_data_basic(self, mock_s3fs):
        """Test basic data retrieval"""
        # Mock S3 filesystem
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        # Mock partition existence and file listing
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['test-bucket/parquet/year=2024/ticker=AAPL/file1.parquet']
        
        # Mock DataFrame reading
        test_df = pd.DataFrame({
            'window_start_et': pd.to_datetime(['2024-01-01']),
            'ticker': ['AAPL'],
            'close': [100]
        })
        
        with patch('pandas.read_parquet', return_value=test_df):
            client = S3StockDataClient(bucket='test-bucket')
            result = client.get_data(tickers='AAPL', years=2024)
            
            assert not result.empty
            assert result['ticker'].iloc[0] == 'AAPL'
            
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_get_data_not_found(self, mock_s3fs):
        """Test data not found error"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        mock_fs.exists.return_value = False
        
        client = S3StockDataClient(bucket='test-bucket')
        
        with pytest.raises(DataNotFoundError):
            client.get_data(tickers='NONEXISTENT', years=2024)
            
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_get_data_date_range_filtering(self, mock_s3fs):
        """Test date range filtering in get_data method"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        # Mock partition existence and file listing
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['test-bucket/parquet/year=2024/ticker=AAPL/file1.parquet']
        
        # Create test DataFrame with timestamps spanning multiple dates
        test_df = pd.DataFrame({
            'window_start_et': pd.to_datetime([
                '2024-01-01 10:00:00',  # Should be included
                '2024-01-02 11:00:00',  # Should be included  
                '2024-01-03 12:00:00',  # Should be included (end date boundary)
                '2024-01-04 13:00:00',  # Should be excluded
                '2023-12-31 14:00:00'   # Should be excluded
            ]),
            'ticker': ['AAPL'] * 5,
            'close': [100, 101, 102, 103, 99],
            'volume': [1000, 1100, 1200, 1300, 900]
        })
        
        with patch('pandas.read_parquet', return_value=test_df):
            client = S3StockDataClient(bucket='test-bucket')
            
            # Test date range filtering
            result = client.get_data(
                tickers='AAPL', 
                years=2024,
                start_date='2024-01-01',
                end_date='2024-01-03'
            )
            
            # Should only include records from 2024-01-01 to 2024-01-03
            assert not result.empty, "Result should not be empty"
            assert len(result) == 3, f"Expected 3 records, got {len(result)}"
            
            # Check that all returned dates are within range
            result_dates = pd.to_datetime(result['window_start_et']).dt.date
            assert result_dates.min() >= date(2024, 1, 1), "Min date should be >= start_date"
            assert result_dates.max() <= date(2024, 1, 3), "Max date should be <= end_date"
            
            # Check specific excluded records
            assert 103 not in result['close'].values, "Record from 2024-01-04 should be excluded"
            assert 99 not in result['close'].values, "Record from 2023-12-31 should be excluded"
            
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_get_data_date_range_no_matches(self, mock_s3fs):
        """Test date range filtering when no data matches the range"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['test-bucket/parquet/year=2024/ticker=AAPL/file1.parquet']
        
        # Create test DataFrame with timestamps outside the query range
        test_df = pd.DataFrame({
            'window_start_et': pd.to_datetime([
                '2024-01-10 10:00:00',  # All outside range
                '2024-01-11 11:00:00',
                '2024-01-12 12:00:00'
            ]),
            'ticker': ['AAPL'] * 3,
            'close': [100, 101, 102]
        })
        
        with patch('pandas.read_parquet', return_value=test_df):
            client = S3StockDataClient(bucket='test-bucket')
            
            # Test date range that doesn't match any data
            with pytest.raises(DataNotFoundError):
                client.get_data(
                    tickers='AAPL',
                    years=2024, 
                    start_date='2024-01-01',
                    end_date='2024-01-05'
                )
                
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_get_data_date_range_boundary_conditions(self, mock_s3fs):
        """Test date range boundary conditions (inclusive/exclusive)"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['test-bucket/parquet/year=2024/ticker=AAPL/file1.parquet']
        
        # Create test DataFrame with exact boundary timestamps
        test_df = pd.DataFrame({
            'window_start_et': pd.to_datetime([
                '2024-01-01 00:00:00',  # Exact start boundary
                '2024-01-01 12:00:00',  # Within range
                '2024-01-03 23:59:59',  # Near end boundary
                '2024-01-04 00:00:00'   # Just outside end boundary
            ]),
            'ticker': ['AAPL'] * 4,
            'close': [100, 101, 102, 103],
            'volume': [1000, 1100, 1200, 1300]
        })
        
        with patch('pandas.read_parquet', return_value=test_df):
            client = S3StockDataClient(bucket='test-bucket')
            
            result = client.get_data(
                tickers='AAPL',
                years=2024,
                start_date='2024-01-01', 
                end_date='2024-01-03'
            )
            
            # Should include start boundary but exclude records after end date
            assert not result.empty, "Result should not be empty"
            # The exact count depends on whether the filtering is inclusive/exclusive
            # This test will reveal the actual behavior
            result_dates = pd.to_datetime(result['window_start_et']).dt.date
            
            # At minimum, should include start date
            assert date(2024, 1, 1) in result_dates.values, "Start date should be included"
            
            # Should not include dates after end date
            assert date(2024, 1, 4) not in result_dates.values, "Dates after end_date should be excluded"
            
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')  
    def test_get_data_cross_year_date_range(self, mock_s3fs):
        """Test date range filtering across multiple years"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        mock_fs.exists.return_value = True 
        mock_fs.glob.return_value = ['test-bucket/parquet/year=2023/ticker=AAPL/file1.parquet',
                                    'test-bucket/parquet/year=2024/ticker=AAPL/file1.parquet']
        
        # Create test DataFrame spanning across years
        test_df = pd.DataFrame({
            'window_start_et': pd.to_datetime([
                '2023-12-30 10:00:00',  # Should be excluded
                '2023-12-31 10:00:00',  # Should be included
                '2024-01-01 10:00:00',  # Should be included
                '2024-01-02 10:00:00'   # Should be excluded
            ]),
            'ticker': ['AAPL'] * 4,
            'close': [99, 100, 101, 102]
        })
        
        with patch('pandas.read_parquet', return_value=test_df):
            client = S3StockDataClient(bucket='test-bucket')
            
            result = client.get_data(
                tickers='AAPL',
                years=[2023, 2024],  # Multiple years
                start_date='2023-12-31',
                end_date='2024-01-01'
            )
            
            assert not result.empty, "Result should not be empty"
            
            # Check that only records within the cross-year range are included
            result_dates = pd.to_datetime(result['window_start_et']).dt.date
            assert date(2023, 12, 30) not in result_dates.values, "Date before range should be excluded"
            assert date(2024, 1, 2) not in result_dates.values, "Date after range should be excluded"
            
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_list_partitions_by_year(self, mock_s3fs):
        """Test partition listing by year"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        # Mock directory structure
        mock_fs.exists.return_value = True
        mock_fs.ls.return_value = ['test-bucket/parquet/year=2024/ticker=AAPL']
        mock_fs.glob.return_value = ['file1.parquet', 'file2.parquet']
        
        client = S3StockDataClient(bucket='test-bucket')
        partitions = client.list_partitions(year=2024)
        
        assert len(partitions) > 0
        assert partitions[0]['year'] == 2024
        assert partitions[0]['ticker'] == 'AAPL'
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_list_partitions_by_ticker(self, mock_s3fs):
        """Test partition listing by ticker across all years"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        # Mock directory structure - multiple years
        mock_fs.ls.side_effect = [
            # First call: list all years
            ['test-bucket/parquet/year=2023', 'test-bucket/parquet/year=2024'],
        ]
        
        # Mock ticker path existence for each year
        def mock_exists(path):
            return 'ticker=AAPL' in path
        mock_fs.exists.side_effect = mock_exists
        
        mock_fs.glob.return_value = ['file1.parquet', 'file2.parquet']
        
        client = S3StockDataClient(bucket='test-bucket')
        partitions = client.list_partitions(ticker='AAPL')
        
        assert len(partitions) == 2
        assert partitions[0]['ticker'] == 'AAPL'
        assert partitions[1]['ticker'] == 'AAPL'
        assert partitions[0]['year'] == 2023
        assert partitions[1]['year'] == 2024
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_list_partitions_by_year_and_ticker(self, mock_s3fs):
        """Test partition listing by both year and ticker"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        # Mock specific partition exists
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['file1.parquet', 'file2.parquet']
        
        client = S3StockDataClient(bucket='test-bucket')
        partitions = client.list_partitions(year=2024, ticker='AAPL')
        
        assert len(partitions) == 1
        assert partitions[0]['year'] == 2024
        assert partitions[0]['ticker'] == 'AAPL'
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_list_partitions_ticker_not_found(self, mock_s3fs):
        """Test partition listing when ticker doesn't exist in any year"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        # Mock directory structure - multiple years exist
        mock_fs.ls.return_value = ['test-bucket/parquet/year=2023', 'test-bucket/parquet/year=2024']
        
        # Mock ticker path doesn't exist for any year
        mock_fs.exists.return_value = False
        
        client = S3StockDataClient(bucket='test-bucket')
        partitions = client.list_partitions(ticker='NONEXISTENT')
        
        assert len(partitions) == 0
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_list_partitions_ticker_case_insensitive(self, mock_s3fs):
        """Test that ticker filtering is case insensitive"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        # Mock directory structure
        mock_fs.ls.return_value = ['test-bucket/parquet/year=2024']
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['file1.parquet']
        
        client = S3StockDataClient(bucket='test-bucket')
        
        # Test with lowercase ticker
        partitions = client.list_partitions(ticker='aapl')
        
        assert len(partitions) == 1
        assert partitions[0]['ticker'] == 'AAPL'  # Should be uppercase in result
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_get_available_tickers(self, mock_s3fs):
        """Test getting available tickers"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        mock_fs.ls.return_value = ['test-bucket/parquet/year=2024/ticker=AAPL',
                                  'test-bucket/parquet/year=2024/ticker=MSFT']
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['file1.parquet']
        
        client = S3StockDataClient(bucket='test-bucket')
        tickers = client.get_available_tickers(year=2024)
        
        assert 'AAPL' in tickers
        assert 'MSFT' in tickers
        assert len(tickers) == 2
        
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_stream_data(self, mock_s3fs):
        """Test data streaming functionality"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        mock_fs.exists.return_value = True
        mock_fs.glob.return_value = ['test-bucket/parquet/year=2024/ticker=AAPL/file1.parquet']
        mock_fs.open.return_value = Mock()
        
        # Mock chunked reading
        test_df = pd.DataFrame({
            'window_start_et': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'ticker': ['AAPL', 'AAPL'],
            'close': [100, 101]
        })
        
        with patch('pandas.read_parquet', return_value=[test_df]):
            client = S3StockDataClient(bucket='test-bucket')
            chunks = list(client.stream_data(tickers='AAPL', years=2024, chunk_size=1))
            
            assert len(chunks) > 0
            
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_health_check_healthy(self, mock_s3fs):
        """Test health check when everything is working"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        mock_fs.ls.return_value = ['some-files']
        mock_fs.exists.return_value = True
        
        client = S3StockDataClient(bucket='test-bucket')
        
        # Mock list_partitions to return some partitions
        with patch.object(client, 'list_partitions', return_value=[{'year': 2024, 'ticker': 'AAPL'}]):
            health = client.health_check()
            
            assert health['status'] == 'healthy'
            assert health['s3_connection'] is True
            assert health['bucket_accessible'] is True
            assert health['partitions_found'] == 1
            
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_health_check_unhealthy(self, mock_s3fs):
        """Test health check when there are issues"""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        mock_fs.ls.side_effect = Exception("Access denied")
        
        client = S3StockDataClient(bucket='test-bucket')
        health = client.health_check()
        
        assert health['status'] == 'unhealthy'
        assert len(health['issues']) > 0


if __name__ == '__main__':
    pytest.main([__file__])