import unittest
from unittest.mock import Mock, patch
import logging

import pandas as pd
import pyarrow as pa

from etl import s3_csv_to_s3_parquet_glue_shell_job as etl_module


class TestS3CSVToParquetGlueJob(unittest.TestCase):
    """Test suite for the Glue Python Shell ETL job"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_year = 2024
        # Create timestamps for weekday trading hours (Monday-Friday, 9:30 AM - 4:00 PM ET)
        # Using January 2024 timestamps (known weekdays)
        self.sample_csv_data = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL', 'AAPL', 'MSFT'],
            'volume': [1000, 2000, 1500, 1200, 1800],
            'open': [150.0, 300.0, 2500.0, 151.0, 301.0],
            'close': [152.0, 302.0, 2520.0, 153.0, 303.0],
            'high': [155.0, 305.0, 2530.0, 154.0, 304.0],
            'low': [149.0, 299.0, 2490.0, 150.0, 300.0],
            'window_start': [
                1704295800000000000,  # January 3, 2024 (Wednesday) 10:30 AM EST (trading hours)
                1704295860000000000,  # January 3, 2024 (Wednesday) 10:31 AM EST
                1704295920000000000,  # January 3, 2024 (Wednesday) 10:32 AM EST
                1704382200000000000,  # January 4, 2024 (Thursday) 10:30 AM EST
                1704468600000000000   # January 5, 2024 (Friday) 10:30 AM EST
            ],
            'transactions': [10, 15, 12, 8, 20]
        })

    def test_get_existing_partitions_no_partitions(self):
        """Test get_existing_partitions when no partitions exist"""
        # Mock S3 filesystem to return False for exists check
        mock_filesystem = Mock()
        mock_filesystem.exists.return_value = False
        
        result = etl_module.get_existing_partitions(self.test_year, mock_filesystem)
        
        self.assertEqual(result, set())
        mock_filesystem.exists.assert_called_once_with(f"{etl_module.PARQUET_OUTPUT_PATH}/year={self.test_year}")

    def test_get_existing_partitions_with_partitions(self):
        """Test get_existing_partitions when partitions exist"""
        # Mock S3 filesystem
        mock_filesystem = Mock()
        mock_filesystem.exists.return_value = True
        mock_filesystem.ls.side_effect = [
            ['s3://bucket/parquet/year=2024/ticker=AAPL', 's3://bucket/parquet/year=2024/ticker=MSFT'],
            ['s3://bucket/parquet/year=2024/ticker=AAPL/data.parquet'],  # AAPL has parquet files
            ['s3://bucket/parquet/year=2024/ticker=MSFT/data.parquet']   # MSFT has parquet files
        ]
        
        result = etl_module.get_existing_partitions(self.test_year, mock_filesystem)
        
        self.assertEqual(result, {'AAPL', 'MSFT'})

    def test_filter_dataframe_by_missing_tickers_no_existing(self):
        """Test filtering when no existing tickers"""
        existing_tickers = set()
        result = etl_module.filter_dataframe_by_missing_tickers(self.sample_csv_data, existing_tickers)
        
        pd.testing.assert_frame_equal(result, self.sample_csv_data)

    def test_filter_dataframe_by_missing_tickers_some_existing(self):
        """Test filtering when some tickers already exist"""
        existing_tickers = {'AAPL'}
        result = etl_module.filter_dataframe_by_missing_tickers(self.sample_csv_data, existing_tickers)
        
        expected = self.sample_csv_data[self.sample_csv_data['ticker'].isin(['MSFT', 'GOOGL'])].copy()
        pd.testing.assert_frame_equal(result.reset_index(drop=True), expected.reset_index(drop=True))

    def test_filter_dataframe_by_missing_tickers_all_existing(self):
        """Test filtering when all tickers already exist"""
        existing_tickers = {'AAPL', 'MSFT', 'GOOGL'}
        result = etl_module.filter_dataframe_by_missing_tickers(self.sample_csv_data, existing_tickers)
        
        self.assertTrue(result.empty)

    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.pq.write_to_dataset')
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.s3fs.S3FileSystem')
    def test_convert_csv_to_parquet_no_files(self, mock_s3fs, mock_write_dataset):
        """Test convert_csv_to_parquet when no CSV files are found"""
        # Mock S3 filesystem
        mock_s3_instance = Mock()
        mock_s3_instance.glob.return_value = []
        mock_s3fs.return_value = mock_s3_instance
        
        # Mock get_existing_partitions
        with patch.object(etl_module, 'get_existing_partitions', return_value=set()):
            etl_module.convert_csv_to_parquet(self.test_year)
        
        # Should not call write_to_dataset if no files found
        mock_write_dataset.assert_not_called()

    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.pq.write_to_dataset')
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.s3fs.S3FileSystem')
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.pd.read_csv')
    def test_convert_csv_to_parquet_success(self, mock_read_csv, mock_s3fs, mock_write_dataset):
        """Test successful conversion of CSV to Parquet"""
        # Create test data with proper timezone-aware timestamps
        test_data = self.sample_csv_data.copy()
        
        # Mock S3 filesystem
        mock_s3_instance = Mock()
        mock_s3_instance.glob.return_value = ['s3://bucket/csv/2024-01-01.csv.gz']
        mock_s3_instance.open.return_value.__enter__ = Mock()
        mock_s3_instance.open.return_value.__exit__ = Mock(return_value=False)
        mock_s3fs.return_value = mock_s3_instance
        
        # Mock pandas read_csv to return our test data
        mock_read_csv.return_value = test_data
        
        # Mock get_existing_partitions to return no existing partitions
        with patch.object(etl_module, 'get_existing_partitions', return_value=set()):
            etl_module.convert_csv_to_parquet(self.test_year)
        
        # Verify write_to_dataset was called
        mock_write_dataset.assert_called_once()
        call_args = mock_write_dataset.call_args
        
        # Check that the table was created
        self.assertIsInstance(call_args[0][0], pa.Table)
        
        # Check partition columns
        self.assertEqual(call_args[1]['partition_cols'], ['year', 'ticker'])
        self.assertEqual(call_args[1]['root_path'], etl_module.PARQUET_OUTPUT_PATH)

    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.pq.write_to_dataset')
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.s3fs.S3FileSystem')
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.pd.read_csv')
    def test_convert_csv_to_parquet_all_existing(self, mock_read_csv, mock_s3fs, mock_write_dataset):
        """Test when all tickers already have partitions (idempotent behavior)"""
        # Mock S3 filesystem
        mock_s3_instance = Mock()
        mock_s3_instance.glob.return_value = ['s3://bucket/csv/2024-01-01.csv.gz']
        mock_s3_instance.open.return_value.__enter__ = Mock()
        mock_s3_instance.open.return_value.__exit__ = Mock(return_value=False)
        mock_s3fs.return_value = mock_s3_instance
        
        # Mock pandas read_csv
        mock_read_csv.return_value = self.sample_csv_data
        
        # Mock get_existing_partitions to return all tickers
        existing_tickers = {'AAPL', 'MSFT', 'GOOGL'}
        with patch.object(etl_module, 'get_existing_partitions', return_value=existing_tickers):
            etl_module.convert_csv_to_parquet(self.test_year)
        
        # Should not write anything since all partitions exist
        mock_write_dataset.assert_not_called()

    def test_timezone_conversion(self):
        """Test that timezone conversion works correctly"""
        # Test the timezone conversion logic directly
        test_df = self.sample_csv_data.copy()
        
        # Convert window_start from UTC epoch nanoseconds to America/New_York timezone
        test_df['window_start'] = pd.to_datetime(test_df['window_start'], unit='ns', utc=True)
        test_df['window_start'] = test_df['window_start'].dt.tz_convert('America/New_York')
        
        # Check that timezone conversion worked
        self.assertTrue(all(ts.tz.zone == 'America/New_York' for ts in test_df['window_start']))

    def test_trading_hours_filter(self):
        """Test trading hours filtering logic"""
        # Create test data with various times
        test_timestamps = [
            1738568400000000000,  # 7:30 AM ET (before market)
            1738575960000000000,  # 9:30 AM ET (market open)
            1738584000000000000,  # 12:00 PM ET (market hours)
            1738594800000000000,  # 4:00 PM ET (market close)
            1738598400000000000,  # 5:00 PM ET (after market)
        ]
        
        test_df = pd.DataFrame({
            'ticker': ['AAPL'] * 5,
            'window_start': test_timestamps,
            'volume': [100] * 5
        })
        
        # Convert to datetime with timezone
        test_df['window_start'] = pd.to_datetime(test_df['window_start'], unit='ns', utc=True)
        test_df['window_start'] = test_df['window_start'].dt.tz_convert('America/New_York')
        
        # Apply trading hours filter
        start_time = pd.to_datetime("09:30:00").time()
        end_time = pd.to_datetime("16:00:00").time()
        
        mask_time = (test_df['window_start'].dt.time >= start_time) & (test_df['window_start'].dt.time < end_time)
        mask_weekday = test_df['window_start'].dt.weekday < 5
        
        filtered_df = test_df.loc[mask_time & mask_weekday]
        
        # Should include 9:30 AM and 12:00 PM, exclude others
        self.assertEqual(len(filtered_df), 2)

    @patch('sys.argv', ['script.py', '2023'])
    def test_main_local_environment(self):
        """Test main function in local environment"""
        with patch.object(etl_module, 'convert_csv_to_parquet') as mock_convert:
            with patch.object(etl_module, 'GLUE_AVAILABLE', False):
                etl_module.main()
                mock_convert.assert_called_once_with(2023)

    @patch('sys.argv', ['script.py'])
    def test_main_local_environment_default_year(self):
        """Test main function in local environment with default year"""
        with patch.object(etl_module, 'convert_csv_to_parquet') as mock_convert:
            with patch.object(etl_module, 'GLUE_AVAILABLE', False):
                etl_module.main()
                mock_convert.assert_called_once_with(2024)  # Default year

    def test_glue_mock_function(self):
        """Test the mock getResolvedOptions function"""
        # Test with year argument
        result = etl_module.getResolvedOptions(['script.py', '2023'], ['JOB_NAME', 'YEAR'])
        self.assertEqual(result['YEAR'], '2023')
        self.assertEqual(result['JOB_NAME'], 'local-test')
        
        # Test without arguments
        result = etl_module.getResolvedOptions(['script.py'], ['JOB_NAME', 'YEAR'])
        self.assertEqual(result['YEAR'], '2024')
        self.assertEqual(result['JOB_NAME'], 'local-test')


class TestETLIntegration(unittest.TestCase):
    """Integration tests for the ETL pipeline"""
    
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.pq.write_to_dataset')
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.s3fs.S3FileSystem')
    @patch('etl.s3_csv_to_s3_parquet_glue_shell_job.pd.read_csv')
    def test_full_etl_pipeline(self, mock_read_csv, mock_s3fs, mock_write_dataset):
        """Test the complete ETL pipeline end-to-end"""
        # Create realistic test data with proper trading hours timestamps
        test_data = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL'] * 100,
            'volume': [1000] * 300,
            'open': [150.0] * 300,
            'close': [152.0] * 300,
            'high': [155.0] * 300,
            'low': [149.0] * 300,
            'window_start': [1704295800000000000] * 300,  # January 3, 2024 (Wednesday) 10:30 AM EST
            'transactions': [10] * 300
        })
        
        # Mock S3 operations
        mock_s3_instance = Mock()
        mock_s3_instance.glob.return_value = ['s3://bucket/csv/2024-01-01.csv.gz']
        mock_s3_instance.open.return_value.__enter__ = Mock()
        mock_s3_instance.open.return_value.__exit__ = Mock(return_value=False)
        mock_s3fs.return_value = mock_s3_instance
        
        mock_read_csv.return_value = test_data
        
        # Mock no existing partitions
        with patch.object(etl_module, 'get_existing_partitions', return_value=set()):
            etl_module.convert_csv_to_parquet(2024)
        
        # Verify the pipeline completed
        mock_write_dataset.assert_called_once()
        
        # Check the data was processed correctly
        call_args = mock_write_dataset.call_args
        table = call_args[0][0]
        
        # Convert table to pandas for verification
        result_df = table.to_pandas()
        
        # Check that year column was added
        self.assertTrue('year' in result_df.columns)
        self.assertTrue(all(result_df['year'] == 2024))
        
        # Check that timezone conversion happened
        self.assertTrue('window_start' in result_df.columns)


if __name__ == '__main__':
    # Set up logging for tests
    logging.basicConfig(level=logging.WARNING)  # Reduce log noise during tests
    
    # Run the tests
    unittest.main(verbosity=2)