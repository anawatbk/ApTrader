"""
S3 Stock Data Client for downloading partitioned stock data from S3
"""

import os
import logging
from typing import List, Optional, Union, Iterator, Dict, Any
from datetime import datetime, date
import pandas as pd
import s3fs

from .query_builder import QueryBuilder
from .exceptions import (
    S3ClientError,
    ConfigurationError,
    PartitionNotFoundError,
    DataNotFoundError,
    S3ConnectionError,
    DataValidationError
)


class S3StockDataClient:
    """
    Client for downloading and querying stock data from S3 in parquet format
    with year/ticker partitioning
    """
    
    def __init__(self, 
                 bucket: str,
                 base_prefix: str = "parquet",
                 aws_profile: str = "default",
                 aws_region: str = "us-east-1",
                 cache_enabled: bool = False,
                 cache_dir: str = "~/.aptrader_cache",
                 max_memory_usage: str = "1GB",
                 **kwargs):
        """
        Initialize the S3 stock data client
        
        Args:
            bucket: S3 bucket name containing the stock data
            base_prefix: Base prefix path in the bucket (default: "parquet")
            aws_profile: AWS profile to use (default: "default")
            aws_region: AWS region (default: "us-east-1")
            cache_enabled: Enable local caching (default: False)
            cache_dir: Local cache directory (default: "~/.aptrader_cache")
            max_memory_usage: Maximum memory usage limit (default: "1GB")
            **kwargs: Additional S3FileSystem parameters
        """
        self.logger = logging.getLogger(__name__)
        
        # Store configuration
        self.config = {
            'bucket': bucket,
            'base_prefix': base_prefix,
            'aws_profile': aws_profile,
            'aws_region': aws_region,
            'cache_enabled': cache_enabled,
            'cache_dir': cache_dir,
            'max_memory_usage': max_memory_usage
        }
        self.config.update(kwargs)
        
        # Validate required parameters
        if not bucket:
            raise ConfigurationError("Bucket name is required")
        
        # Initialize S3 filesystem
        try:
            s3_kwargs = {
                'profile': aws_profile,
                'anon': False
            }
            # Add any additional S3FileSystem kwargs
            s3_kwargs.update({k: v for k, v in kwargs.items() 
                            if k not in self.config})
            
            self.s3fs = s3fs.S3FileSystem(**s3_kwargs)
        except Exception as e:
            raise S3ConnectionError(f"Failed to initialize S3 connection: {e}")
            
        self.base_path = f"s3://{bucket}/{base_prefix}/minute_aggs"
        
    def get_data(self, 
                 tickers: Optional[Union[str, List[str]]] = None,
                 years: Optional[Union[int, List[int]]] = None,
                 start_date: Optional[Union[str, date, datetime]] = None,
                 end_date: Optional[Union[str, date, datetime]] = None,
                 columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get stock data based on specified filters
        
        Args:
            tickers: Single ticker or list of tickers
            years: Single year or list of years
            start_date: Start date for filtering
            end_date: End date for filtering
            columns: Specific columns to return
            
        Returns:
            DataFrame with filtered stock data
        """
        query = QueryBuilder()
        
        if tickers:
            query.with_tickers(tickers)
        if years:
            query.with_years(years)
        if start_date and end_date:
            query.with_date_range(start_date, end_date)
        if columns:
            query.with_columns(columns)
            
        return self._execute_query(query)
        
    def stream_data(self, 
                   tickers: Optional[Union[str, List[str]]] = None,
                   years: Optional[Union[int, List[int]]] = None,
                   start_date: Optional[Union[str, date, datetime]] = None,
                   end_date: Optional[Union[str, date, datetime]] = None,
                   chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """
        Stream stock data in chunks to handle large datasets
        
        Args:
            tickers: Single ticker or list of tickers
            years: Single year or list of years
            start_date: Start date for filtering
            end_date: End date for filtering
            chunk_size: Number of rows per chunk
            
        Yields:
            DataFrame chunks with filtered stock data
        """
        query = QueryBuilder()
        
        if tickers:
            query.with_tickers(tickers)
        if years:
            query.with_years(years)
        if start_date and end_date:
            query.with_date_range(start_date, end_date)
            
        partition_paths = query.get_partition_paths(self.base_path)
        
        for path in partition_paths:
            try:
                # Check if partition exists
                if not self._partition_exists(path):
                    self.logger.warning(f"Partition not found: {path}")
                    continue
                    
                # Read parquet files in the partition
                parquet_files = self._get_parquet_files(path)
                
                for file_path in parquet_files:
                    try:
                        # Read in chunks
                        parquet_file = self.s3fs.open(file_path, 'rb')
                        df_chunks = pd.read_parquet(parquet_file, chunksize=chunk_size)
                        
                        for chunk in df_chunks:
                            filtered_chunk = query.apply_filters(chunk)
                            if not filtered_chunk.empty:
                                yield filtered_chunk
                                
                    except Exception as e:
                        self.logger.error(f"Error reading file {file_path}: {e}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error processing partition {path}: {e}")
                continue

    def list_partitions(self,
                        year: Optional[int] = None,
                        ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List available partitions in S3
        
        Args:
            year: Filter by specific year (optional)
            ticker: Filter by specific ticker across all years (optional)
            
        Returns:
            List of partition information dictionaries with keys:
            - 'year': Year of the partition
            - 'ticker': Ticker symbol
            - 'path': S3 path to the partition
            - 'files': Number of parquet files in the partition
            
        Examples:
            list_partitions()  # All partitions
            list_partitions(year=2024)  # All tickers for 2024
            list_partitions(ticker="AAPL")  # AAPL across all years
            list_partitions(year=2024, ticker="AAPL")  # AAPL for 2024 only
        """
        partitions = []
        
        try:
            # anawatp-us-stocks/parquet/minute_aggs
            base_search_path = self.base_path.replace('s3://', '')
            
            if year and ticker:
                search_path = f"{base_search_path}/year={year}/ticker={ticker.upper()}"
                if self.s3fs.exists(search_path):
                    partitions.append({
                        'year': year,
                        'ticker': ticker.upper(),
                        'path': f"s3://{search_path}",
                    })
            elif year:
                search_path = f"{base_search_path}/year={year}"
                if self.s3fs.exists(search_path):
                    ticker_paths = self.s3fs.ls(search_path, detail=False)
                    for ticker_path in ticker_paths:
                        ticker_name = ticker_path.split('ticker=')[-1]
                        partitions.append({
                            'year': year,
                            'ticker': ticker_name,
                            'path': f"s3://{ticker_path}",
                        })
            elif ticker:
                # List all years for a specific ticker
                year_paths = self.s3fs.ls(base_search_path, detail=False)
                for year_path in year_paths:
                    if 'year=' in year_path:
                        year_num = int(year_path.split('year=')[-1])
                        ticker_path = f"{year_path}/ticker={ticker.upper()}"
                        if self.s3fs.exists(ticker_path):
                            partitions.append({
                                'year': year_num,
                                'ticker': ticker.upper(),
                                'path': f"s3://{ticker_path}",
                            })
            else:
                # List all years
                year_paths = self.s3fs.ls(base_search_path)
                for year_path in year_paths:
                    if 'year=' in year_path:
                        year_num = int(year_path.split('year=')[-1])
                        ticker_paths = self.s3fs.ls(year_path, detail=False)
                        for ticker_path in ticker_paths:
                            if 'ticker=' in ticker_path:
                                ticker_name = ticker_path.split('ticker=')[-1]
                                partitions.append({
                                    'year': year_num,
                                    'ticker': ticker_name,
                                    'path': f"s3://{ticker_path}",
                                })
                                
        except Exception as e:
            raise S3ClientError(f"Failed to list partitions: {e}")
            
        return partitions
        
    def get_available_tickers(self, year: Optional[int] = None) -> List[str]:
        """
        Get list of available tickers
        
        Args:
            year: Filter by specific year
            
        Returns:
            List of ticker symbols
        """
        partitions = self.list_partitions(year=year)
        return sorted(list(set(p['ticker'] for p in partitions)))
        
    def get_available_years(self) -> List[int]:
        """
        Get list of available years
        
        Returns:
            List of years
        """
        partitions = self.list_partitions()
        return sorted(list(set(p['year'] for p in partitions)))
        
    def _execute_query(self, query: QueryBuilder) -> pd.DataFrame:
        """Execute a query and return the results as a DataFrame"""
        partition_paths = query.get_partition_paths(self.base_path)
        dataframes = []
        
        for path in partition_paths:
            try:
                if not self._partition_exists(path):
                    self.logger.warning(f"Partition not found: {path}")
                    continue
                    
                # Get all parquet files in the partition
                parquet_files = self._get_parquet_files(path)
                
                for file_path in parquet_files:
                    try:
                        df = pd.read_parquet(file_path, filesystem=self.s3fs)
                        dataframes.append(df)
                    except Exception as e:
                        self.logger.error(f"Error reading file {file_path}: {e}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error processing partition {path}: {e}")
                continue
                
        if not dataframes:
            raise DataNotFoundError(query.get_query_summary())
            
        # Combine all DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Apply additional filters
        filtered_df = query.apply_filters(combined_df)
        
        if filtered_df.empty:
            raise DataNotFoundError(query.get_query_summary())
            
        return filtered_df
        
    def _partition_exists(self, partition_path: str) -> bool:
        """Check if a partition path exists in S3"""
        try:
            s3_path = partition_path.replace('s3://', '')
            return self.s3fs.exists(s3_path)
        except Exception:
            return False
            
    def _get_parquet_files(self, partition_path: str) -> List[str]:
        """Get list of parquet files in a partition"""
        try:
            s3_path = partition_path.replace('s3://', '')
            files = self.s3fs.glob(f"{s3_path}/*.parquet")
            return [f"s3://{f}" for f in files]
        except Exception as e:
            self.logger.error(f"Error listing files in {partition_path}: {e}")
            return []
            
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the S3 connection and data availability
        
        Returns:
            Dictionary with health check results
        """
        health_status = {
            'status': 'healthy',
            's3_connection': False,
            'bucket_accessible': False,
            'partitions_found': 0,
            'issues': []
        }
        
        try:
            # Test S3 connection
            self.s3fs.ls('/')
            health_status['s3_connection'] = True
        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['issues'].append(f"S3 connection failed: {e}")
            
        try:
            # Test bucket access
            bucket = self.config.get('bucket')
            if bucket:
                self.s3fs.ls(bucket)
                health_status['bucket_accessible'] = True
            else:
                health_status['issues'].append("Data bucket not configured")
        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['issues'].append(f"Bucket access failed: {e}")
            
        try:
            # Count available partitions
            partitions = self.list_partitions()
            health_status['partitions_found'] = len(partitions)
            if len(partitions) == 0:
                health_status['issues'].append("No data partitions found")
        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['issues'].append(f"Failed to list partitions: {e}")
            
        return health_status