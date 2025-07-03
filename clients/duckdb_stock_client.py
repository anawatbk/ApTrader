"""
DuckDB Stock Data Client for high-performance analytical queries on S3 parquet data
"""

from typing import List, Optional, Union
from datetime import datetime, date
import pandas as pd
import duckdb


class DuckDBStockClient:
    """
    Simple DuckDB client for querying stock data from S3 parquet files
    """
    
    def __init__(self, bucket: str = "anawatp-us-stocks", region: str = "us-west-2"):
        """
        Initialize the DuckDB stock data client
        
        Args:
            bucket: S3 bucket name containing the stock data (default: "anawatp-us-stocks")
            region: AWS region (default: "us-west-2")
        """
        self.bucket = bucket
        self.con = duckdb.connect()
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute(f"SET s3_region='{region}';")
        
        # Set AWS credentials from boto3 session
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials:
            self.con.execute(f"SET s3_access_key_id='{credentials.access_key}';")
            self.con.execute(f"SET s3_secret_access_key='{credentials.secret_key}';")
            if credentials.token:
                self.con.execute(f"SET s3_session_token='{credentials.token}';")
            
        # Ensure we use the correct region for the bucket
        self.con.execute(f"SET s3_region='{region}';")  # Use the bucket's actual region
        
        # Optimize DuckDB performance settings
        self.con.execute("SET memory_limit = '6GB';")  # Optimal memory limit
        self.con.execute("SET threads = 6;")  # Optimal thread count
        self.con.execute("SET enable_object_cache = true;")  # Enable S3 object caching
        self.con.execute("SET s3_url_style = 'vhost';")  # Use virtual hosted-style URLs
        self.con.execute("SET s3_use_ssl = true;")  # Ensure SSL is enabled
    
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
            start_date: Start date for filtering (YYYY-MM-DD format)
            end_date: End date for filtering (YYYY-MM-DD format)
            columns: Specific columns to return
            
        Returns:
            DataFrame with filtered stock data
        """
        # Build SELECT clause
        select_columns = "*" if not columns else ", ".join(columns)
        
        # Build WHERE clause (only for non-partition filters since partition targeting is in paths)
        non_partition_conditions = []
        if start_date and end_date:
            non_partition_conditions.append(f"window_start_et::date BETWEEN '{start_date}'::date AND '{end_date}'::date")
        
        where_clause = " AND ".join(non_partition_conditions) if non_partition_conditions else "1=1"
        
        # Build smart S3 paths based on filters to avoid full table scans
        s3_paths = self._build_optimized_s3_paths(tickers, years)
        
        # Build final query with targeted partition paths using array syntax
        if len(s3_paths) == 1:
            # Single path - more efficient
            query = f"""
                SELECT {select_columns}
                FROM read_parquet(
                    '{s3_paths[0]}',
                    hive_partitioning = 1
                )
                WHERE {where_clause}
                ORDER BY window_start_et, ticker
            """
        else:
            # Multiple paths - use read_parquet array syntax (faster than UNION ALL)
            paths_str = "', '".join(s3_paths)
            query = f"""
                SELECT {select_columns}
                FROM read_parquet(
                    ['{paths_str}'],
                    hive_partitioning = 1
                )
                WHERE {where_clause}
                ORDER BY window_start_et, ticker
            """
        
        return self.con.execute(query).df()
    
    def _build_optimized_s3_paths(self, tickers, years):
        """
        Build specific S3 paths based on query filters to avoid full table scans
        """
        base_path = f"s3://{self.bucket}/parquet/minute_aggs"
        
        # Default to current year if no years specified
        if not years:
            from datetime import datetime
            years = [datetime.now().year]
        elif isinstance(years, int):
            years = [years]
            
        # Default to all tickers if none specified (use wildcard for single year)
        if not tickers:
            if len(years) == 1:
                # Single year, all tickers - use year-level wildcard
                return [f"{base_path}/year={years[0]}/ticker=*/*.parquet"]
            else:
                # Multiple years, all tickers - fallback to full wildcard
                return [f"{base_path}/year=*/ticker=*/*.parquet"]
        else:
            # Specific tickers - use read_parquet with array of paths
            if isinstance(tickers, str):
                tickers = [tickers]
            
            # Build array of specific partition paths for read_parquet
            paths = []
            for year in years:
                for ticker in tickers:
                    paths.append(f"{base_path}/year={year}/ticker={ticker.upper()}/*.parquet")
            
            # If multiple paths, return as single pattern for read_parquet to handle
            if len(paths) == 1:
                return paths
            else:
                # For multiple specific paths, return each path separately for UNION ALL
                return paths