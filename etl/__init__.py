"""
ETL Package for ApTrader

This package contains ETL scripts for processing stock market data:
- CSV to Parquet conversion with timezone handling
- S3 data processing pipelines
- AWS Glue job implementations
- Local development utilities
"""

__version__ = "1.0.0"
__author__ = "ApTrader Team"

# Import main ETL functions for easy access
try:
    from .s3_csv_to_s3_parquet_job import convert_csv_to_parquet as local_convert_csv_to_parquet
except ImportError:
    local_convert_csv_to_parquet = None

try:
    from .s3_csv_to_s3_parquet_glue_shell_job import (
        convert_csv_to_parquet as glue_convert_csv_to_parquet,
        get_existing_partitions,
        filter_dataframe_by_missing_tickers,
        main as glue_main
    )
except ImportError:
    glue_convert_csv_to_parquet = None
    get_existing_partitions = None
    filter_dataframe_by_missing_tickers = None
    glue_main = None

# Define what gets imported with "from etl import *"
__all__ = [
    'local_convert_csv_to_parquet',
    'glue_convert_csv_to_parquet',
    'get_existing_partitions',
    'filter_dataframe_by_missing_tickers',
    'glue_main',
]