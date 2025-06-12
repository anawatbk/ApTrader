"""
S3 Stock Data Client Package

This package provides clients for downloading and querying stock data from S3
in parquet format with year/ticker partitioning.
"""

from .s3_stock_client import S3StockDataClient
from .query_builder import QueryBuilder
from .exceptions import (
    S3ClientError,
    PartitionNotFoundError,
    ConfigurationError,
    DataNotFoundError,
    S3ConnectionError,
    DataValidationError
)

__all__ = [
    'S3StockDataClient',
    'QueryBuilder', 
    'S3ClientError',
    'PartitionNotFoundError',
    'ConfigurationError',
    'DataNotFoundError',
    'S3ConnectionError',
    'DataValidationError'
]