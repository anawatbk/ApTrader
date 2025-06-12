"""
Custom exceptions for S3 Stock Data Client
"""


class S3ClientError(Exception):
    """Base exception for S3 stock data client errors"""
    pass


class ConfigurationError(S3ClientError):
    """Raised when there are configuration issues"""
    pass


class PartitionNotFoundError(S3ClientError):
    """Raised when requested partition doesn't exist in S3"""
    
    def __init__(self, partition_path: str, message: str = None):
        self.partition_path = partition_path
        if message is None:
            message = f"Partition not found: {partition_path}"
        super().__init__(message)


class DataNotFoundError(S3ClientError):
    """Raised when no data is found for the given query parameters"""
    
    def __init__(self, query_params: dict, message: str = None):
        self.query_params = query_params
        if message is None:
            message = f"No data found for query: {query_params}"
        super().__init__(message)


class S3ConnectionError(S3ClientError):
    """Raised when there are S3 connection issues"""
    pass


class DataValidationError(S3ClientError):
    """Raised when data doesn't meet expected format or schema"""
    pass