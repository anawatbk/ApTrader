"""
Query builder for S3 stock data filtering and selection
"""

from typing import List, Optional, Union, Tuple
from datetime import datetime, date
import pandas as pd
from .exceptions import DataValidationError


class QueryBuilder:
    """
    Builder class for constructing queries to filter stock data from S3 partitions
    """
    
    def __init__(self):
        self.tickers: Optional[List[str]] = None
        self.years: Optional[List[int]] = None
        self.start_date: Optional[date] = None
        self.end_date: Optional[date] = None
        self.columns: Optional[List[str]] = None
        
    def with_tickers(self, tickers: Union[str, List[str]]) -> 'QueryBuilder':
        """
        Filter by specific ticker symbols
        
        Args:
            tickers: Single ticker string or list of ticker strings
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if isinstance(tickers, str):
            self.tickers = [tickers.upper()]
        else:
            self.tickers = [t.upper() for t in tickers]
        return self
        
    def with_years(self, years: Union[int, List[int]]) -> 'QueryBuilder':
        """
        Filter by specific years
        
        Args:
            years: Single year integer or list of year integers
            
        Returns:
            QueryBuilder instance for method chaining
        """
        if isinstance(years, int):
            self.years = [years]
        else:
            self.years = years
        return self
        
    def with_date_range(self, start_date: Union[str, date, datetime], 
                       end_date: Union[str, date, datetime]) -> 'QueryBuilder':
        """
        Filter by date range
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self.start_date = self._parse_date(start_date)
        self.end_date = self._parse_date(end_date)
        
        if self.start_date > self.end_date:
            raise DataValidationError("Start date must be before or equal to end date")
            
        # Extract years from date range for partition filtering
        start_year = self.start_date.year
        end_year = self.end_date.year
        self.years = list(range(start_year, end_year + 1))
        
        return self
        
    def with_columns(self, columns: List[str]) -> 'QueryBuilder':
        """
        Select specific columns to return
        
        Args:
            columns: List of column names
            
        Returns:
            QueryBuilder instance for method chaining
        """
        self.columns = columns
        return self
        
    def get_partition_paths(self, base_path: str) -> List[str]:
        """
        Generate S3 partition paths based on query parameters
        
        Args:
            base_path: Base S3 path for the data
            
        Returns:
            List of partition paths to query
        """
        paths = []
        
        years = self.years or [datetime.now().year]
        tickers = self.tickers or ['*']  # Wildcard for all tickers
        
        for year in years:
            for ticker in tickers:
                if ticker == '*':
                    # When no specific tickers, get all tickers for the year
                    path = f"{base_path}/year={year}"
                else:
                    path = f"{base_path}/year={year}/ticker={ticker}"
                paths.append(path)
                
        return paths
        
    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply additional filters to the DataFrame after loading from S3
        
        Args:
            df: DataFrame to filter
            
        Returns:
            Filtered DataFrame
        """
        filtered_df = df.copy()
        
        # Apply date range filter if specified
        if self.start_date and self.end_date and 'window_start_et' in df.columns:
            # Convert timestamp column to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(filtered_df['window_start_et']):
                filtered_df['window_start_et'] = pd.to_datetime(filtered_df['window_start_et'])
                
            filtered_df = filtered_df[
                (filtered_df['window_start_et'].dt.date >= self.start_date) &
                (filtered_df['window_start_et'].dt.date <= self.end_date)
            ]
            
        # Apply ticker filter if specified and not already filtered by partition
        if self.tickers and 'ticker' in df.columns:
            filtered_df = filtered_df[filtered_df['ticker'].isin(self.tickers)]
            
        # Select specific columns if specified
        if self.columns:
            # Ensure requested columns exist
            available_columns = set(filtered_df.columns)
            requested_columns = set(self.columns)
            missing_columns = requested_columns - available_columns
            
            if missing_columns:
                raise DataValidationError(f"Columns not found: {missing_columns}")
                
            filtered_df = filtered_df[self.columns]
            
        return filtered_df
        
    def _parse_date(self, date_input: Union[str, date, datetime]) -> date:
        """
        Parse various date input formats to date object
        
        Args:
            date_input: Date in string, date, or datetime format
            
        Returns:
            date object
        """
        if isinstance(date_input, str):
            try:
                return datetime.strptime(date_input, '%Y-%m-%d').date()
            except ValueError:
                try:
                    return datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S').date()
                except ValueError:
                    raise DataValidationError(f"Invalid date format: {date_input}")
        elif isinstance(date_input, datetime):
            return date_input.date()
        elif isinstance(date_input, date):
            return date_input
        else:
            raise DataValidationError(f"Unsupported date type: {type(date_input)}")
            
    def get_query_summary(self) -> dict:
        """
        Get a summary of the current query parameters
        
        Returns:
            Dictionary with query summary
        """
        return {
            'tickers': self.tickers,
            'years': self.years,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'columns': self.columns
        }
    
    def to_sql(self, table_pattern: str = "s3://bucket/path/year=*/ticker=*.parquet") -> str:
        """
        Generate SQL query for DuckDB based on query parameters
        
        Args:
            table_pattern: S3 path pattern for parquet files
            
        Returns:
            SQL query string optimized for DuckDB execution
        """
        # Build SELECT clause
        if self.columns:
            select_clause = ", ".join(self.columns)
        else:
            select_clause = "*"
        
        # Build FROM clause with S3 path pattern
        from_clause = f"'{table_pattern}'"
        
        # Build WHERE clause
        where_conditions = []
        
        # Add year filter for partition pruning
        if self.years:
            if len(self.years) == 1:
                where_conditions.append(f"year = {self.years[0]}")
            else:
                year_list = ", ".join(str(year) for year in self.years)
                where_conditions.append(f"year IN ({year_list})")
        
        # Add ticker filter
        if self.tickers:
            if len(self.tickers) == 1:
                where_conditions.append(f"ticker = '{self.tickers[0]}'")
            else:
                ticker_list = ", ".join(f"'{ticker}'" for ticker in self.tickers)
                where_conditions.append(f"ticker IN ({ticker_list})")
        
        # Add date range filter
        if self.start_date and self.end_date:
            where_conditions.append(
                f"window_start_et >= '{self.start_date}' AND window_start_et <= '{self.end_date}'"
            )
        
        # Combine query parts
        query = f"SELECT {select_clause} FROM {from_clause}"
        
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        
        return query