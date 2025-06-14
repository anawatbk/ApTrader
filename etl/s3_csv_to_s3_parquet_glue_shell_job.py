import logging
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# Handle AWS Glue imports for local development
try:
    from awsglue.utils import getResolvedOptions
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False
    def getResolvedOptions(argv, options):
        """Mock implementation for local testing"""
        if len(argv) < 2:
            print("Local testing mode - using default year 2024")
            return {'JOB_NAME': 'local-test', 'YEAR': '2024'}
        else:
            # Parse local command line arguments
            year = argv[1] if len(argv) > 1 else '2024'
            return {'JOB_NAME': 'local-test', 'YEAR': year}

S3_CSV_INPUT_TEMPLATE = 's3://anawatp-us-stocks/csv/{year}-*.csv.gz'
PARQUET_OUTPUT_PATH = 's3://anawatp-us-stocks/parquet'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize s3fs
BUCKET = 'anawatp-us-stocks'
PREFIX = 'csv'  # base prefix, we'll filter for 2025 later


def get_existing_partitions(year: int, s3_filesystem):
    """
    Check for existing parquet partitions for a given year.
    Returns a set of ticker symbols that already have partitions.
    """
    year_partition_path = f"{PARQUET_OUTPUT_PATH}/year={year}"
    
    try:
        # Check if year partition exists
        if not s3_filesystem.exists(year_partition_path):
            logger.info(f"No existing partitions found for year {year}")
            return set()
        
        # List all ticker partitions for this year
        ticker_dirs = s3_filesystem.ls(year_partition_path, detail=False)
        existing_tickers = set()
        
        for ticker_dir in ticker_dirs:
            # Extract ticker from path like "s3://bucket/parquet/year=2024/ticker=AAPL"
            if '/ticker=' in ticker_dir:
                ticker = ticker_dir.split('/ticker=')[-1]
                # Check if partition has actual parquet files
                partition_files = s3_filesystem.ls(ticker_dir, detail=False)
                parquet_files = [f for f in partition_files if f.endswith('.parquet')]
                if parquet_files:
                    existing_tickers.add(ticker)
        
        logger.info(f"Found existing partitions for {len(existing_tickers)} tickers in year {year}: {sorted(list(existing_tickers))}")
        return existing_tickers
        
    except Exception as e:
        logger.warning(f"Error checking existing partitions: {str(e)}")
        return set()


def filter_dataframe_by_missing_tickers(df, existing_tickers):
    """
    Filter dataframe to only include tickers that don't have existing partitions.
    """
    if not existing_tickers:
        return df
    
    all_tickers = set(df['ticker'].unique())
    missing_tickers = all_tickers - existing_tickers
    
    if not missing_tickers:
        logger.info("All tickers already have partitions, no processing needed")
        return pd.DataFrame()  # Return empty dataframe
    
    logger.info(f"Processing {len(missing_tickers)} missing tickers: {sorted(list(missing_tickers))}")
    logger.info(f"Skipping {len(existing_tickers)} existing tickers: {sorted(list(existing_tickers))}")
    
    return df[df['ticker'].isin(missing_tickers)].copy()


def convert_csv_to_parquet(year: int):
    # Initialize S3 filesystem
    s3 = s3fs.S3FileSystem()

    # Check for existing partitions first (idempotent behavior)
    logger.info(f"Checking for existing partitions for year {year}")
    existing_tickers = get_existing_partitions(year, s3)

    # Get input path for specified year
    input_path = S3_CSV_INPUT_TEMPLATE.format(year=year)

    # Get list of files matching pattern
    files = s3.glob(input_path)
    logger.info(f"Found {len(files)} CSV files for year {year}")

    if not files:
        logger.warning(f"No CSV files found for year {year}")
        return

    # Read and concatenate all CSV files
    df_list = []
    for file in files:
        logger.info(f"Processing file: {file}")
        with s3.open(file) as f:
            df = pd.read_csv(f, compression='gzip')
            df_list.append(df)

    if not df_list:
        logger.error("No data loaded from CSV files")
        return

    final_df = pd.concat(df_list, ignore_index=True)
    logger.info(f"Loaded {len(final_df)} total rows")

    # Filter out tickers that already have partitions (idempotent behavior)
    final_df = filter_dataframe_by_missing_tickers(final_df, existing_tickers)
    
    if len(final_df) == 0:
        logger.info("All tickers already processed - ETL job complete (idempotent)")
        return

    # Convert window_start from UTC epoch nanoseconds to America/New_York timezone
    logger.info("Converting timezone from UTC to America/New_York")
    final_df['window_start'] = pd.to_datetime(final_df['window_start'], unit='ns', utc=True)
    final_df['window_start'] = final_df['window_start'].dt.tz_convert('America/New_York')
    
    # Filter to trading hours only (9:30 AM - 4:00 PM ET)
    logger.info("Filtering to trading hours (9:30 AM - 4:00 PM ET)")
    start_time = pd.to_datetime("09:30:00").time()
    end_time = pd.to_datetime("16:00:00").time()
    
    # Filter for trading hours and weekdays only
    mask_time = (final_df['window_start'].dt.time >= start_time) & (final_df['window_start'].dt.time < end_time)
    mask_weekday = final_df['window_start'].dt.weekday < 5  # Monday=0, Friday=4
    
    final_df = final_df.loc[mask_time & mask_weekday].copy()
    logger.info(f"After filtering: {len(final_df)} rows remain")

    if len(final_df) == 0:
        logger.warning("No data remains after filtering")
        return

    # Add year column for partitioning
    final_df['year'] = year

    # Convert to PyArrow table for efficient partitioned writing
    table = pa.Table.from_pandas(final_df)
    
    # Write partitioned parquet to S3 (partitioned by year and ticker)
    processed_tickers = sorted(final_df['ticker'].unique())
    logger.info(f"Writing partitioned parquet to {PARQUET_OUTPUT_PATH}")
    logger.info(f"Processing {len(processed_tickers)} tickers: {processed_tickers}")
    
    pq.write_to_dataset(
        table,
        root_path=PARQUET_OUTPUT_PATH,
        partition_cols=['year', 'ticker'],
        filesystem=s3
    )
    logger.info(f"Parquet export completed successfully for {len(processed_tickers)} tickers")


def main():
    """
    Main function for Glue Python Shell job (supports local testing)
    """
    try:
        # Detect environment and get parameters
        if GLUE_AVAILABLE:
            logger.info("Running in AWS Glue environment")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YEAR'])
        else:
            logger.info("Running in local development environment")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YEAR'])
        
        job_name = args['JOB_NAME']
        year = int(args['YEAR'])
        
        logger.info(f"ETL job: {job_name}")
        logger.info(f"Processing year: {year}")
        logger.info(f"Glue libraries available: {GLUE_AVAILABLE}")
        
        # Run the ETL process
        convert_csv_to_parquet(year)
        
        if GLUE_AVAILABLE:
            logger.info("AWS Glue Python Shell ETL job completed successfully")
        else:
            logger.info("Local ETL job completed successfully")
        
    except ValueError as e:
        logger.error(f"Invalid year parameter: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Glue Python Shell ETL job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()