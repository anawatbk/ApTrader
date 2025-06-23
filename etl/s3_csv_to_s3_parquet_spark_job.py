"""
AWS Glue Spark ETL Job: S3 CSV to S3 Parquet Converter

This script converts CSV files from S3 to a partitioned Parquet format using PySpark.

Features:
- Distributed processing with Spark
- Single parquet file per year/ticker partition
- Timezone conversion (UTC to America/New_York) 
- Trading hours filtering (9:30 AM-4:00 PM ET)
- Idempotent processing (skip existing partitions)
- Local testing support
"""

from datetime import datetime

# Handle AWS Glue imports with local fallback
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False
    # Mock for local testing
    def getResolvedOptions(argv, options):
        if len(argv) < 2:
            return {'JOB_NAME': 'local-spark-test', 'YEAR': '2025'}
        else:
            year = argv[1] if len(argv) > 1 else '2025'
            return {'JOB_NAME': 'local-spark-test', 'YEAR': year}

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
S3_CSV_INPUT_TEMPLATE = 's3://anawatp-us-stocks/csv/{year}-*.csv.gz'
PARQUET_OUTPUT_PATH = 's3://anawatp-us-stocks/parquet'

def create_spark_session(app_name: str) -> tuple[Any, Any, Any] | tuple[SparkSession, None, None]:
    """Create a Spark session with optimized configuration"""
    if GLUE_AVAILABLE:
        # AWS Glue context
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        return spark, glue_context, job
    else:
        # Local Spark session with optimized configuration
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .getOrCreate()
        
        # Set log level to reduce noise in local testing
        spark.sparkContext.setLogLevel("WARN")
        return spark, None, None

def get_existing_partitions(spark: SparkSession, target_year: int) -> set:
    """
    Get list of existing ticker partitions for a given year to enable idempotent processing.
    Optimized to use filesystem operations instead of reading all parquet data.
    """
    year_partition_path = f"{PARQUET_OUTPUT_PATH}/year={target_year}"
    
    try:
        # Use Spark's filesystem to list partition directories
        hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(year_partition_path)
        
        if not hadoop_fs.exists(path):
            print(f"No existing partitions found for year {target_year}")
            return set()
        
        # List ticker partition directories
        file_statuses = hadoop_fs.listStatus(path)
        existing_tickers = set()
        
        for file_status in file_statuses:
            if file_status.isDirectory():
                dir_name = file_status.getPath().getName()
                # Extract ticker from "ticker=SYMBOL" directory name
                if dir_name.startswith("ticker="):
                    ticker = dir_name.replace("ticker=", "")
                    existing_tickers.add(ticker)
        
        print(f"Found {len(existing_tickers)} existing ticker partitions for year {target_year}")
        return existing_tickers
        
    except Exception as e:
        print(f"Error checking existing partitions for year {target_year}: {str(e)}")
        print(f"Proceeding without existing partition check")
        return set()

def process_csv_to_parquet(spark: SparkSession, target_year: int) -> bool:
    """
    Main processing function using Spark DataFrames
    
    Returns:
        bool: True if processing completed successfully
    """
    print(f"Starting Spark ETL processing for year {target_year}")
    
    # Check for existing partitions (idempotent behavior)
    existing_tickers = get_existing_partitions(spark, target_year)
    
    # Define an input path pattern
    input_path = S3_CSV_INPUT_TEMPLATE.format(year=target_year)
    print(f"Reading CSV files from: {input_path}")
    
    try:
        # Read all CSV files with Spark (distributed) - using header-based approach
        # Read CSV with headers, let Spark handle column detection
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "false") \
            .csv(input_path)
        
        # Validate expected columns exist
        expected_columns = {"ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"}
        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        
        print(f"CSV columns detected: {df.columns}")
        
        # Explicitly select and cast columns by name (order-independent)
        df = df.select(
            col("ticker").cast(StringType()).alias("ticker"),
            col("volume").cast(LongType()).alias("volume"),
            col("open").cast(DoubleType()).alias("open"),
            col("close").cast(DoubleType()).alias("close"),
            col("high").cast(DoubleType()).alias("high"),
            col("low").cast(DoubleType()).alias("low"),
            col("window_start").cast(LongType()).alias("window_start"),
            col("transactions").cast(LongType()).alias("transactions")
        )

        print(f"Successfully loaded records from CSV")

        print("Loaded CSV files from input path")

        print("Starting data transformation")

        # Early filtering by year for better performance (predicate pushdown)
        # df = df.filter(year((col("window_start") / 1e9).cast("timestamp")) == lit(target_year))
        
        # Filter out existing tickers for idempotent processing
        if existing_tickers:
            print(f"Filtering out {len(existing_tickers)} existing tickers")
            df = df.filter(~col("ticker").isin(list(existing_tickers)))

        # Debug: Sample raw timestamps before conversion
        print("=== DEBUGGING TIMESTAMPS ===")
        print("Sample raw window_start values:")
        df.select("ticker", "window_start").show(5, truncate=False)
        
        # Convert epoch nanoseconds to timestamp and timezone in one step
        print("Converting timestamps and timezone from UTC to America/New_York")
        df = df.withColumn(
            "window_start_et",
            from_utc_timestamp(from_unixtime((col("window_start") / 1_000_000_000)), "America/New_York")
        )
        
        # Debug: Sample converted timestamps
        print("Sample converted timestamps:")
        df.select("ticker", "window_start", "window_start_et").show(5, truncate=False)
        
        # Debug: Add hour, minute, dayofweek for inspection
        df_debug = df.withColumn("hour_et", hour(col("window_start_et"))) \
                    .withColumn("minute_et", minute(col("window_start_et"))) \
                    .withColumn("dayofweek_et", dayofweek(col("window_start_et")))
        
        print("Sample hour/minute/dayofweek values:")
        df_debug.select("ticker", "window_start_et", "hour_et", "minute_et", "dayofweek_et").show(10, truncate=False)

        # Filter to trading hours (9:30 AM-4:00 PM ET) and weekdays only
        print("Filtering to trading hours (9:30 AM - 4:00 PM ET) and weekdays")
        
        # Step 1: Filter by weekdays first
        df_weekdays = df.filter(dayofweek(col("window_start_et")).between(2, 6))  # Monday=2, Friday=6

        # Step 2: Filter by trading hours
        df_trading_hours = df_weekdays.filter(
            ((hour(col("window_start_et")) == 9) & (minute(col("window_start_et")) >= 30)) |
            ((hour(col("window_start_et")) > 9) & (hour(col("window_start_et")) < 16))
        )

        # Use the filtered dataframe
        df = df_trading_hours
        
        print("Applied trading hours filter")
        
        # Add year column for partitioning
        df = df.withColumn("year", year(col("window_start_et")))
        
        # Select final columns (excluding intermediate columns)
        df_final = df.select(
            "ticker",
            "window_start_et",
            "open", "high", "low", "close", "volume", "transactions",
            "year"
        )
        
        # Cache the final dataframe since we'll use it multiple times
        df_final.cache()

        print("Data processing pipeline prepared")
        
        # Write to S3 as partitioned Parquet
        print(f"Writing partitioned Parquet files to: {PARQUET_OUTPUT_PATH}")
        
        # Write with single file per partition using coalesce
        df_final.repartition("year", "ticker")
        df_final.write \
            .mode("overwrite") \
            .partitionBy("year", "ticker") \
            .parquet(PARQUET_OUTPUT_PATH)
        
        # Get ticker count after write for reporting (only computed once)
        ticker_count = df_final.select("ticker").distinct().count()
        print(f"Successfully processed {ticker_count} unique tickers")
        return True
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise

def main():
    """Main function for Spark ETL job"""
    start_time = datetime.now()
    
    try:
        print("=" * 60)
        print(f"SPARK ETL JOB START: {start_time}")
        print("=" * 60)
        
        # PySpark is now a hard requirement - no need to check
        
        # Get job parameters
        if GLUE_AVAILABLE:
            print("Running in AWS Glue environment")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YEAR'])
        else:
            print("Running in local development environment")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YEAR'])
        
        job_name = args['JOB_NAME']
        year = int(args['YEAR'])
        
        print(f"Job name: {job_name}")
        print(f"Processing year: {year}")
        print(f"Glue libraries available: {GLUE_AVAILABLE}")

        # Create Spark session
        print("Initializing Spark session...")
        spark, glue_context, job = create_spark_session(job_name)
        
        if GLUE_AVAILABLE and job:
            job.init(job_name, args)
        
        print(f"Spark version: {spark.version}")
        print(f"Spark application ID: {spark.sparkContext.applicationId}")
        
        # Process data
        success = process_csv_to_parquet(spark, year)
        
        if GLUE_AVAILABLE and job:
            job.commit()
        
        # Calculate duration
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("=" * 60)
        print(f"JOB END: {end_time}")
        print(f"DURATION: {duration}")
        print(f"STATUS: {'SUCCESS' if success else 'COMPLETED_NO_DATA'}")
        print("=" * 60)
        
        if success:
            print("Spark ETL job completed successfully")
        else:
            print("Spark ETL job completed - no data to process")
            
    except Exception as e:
        print(f"Spark ETL job failed: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()