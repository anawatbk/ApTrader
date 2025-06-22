"""
AWS Glue Spark ETL Job: S3 CSV to S3 Parquet Converter

This script converts CSV files from S3 to partitioned Parquet format using PySpark.
Designed for better scalability and memory management compared to Python Shell version.

Features:
- Distributed processing with Spark
- Single parquet file per year/ticker partition
- Timezone conversion (UTC to America/New_York) 
- Trading hours filtering (9:30 AM - 4:00 PM ET)
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

def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session with optimized configuration"""
    if GLUE_AVAILABLE:
        # AWS Glue context
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        return spark, glueContext, job
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

def get_existing_partitions(spark: SparkSession, year: int) -> set:
    """
    Get list of existing ticker partitions for a given year to enable idempotent processing
    """
    year_partition_path = f"{PARQUET_OUTPUT_PATH}/year={year}"
    
    try:
        # Try to list partitions using Spark
        existing_df = spark.read.parquet(year_partition_path)
        existing_tickers = existing_df.select("ticker").distinct().rdd.flatMap(lambda x: x).collect()
        print(f"Found {len(existing_tickers)} existing ticker partitions for year {year}")
        return set(existing_tickers)
    except Exception:
        print(f"No existing partitions found for year {year}")
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
    
    # Define input path pattern
    input_path = S3_CSV_INPUT_TEMPLATE.format(year=target_year)
    print(f"Reading CSV files from: {input_path}")
    
    try:
        # Read all CSV files with Spark (distributed)
        # Define schema for better performance
        schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("window_start", LongType(), True),  # epoch nanoseconds
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("transactions", LongType(), True)
        ])
        
        # Read CSV files
        df = spark.read \
            .option("header", "true") \
            .option("compression", "gzip") \
            .schema(schema) \
            .csv(input_path)
        
        print("Loaded CSV files from input path")
        
        # Early filtering by year for better performance (predicate pushdown)
        df = df.filter(year((col("window_start") / 1e9).cast("timestamp")) == lit(target_year))
        
        # Filter out existing tickers for idempotent processing
        if existing_tickers:
            print(f"Filtering out {len(existing_tickers)} existing tickers")
            df = df.filter(~col("ticker").isin(list(existing_tickers)))
            print("Applied existing ticker filter")
        
        # Convert epoch nanoseconds to timestamp and timezone in one step
        print("Converting timestamps and timezone from UTC to America/New_York")
        df = df.withColumn(
            "window_start_et",
            from_utc_timestamp((col("window_start") / 1e9).cast("timestamp"), "America/New_York")
        )
        
        # Filter to trading hours (9:30 AM - 4:00 PM ET) and weekdays only
        print("Filtering to trading hours (9:30 AM - 4:00 PM ET) and weekdays")
        df = df.filter(
            (
                ((hour(col("window_start_et")) == 9) & (minute(col("window_start_et")) >= 30)) |
                ((hour(col("window_start_et")) > 9) & (hour(col("window_start_et")) < 16))
            ) &
            (dayofweek(col("window_start_et")).between(2, 6))  # Monday=2, Friday=6
        )
        
        print("Applied trading hours filter")
        
        # Add year column for partitioning
        df = df.withColumn("year", year(col("window_start_et")))
        
        # Select final columns (excluding intermediate columns)
        df_final = df.select(
            "ticker",
            col("window_start_et").alias("window_start"),
            "open", "high", "low", "close", "volume", "transactions",
            "year"
        )
        
        # Cache the final dataframe since we'll use it multiple times
        df_final.cache()
        
        print("Data processing pipeline prepared")
        
        # Write to S3 as partitioned Parquet
        print(f"Writing partitioned Parquet files to: {PARQUET_OUTPUT_PATH}")
        
        # Write with single file per partition using coalesce
        df_final.coalesce(1) \
            .write \
            .mode("append") \
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