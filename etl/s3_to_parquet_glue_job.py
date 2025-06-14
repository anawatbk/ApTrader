import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, hour, minute, to_date, from_unixtime, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from datetime import datetime
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

S3_CSV_INPUT_TEMPLATE = 's3://anawatp-us-stocks/csv/{year}-*.csv.gz'
PARQUET_OUTPUT_PATH = 's3://anawatp-us-stocks/parquet'


class GlueCSVToParquetETL:
    def __init__(self, glue_context):
        """
        Initialize Glue ETL job

        Args:
            glue_context: AWS Glue context
            spark_context: Spark context
            job: Glue job object
        """
        self.glueContext = glue_context
        self.spark = glue_context.spark_session
        self.s3_client = boto3.client('s3')

    @staticmethod
    def get_schema():
        """
        Define schema for minute aggregates data based on Polygon.io format
        Adjust this schema based on your actual CSV structure
        """
        return StructType([
            StructField("ticker", StringType(), True),
            StructField("volume", LongType(), True),
            StructField("open", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("window_start", LongType(), True),
            StructField("transactions", LongType(), True)
        ])

    def read_csv_with_spark(self, year: int):
        """
        Alternative method: Read CSV directly with Spark (for more control over schema)

        Args:
            s3_input_path (str): S3 path to CSV files

        Returns:
            DataFrame: Spark DataFrame with loaded data
        """
        s3_input_path = S3_CSV_INPUT_TEMPLATE.format(year=year)

        logger.info(f"Reading CSV files with Spark from: {s3_input_path}")

        schema = self.get_schema()

        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(schema) \
                .csv(s3_input_path)

            logger.info(f"Successfully loaded {df.count()} records from CSV")
            return df

        except Exception as e:
            logger.error(f"Error reading CSV files with Spark: {str(e)}")
            raise

    def transform_data(self, sdf):
        """
        Transform the data and add partitioning columns

        Args:
            df (DataFrame): Input DataFrame

        Returns:
            DataFrame: Transformed DataFrame with partition columns
        """
        logger.info("Starting data transformation")

        try:
            # Convert timestamp (milliseconds) to proper date and datetime
            transformed_df = (sdf.withColumn(
                "datetime",
                from_utc_timestamp(
                    from_unixtime((col("timestamp") / 1_000_000_000)),
                    "America/New_York"
                )
            ).withColumn(
                "year",
                year(col("datetime")).cast("integer")
            ))

            # Filter for trading hours: 09:30:00 - 16:00:00
            filtered_df = transformed_df.filter(

                ((hour(col("datetime")) == 9) & (minute(col("datetime")) >= 30))
                 |
                ((hour(col("datetime")) >= 10) & (hour(col("datetime")) < 16))
            )

            # Filter out invalid records and select final columns
            final_df = transformed_df.filter(
                col("ticker").isNotNull() &
                (col("ticker") != "") &
                col("year").isNotNull()
            )

            logger.info(f"Data transformation completed. Final record count: {final_df.count()}")
            return final_df

        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise

    def write_to_s3_as_parquet(self, sdf, partition_cols=["year", "ticker"]):
        """
        Write DataFrame to S3 as partitioned Parquet using Glue

        Args:
            df (Spark DataFrame): DataFrame to write
            partition_cols (list): List of columns to partition by
        """
        logger.info(f"Writing Parquet files to: {PARQUET_OUTPUT_PATH}")
        logger.info(f"Partitioning by: {partition_cols}")

        try:
            # Convert Spark DataFrame to Glue DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(sdf, self.glueContext)

            # Write using Glue's write_dynamic_frame
            self.glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": PARQUET_OUTPUT_PATH,
                    "partitionKeys": partition_cols
                },
                format="parquet",
                format_options={
                    "compression": "snappy",
                }
            )

            logger.info("Parquet files written successfully using Glue")

        except Exception as e:
            logger.error(f"Error writing Parquet files: {str(e)}")
            raise

    def run_etl_job(self):
        """
        Run the complete ETL job

        Args:
            s3_input_path (str): S3 path to input CSV files
            s3_output_path (str): S3 path for output Parquet files
            database_name (str, optional): Glue catalog database name
            table_name (str, optional): Glue catalog table name
            use_catalog (bool): Whether to register table in Glue catalog
        """
        try:
            logger.info("Starting Glue ETL job")
            start_time = datetime.now()

            # iterate each year

            # Step 1: Read CSV data from S3 using Spark (for better schema control)
            sdf = self.read_csv_with_spark(2025)

            # Step 2: Transform data
            transformed_df = self.transform_data(sdf)

            # Step 3: Write Parquet to S3
            self.write_to_s3_as_parquet(transformed_df)

            end_time = datetime.now()
            duration = end_time - start_time

            logger.info(f"Glue ETL job completed successfully in {duration}")

        except Exception as e:
            logger.error(f"Glue ETL job failed: {str(e)}")
            raise


def main():
    """
    Main function for Glue job
    """
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME'
    ])

    # Initialize Glue context and job
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Initialize ETL job
    etl = GlueCSVToParquetETL(glueContext)

    try:
        etl.run_etl_job()

    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        job.commit()
        raise

    # Commit the job
    job.commit()
    logger.info("Glue job committed successfully")


# if __name__ == "__main__":
main() # glue does not need __name__ == "__main__"