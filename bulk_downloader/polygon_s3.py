import boto3
from botocore.config import Config
import os
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Storage
SAVE_DIR = '~/workspace/data/us_stocks/'


def download(prefix: str, start_date: str, end_date: str = None):
    bucket_name = 'flatfiles'
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date is None:
        end_date = datetime.now().date()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    logger.info(f"Downloading data from {start_date} to {end_date}")
    access_key = os.environ.get("POLYGON_ACCESS_KEY")
    secret_access_key = os.environ.get("POLYGON_SECRET_KEY")
    logger.info(
        f"Connecting to Polygon S3 with aws_access_key_id={access_key} aws_secret_access_key={secret_access_key}")

    if access_key is None or secret_access_key is None:
        logger.info("Access or secret key not found. Terminating the program.")
        return

    # Initialize a session using your credentials
    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

    s3 = session.client(
        service_name='s3',
        endpoint_url='https://files.polygon.io',
        config=Config(signature_version='s3v4'),
    )

    # List Example
    # Initialize a paginator for listing objects
    paginator = s3.get_paginator('list_objects_v2')

    # Choose the appropriate prefix depending on the data you need:
    # - 'global_crypto' for global cryptocurrency data
    # - 'global_forex' for global forex data
    # - 'us_indices' for US indices data
    # - 'us_options_opra' for US options (OPRA) data
    # - 'us_stocks_sip' for US stocks (SIP) data
    #       us_stocks_sip/day_aggs_v1
    #       us_stocks_sip/minute_aggs_v1
    #       us_stocks_sip/quotes_v1
    #       us_stocks_sip/trades_v1

    # unique_dirs = set()

    # List objects inside flatfiles:prefix/
    obj_key_list = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page['Contents']:
            key = obj["Key"]
            date = datetime.strptime(key.split('/')[-1][:10], '%Y-%m-%d').date()
            if start_date <= date <= end_date:
                obj_key_list.append(key)

    #print(obj_key_list)
    #     parts = key.split("/")
    #     if len(parts) > 1:
    #         first_level_dir = "/".join(parts[:2])  # Extract "flatfiles/data1"
    #         if first_level_dir not in unique_dirs:
    #             print(first_level_dir)
    #             unique_dirs.add(first_level_dir)  # Store to prevent duplicates

    # Copy example
    # Specify the bucket name
    # Specify the S3 object key name
    # Specify the local file name and path to save the downloaded file
    # This splits the object_key string by '/' and takes the last segment as the file name
    # local_file_name = object_key.split('/')[-1]
    # # This constructs the full local file path
    # local_file_path = SAVE_DIR + local_file_name
    # #
    # # # Download the file
    for obj_key in obj_key_list:
        local_file_name = obj_key.split('/')[-1]
        local_file_path = Path(SAVE_DIR + local_file_name).expanduser()
        logger.info(f"Downloading {obj_key} to {local_file_path}")
        if local_file_path.exists():
            logger.info("local_file_path already exists. Skipping.")
            continue
        s3.download_file(bucket_name, obj_key, local_file_path)
    logger.info(f"Finished Downloading.")


if __name__ == "__main__":
    download('us_stocks_sip/minute_aggs_v1/', '2015-02-17')

