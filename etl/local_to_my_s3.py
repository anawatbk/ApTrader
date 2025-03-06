import boto3
import os
import logging
from pathlib import Path
import tempfile
import pandas as pd
import time
import functools

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Storage
LOAD_DIR = '~/workspace/data/us_stocks/'
access_key = os.environ.get("AWS_ACCESS_KEY")
secret_access_key = os.environ.get("AWS_SECRET_KEY")

bucket_name = 'anawatp-us-stocks'
prefix = '1-mins-aggregate/'


def timing(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed = end - start
        # Convert elapsed seconds to hours, minutes, and seconds
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        print(f"{func.__name__} executed in {int(hours)}h {int(minutes)}m {seconds:.2f}s")
        return result

    return wrapper


def upload_file(filename, _s3_client):
    logger.info(
        f"Connecting to S3 with aws_access_key_id={access_key} aws_secret_access_key={secret_access_key}")

    loading_path = Path(LOAD_DIR + filename)
    df = pd.read_csv(loading_path)

    # transform
    df['window_start'] = pd.to_datetime(df['window_start'], unit='ns', utc=True).dt.tz_convert('America/New_York')
    df.set_index("window_start", inplace=True)
    df.sort_index(inplace=True)

    # Create a mask for trading hours
    # Note: .index.time returns an array of time objects which can be used in a vectorized comparison
    start_time = pd.to_datetime("09:30:00").time()
    end_time = pd.to_datetime("16:00:00").time()
    mask_time = (df.index.time >= start_time) & (df.index.time < end_time)

    # Step 1: Write the DataFrame to a temporary HDF5 file
    with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
        tmp_filename = tmp.name

    df.loc[mask_time].to_hdf(tmp_filename, key="df", mode="w",
                             format="table", complevel=9, complib="blosc",
                             data_columns=['ticker', 'window_start'])

    object_key = prefix + filename.split('.')[0] + '.h5'
    _s3_client.upload_file(tmp_filename, bucket_name, object_key)
    # Optionally, remove the temporary file now that it's uploaded
    os.remove(tmp_filename)
    logger.info(f"Uploaded HDF5 file to {object_key}")


@timing
def read_h5_from_s3(object_key, _s3_client):
    # Step 1: Download the HDF5 file from S3 to a temporary file
    with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp_read:
        tmp_read_filename = tmp_read.name
    _s3_client.download_file(bucket_name, object_key, tmp_read_filename)
    # Step 2: Read the HDF5 file into a DataFrame
    df_downloaded = pd.read_hdf(tmp_read_filename, key="df", where="ticker == 'AAPL'")

    # Clean up the temporary file
    os.remove(tmp_read_filename)
    return df_downloaded


if __name__ == "__main__":
    session = boto3.Session(aws_access_key_id=str(access_key),
                            aws_secret_access_key=str(secret_access_key))
    # s3_resource = session.resource("s3")
    s3_client = session.client("s3")

    # upload_file('2015-02-17.csv.gz', s3_client)
    print(read_h5_from_s3('1-mins-aggregate/2015-02-17.h5', s3_client))
