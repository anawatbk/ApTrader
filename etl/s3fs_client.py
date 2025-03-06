import pandas as pd
import s3fs
import logging
import os
import pyarrow.parquet as pq
import pyarrow.fs as pfs
import pyarrow as pa
from pathlib import Path

# from ..utils import timing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

access_key = os.environ.get("AWS_ACCESS_KEY")
secret_access_key = os.environ.get("AWS_SECRET_KEY")

CSV_DIR = '~/workspace/data/us_stocks/'
bucket_name = 'anawatp-us-stocks'


def s3_upload(filename):
    fs = s3fs.S3FileSystem(anon=False, key=access_key, secret=secret_access_key)

    csv_path = CSV_DIR + filename
    df = pd.read_csv(csv_path)
    df['window_start'] = pd.to_datetime(df['window_start'], unit='ns', utc=True).dt.tz_convert('America/New_York')
    # Create a new 'date' column for partitioning by date
    df['date'] = df['window_start'].dt.date.astype(str)

    # Create a mask for trading hours
    # Note: .index.time returns an array of time objects which can be used in a vectorized comparison
    start_time = pd.to_datetime("09:30:00").time()
    end_time = pd.to_datetime("16:00:00").time()
    mask_time = (df.window_start.dt.time >= start_time) & (df.window_start.dt.time < end_time)

    df = df.loc[mask_time]

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write a partitioned dataset directly to S3.
    # This will automatically create partitions based on the 'date' and 'ticker' columns.
    pq.write_to_dataset(
        table,
        root_path=f"s3://{bucket_name}/data_partitioned",
        use_threads=True,
        partition_cols=["date", "ticker"],
        filesystem=fs,
        compression="snappy"
    )


def local_csv_to_parquet_fs(filename):
    logger.info(f"Creating parquet from local {filename}")

    # arrow file system
    uri = f'~/workspace/data/parquet_filesystem/us_equities'
    local_root_fs, local_path = pfs.FileSystem.from_uri(uri)

    # Load csv and transform
    csv_path = filename
    df = pd.read_csv(csv_path)
    df['window_start'] = pd.to_datetime(df['window_start'], unit='ns', utc=True).dt.tz_convert('America/New_York')
    df['timeframe'] = '1-min'

    # create a new 'date' column for partitioning by date
    df['year'] = df['window_start'].dt.year.astype(str)

    # create a mask for trading hours
    start_time = pd.to_datetime("09:30:00").time()
    end_time = pd.to_datetime("16:00:00").time()
    mask_time = (df.window_start.dt.time >= start_time) & (df.window_start.dt.time < end_time)

    # filter to only trading hours
    df = df.loc[mask_time]

    # convert DataFrame to PyArrowTable
    table = pa.Table.from_pandas(df)

    # load into file system
    # write a partitioned dataset directly to local fs
    # This will automatically create partitions based on the 'date' and 'ticker' columns.
    pq.write_to_dataset(
        table,
        root_path=local_path,
        use_threads=True,
        partition_cols=["year", "timeframe", "ticker"],
        filesystem=local_root_fs,
        compression="snappy"
    )

    logger.info(f"Finished creating parquet for {filename}")


if __name__ == '__main__':
    # upload('2015-02-17.csv.gz')
    all_csv_path = Path(CSV_DIR).expanduser()
    for path in all_csv_path.rglob("*.csv.gz"):
        if "2015-02-17" in str(path):
            continue
        local_csv_to_parquet_fs(path)
