import glob
import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pfs
import pyarrow.parquet as pq
import s3fs
from tqdm import tqdm
import uuid

# from ..utils import timing

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

access_key = os.environ.get("AWS_ACCESS_KEY")
secret_access_key = os.environ.get("AWS_SECRET_KEY")

CSV_DIR = '~/workspace/data/us_stocks/'
PARQUET_FS_DIR = '~/workspace/data/parquet_filesystem/us_equities'
# bucket_name = 'anawatp-us-stocks'


# def s3_upload(filename):
#     fs = s3fs.S3FileSystem(anon=False, key=access_key, secret=secret_access_key)
#
#     csv_path = CSV_DIR + filename
#     df = pd.read_csv(csv_path)
#     df['window_start'] = pd.to_datetime(df['window_start'], unit='ns', utc=True).dt.tz_convert('America/New_York')
#     # Create a new 'date' column for partitioning by date
#     df['date'] = df['window_start'].dt.date.astype(str)
#
#     # Create a mask for trading hours
#     # Note: .index.time returns an array of time objects which can be used in a vectorized comparison
#     start_time = pd.to_datetime("09:30:00").time()
#     end_time = pd.to_datetime("16:00:00").time()
#     mask_time = (df.window_start.dt.time >= start_time) & (df.window_start.dt.time < end_time)
#
#     df = df.loc[mask_time]
#
#     # Convert DataFrame to PyArrow Table
#     table = pa.Table.from_pandas(df)
#
#     # Write a partitioned dataset directly to S3.
#     # This will automatically create partitions based on the 'date' and 'ticker' columns.
#     pq.write_to_dataset(
#         table,
#         root_path=f"s3://{bucket_name}/data_partitioned",
#         use_threads=True,
#         partition_cols=["date", "ticker"],
#         filesystem=fs,
#         compression="snappy"
#     )


def local_csv_to_local_parquet_fs(daily_csv_path, filter_tickers: list, _use_thread):
    """
    To be added
    """
    # Parquet file system DIR
    local_root_fs, local_path = pfs.FileSystem.from_uri(PARQUET_FS_DIR)

    # Load csv and transform timestamp
    df = pd.read_csv(daily_csv_path)
    df['window_start'] = pd.to_datetime(df['window_start'], unit='ns', utc=True).dt.tz_convert('America/New_York')
    df['timeframe'] = '1-min'

    # create a new 'date' column for partitioning by date
    df['year'] = df['window_start'].dt.year.astype(str)

    # Create a mask for trading hours
    start_time = pd.to_datetime("09:30:00").time()
    end_time = pd.to_datetime("16:00:00").time()
    mask_time = (df.window_start.dt.time >= start_time) & (df.window_start.dt.time < end_time)
    df = df.loc[mask_time]

    if filter_tickers:
        df = df[df["ticker"].isin(filter_tickers)]

    # convert DataFrame to PyArrowTable
    pyarrow_table = pa.Table.from_pandas(df)

    # load into file system
    # write a partitioned dataset directly to local fs
    # This will automatically create partitions based on the 'date' and 'ticker' columns.

    curr_date = str(daily_csv_path.stem.split(".")[0])

    pq.write_to_dataset(
        pyarrow_table,
        root_path=local_path,
        use_threads=_use_thread,
        partition_cols=["year", "timeframe", "ticker"],
        filesystem=local_root_fs,
        basename_template=curr_date + "-part-{i}.parquet",
        compression="snappy"
    )


def merging_parquets(year_filter, timeframe='1-min', filter_tickers=[], should_delete_corrupted=False):
    logger.info(f"Optimizing parquets for year={year_filter} timeframe={timeframe}")
    parquet_path = Path(f'{PARQUET_FS_DIR}/year={year_filter}/timeframe={timeframe}').expanduser()

    ticker_paths = [entry.path for entry in os.scandir(parquet_path) if entry.is_dir()]
    _corrupted_tickers = []
    for i, ticker_path in tqdm(enumerate(ticker_paths)):
        try:
            if filter_tickers and not any(ticker == ticker_path.split("/")[-1].split("=")[-1] for ticker in filter_tickers):
                continue

            # print(f'Merging {ticker_path}')
            # Load all parquets for that year & ticker into dataset(lazy object)
            dataset = ds.dataset(ticker_path, format="parquet", partitioning="hive", ignore_prefixes=[".", "_"])
            df = dataset.to_table(use_threads=True)\
                .to_pandas()\
                .drop_duplicates()

            merged_table = pa.Table.from_pandas(df)

            for file in glob.glob(f"{ticker_path}/*.parquet"):
                os.remove(file)
            pq.write_table(merged_table, os.path.join(ticker_path, f"merged_{uuid.uuid4()}.parquet"),
                           compression="snappy")
        except Exception as e:
            print(f"🚨 Corrupted file detected: {ticker_path} {e}")
            if should_delete_corrupted:
                for file in glob.glob(f"{ticker_path}/*.parquet"):
                    os.remove(file)
            _corrupted_tickers.append(ticker_path.split("/")[-1].split("=")[-1])
    return _corrupted_tickers


if __name__ == '__main__':
    # upload('2015-02-17.csv.gz')

    all_daily_csv_path = Path(CSV_DIR).expanduser()
    for year in range(2021, 2022, 1):  # Done 2025,2024, 2023, 2022, 2021

        logger.info(f"Creating arrow fs for year={year}")
        # Iterate all csv.gz files
        for j, one_daily_csv_path in tqdm(enumerate(all_daily_csv_path.rglob(f"{year}-*.csv.gz"))):
            local_csv_to_local_parquet_fs(one_daily_csv_path, None, True)

            # if j != 0 and j % 50 == 0:
            #     merging_parquets(year, "1-min", False)

        # Last optimization before moving the next year
        corrupted_tickers = merging_parquets(year, "1-min", True)  # Delete corrupted ticker
        print(f'Corrupted tickers: {corrupted_tickers}')

        # Repair corrupted_tickers
        # corrupted_tickers = ['BCpC', 'CpK']
        for j, one_daily_csv_path in tqdm(enumerate(all_daily_csv_path.rglob(f"{year}-*.csv.gz"))):
            # Create .parquet from csv again
            local_csv_to_local_parquet_fs(one_daily_csv_path, corrupted_tickers, False)  # no thread save to repair it

        # Final Merging
        corrupted_tickers = merging_parquets(year, '1-min', corrupted_tickers, True)
        print(f'Final Corrupted tickers: {corrupted_tickers}')
        # logger.info(f"Corrupted_tickers: {corrupted_tickers}")

    '''
    batch data -> back test db
    
    live data -> in memory db? for quick feature compute and ml inference
    '''
