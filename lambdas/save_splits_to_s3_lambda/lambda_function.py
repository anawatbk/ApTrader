import io

from polygon import RESTClient
import os
import csv
import boto3
import json

# from pathlib import Path
from datetime import date

# Instantiate Polygon Keys
api_key = os.environ.get("POLYGON_API_KEY")

TO_BUCKET_NAME = "anawatp-us-stocks-reference"

# Configure Storage DIR
LOCAL_SAVE_DIR = '~/workspace/data/us_stocks_reference/'


def lambda_handler(event, context):
    api_result = {}

    client = RESTClient(api_key=api_key)
    today_str = date.today().strftime("%Y-%m-%d")

    result = client.list_splits(execution_date_lte=today_str, order="asc", sort="execution_date", )

    # Local execution
    # local_file_path = Path(LOCAL_SAVE_DIR + f'split_data_{today_str}.csv').expanduser()

    # Create an in-memory string buffer
    buffer = io.StringIO()
    writer = None

    for split in result:
        row = vars(split)
        if writer is None:
            writer = csv.DictWriter(buffer, fieldnames=row.keys())
            writer.writeheader()

        writer.writerow(row)
        print(f"Read split of {split.ticker} on date={split.execution_date}")

    csv_content = buffer.getvalue()

    # with open(local_file_path, "w", newline="") as f:
    #     f.write(csv_content)

    file_key = f'split_data_{today_str}.csv'

    my_s3 = boto3.client('s3')
    my_s3.put_object(
        Bucket=TO_BUCKET_NAME,
        Key=file_key,
        Body=csv_content
    )

    api_result['saved_file'] = TO_BUCKET_NAME + '/' + file_key
    print(f"Successfully saved stocks split to {TO_BUCKET_NAME}/{file_key}")

    return {
        'statusCode': 200,
        'body': json.dumps(api_result)
    }


if __name__ == "__main__":
    lambda_handler(None, None)
