import pyarrow as pa
import pyarrow.parquet as pq
import random
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def generate_parquet_files():
    # Define some sample data that we will use in our Parquet files
    data = [
        # Sample 1: "system_logs/*", "userRole", "error_code", etc.
        {
            "index": "system_logs/error_tracking/*", "userRole": "admin", "error_code": 12, "status": "error",
            "errorCode": 403, "earliest": "2024-01-01", "latest": "2024-01-10", "x": 4, "y": 5, "z": 6
        },
        {
            "index": "system_logs/error_tracking/*", "userRole": "ericadmin", "error_code": 12, "status": "critical",
            "errorCode": 404, "earliest": "2024-02-01", "latest": "2024-02-10", "x": 3, "y": 8, "z": 10
        },
        {
            "index": "system_logs/successes/*", "userRole": "user", "error_code": 0, "status": "success",
            "errorCode": 0, "earliest": "2024-03-01", "latest": "2024-03-10", "x": 5, "y": 7, "z": 1
        },
        {
            "index": "system_logs/successes/*", "userRole": "admin", "error_code": 0, "status": "success",
            "errorCode": 0, "earliest": "2024-01-01", "latest": "2024-01-01", "x": 2, "y": 3, "z": 4
        },
        {
            "index": "system_logs/error_tracking/*", "userRole": "user", "error_code": 404, "status": "error",
            "errorCode": 404, "earliest": "2024-02-01", "latest": "2024-02-28", "x": 10, "y": 6, "z": 2
        },
        {
            "index": "system_logs/error_tracking/*", "userRole": "admin", "error_code": 404, "status": "critical",
            "errorCode": 404, "earliest": "2024-03-01", "latest": "2024-03-10", "x": 3, "y": 3, "z": 1
        }
    ]

    # Convert data into a pandas DataFrame
    df = pd.DataFrame(data)

    # Convert the pandas DataFrame into a PyArrow Table
    table = pa.Table.from_pandas(df)

    # File paths for the Parquet files that we will generate
    output_files = [
        'test0.parquet',  # System logs error tracking
        'test1.parquet',  # System logs successes
    ]

    # Write the table to Parquet files
    for file_path in output_files:
        pq.write_table(table, file_path)
        logging.info(f"Parquet file generated: {file_path}")

    # Adding another example for validation
    data2 = [
        {
            "index": "output_parquets/test0.parquet", "earliest": "2024-01-01", "latest": "2024-01-01", "x": 4, "y": 5,
            "z": 6
        },
        {
            "index": "output_parquets/test1.parquet", "earliest": "2024-01-02", "latest": "2024-02-02", "x": 3, "y": 8,
            "z": 10
        },
        {
            "index": "output_parquets/test0.parquet", "earliest": "2024-02-01", "latest": "2024-02-02", "x": 3, "y": 5,
            "z": 9
        },
        {
            "index": "output_parquets/test1.parquet", "earliest": "2024-02-01", "latest": "2024-02-28", "x": 4, "y": 7,
            "z": 2
        },
        {
            "index": "output_parquets/test0.parquet", "earliest": "2024-03-01", "latest": "2024-03-05", "x": 5, "y": 9,
            "z": 1
        },
        {
            "index": "output_parquets/test1.parquet", "earliest": "2024-04-01", "latest": "2024-04-20", "x": 8, "y": 7,
            "z": 5
        }
    ]

    # Convert additional data into a pandas DataFrame
    df2 = pd.DataFrame(data2)

    # Convert the pandas DataFrame into a PyArrow Table
    table2 = pa.Table.from_pandas(df2)

    # Write the table to Parquet file
    pq.write_table(table2, 'test2.parquet')
    logging.info(f"Parquet file generated: {'output_parquets/test2.parquet'}")


# Generate the required Parquet files
generate_parquet_files()
