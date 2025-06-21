import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging


def setup_logging():
    """Configures the logging settings."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(message)s'
    )


def create_directories(base_path: str, directories: list):
    """
    Creates directories if they do not exist.

    Args:
        base_path (str): The base path where directories will be created.
        directories (list): A list of directory paths relative to the base path.
    """
    for dir_rel_path in directories:
        dir_full_path = os.path.join(base_path, dir_rel_path)
        os.makedirs(dir_full_path, exist_ok=True)
        logging.info(f"Created directory: {dir_full_path}")


def create_system_logs_error_tracking():
    """
    Creates Parquet files for 'system_logs/error_tracking' with predefined data.
    """
    base_path = 'indexes/system_logs/error_tracking'

    # Define data for error1.parquet
    data_error1 = {
        'timestamp': [
            '2023-05-10',  # Matches (status="error" AND errorCode IN (403, 404))
            '2023-05-15',  # Matches (priority="high" AND action="update")
            '2023-05-20'  # Matches (status="critical" AND errorCode IN (403, 404))
        ],
        'status': [
            'error',
            'info',
            'critical'
        ],
        'errorCode': [
            403,
            200,
            404
        ],
        'priority': [
            'medium',
            'high',
            'low'
        ],
        'action': [
            'create',
            'update',
            'delete'
        ]
    }
    df_error1 = pd.DataFrame(data_error1)

    # Define data for error2.parquet
    data_error2 = {
        'timestamp': [
            '2023-05-25',
            # Matches both (status="critical" AND errorCode IN (403, 404)) and (priority="high" AND action="update")
            '2023-05-28'  # Does not match any filter
        ],
        'status': [
            'critical',
            'warning'
        ],
        'errorCode': [
            404,
            500
        ],
        'priority': [
            'high',
            'medium'
        ],
        'action': [
            'update',
            'create'
        ]
    }
    df_error2 = pd.DataFrame(data_error2)

    # Write to Parquet files
    pq.write_table(pa.Table.from_pandas(df_error1), os.path.join(base_path, 'error1.parquet'))
    logging.info(f"Created 'error1.parquet' with {len(df_error1)} rows.")

    pq.write_table(pa.Table.from_pandas(df_error2), os.path.join(base_path, 'error2.parquet'))
    logging.info(f"Created 'error2.parquet' with {len(df_error2)} rows.")


def create_finance_logs_transactions():
    """
    Creates Parquet files for 'finance_logs/transactions' with predefined data.
    """
    base_path = 'indexes/finance_logs/transactions'

    # Define data for txn1.parquet
    data_txn1 = {
        'timestamp': [
            '2023-06-05',  # Matches (transactionStatus="pending" AND amount > 1000) AND customerType="VIP"
            '2023-06-15',  # Does not match (transactionStatus is not "pending")
            '2023-06-20'  # Does not match (amount <= 1000)
        ],
        'transactionStatus': [
            'pending',
            'completed',
            'pending'
        ],
        'amount': [
            1500,
            2000,
            800
        ],
        'customerType': [
            'VIP',
            'VIP',
            'VIP'
        ]
    }
    df_txn1 = pd.DataFrame(data_txn1)

    # Write to Parquet file
    pq.write_table(pa.Table.from_pandas(df_txn1), os.path.join(base_path, 'txn1.parquet'))
    logging.info(f"Created 'txn1.parquet' with {len(df_txn1)} rows.")


def main():
    """Main function to create test data."""
    setup_logging()
    project_root = os.getcwd()
    logging.info(f"Project Root: {project_root}")

    # Define directories to create
    directories = [
        'indexes/system_logs/error_tracking',
        'indexes/finance_logs/transactions'
    ]

    # Create directories
    create_directories(project_root, directories)

    # Create Parquet files with test data
    create_system_logs_error_tracking()
    create_finance_logs_transactions()

    logging.info("Test data creation completed successfully.")


if __name__ == '__main__':
    main()
