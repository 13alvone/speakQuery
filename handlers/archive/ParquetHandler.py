#!/usr/bin/env python3

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import logging
import time
import uuid
import os

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


class ParquetHandler:
    def __init__(self):
        self.jobs_path = '../../jobs'

    @staticmethod
    def check_headers_in_parquet(headers, parquet_file_path):
        headers = [str(x) for x in headers]  # Ensure that headers are string values and not TerminalNodeImpl types
        schema = pq.read_schema(parquet_file_path)  # Read the schema of the system4.system4.parquet file
        parquet_columns = schema.names  # Extract the names of all columns from the schema
        return all(header in parquet_columns for header in headers)  # Check if all headers are present in the system4.system4.parquet

    @staticmethod
    def read_parquet_file(file_path: str) -> pd.DataFrame:
        if not os.path.isfile(file_path):
            logging.error(f"File does not exist: {file_path}")
            return pd.DataFrame()

        try:
            df = pd.read_parquet(file_path)
            logging.info(f"File {file_path} read successfully.")
            return df
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            return pd.DataFrame()

    @staticmethod
    def combine_dataframes_vertically(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Combine two DataFrames by appending the rows of the second DataFrame to the first.
        Ensures that the DataFrames have identical columns.

        Parameters:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.

        Returns:
        pd.DataFrame: The combined DataFrame with rows from both input DataFrames.
        """
        df1, df2 = df1.align(df2, join='outer', axis=1, fill_value=None)  # Align the columns of both DataFrames
        return pd.concat([df1, df2], ignore_index=True)  # Concatenate the DataFrames vertically

    @staticmethod
    def combine_dataframes_horizontally(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Combine two DataFrames by adding the columns of the second DataFrame to the first.
        Handles the case where DataFrames have different indices by resetting them.

        Parameters:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.

        Returns:
        pd.DataFrame: The combined DataFrame with columns from both input DataFrames.
        """
        # Reset index to ensure the DataFrames can be concatenated horizontally
        df1.reset_index(drop=True, inplace=True)
        df2.reset_index(drop=True, inplace=True)
        return pd.concat([df1, df2], axis=1)  # Concatenate the DataFrames horizontally

    @staticmethod
    def smart_combine_dataframes(df1, df2):
        """
        Combines two DataFrames vertically if all header names in df1 are in df2 and the respective headers have
        the same data types. Otherwise, combines them horizontally and ensures unique header names.

        Parameters:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.

        Returns:
        pd.DataFrame: The combined DataFrame.
        """
        # Check if all header names in df1 are in df2 and have the same data types
        if all(col in df2.columns for col in df1.columns) and all(
                df1[col].dtype == df2[col].dtype for col in df1.columns if col in df2.columns):
            combined_df = pd.concat([df1, df2], ignore_index=True)  # Combine DataFrames vertically
        else:
            df1.reset_index(drop=True, inplace=True)  # Reset index to ensure correct horizontal concatenation
            df2.reset_index(drop=True, inplace=True)
            combined_df = pd.concat([df1, df2], axis=1)  # Combine DataFrames horizontally
            cols = pd.Series(combined_df.columns)  # Ensure unique header names for the horizontally combined DataFrame
            for dup in cols[cols.duplicated()].unique():
                cols[cols[cols == dup].index.values.tolist()] = \
                    [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            combined_df.columns = cols

        return combined_df

    @staticmethod
    def get_directory_size(path):
        total = sum(
            os.path.getsize(os.path.join(path, f)) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)))
        return total

    @staticmethod
    def delete_oldest_file(path):
        files = [os.path.join(path, f) for f in os.listdir(path)]
        files.sort(key=lambda x: os.path.getmtime(x))
        os.remove(files[0])
        logging.info(f"[i] Oldest jobs file removed: {files[0]}")

    def save_dataframe_to_parquet(self, main_df, max_files=30, max_gig=10):
        max_size = max_gig * 1024 ** 3
        if not os.path.exists(self.jobs_path):  # Check directory conditions
            os.makedirs(self.jobs_path)
            logging.info(f"[i] Jobs directory created: {self.jobs_path}")

        while len(os.listdir(self.jobs_path)) >= max_files or self.get_directory_size(self.jobs_path) >= max_size:
            self.delete_oldest_file(self.jobs_path)

        epoch_time = str(int(time.time()))  # Define the filename
        guid = str(uuid.uuid4())
        filename = f"{epoch_time}_{guid}.system4.system4.parquet"
        filepath = os.path.join(self.jobs_path, filename)

        main_df.to_parquet(filepath)  # Save dataframe to Parquet
        logging.info(f"[i] Job's dataframe saved to {filepath}")

    @staticmethod
    def read_parquet_files_with_filter(file_paths, filters):
        """
        Read multiple system4.system4.parquet files with a given filter and return a concatenated DataFrame.

        Parameters:
        file_paths (list): List of file paths to read.
        filters: PyArrow filter expression to apply when reading the files.

        Returns:
        pd.DataFrame: Combined DataFrame with applied filters.
        """
        if not file_paths:
            logging.error("[x] No file paths provided for reading system4.system4.parquet files.")
            return pd.DataFrame()

        try:
            dataframes = []
            for file_path in file_paths:
                dataset = pa.dataset.dataset(file_path, format='system4.system4.parquet')
                table = dataset.to_table(filter=filters)
                df = table.to_pandas()
                dataframes.append(df)

            # Ensure all DataFrames have identical columns by aligning them
            aligned_dataframes = []
            all_columns = sorted(set().union(*(df.columns for df in dataframes)))
            for df in dataframes:
                aligned_df = df.reindex(columns=all_columns, fill_value=None)
                aligned_dataframes.append(aligned_df)

            combined_df = pd.concat(aligned_dataframes, ignore_index=True)
        except pa.ArrowInvalid as e:
            logging.error(f"[x] ArrowInvalid error: {e}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"[x] Error reading system4.system4.parquet files with filters: {e}")
            return pd.DataFrame()

        logging.info("[i] Parquet files read and combined successfully with schema alignment.")
        return combined_df
