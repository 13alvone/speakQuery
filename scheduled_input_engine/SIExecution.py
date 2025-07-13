#!/usr/bin/env python3
import ast
import time
import uuid
import logging
import os
import pandas as pd
from pathlib import Path
from RestrictedPython import compile_restricted
from RestrictedPython.Guards import safe_builtins
from RestrictedPython import utility_builtins

# Module logger
logger = logging.getLogger(__name__)


class SIExecution:
    def __init__(self, code: str, test_mode: bool = False, timestamp_fields=None):
        self.original_code = code
        self.test_mode = test_mode
        self.df_variable = None
        self.output_path = None
        self.result_df = None
        self.timestamp_fields = timestamp_fields or ["TIMESTAMP"]  # List of timestamp fields to try

        # Define INDEXES_DIR inside the class constructor
        self.INDEXES_DIR = Path("../indexes")  # Adjust this path as per your directory structure

        self.modified_code = self.process_code()

        # On initialization, recursively ensure all system4.system4.parquet files have _epoch column
        self.ensure_epoch_in_all_files()

    def add_epoch_column(self, df):
        """
        Adds the _epoch column to the DataFrame if it doesn't exist.
        Uses the specified timestamp-like field to generate the epoch.
        """
        if '_epoch' not in df.columns:
            for field in self.timestamp_fields:
                if field in df.columns:
                    try:
                        df['_epoch'] = pd.to_datetime(df[field], errors='coerce').astype(int) // 10**9
                        logger.info(f"[i] Added _epoch column using {field} field.")
                        break
                    except Exception as e:
                        logger.error(f"[x] Failed to parse {field} field into epoch: {e}")
            if '_epoch' not in df.columns:
                raise ValueError(f"None of the timestamp fields {self.timestamp_fields} could be parsed.")
        else:
            logger.info("[i] _epoch column already exists.")

    def ensure_epoch_in_all_files(self):
        """
        Recursively checks each system4.system4.parquet file in the indexes/ directory.
        Adds the _epoch column if it doesn't exist.
        """
        logger.info("[i] Checking and ensuring _epoch column in all Parquet files within indexes/")
        for root, dirs, files in os.walk(self.INDEXES_DIR):
            for file in files:
                if file.endswith(".system4.system4.parquet"):
                    file_path = Path(root) / file
                    try:
                        logger.info(f"[i] Processing Parquet file: {file_path}")
                        df = pd.read_parquet(file_path)
                        if '_epoch' not in df.columns:
                            self.add_epoch_column(df)
                            df.to_parquet(file_path, index=False, compression='gzip')
                            logger.info(f"[i] Updated Parquet file {file_path} with _epoch column.")
                        else:
                            logger.info(f"[i] Parquet file {file_path} already contains _epoch column.")
                    except Exception as e:
                        logger.error(f"[x] Failed to process Parquet file {file_path}: {str(e)}")

    def process_code(self):
        """
        Processes the user-provided code safely by:

        - Parsing the code using the AST module.
        - Ensuring that only allowed nodes are present.
        - In execution mode, replaces GENERATE_RESULTS(df_variable, 'output_path') with code to save the DataFrame.
        """
        try:
            # Parse the code into an AST
            tree = ast.parse(self.original_code, mode='exec')

            # Only apply the transformer if not in test mode
            if not self.test_mode:
                # Walk through the AST to find and process GENERATE_RESULTS
                transformer = GenerateResultsTransformer()
                transformer.visit(tree)

                if not transformer.found_generate_results:
                    raise ValueError("Code must contain GENERATE_RESULTS(<df_variable>) at the end.")

                self.df_variable = transformer.df_variable
                self.output_path = transformer.output_path

                # Ensure the output path is a safe string
                if not isinstance(self.output_path, str) or not self.output_path.endswith('.system4.system4.parquet'):
                    raise ValueError("Output path must be a string ending with '.system4.system4.parquet'.")
            else:
                # In test mode, we don't modify the code but need to extract df_variable
                # Walk through the AST to find GENERATE_RESULTS call
                extractor = GenerateResultsExtractor()
                extractor.visit(tree)

                if not extractor.found_generate_results:
                    raise ValueError("Code must contain GENERATE_RESULTS(<df_variable>) at the end.")

                self.df_variable = extractor.df_variable

            # Compile the AST into code
            modified_code = compile_restricted(
                ast.fix_missing_locations(tree),
                filename='<string>',
                mode='exec'
            )
            return modified_code

        except Exception as e:
            logger.error(f"Error processing code: {str(e)}")
            raise

    # def execute_code(self):
    #     """
    #     Executes the modified code within a restricted environment and saves the DataFrame.
    #     """
    #     try:
    #         # Prepare the secure built-ins and globals
    #         restricted_globals = dict(safe_builtins)
    #         restricted_globals.update(utility_builtins)
    #         restricted_globals['_getiter_'] = iter
    #         restricted_globals['_getitem_'] = lambda obj, index: obj[index]
    #         restricted_globals['_print_'] = print
    #         restricted_globals['_getattr_'] = getattr
    #         restricted_globals['_write_'] = lambda obj: obj
    #
    #         # Add built-in functions that are safe
    #         restricted_globals['sorted'] = sorted
    #         restricted_globals['len'] = len
    #         restricted_globals['range'] = range
    #         restricted_globals['enumerate'] = enumerate
    #         restricted_globals['min'] = min
    #         restricted_globals['max'] = max
    #
    #         # Include allowed modules
    #         restricted_globals['pd'] = pd
    #         restricted_globals['pandas'] = pd
    #
    #         # Provide FETCH_API_DATA to the user's script
    #         restricted_globals['FETCH_API_DATA'] = fetch_api_data
    #
    #         # Execute the code in a restricted environment
    #         restricted_locals = {}
    #         exec(self.modified_code, restricted_globals, restricted_locals)
    #
    #         # Retrieve the DataFrame
    #         if self.df_variable in restricted_locals:
    #             result_df = restricted_locals[self.df_variable]
    #             if isinstance(result_df, pd.DataFrame):
    #                 # Save the DataFrame to the specified output path
    #                 result_df.to_parquet(self.output_path, index=False, compression='gzip')
    #                 logger.info(f"DataFrame saved to {self.output_path}")
    #                 return result_df
    #             else:
    #                 raise ValueError(f"The variable '{self.df_variable}' is not a pandas DataFrame.")
    #         else:
    #             raise ValueError(f"DataFrame variable '{self.df_variable}' not found in the executed code.")
    #
    #     except Exception as e:
    #         logger.error(f"Error executing code: {str(e)}")
    #         raise

    def execute_code(self, extra_globals: dict | None = None):
        """Execute the modified code with optional helper functions.

        Parameters
        ----------
        extra_globals : dict | None
            Mapping of additional globals exposed to the executed script.
        """
        try:
            restricted_globals = dict(safe_builtins)
            restricted_globals['pd'] = pd
            if extra_globals:
                restricted_globals.update(extra_globals)

            restricted_locals = {}
            exec(self.modified_code, restricted_globals, restricted_locals)  # nosec B102 - dynamic code execution required for user jobs

            if self.df_variable in restricted_locals:
                result_df = restricted_locals[self.df_variable]
                if isinstance(result_df, pd.DataFrame):
                    # Add _epoch column if missing
                    self.add_epoch_column(result_df)
                    # Save the DataFrame
                    result_df.to_parquet(self.output_path, index=False, compression='gzip')
                    logger.info(f"DataFrame saved to {self.output_path}")
                    return result_df
                else:
                    raise ValueError(
                        f"[x] The variable '{self.df_variable}' is not a pandas DataFrame. Received {type(result_df)}")
            else:
                raise ValueError(f"[x] DataFrame variable '{self.df_variable}' not found in the executed code.")

        except Exception as e:
            logger.error(f"Error executing code: {e}")
            raise

    def execute_scheduled_code(self, title, code, overwrite, subdirectory):
        """
        Executes the user's code in a secure environment.
        This method is called by the scheduler at the scheduled times.
        """
        logger.info(f"Executing scheduled input '{title}'")
        try:
            # Create an instance of SIExecution with a list of possible timestamp fields
            executor = SIExecution(code, timestamp_fields=['TIMESTAMP', 'DATE', 'CREATED_AT'])

            # Execute the code and get the result DataFrame
            result_df = executor.execute_code()

            # Handle the output file path
            output_filename = executor.output_path

            # Determine the output directory
            if subdirectory:
                target_dir = self.INDEXES_DIR / subdirectory
            else:
                target_dir = self.INDEXES_DIR

            target_dir = target_dir.resolve()

            if not str(target_dir).startswith(str(self.INDEXES_DIR.resolve())):
                raise ValueError("Invalid subdirectory path.")

            target_dir.mkdir(parents=True, exist_ok=True)
            output_path = target_dir / output_filename

            if output_path.exists():
                if overwrite:
                    result_df.to_parquet(output_path, index=False, compression='gzip')
                    logger.info(f"Overwritten existing file at {output_path}.")
                else:
                    base, ext = os.path.splitext(output_filename)
                    timestamp = int(time.time())
                    new_filename = f"{base}_{timestamp}{ext}"
                    output_path = target_dir / new_filename
                    result_df.to_parquet(output_path, index=False, compression='gzip')
                    logger.info(f"Saved results to {output_path}.")
            else:
                result_df.to_parquet(output_path, index=False, compression='gzip')
                logger.info(f"Saved results to {output_path}.")

        except Exception as e:
            logger.error(f"Error executing scheduled input '{title}': {e}")

    def execute_code_test(self):
        """
        Executes the code in test mode and returns a summary of the DataFrame.
        """
        try:
            # Prepare the secure built-ins and globals
            restricted_globals = dict(safe_builtins)
            restricted_globals.update(utility_builtins)
            restricted_globals['_getiter_'] = iter
            restricted_globals['_getitem_'] = lambda obj, index: obj[index]
            restricted_globals['_print_'] = print
            restricted_globals['_getattr_'] = getattr
            restricted_globals['_write_'] = lambda obj: obj

            # Add built-in functions that are safe
            restricted_globals['sorted'] = sorted
            restricted_globals['len'] = len
            restricted_globals['range'] = range
            restricted_globals['enumerate'] = enumerate
            restricted_globals['min'] = min
            restricted_globals['max'] = max

            # Include allowed modules
            restricted_globals['pd'] = pd
            restricted_globals['pandas'] = pd

            # Provide FETCH_API_DATA to the user's script
            restricted_globals['FETCH_API_DATA'] = fetch_api_data

            # Provide a dummy GENERATE_RESULTS function
            def GENERATE_RESULTS(df):
                self.result_df = df  # Store the DataFrame for summary

            restricted_globals['GENERATE_RESULTS'] = GENERATE_RESULTS

            # Execute the code in a restricted environment
            restricted_locals = {}
            exec(self.modified_code, restricted_globals, restricted_locals)  # nosec B102 - dynamic code execution required for user jobs

            # Generate a summary of the DataFrame
            if hasattr(self, 'result_df') and isinstance(self.result_df, pd.DataFrame):
                df_head = self.result_df.head().to_string()
                from io import StringIO
                buffer = StringIO()
                self.result_df.info(buf=buffer)
                df_info = buffer.getvalue()
                summary = f"DataFrame Head:\n{df_head}\n\nDataFrame Info:\n{df_info}"
                return summary
            else:
                raise ValueError("No DataFrame found. Ensure you pass a pandas DataFrame to GENERATE_RESULTS().")

        except Exception as e:
            logger.error(f"Error executing code in test mode: {str(e)}")
            raise

    def get_modified_code(self):
        """
        Returns the modified code object.
        """
        return self.modified_code

class GenerateResultsTransformer(ast.NodeTransformer):
    """
    AST transformer that processes GENERATE_RESULTS calls.
    """
    def __init__(self):
        self.found_generate_results = False
        self.df_variable = None
        self.output_path = None

    def visit_Expr(self, node):
        # Check if the node is an expression containing a call to GENERATE_RESULTS
        if isinstance(node.value, ast.Call):
            call_node = node.value
            if isinstance(call_node.func, ast.Name) and call_node.func.id == 'GENERATE_RESULTS':
                self.found_generate_results = True

                # Handle arguments
                if len(call_node.args) == 1:
                    # Only DataFrame variable provided
                    if isinstance(call_node.args[0], ast.Name):
                        self.df_variable = call_node.args[0].id
                    else:
                        raise ValueError("The argument to GENERATE_RESULTS must be the DataFrame variable name.")
                    # Generate a unique output path
                    self.output_path = f'{time.time()}_{uuid.uuid4()}.system4.system4.parquet'
                elif len(call_node.args) == 2:
                    # DataFrame variable and output path provided
                    if isinstance(call_node.args[0], ast.Name):
                        self.df_variable = call_node.args[0].id
                    else:
                        raise ValueError("First argument to GENERATE_RESULTS must be the DataFrame variable name.")
                    if isinstance(call_node.args[1], ast.Str):
                        self.output_path = call_node.args[1].s
                    else:
                        raise ValueError("Second argument to GENERATE_RESULTS must be the output path as a string.")
                else:
                    raise ValueError("GENERATE_RESULTS must be called with one or two arguments.")

                # Remove the GENERATE_RESULTS call by returning None
                return None  # This effectively removes the node
        return self.generic_visit(node)

class GenerateResultsExtractor(ast.NodeVisitor):
    """
    AST visitor that extracts the DataFrame variable name from GENERATE_RESULTS call without modifying the AST.
    """
    def __init__(self):
        self.found_generate_results = False
        self.df_variable = None

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name) and node.func.id == 'GENERATE_RESULTS':
            self.found_generate_results = True

            if len(node.args) >= 1:
                if isinstance(node.args[0], ast.Name):
                    self.df_variable = node.args[0].id
                else:
                    raise ValueError("The argument to GENERATE_RESULTS must be the DataFrame variable name.")
            else:
                raise ValueError("GENERATE_RESULTS must be called with at least one argument.")

        # Continue visiting
        self.generic_visit(node)

def fetch_api_data(url):
    import requests
    from urllib.parse import urlparse

    # Define allowed domains
    allowed_domains = ['financialmodelingprep.com', 'api.example.com', 'world.openpetfoodfacts.org']

    # Parse the URL
    parsed_url = urlparse(url)
    domain = parsed_url.netloc

    # Validate the domain
    if domain not in allowed_domains:
        raise ValueError(f"Domain '{domain}' is not allowed.")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise ValueError(f"Error fetching data from {url}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # User-provided code (as a string)
    user_code = """
import pandas as pd

data = FETCH_API_DATA('https://world.openpetfoodfacts.org/api/v0/product/20106836.json')

df = pd.DataFrame([data])

# Generate results
GENERATE_RESULTS(df)
"""

    try:
        processor = SIExecution(user_code, test_mode=True)
        df_summary = processor.execute_code_test()  # Execute the code in test mode
        logger.info("Processed code executed successfully.")
        logger.info("DataFrame Summary:")
        logger.info(df_summary)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
