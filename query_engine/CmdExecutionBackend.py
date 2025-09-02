#!/usr/bin/env python3
import logging
import antlr4
import uuid
import time
import sys
import os
import re
import pandas as pd
from pathlib import Path

# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

from lexers.antlr4_active.speakQueryLexer import speakQueryLexer
from lexers.antlr4_active.speakQueryParser import speakQueryParser
from lexers.speakQueryListener import speakQueryListener

from handlers.JavaHandler import JavaHandler
from validation.SavedSearchValidation import SavedSearchValidation

CURRENT_SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_SCRIPT_DIR.parent

java_handler = JavaHandler()
validator = SavedSearchValidation()
uuid_regex = re.compile(r'^[0-9]{10}\.[0-9a-fA-F]{6,7}_[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$')
target_db = 'saved_searches.db'
conn = ''

logger = logging.getLogger(__name__)


def run_query_and_return_results_df(query):
    logging.info(f"[i] Query received: {repr(query)}")
    try:
        query = query.replace("\r\n", "\n")  # Clean newlines from non-unix formats
        result_df = execute_query(f'{query}\n')
        logging.info(f"[i] Query result before processing: {result_df}")

        if result_df is None:
            error_msg = "[!] No data returned from query. Received NoneType result."
            logging.error(error_msg)
            return None

        if isinstance(result_df, pd.DataFrame) and result_df.empty:
            error_msg = "[!] No data returned from query. DataFrame is empty."
            logging.error(error_msg)
            return None

        result_df = result_df.fillna('')  # Fill NaN values with a default value like an empty string
        logging.info(f"[i] Query result after processing NaN values: {result_df}")

        request_id = f'{time.time()}_{str(uuid.uuid4())}'
        sanitized_df = sanitize_dataframe(result_df)
        save_dataframe(request_id, sanitized_df)

        return sanitized_df  # Return the actual DataFrame

    except Exception as e:
        logging.error(f"[x] Error processing query: {str(e)}")
        return None


def execute_query(_speak_query):
    logging.info("[i] Starting the parsing process.")
    if not isinstance(_speak_query, str):
        raise ValueError("Query must be a string")

    input_stream = antlr4.InputStream(_speak_query)
    lexer = speakQueryLexer(input_stream)
    stream = antlr4.CommonTokenStream(lexer)
    parser = speakQueryParser(stream)
    tree = parser.speakQuery()
    listener = speakQueryListener(_speak_query)
    walker = antlr4.ParseTreeWalker()
    walker.walk(listener, tree)

    # Assuming listener.main_df is the DataFrame that contains the query result
    return listener.main_df if hasattr(listener, 'main_df') else pd.DataFrame()


def sanitize_dataframe(df):
    global java_handler
    for col in df.columns:
        if df[col].dtype.name == 'object':
            df[col] = df[col].apply(lambda x: int(x) if java_handler.is_java_long(x) else x)
    return df


def save_dataframe(request_id, df):
    """Save the DataFrame to a pickle in the project's temp directory."""
    temp_dir = PROJECT_ROOT / 'frontend' / 'static' / 'temp'
    temp_dir.mkdir(parents=True, exist_ok=True)
    file_path = temp_dir / f"{request_id}.pkl"
    df.to_pickle(file_path)


# Add this function to allow direct import and usage in other scripts
def process_query(query):
    return run_query_and_return_results_df(query)


# If this script is executed directly, call the main function
if __name__ == '__main__':
    query = ' '.join(sys.argv[1:])
    process_query(query)
