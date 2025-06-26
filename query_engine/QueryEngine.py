#!/usr/bin/env python3

import asyncio
import logging
import aiosqlite
import pandas as pd
import uuid
import time
import sys
import os
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Add current directory to PYTHONPATH and import custom classes
current_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_script_dir)

# Adjust the project root path
project_root = os.path.abspath(os.path.join(current_script_dir, '..'))
sys.path.append(project_root)

try:
    from CmdExecutionBackend import process_query  # Import the process_query function directly
    from Alert import email_results  # Import the email_results function from Alert.py
    from functionality.ParquetEpochAdder import ParquetEpochAdder  # Import ParquetEpochAdder
except Exception as e:
    raise e

# Reuse the logger configuration
logger = logging.getLogger(__name__)

# Retry parameters
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # Exponential backoff factor

# File output and database locations
SEARCHES_DB = f'{current_script_dir}/../saved_searches.db'
HISTORY_DB = f'{current_script_dir}/../saved_search_history.db'
RESULTS_DIR = Path(f'{current_script_dir}/../executed_scheduled_searches')
RESULTS_DIR.mkdir(parents=True, exist_ok=True)  # Ensure the results directory exists or create it

# APScheduler instance
scheduler = AsyncIOScheduler()


# Function to recursively find all Parquet files in the indexes directory
def find_parquet_files(indexes_dir):
    parquet_files = []
    for root, dirs, files in os.walk(indexes_dir):
        for file in files:
            if file.endswith('.system4.system4.parquet'):
                file_path = os.path.join(root, file)
                parquet_files.append(file_path)
    return parquet_files


# Function to process Parquet files and add '_epoch' column
def process_parquet_files(parquet_files, date_field_name='timestamp'):
    for parquet_file in parquet_files:
        try:
            adder = ParquetEpochAdder(parquet_file, date_field_name)
            adder.process(output_file_path=parquet_file)  # Overwrite the original file
            logger.info(f"Processed Parquet file: {parquet_file}")
        except Exception as e:
            logger.error(f"Error processing Parquet file {parquet_file}: {str(e)}")


# Function to execute a task (which in this case is a query)
async def execute_query(task_id, query, title, retry_count=0):
    try:
        logger.info(f"[i] Executing task {task_id}:\nattempt {retry_count + 1}\n{query}\n")

        # Capture execution start time
        execution_start_time = time.time()

        # Execute the query using the process_query function
        result_df = process_query(query)

        # Log the result for debugging
        if result_df is None:
            logger.error(f"[x] Task {task_id} - {title} returned None. Skipping save and telemetry.")
            return

        # Check if the execution was successful
        if isinstance(result_df, pd.DataFrame) and not result_df.empty:
            execution_end_time = time.time()
            runtime = execution_end_time - execution_start_time

            # Generate a unique filename for the results
            filename = f"{int(execution_start_time)}.{uuid.uuid4()}.system4.system4.parquet"
            saved_search_path = RESULTS_DIR / filename

            # Save the result dataframe to a Parquet file (efficient storage)
            try:
                result_df.to_parquet(saved_search_path, index=False, compression='gzip')
                logger.info(f"[i] Task {task_id} - {title} results saved to {saved_search_path}.")
            except Exception as e:
                logger.error(f"[x] Error saving results for task {task_id} - {title}: {str(e)}")
                raise e

            # Store telemetry in the history database
            try:
                await store_execution_telemetry(
                    task_id, title, runtime, execution_start_time, execution_end_time, saved_search_path, len(result_df)
                )
                logger.info(f"[i] Task {task_id} - {title} telemetry stored.")
            except Exception as e:
                logger.error(f"[x] Error storing telemetry for task {task_id} - {title}: {str(e)}")
                raise e

        else:
            raise Exception(f"No data returned from query {task_id} - {title}")

    except Exception as e:
        logger.error(f"[x] Task {task_id} - {title} encountered an error: {str(e)}")
        if retry_count < MAX_RETRIES:
            backoff_time = BACKOFF_FACTOR ** retry_count
            logger.warning(f"[!] Task {task_id} - {title} failed. Retrying in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)
            await execute_query(task_id, query, title, retry_count + 1)
        else:
            logger.error(f"[x] Task {task_id} - {title} failed after {MAX_RETRIES} attempts. Error: {str(e)}")


# Function to store telemetry in the history database
async def store_execution_telemetry(task_id, query_name, runtime, execution_start_time, execution_end_time, saved_search_path, original_result_count):
    try:
        async with aiosqlite.connect(HISTORY_DB) as db:
            await db.execute('''
                INSERT INTO execution_history (
                    saved_search_filename, runtime, execution_start_time, execution_end_time, 
                    query_name, saved_search_path, original_result_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                saved_search_path.name, runtime, execution_start_time, execution_end_time,
                query_name, str(saved_search_path), original_result_count
            ))
            await db.commit()
            logger.info(f"[i] Logged execution telemetry for task {task_id}.")

    except Exception as e:
        logger.error(f"[x] Error storing telemetry for task {task_id}: {str(e)}")


# Function to initialize the history database and ensure the required table exists
async def initialize_history_db():
    try:
        async with aiosqlite.connect(HISTORY_DB) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS execution_history (
                    saved_search_filename TEXT PRIMARY KEY,
                    runtime REAL,
                    execution_start_time REAL,
                    execution_end_time REAL,
                    query_name TEXT,
                    saved_search_path TEXT,
                    original_result_count INTEGER
                )
            ''')
            await db.commit()
            logger.info("[i] Initialized history database and ensured the execution_history table exists.")
    except Exception as e:
        logger.error(f"[x] Error initializing the history database: {str(e)}")


# Function to initialize the saved searches database
async def initialize_saved_searches_db():
    """Ensure the saved_searches table exists in SEARCHES_DB."""
    try:
        async with aiosqlite.connect(SEARCHES_DB) as db:
            await db.execute(
                '''
                CREATE TABLE IF NOT EXISTS saved_searches (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    description TEXT,
                    query TEXT,
                    cron_schedule TEXT,
                    trigger TEXT,
                    lookback TEXT,
                    throttle TEXT,
                    throttle_time_period TEXT,
                    throttle_by TEXT,
                    event_message TEXT,
                    send_email TEXT,
                    email_address TEXT,
                    email_content TEXT,
                    file_location TEXT,
                    owner_id INTEGER REFERENCES users(id)
                )
                '''
            )
            await db.commit()
            logger.info(
                "[i] Initialized saved searches database and ensured the saved_searches table exists."
            )
    except Exception as e:
        logger.error(f"[x] Error initializing the saved searches database: {str(e)}")


# Function to fetch tasks from the database
async def fetch_tasks():
    try:
        async with aiosqlite.connect(SEARCHES_DB) as db:
            async with db.execute("SELECT id, title, query, cron_schedule FROM saved_searches") as cursor:
                tasks = await cursor.fetchall()
                logger.info(f"[i] Retrieved {len(tasks)} tasks from the database.")
                return tasks
    except Exception as e:
        logger.error(f"[x] Error fetching tasks from the database: {str(e)}")
        return []


# Function to schedule tasks with APScheduler
async def schedule_tasks():
    tasks = await fetch_tasks()

    for task in tasks:
        task_id, title, query, cron_schedule = task
        scheduler.add_job(
            execute_query,
            CronTrigger.from_crontab(cron_schedule),
            args=[task_id, query, title],
            id=str(task_id)
        )
        logger.info(f"[i] Scheduled task {task_id} - {title} with cron: {cron_schedule}")

    # Start the scheduler
    scheduler.start()
    logger.info("[i] Scheduler started and tasks are now running according to their schedules.")


# Main function to start everything
async def main():
    # Process Parquet files first
    indexes_dir = os.path.abspath(os.path.join(project_root, 'indexes'))
    parquet_files = find_parquet_files(indexes_dir)
    logger.info(f"Found {len(parquet_files)} Parquet files to process.")
    process_parquet_files(parquet_files, date_field_name='timestamp')  # Adjust 'timestamp' if needed

    await initialize_history_db()  # Initialize the history database
    await initialize_saved_searches_db()  # Ensure saved searches table exists
    await schedule_tasks()  # Schedule and run tasks

    # Run indefinitely
    while True:
        await asyncio.sleep(3600)  # Sleep for an hour and keep the loop alive


def crank_query_engine():
    try:
        asyncio.run(main())  # Start the main function with asyncio.run
    except (KeyboardInterrupt, SystemExit):
        logger.info("[i] Execution process terminated.")
        scheduler.shutdown()  # Properly shutdown the scheduler


if __name__ == "__main__":
    try:
        asyncio.run(main())  # Start the main function with asyncio.run
    except (KeyboardInterrupt, SystemExit):
        logger.info("[i] Execution process terminated.")
        scheduler.shutdown()  # Properly shutdown the scheduler
