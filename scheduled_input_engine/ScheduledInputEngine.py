#!/usr/bin/env python3

import asyncio
import logging
import aiosqlite
import time
import sys
import os
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import signal

# Add current directory to PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from ScheduledInputBackend import ScheduledInputBackend  # Import the backend
    from SICleanup import cleanup_indexes  # Import the cleanup function
except Exception as e:
    raise e

# Setup logger
logger = logging.getLogger(__name__)

# Database paths
CURRENT_SCRIPT_DIR = Path(__file__).parent.resolve()
SCHEDULED_INPUTS_DB = CURRENT_SCRIPT_DIR.parent / 'scheduled_inputs.db'
HISTORY_DB = CURRENT_SCRIPT_DIR.parent / 'scheduled_inputs_history.db'

# Scheduler instance
scheduler = AsyncIOScheduler()

# Event loop
loop = asyncio.get_event_loop()

# Shutdown event
shutdown_event = asyncio.Event()


# Function to execute a scheduled input task
async def execute_task(task_id, title, code, cron_schedule, overwrite, subdirectory, api_url, retry_count=0):
    try:
        logger.info(f"Executing task {task_id} (Title: {title}, attempt {retry_count + 1})")

        # Capture execution start time
        execution_start_time = time.time()

        # Initialize the backend
        engine = ScheduledInputBackend()

        # Execute the task using the backend
        await loop.run_in_executor(None, engine.execute_task, title, code, overwrite, subdirectory, api_url)

        # Capture execution end time and calculate runtime
        execution_end_time = time.time()
        runtime = execution_end_time - execution_start_time

        # Store telemetry about this execution
        await store_execution_telemetry(task_id, title, runtime, execution_start_time, execution_end_time)

        logger.info(f"Task {task_id} (Title: {title}) completed successfully.")

    except Exception as e:
        logger.error(f"Task {task_id} (Title: {title}) encountered an error: {str(e)}")

        if retry_count < 3:
            backoff_time = min(2 ** retry_count, 60)  # Max backoff time of 60 seconds
            logger.warning(f"Task {task_id} (Title: {title}) failed. Retrying in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)
            await execute_task(task_id, title, code, cron_schedule, overwrite, subdirectory, api_url, retry_count + 1)
        else:
            logger.error(f"Task {task_id} (Title: {title}) failed after maximum retries.")


# Function to store telemetry in the history database
async def store_execution_telemetry(task_id, title, runtime, execution_start_time, execution_end_time):
    try:
        async with aiosqlite.connect(HISTORY_DB) as db:
            await db.execute('''
                INSERT INTO execution_history (
                    task_id, title, runtime, execution_start_time, execution_end_time
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                task_id, title, runtime, execution_start_time, execution_end_time
            ))
            await db.commit()
            logger.info(f"Logged execution telemetry for task {task_id} (Title: {title}).")

    except Exception as e:
        logger.error(f"Error storing telemetry for task {task_id} (Title: {title}): {str(e)}")


# Function to fetch scheduled inputs from the database
async def fetch_scheduled_inputs():
    try:
        async with aiosqlite.connect(SCHEDULED_INPUTS_DB) as db:
            async with db.execute('SELECT id, title, code, cron_schedule, overwrite, subdirectory, api_url FROM scheduled_inputs') as cursor:
                tasks = await cursor.fetchall()
                logger.info(f"Retrieved {len(tasks)} scheduled inputs from the database.")
                return tasks
    except Exception as e:
        logger.error(f"Error fetching scheduled inputs: {str(e)}")
        return []


# Function to schedule input tasks with APScheduler
async def schedule_input_tasks():
    tasks = await fetch_scheduled_inputs()

    for task in tasks:
        task_id, title, code, cron_schedule, overwrite, subdirectory, api_url = task
        scheduler.add_job(
            execute_task,
            CronTrigger.from_crontab(cron_schedule),
            args=[task_id, title, code, cron_schedule, overwrite, subdirectory, api_url],
            id=str(task_id),
            replace_existing=True
        )
        logger.info(f"Scheduled input task '{title}' with cron: {cron_schedule}")

    logger.info("Scheduler started for scheduled input tasks.")


# Function to store deletion events in the history database
async def store_deletion_event(deleted_file, reason):
    try:
        async with aiosqlite.connect(HISTORY_DB) as db:
            deletion_time = time.time()
            await db.execute('''
                INSERT INTO deletion_history (
                    deleted_file, deletion_time, reason
                ) VALUES (?, ?, ?)
            ''', (str(deleted_file), deletion_time, reason))
            await db.commit()
            logger.info(f"Logged deletion event for file: {deleted_file}, Reason: {reason}")

    except Exception as e:
        logger.error(f"Error storing deletion event: {str(e)}")


# Function to perform cleanup with telemetry recording
async def perform_cleanup():
    try:
        logger.info("Starting cleanup of indexes directory and subdirectories...")
        deleted_files = cleanup_indexes()  # Perform the cleanup

        if deleted_files:
            for file, reason in deleted_files:
                await store_deletion_event(file, reason)  # Log the deletion event

        logger.info("Cleanup completed.")

    except Exception as e:
        logger.error(f"Error during cleanup process: {str(e)}")


# Function to initialize the history database (if it doesn't exist)
async def initialize_history_db():
    try:
        async with aiosqlite.connect(HISTORY_DB) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS execution_history (
                    task_id INTEGER,
                    title TEXT,
                    runtime REAL,
                    execution_start_time REAL,
                    execution_end_time REAL
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS deletion_history (
                    deleted_file TEXT,
                    deletion_time REAL,
                    reason TEXT
                )
            ''')
            await db.commit()
            logger.info("Initialized history database and ensured the required tables exist.")
    except Exception as e:
        logger.error(f"Error initializing the history database: {str(e)}")


# Signal handler for graceful shutdown
def handle_shutdown_signal(signum, frame):
    logger.info(f"Received shutdown signal ({signum}). Initiating graceful shutdown...")
    shutdown_event.set()


# Main function to start everything
async def main():
    await initialize_history_db()  # Initialize the history database

    # Schedule the cleanup task to run every 4 hours
    scheduler.add_job(
        perform_cleanup,
        CronTrigger.from_crontab('0 */4 * * *'),  # Every 4 hours
        id='cleanup_job',
        replace_existing=True
    )

    # Schedule input tasks
    await schedule_input_tasks()

    # Start the scheduler
    scheduler.start()

    # Register signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown_signal, sig, None)

    logger.info("Scheduler started. Awaiting tasks...")

    # Wait indefinitely until a shutdown signal is received
    await shutdown_event.wait()

    logger.info("Shutting down scheduler...")
    scheduler.shutdown(wait=True)
    logger.info("Scheduler shut down.")


def crank_scheduled_input_engine():
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Execution process terminated.")
        scheduler.shutdown(wait=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Execution process terminated.")
        scheduler.shutdown(wait=True)
