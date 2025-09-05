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
from .subprocess_runner import run_in_subprocess

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
INPUT_REPOS_ROOT = (CURRENT_SCRIPT_DIR.parent / 'input_repos').resolve()

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
        await loop.run_in_executor(
            None,
            engine.execute_scheduled_code,
            title,
            code,
            overwrite,
            subdirectory,
        )

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

# --- New logic for repository scripts ---
async def fetch_repo_scripts():
    """Retrieve active repo scripts with their output settings."""
    try:
        async with aiosqlite.connect(SCHEDULED_INPUTS_DB) as db:
            query = (
                'SELECT rs.id, ir.path, rs.script_name, rs.cron_schedule, '
                'rs.output_subdir, rs.overwrite '
                'FROM repo_scripts rs JOIN input_repos ir ON rs.repo_id = ir.id '
                'WHERE ir.active = 1'
            )
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                logger.info(
                    f"Retrieved {len(rows)} repo scripts from the database."
                )
                return rows
    except Exception as e:
        logger.error(f"Error fetching repo scripts: {str(e)}")
        return []

async def execute_repo_script(
    script_id,
    repo_path,
    script_name,
    cron_schedule,
    output_subdir,
    overwrite,
    retry_count=0,
):
    """Run a repo script in an isolated subprocess."""
    # Resolve repository and script paths to prevent path traversal
    repo_root = Path(repo_path).resolve()
    script_path = (repo_root / script_name).resolve()

    # Ensure repository resides within the expected root directory
    if not repo_root.is_relative_to(INPUT_REPOS_ROOT):
        logger.error(
            f"[x] Repo path {repo_root} is outside allowed root {INPUT_REPOS_ROOT}"
        )
        return

    # Ensure the script is inside the repository directory
    if not script_path.is_relative_to(repo_root):
        logger.error(
            f"[x] Script path {script_path} escapes repo root {repo_root}"
        )
        return

    start = time.time()
    try:
        output_dir = Path('indexes')
        if output_subdir:
            output_dir /= output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)

        env = {
            'SPEAKQUERY_OUTPUT_DIR': str(output_dir),
            'SPEAKQUERY_OVERWRITE': '1' if overwrite else '0',
        }

        result = await run_in_subprocess(script_path, env=env)
    except FileNotFoundError:
        logger.error(f"[x] Repo script not found: {script_path}")
        return
    except Exception as exc:
        logger.error(f"[x] Failed running repo script {script_path}: {exc}")
        return

    end = time.time()
    await store_execution_telemetry(
        f"repo-{script_id}", script_name, end - start, start, end
    )

    if result.returncode != 0:
        logger.error(f"[x] Repo script {script_name} failed")

async def schedule_repo_scripts():
    """Schedule all active repo scripts."""
    scripts = await fetch_repo_scripts()
    for script in scripts:
        s_id, repo_path, name, cron, out_dir, overwrite = script
        scheduler.add_job(
            execute_repo_script,
            CronTrigger.from_crontab(cron),
            args=[s_id, repo_path, name, cron, out_dir, overwrite],
            id=f"repo_{s_id}",
            replace_existing=True,
        )
        logger.info(f"Scheduled repo script '{name}' with cron: {cron}")


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


# Function to initialize the scheduled inputs database (if it doesn't exist)
async def initialize_scheduled_inputs_db():
    """Ensure the scheduled_inputs table exists in SCHEDULED_INPUTS_DB."""
    try:
        async with aiosqlite.connect(SCHEDULED_INPUTS_DB) as db:
            await db.execute(
                '''
                CREATE TABLE IF NOT EXISTS scheduled_inputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT UNIQUE,
                    description TEXT,
                    code TEXT,
                    cron_schedule TEXT,
                    overwrite BOOLEAN,
                    subdirectory TEXT,
                    api_url TEXT,
                    created_at REAL,
                    disabled BOOLEAN DEFAULT 0
                )
                '''
            )
            async with db.execute("PRAGMA table_info(scheduled_inputs)") as cur:
                cols = [row[1] async for row in cur]
                if 'api_url' not in cols:
                    await db.execute('ALTER TABLE scheduled_inputs ADD COLUMN api_url TEXT')
            await db.commit()
            logger.info(
                "[i] Initialized scheduled inputs database and ensured the scheduled_inputs table exists."
            )
    except Exception as e:
        logger.error(f"[x] Error initializing the scheduled inputs database: {str(e)}")


# Signal handler for graceful shutdown
def handle_shutdown_signal(signum, frame):
    logger.info(f"Received shutdown signal ({signum}). Initiating graceful shutdown...")
    shutdown_event.set()


# Main function to start everything
async def main():
    await initialize_history_db()  # Initialize the history database
    await initialize_scheduled_inputs_db()  # Ensure scheduled inputs table exists

    # Schedule the cleanup task to run every 4 hours
    scheduler.add_job(
        perform_cleanup,
        CronTrigger.from_crontab('0 */4 * * *'),  # Every 4 hours
        id='cleanup_job',
        replace_existing=True
    )

    # Schedule input tasks
    await schedule_input_tasks()
    await schedule_repo_scripts()

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
