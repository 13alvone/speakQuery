#!/usr/bin/env python3
import logging
import sqlite3
import time
import sys
import os
import re
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path

# Module logger
logger = logging.getLogger(__name__)

# Add current directory to PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from SICleanup import cleanup_indexes
    from SIExecution import SIExecution
    from cache import get_cached_or_fetch
except Exception as e:
    raise e


class ScheduledInputBackend:
    CURRENT_SCRIPT_DIR = Path(__file__).parent.resolve()
    SCHEDULED_INPUTS_DB = CURRENT_SCRIPT_DIR.parent / 'scheduled_inputs.db'
    INDEXES_DIR = CURRENT_SCRIPT_DIR.parent / 'indexes'

    # Input validation regex patterns
    VALID_NAME = re.compile(r'^[a-zA-Z0-9 _.-]+$')  # Allow letters, numbers, spaces, underscores, hyphens, and periods
    INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*]')  # Invalid characters for file/directory names

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.initialize_database()
        self.scheduler.start()
        logger.info("Scheduler started.")

        # Schedule the cleanup process to run periodically
        self.schedule_cleanup()

    def initialize_database(self):
        """Initialize the SQLite database for scheduled inputs."""
        with sqlite3.connect(self.SCHEDULED_INPUTS_DB) as conn:
            cursor = conn.cursor()
            cursor.execute(
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
            cursor.execute('PRAGMA table_info(scheduled_inputs)')
            cols = [c[1] for c in cursor.fetchall()]
            if 'api_url' not in cols:
                cursor.execute('ALTER TABLE scheduled_inputs ADD COLUMN api_url TEXT')
            conn.commit()

    def schedule_cleanup(self):
        """Schedule the cleanup process to run every 4 hours."""
        cron_schedule = "0 */4 * * *"  # Every 4 hours
        self.scheduler.add_job(
            cleanup_indexes,
            CronTrigger.from_crontab(cron_schedule),
            id="cleanup_job",
            replace_existing=True
        )
        logger.info("Scheduled cleanup task to run every 4 hours.")

    def validate_inputs(self, title, description, cron_schedule, overwrite, subdirectory):
        # Validate title
        if not title or not self.VALID_NAME.match(title) or self.INVALID_PATH_CHARS.search(title):
            raise ValueError('Invalid input in title field.')

        # Validate description (optional)
        if description and (not self.VALID_NAME.match(description) or self.INVALID_PATH_CHARS.search(description)):
            raise ValueError('Invalid input in description field.')

        # Validate cron schedule
        if not self.is_valid_cron(cron_schedule):
            raise ValueError('Invalid cron schedule.')

        # Validate overwrite
        if overwrite.lower() not in ['true', 'false']:
            raise ValueError("Overwrite must be 'true' or 'false'.")

        # Validate subdirectory (optional)
        if subdirectory:
            if not self.VALID_NAME.match(subdirectory) or self.INVALID_PATH_CHARS.search(subdirectory):
                raise ValueError('Invalid input in subdirectory field.')

    @staticmethod
    def is_valid_cron(cron_string):
        """Validate the cron expression."""
        try:
            CronTrigger.from_crontab(cron_string)
            return True
        except ValueError:
            return False

    def add_scheduled_input(self, title, description, code, cron_schedule, overwrite, subdirectory, api_url=None):
        # Validate inputs
        self.validate_inputs(title, description, cron_schedule, overwrite, subdirectory)

        # Convert overwrite to boolean
        overwrite_bool = overwrite.lower() == 'true'

        # Save to database
        try:
            with sqlite3.connect(self.SCHEDULED_INPUTS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''
                    INSERT INTO scheduled_inputs (
                        title, description, code, cron_schedule, overwrite,
                        subdirectory, api_url, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        title,
                        description,
                        code,
                        cron_schedule,
                        overwrite_bool,
                        subdirectory,
                        api_url,
                        time.time(),
                    ),
                )
                conn.commit()
        except sqlite3.IntegrityError:
            raise ValueError('Title must be unique.')

        # Schedule the task with APScheduler
        self.schedule_task(title, code, cron_schedule, overwrite_bool, subdirectory)

    def schedule_task(self, title, code, cron_schedule, overwrite, subdirectory):
        """
        Schedules the execution of the user's code using APScheduler.
        """
        # Remove any existing job with the same title
        if self.scheduler.get_job(job_id=title):
            self.scheduler.remove_job(job_id=title)

        # Parse the cron schedule
        try:
            trigger = CronTrigger.from_crontab(cron_schedule)
        except ValueError as e:
            raise ValueError(f"Invalid cron schedule: {str(e)}")

        # Schedule the job
        self.scheduler.add_job(
            func=self.execute_scheduled_code,
            trigger=trigger,
            args=[title, code, overwrite, subdirectory],
            id=title,
            name=title,
            replace_existing=True
        )
        logger.info(f"Scheduled task '{title}' with cron schedule '{cron_schedule}'.")

    def execute_scheduled_code(self, title, code, overwrite, subdirectory):
        """
        Executes the user's code in a secure environment.
        This method is called by the scheduler at the scheduled times.
        """
        logger.info(f"Executing scheduled input '{title}'")
        try:
            # Create an instance of SIExecution
            executor = SIExecution(code)
            # Execute the code with cache helper
            result_df = executor.execute_code({'get_cached_or_fetch': get_cached_or_fetch})

            # Handle the output file path
            output_filename = executor.output_path  # This is set during code execution

            # Determine the output directory
            if subdirectory:
                target_dir = self.INDEXES_DIR / subdirectory
            else:
                target_dir = self.INDEXES_DIR

            target_dir = target_dir.resolve()

            # Prevent directory traversal attacks
            if not str(target_dir).startswith(str(self.INDEXES_DIR.resolve())):
                raise ValueError("Invalid subdirectory path.")

            target_dir.mkdir(parents=True, exist_ok=True)

            # Construct the full output path
            output_path = target_dir / output_filename

            # Handle overwrite
            if output_path.exists():
                if overwrite:
                    # Overwrite the existing file
                    result_df.to_parquet(output_path, index=False, compression='gzip')
                    logger.info(f"Overwritten existing file at {output_path}.")
                else:
                    # Generate a new filename to avoid conflict
                    base, ext = os.path.splitext(output_filename)
                    timestamp = int(time.time())
                    new_filename = f"{base}_{timestamp}{ext}"
                    output_path = target_dir / new_filename
                    result_df.to_parquet(output_path, index=False, compression='gzip')
                    logger.info(f"Saved results to {output_path}.")
            else:
                # Save the DataFrame to the output path
                result_df.to_parquet(output_path, index=False, compression='gzip')
                logger.info(f"Saved results to {output_path}.")

        except Exception as e:
            logger.error(f"Error executing scheduled input '{title}': {str(e)}")

