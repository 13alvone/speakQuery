#!/usr/bin/env python3

import os
import logging
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import time

# Logger setup
logger = logging.getLogger(__name__)

# Configuration (default values, configurable)
MAX_DIR_SIZE_GIG = 5  # Maximum allowed space per subdirectory (in GB)
MAX_TOTAL_SIZE_GIG = 100  # Total allowed space for the entire indexes/ directory (in GB)
CLEANUP_INTERVAL_HOURS = 4  # Default interval for cleanup (in hours)
INDEXES_DIR = Path("../indexes")  # Root indexes directory

# Convert gigabytes to bytes for size calculations
GIGABYTE = 1024 ** 3
MAX_DIR_SIZE_BYTES = MAX_DIR_SIZE_GIG * GIGABYTE
MAX_TOTAL_SIZE_BYTES = MAX_TOTAL_SIZE_GIG * GIGABYTE


# Helper function to get the size of a directory
def get_directory_size(directory):
    total_size = 0
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
    return total_size


# Helper function to delete the oldest file in a directory
def delete_oldest_file(directory):
    try:
        # Get all files in the directory sorted by modification time (oldest first)
        files = sorted(Path(directory).glob('*'), key=lambda f: f.stat().st_mtime)
        for file in files:
            if file.is_file():
                logger.info(f"[i] Deleting file {file} from {directory}")
                file.unlink()
                return
    except Exception as e:
        logger.error(f"[x] Error deleting file in {directory}: {str(e)}")


# Main cleanup function
def cleanup_indexes():
    try:
        if not INDEXES_DIR.exists():
            logger.info(f"[i] Indexes directory {INDEXES_DIR} does not exist, skipping cleanup.")
            return

        total_size = 0
        subdirs = []

        # Step 1: Enforce per-subdirectory limits
        for subdir in INDEXES_DIR.glob('**/'):
            if not subdir.is_dir():
                continue

            dir_size = get_directory_size(subdir)
            logger.info(f"[i] Checking directory {subdir} with size {dir_size / GIGABYTE:.2f} GB")

            # If subdir exceeds the size limit, delete the oldest files
            while dir_size > MAX_DIR_SIZE_BYTES:
                logger.warning(f"[!] Directory {subdir} exceeds {MAX_DIR_SIZE_GIG} GB. Starting cleanup.")
                delete_oldest_file(subdir)
                dir_size = get_directory_size(subdir)

            subdirs.append((subdir, dir_size))
            total_size += dir_size

        # Step 2: Enforce the overall total size limit across all subdirectories
        if MAX_TOTAL_SIZE_GIG > 0 and total_size > MAX_TOTAL_SIZE_BYTES:
            logger.warning(f"[!] Total size exceeds {MAX_TOTAL_SIZE_GIG} GB. Starting round-robin cleanup.")
            # Round-robin deletion across all subdirectories
            while total_size > MAX_TOTAL_SIZE_BYTES:
                for subdir, _ in subdirs:
                    delete_oldest_file(subdir)
                    total_size = sum(get_directory_size(s[0]) for s in subdirs)
                    if total_size <= MAX_TOTAL_SIZE_BYTES:
                        break
    except Exception as e:
        logger.error(f"[x] Error during cleanup: {str(e)}")


# Function to periodically run cleanup based on the cron schedule
def schedule_cleanup():
    scheduler = AsyncIOScheduler()

    # Add a cron job for the cleanup task, with a configurable schedule
    scheduler.add_job(
        cleanup_indexes,
        CronTrigger(hour=f'*/{CLEANUP_INTERVAL_HOURS}', minute=0),
        id="cleanup_job"
    )

    # Start the scheduler
    scheduler.start()
    logger.info(f"[i] Cleanup scheduled to run every {CLEANUP_INTERVAL_HOURS} hours.")


# Main function to start everything
if __name__ == "__main__":
    try:
        schedule_cleanup()

        # Keep the script running
        while True:
            time.sleep(3600)  # Sleep for an hour to keep the script alive
    except (KeyboardInterrupt, SystemExit):
        logger.info("[i] Cleanup process terminated.")
