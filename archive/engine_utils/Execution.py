#!/usr/bin/env python3

import asyncio
import logging
import subprocess

# Reuse the logger configuration from the Scheduler
logger = logging.getLogger(__name__)

# Retry parameters
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # Exponential backoff factor


async def execute_task(task_id, script, retry_count=0):
    try:
        logger.info(f"[i] Executing task {task_id} - {script}, attempt {retry_count + 1}")
        proc = await asyncio.create_subprocess_exec(
            'python3', '../query_engine/CmdExecutionBackend.py', script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            logger.info(f"[i] Task {task_id} completed successfully.")
        else:
            logger.error(f"[x] Task {task_id} failed with error: {stderr.decode().strip()}")
            raise subprocess.CalledProcessError(proc.returncode, script, output=stdout, stderr=stderr)

    except subprocess.CalledProcessError as e:
        if retry_count < MAX_RETRIES:
            backoff_time = BACKOFF_FACTOR ** retry_count
            logger.warning(f"[!] Task {task_id} failed. Retrying in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)
            await execute_task(task_id, script, retry_count + 1)
        else:
            logger.error(f"[x] Task {task_id} failed after {MAX_RETRIES} attempts.")

    except Exception as e:
        logger.error(f"[x] Unexpected error executing task {task_id}: {str(e)}")


# Example execution within the Scheduler context
async def schedule_and_execute():
    tasks = [
        {"id": 1, "script": "example_task.py", "cron_expression": "* * * * *"},
        # Add more tasks as needed
    ]

    for task in tasks:
        # This would typically be called by the APScheduler cron jobs
        await execute_task(task["id"], task["script"])

# Main event loop to demonstrate execution
if __name__ == "__main__":
    asyncio.run(schedule_and_execute())
