#!/usr/bin/env python3

import sqlite3
from collections import Counter
import logging
from croniter import croniter
from datetime import datetime, timedelta, timezone


def suggest_next_cron_runtime(db_path='saved_searches.db'):
    """
    Suggests the next best minute and hour for a cron job to balance the load based on existing schedules,
    adjusted to the user's local time zone.

    :param db_path: Path to the SQLite database file.
    :return: Suggested cron time as a string in the format 'minute hour * * *'.
    """

    try:
        # Connect to the SQLite database
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Query to retrieve all cron schedules
            cursor.execute("SELECT cron_schedule FROM saved_searches")
            cron_schedules = cursor.fetchall()

        # Initialize a Counter for minutes in a 24-hour period (0-59 for each hour)
        cron_count = Counter()

        # Current time reference in local timezone
        now = datetime.now().astimezone()

        # Process each cron schedule
        for schedule in cron_schedules:
            if schedule and schedule[0]:
                cron_expr = schedule[0]
                try:
                    # Initialize croniter with the cron expression and current time
                    cron = croniter(cron_expr, now)

                    # Get the next 24 occurrences to cover a full day
                    for _ in range(24):
                        next_time = cron.get_next(datetime)
                        # Convert next_time to local timezone if not already
                        if next_time.tzinfo is None:
                            next_time = next_time.replace(tzinfo=timezone.utc).astimezone(tz=now.tzinfo)
                        else:
                            next_time = next_time.astimezone(now.tzinfo)
                        # Count the minute and hour
                        minute = next_time.minute
                        hour = next_time.hour
                        cron_count[(hour, minute)] += 1
                except Exception as e:
                    logging.error(f"[x] Failed to parse cron expression '{cron_expr}': {e}")
                    continue

            # Now, find the minute and hour with the least scheduled jobs
        least_jobs = float('inf')
        best_time = None

        for hour in range(24):
            for minute in range(0, 60, 5):  # Using 5-minute buckets
                count = cron_count.get((hour, minute), 0)
                if count < least_jobs:
                    least_jobs = count
                    best_time = (hour, minute)

        if best_time:
            suggested_time = f"{best_time[1]:02d} {best_time[0]:02d} * * *"
            logging.info(f"[i] Suggested next best cron runtime: {suggested_time}")
            return suggested_time
        else:
            logging.error("[x] Failed to determine the best time slot.")
            return None

    except sqlite3.Error as e:
        logging.error(f"[x] SQLite error: {str(e)}")
        return None


# Example usage
if __name__ == '__main__':
    suggested_cron_time = suggest_next_cron_runtime()
    if suggested_cron_time:
        logging.info(f"[i] Suggested Cron Time: {suggested_cron_time}")
    else:
        logging.info("[i] Could not determine a suggested cron time.")
