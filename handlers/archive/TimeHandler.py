#!/usr/bin/env python3

import logging
from datetime import datetime, timedelta, timezone
import re
import sys


class TimeHandler:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(message)s')

    @staticmethod
    def parse_relative_time(input_string, reference_date=None):
        match = re.match(r"^(-?\d+)([dhms])$", input_string)
        if match:
            value, unit = match.groups()
            value = int(value)

            if reference_date is None:
                reference_date = datetime.now(timezone.utc)

            if unit == 'd':
                return reference_date - timedelta(days=abs(value))
            elif unit == 'h':
                return reference_date - timedelta(hours=abs(value))
            elif unit == 'm':
                return reference_date - timedelta(minutes=abs(value))
            elif unit == 's':
                return reference_date - timedelta(seconds=abs(value))
        return None

    def parse_input_date(self, input_val):
        if isinstance(input_val, list):
            return [self._parse_single_input_date(val) for val in input_val]
        else:
            return self._parse_single_input_date(input_val)

    @staticmethod
    def _parse_single_input_date(input_string):
        if not input_string:
            return None

        if input_string.strip().lower() == "now":
            return datetime.now(timezone.utc)

        try:
            epoch_time = float(input_string)
            utc_dt = datetime.utcfromtimestamp(epoch_time).replace(tzinfo=timezone.utc)
            return utc_dt.astimezone()
        except ValueError:
            pass

        formats = [
            "%Y-%m-%d %H:%M:%S.%f",     # YYYY-MM-DD HH:MM:SS.mmmmmm
            "%Y-%m-%d %H:%M:%S",        # YYYY-MM-DD HH:MM:SS
            "%m/%d/%Y %H:%M:%S.%f",     # MM/DD/YYYY HH:MM:SS.mmmmmm
            "%m/%d/%Y %H:%M:%S",        # MM/DD/YYYY HH:MM:SS
            "%m-%d-%Y %H:%M:%S.%f",     # MM-DD-YYYY HH:MM:SS.mmmmmm
            "%m-%d-%Y %H:%M:%S",        # MM-DD-YYYY HH:MM:SS
            "%m/%d/%Y",                 # MM/DD/YYYY
            "%m-%d-%Y",                 # MM-DD-YYYY
            "%m/%d/%y",                 # MM/DD/YY
            "%m-%d-%y",                 # MM-DD-YY
            "%d-%m-%Y %H:%M:%S.%f",     # DD-MM-YYYY HH:MM:SS.mmmmmm
            "%d-%m-%Y %H:%M:%S",        # DD-MM-YYYY HH:MM:SS
            "%d/%m/%Y %H:%M:%S.%f",     # DD/MM/YYYY HH:MM:SS.mmmmmm
            "%d/%m/%Y %H:%M:%S",        # DD/MM/YYYY HH:MM:SS
            "%Y/%m/%d %H:%M:%S.%f",     # YYYY/MM/DD HH:MM:SS.mmmmmm
            "%Y/%m/%d %H:%M:%S",        # YYYY/MM/DD HH:MM:SS
            "%Y-%m-%d",                 # YYYY-MM-DD
            "%Y-%m-%dT%H:%M:%S.%f",     # YYYY-MM-DDTHH:MM:SS.mmmmmm
            "%Y-%m-%dT%H:%M:%S",        # YYYY-MM-DDTHH:MM:SS
            "%B %d, %Y %H:%M:%S.%f",    # October 23, 2023 14:20:30.123456
            "%B %d, %Y %H:%M:%S",       # October 23, 2023 14:20:30
            "%d %B %Y %H:%M:%S.%f",     # 23 October 2023 14:20:30.123456
            "%d %B %Y %H:%M:%S",        # 23 October 2023 14:20:30
            "%m/%d/%Y %I:%M:%S %p",     # 10/23/2023 02:20:30 PM
            "%m-%d-%Y %I:%M:%S %p",     # 10-23-2023 02:20:30 PM
            "%Y%m%d%H%M%S",             # 20231023142030
            "%Y-W%W-%w %H:%M:%S.%f",    # 2023-W43-1 14:20:30.123456 (Monday as first day)
            "%Y-W%U-%w %H:%M:%S.%f",    # 2023-W42-7 14:20:30.123456 (Sunday as first day)
        ]

        for fmt in formats:
            try:
                return datetime.strptime(input_string, fmt).replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue

        return None

    @staticmethod
    def to_epoch(dt_object):
        return dt_object.timestamp()

    @staticmethod
    def human_readable_time_difference(time_difference):
        logging.debug("[DEBUG] Received time_difference: %s", time_difference)
        remaining_seconds = time_difference.seconds
        days = time_difference.days
        hours, remaining_seconds = divmod(remaining_seconds, 3600)
        minutes, remaining_seconds = divmod(remaining_seconds, 60)
        parts = []
        if days != 0:
            parts.append(f"{abs(days)} {'day' if abs(days) == 1 else 'days'}")
        if hours != 0:
            parts.append(f"{abs(hours)} {'hour' if abs(hours) == 1 else 'hours'}")
        if minutes != 0:
            parts.append(f"{abs(minutes)} {'minute' if abs(minutes) == 1 else 'minutes'}")
        if remaining_seconds != 0 or not parts:
            parts.append(f"{abs(round(remaining_seconds, 2))} {'second' if abs(remaining_seconds) == 1 else 'seconds'}")
        human_readable = ", ".join(parts)
        if time_difference.days < 0 or time_difference.seconds < 0:
            human_readable = "-" + human_readable
        return human_readable

    def parse_time_duration(self, start_str, end_str, silent=True):
        start_date_obj = self.parse_input_date(start_str) or self.parse_relative_time(start_str)
        if start_date_obj is None:
            logging.error("[x] Invalid start date format.")
            return

        if end_str:
            end_date_obj = self.parse_relative_time(end_str, start_date_obj) or self.parse_input_date(end_str)
        else:
            end_date_obj = datetime.now(timezone.utc)

        if end_date_obj is None:
            logging.error("[x] Invalid end date format.")
            return

        if start_date_obj > end_date_obj:
            start_date_obj, end_date_obj = end_date_obj, start_date_obj

        time_difference = end_date_obj - start_date_obj
        human_readable_difference = self.human_readable_time_difference(time_difference)
        start_epoch = str(int(self.to_epoch(start_date_obj)))
        end_epoch = str(int(self.to_epoch(end_date_obj)))
        time_msg = f'earliest="{start_epoch}" latest="{end_epoch}" ' \
                   f'# START: {start_date_obj.strftime("%m/%d/%Y %H:%M:%S")} - ' \
                   f'END: {end_date_obj.strftime("%m/%d/%Y %H:%M:%S")} ({human_readable_difference})```'

        if not silent:
            logging.info(time_msg)
        return int(start_epoch), int(end_epoch), time_msg

    def filter_dates(self, *args):
        args = args[0]
        if len(args) != 3:
            logging.error('[x] Failed to parse the TIMERANGE. Expected 3 arguments.')
            return

        try:
            start = self.parse_input_date(args[0].strip('"'))
            finish = self.parse_input_date(args[1].strip('"'))
            input_dates = {x: self.parse_input_date(x) for x in args[-1] if x}
            return {_: epoch for _, epoch in input_dates.items() if start <= epoch <= finish}

        except Exception as e:
            logging.error(f'[x] An error occurred: {e}')

    @staticmethod
    def bin_time_data(df, args):
        """
        Bin data in a DataFrame based on a time span provided in a list of arguments.

        Args:
            df (pd.DataFrame): DataFrame to bin.
            args (list): List of arguments in the format containing:
                - 'bin' keyword
                - column_name (e.g., '_epoch')
                - 'span' keyword
                - '=' sign
                - number
                - unit (e.g., 'day' or 'days')

        Returns:
            pd.DataFrame: DataFrame with an additional column for the binned data.
        """
        # Initialize variables
        epoch_column = None
        number = None
        unit = None

        # Define time span units in seconds (case-insensitive, singular and plural)
        units = {
            'second': 1, 'seconds': 1,
            'minute': 60, 'minutes': 60,
            'hour': 3600, 'hours': 3600,
            'day': 86400, 'days': 86400,
            'week': 604800, 'weeks': 604800,
            'year': 31536000, 'years': 31536000
        }

        # Reserved keywords
        reserved_keywords = {'bin', 'span', '='}

        # Parse args
        i = 0
        while i < len(args):
            if args[i].lower() == 'bin':
                i += 1
                continue
            elif args[i].lower() == 'span':
                i += 1
                continue
            elif args[i] == '=':
                if i + 2 < len(args):
                    try:
                        number = float(args[i + 1])
                        unit = args[i + 2].lower()
                    except ValueError:
                        logging.error("[x] Invalid number value provided for time span.")
                        return df
                i += 3
            elif isinstance(args[i], str) and args[i].lower() not in reserved_keywords:
                epoch_column = args[i]
                i += 1
            else:
                i += 1

        # Check if essential components are extracted
        if epoch_column is None or number is None or unit is None:
            logging.error("[x] 'bin', '=', or their corresponding values are missing from the arguments.")
            return df

        # Validate epoch column
        if epoch_column not in df.columns:
            logging.error(f"[x] {epoch_column} column not found in the DataFrame.")
            return df

        # Validate unit
        if unit not in units:
            logging.error("[x] Invalid time unit specified.")
            return df

        # Calculate bin size
        bin_size = int(number * units[unit])

        # Create binning labels
        df['_epoch_bin'] = (df[epoch_column] // bin_size) * bin_size
        logging.info("[i] DataFrame rows have been binned successfully.")
        return df


if __name__ == "__main__":
    parser = TimeHandler()
    start_date_str, end_date_str = None, None
    if len(sys.argv) > 1:
        start_date_str = sys.argv[1]
    if len(sys.argv) > 2:
        end_date_str = sys.argv[2]
    parser.parse_time_duration(start_date_str, end_date_str)
