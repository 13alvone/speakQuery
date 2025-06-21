import re
from email.utils import parseaddr
from croniter import croniter


class SavedSearchValidation:
    """
    A class containing static methods for validating saved search parameters.
    """

    # Pre-compiled regex patterns
    UTF8_REGEX = re.compile(r"^[\w\s\(\)\[\]\-\_\.\,]*$", re.UNICODE)
    TIME_PERIOD_REGEX = re.compile(r"^\d+[smhdw]$")
    LOOKBACK_REGEX = re.compile(r"^(-\d+[smhdw])+$")

    @staticmethod
    def validate_utf8(value):
        """
        Validates that the value contains only UTF-8 characters and allowed special characters.

        :param value: The string to validate.
        :return: The original value if valid.
        :raises ValueError: If the value contains invalid characters.
        """
        if not isinstance(value, str) or not SavedSearchValidation.UTF8_REGEX.match(value):
            raise ValueError(
                f"Invalid characters detected in '{value}'. "
                "Only UTF-8 word characters and the allowed special characters '(),[]-_.' are permitted."
            )
        return value

    @staticmethod
    def validate_email(value):
        """
        Validates the email address format.

        :param value: The email address to validate.
        :return: The original value if valid.
        :raises ValueError: If the email address format is invalid.
        """
        if not isinstance(value, str) or '@' not in parseaddr(value)[1]:
            raise ValueError(f"Invalid email address format: '{value}'.")
        return value

    @staticmethod
    def validate_cron_schedule(value):
        """
        Validates the cron schedule format using croniter.

        :param value: The cron schedule string to validate.
        :return: The original value if valid.
        :raises ValueError: If the cron schedule format is invalid.
        """
        if not isinstance(value, str) or not croniter.is_valid(value):
            raise ValueError(f"Invalid cron schedule format: '{value}'.")
        return value

    @staticmethod
    def validate_trigger(value):
        """
        Validates the trigger value.

        :param value: The trigger value to validate.
        :return: The lowercase trigger value if valid.
        :raises ValueError: If the trigger value is invalid.
        """
        if not isinstance(value, str) or value.lower() not in {"once", "per result"}:
            raise ValueError("Trigger must be either 'Once' or 'Per Result'.")
        return value.lower()

    @staticmethod
    def validate_lookback(value):
        """
        Validates the lookback period format.

        :param value: The lookback string to validate.
        :return: The original value if valid.
        :raises ValueError: If the lookback format is invalid.
        """
        if not isinstance(value, str):
            raise ValueError(
                f'Lookback value "{value}" is not natively a string!'
            )

        elif not SavedSearchValidation.LOOKBACK_REGEX.match(value):
            raise ValueError(
                f"Invalid lookback format: '{value}'. "
                "Lookback must be a sequence of time periods like '-1s', '-1m', '-1h', '-1d', '-1w'."
            )
        return value

    @staticmethod
    def validate_boolean(value):
        """
        Validates a boolean value represented as 'yes' or 'no'.

        :param value: The value to validate.
        :return: The lowercase value if valid.
        :raises ValueError: If the value is not 'yes' or 'no'.
        """
        if not isinstance(value, str) or value.lower() not in {"yes", "no"}:
            raise ValueError("Value must be 'yes' or 'no'.")
        return value.lower()

    @staticmethod
    def validate_throttle_time_period(value):
        """
        Validates the throttle time period format.

        :param value: The throttle time period string to validate.
        :return: The original value if valid.
        :raises ValueError: If the format is invalid.
        """
        return SavedSearchValidation.validate_lookback(value)
