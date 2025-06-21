#!/usr/bin/env python3

import jpype
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Module-level variables
_java_classes_cache = {}


def start_jvm():
    """
    Starts the JVM if it is not already started.
    """
    if not jpype.isJVMStarted():
        try:
            jpype.startJVM()
            logging.info("JVM started.")
        except Exception as e:
            logging.error(f"Error starting JVM: {e}")
            raise
    else:
        logging.debug("JVM is already started.")


def shutdown_jvm():
    """
    Shuts down the JVM if it is running.
    """
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
        logging.info("JVM shut down.")
    else:
        logging.debug("JVM is not running.")


def get_java_class(class_name):
    """
    Retrieves a Java class from the cache or loads it if not already cached.

    Parameters:
    -----------
    class_name : str
        The fully qualified name of the Java class.

    Returns:
    --------
    jpype.JClass
        The Java class.
    """
    if class_name not in _java_classes_cache:
        try:
            _java_classes_cache[class_name] = jpype.JClass(class_name)
        except Exception as e:
            logging.error(f"Error loading Java class '{class_name}': {e}")
            raise
    return _java_classes_cache[class_name]


class JavaHandler:
    """
    A class to efficiently check Java primitive types in Python using JPype.
    """

    @staticmethod
    def is_java_long(entry):
        """
        Checks if the given entry is an instance of java.lang.Long.

        Parameters:
        -----------
        entry : object
            The entry to check.

        Returns:
        --------
        bool
            True if entry is an instance of java.lang.Long, False otherwise.
        """
        start_jvm()
        java_long = get_java_class('java.lang.Long')
        return isinstance(entry, java_long)

    @staticmethod
    def is_java_integer(entry):
        """
        Checks if the given entry is an instance of java.lang.Integer.
        """
        start_jvm()
        java_integer = get_java_class('java.lang.Integer')
        return isinstance(entry, java_integer)

    @staticmethod
    def is_java_double(entry):
        """
        Checks if the given entry is an instance of java.lang.Double.
        """
        start_jvm()
        java_double = get_java_class('java.lang.Double')
        return isinstance(entry, java_double)

    @staticmethod
    def is_java_float(entry):
        """
        Checks if the given entry is an instance of java.lang.Float.
        """
        start_jvm()
        java_float = get_java_class('java.lang.Float')
        return isinstance(entry, java_float)

    @staticmethod
    def is_num(entry):
        """
        Checks if the given entry is an instance of any Java numeric type.

        Returns:
        --------
        bool
            True if entry is an instance of a Java numeric type, False otherwise.
        """
        start_jvm()
        java_numeric_types = (
            get_java_class('java.lang.Long'),
            get_java_class('java.lang.Integer'),
            get_java_class('java.lang.Double'),
            get_java_class('java.lang.Float'),
        )
        return isinstance(entry, java_numeric_types)

    @staticmethod
    def process_entries(entries, java_type):
        """
        Processes a batch of entries and checks if they are instances of the specified Java type.

        Parameters:
        -----------
        entries : list
            The list of entries to check.
        java_type : str
            The Java type to check against (e.g., 'java.lang.Long').

        Returns:
        --------
        list
            A list of booleans indicating if each entry is an instance of the specified Java type.
        """
        start_jvm()
        try:
            java_class = get_java_class(java_type)
        except Exception as e:
            logging.error(f"Error processing entries: {e}")
            return [False] * len(entries)
        results = [isinstance(entry, java_class) for entry in entries]
        return results


# Example usage (can be moved to a separate test module)
if __name__ == "__main__":
    import numpy as np

    # Example NumPy array
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)

    # Start the JVM
    start_jvm()

    # Get the Java Long class
    java_long_class = get_java_class('java.lang.Long')

    # Example conversion to Java Long for simulation
    entries = [java_long_class(int(item)) for item in arr]

    # Process entries in batch and check if they are instances of java.lang.Long
    results = JavaHandler.process_entries(entries, 'java.lang.Long')

    for entry, result in zip(entries, results):
        logging.info(f"{entry} is an instance of java.lang.Long: {result}")

    # Check if entries are any Java numeric type
    for entry in entries:
        is_num = JavaHandler.is_num(entry)
        logging.info(f"{entry} is a Java numeric type: {is_num}")

    # Shutdown JVM if no further interaction with Java is needed
    shutdown_jvm()
