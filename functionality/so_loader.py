#!/usr/bin/env python3
"""
Module: so_loader.py
Purpose: Dynamically resolve and securely import compiled C++ shared object (.so) modules.
         This module provides the function 'resolve_and_import_so' which, given a directory and
         module prefix, selects the newest matching .so file and loads it using importlib.
"""

import os
import stat
import logging
import importlib.util
from types import ModuleType

logger = logging.getLogger(__name__)


def resolve_and_import_so(module_dir: str, module_prefix: str) -> ModuleType:
    """
    Resolve and securely import a shared object (.so) file from the specified directory.

    This function searches the provided directory for files starting with the given module_prefix
    and ending with '.so'. It validates that the file is strictly located within the provided
    directory, is readable, and is not world-writable. Among the valid files, the newest one
    (based on modification time) is selected and dynamically imported using importlib.

    Args:
        module_dir (str): The directory containing the .so files.
        module_prefix (str): The expected prefix of the .so file (also the module's short name).

    Returns:
        ModuleType: The imported module.

    Raises:
        FileNotFoundError: If the directory does not exist or no valid .so files are found.
        ImportError: If the module cannot be imported.
    """
    # Resolve the absolute and real path of the module directory.
    real_module_dir = os.path.realpath(os.path.abspath(module_dir))
    logging.info(f"[i] Resolved module_dir to {real_module_dir}")

    # Validate that the provided directory exists and is indeed a directory.
    if not os.path.isdir(real_module_dir):
        logging.error(
            f"[x] The module directory {real_module_dir} does not exist or is not a directory."
            " Run 'python build_custom_components.py' from the project root."
        )
        raise FileNotFoundError(
            f"The module directory {real_module_dir} does not exist or is not a directory."
            " Run 'python build_custom_components.py' from the project root."
        )

    valid_so_files = []
    try:
        # Iterate over all entries in the directory.
        for entry in os.listdir(real_module_dir):
            # Check if the file name starts with the module_prefix and ends with '.so'
            if not (entry.startswith(module_prefix) and entry.endswith(".so")):
                logging.debug(
                    "[DEBUG] Skipping file '%s' as it does not match prefix and suffix criteria",
                    entry,
                )
                continue

            candidate_path = os.path.realpath(os.path.join(real_module_dir, entry))
            logging.debug(f"[DEBUG] Evaluating candidate file: {candidate_path}")

            # Ensure that the candidate file is strictly within the module_dir.
            if not candidate_path.startswith(real_module_dir):
                logging.warning(
                    f"[!] File '{candidate_path}' is not within the specified module directory"
                )
                continue

            # Ensure the candidate is a file.
            if not os.path.isfile(candidate_path):
                logging.debug(f"[DEBUG] '{candidate_path}' is not a file")
                continue

            # Check that the file is readable.
            if not os.access(candidate_path, os.R_OK):
                logging.warning(f"[!] File '{candidate_path}' is not readable")
                continue

            # Check that the file is not world-writable.
            file_stat = os.stat(candidate_path)
            if file_stat.st_mode & stat.S_IWOTH:
                logging.warning(f"[!] File '{candidate_path}' is world-writable, skipping")
                continue

            # If all checks pass, add the candidate to the list.
            valid_so_files.append(candidate_path)
    except Exception as e:
        logging.error(f"[x] Error while listing directory '{real_module_dir}': {e}")
        raise e

    # Ensure at least one valid .so file was found.
    if not valid_so_files:
        logging.error(
            f"[x] No valid .so files found in '{real_module_dir}' with prefix '{module_prefix}'."
            " Run 'python build_custom_components.py' from the project root to build them."
        )
        raise FileNotFoundError(
            f"No valid .so files found in '{real_module_dir}' with prefix '{module_prefix}'."
            " Run 'python build_custom_components.py' from the project root to build them."
        )

    # Select the newest file based on modification time.
    newest_file = max(valid_so_files, key=lambda f: os.stat(f).st_mtime)
    logging.info(f"[i] Selected newest file: {newest_file}")

    # Dynamically import the .so file using importlib.
    try:
        # Use the provided module_prefix as the module's name so it maps to the
        # exported functions correctly.
        module_name = module_prefix
        spec = importlib.util.spec_from_file_location(module_name, newest_file)
        if spec is None:
            logging.error(f"[x] Could not create a module spec for '{newest_file}'")
            raise ImportError(f"Could not create a module spec for '{newest_file}'")

        module = importlib.util.module_from_spec(spec)
        # Execute the module to load it.
        spec.loader.exec_module(module)
        logging.info(f"[i] Successfully imported module '{module_name}' from '{newest_file}'")
        return module
    except Exception as e:
        logging.error(f"[x] Failed to import module from '{newest_file}': {e}")
        raise ImportError(f"Failed to import module from '{newest_file}'") from e


# For direct module testing (if needed)
if __name__ == "__main__":
    try:
        # Example usage; adjust the paths and module_prefix as needed.
        test_module = resolve_and_import_so("functionality/cpp_index_call/build", "cpp_index_call")
        logging.info(f"[i] Test module loaded: {test_module}")
    except Exception as error:
        logging.error(f"[x] Error during module loading: {error}")
