#!/usr/bin/env python3
"""
Script: build_custom_components.py
Purpose: Build/rebuild the custom compiled components (.so files) for the project.
         This script automates the build process for the custom C++ extensions located in:
            - functionality/cpp_index_call
            - functionality/cpp_datetime_parser
         It supports building individual components or all of them, with an optional force rebuild.

Usage Examples:
    # Build all components (default)
    ./build_custom_components.py

    # Build only the cpp_index_call component
    ./build_custom_components.py --component cpp_index_call

    # Force a clean rebuild of all components
    ./build_custom_components.py --rebuild
"""

import os
import sys
import argparse
import logging
import subprocess
import shutil
import sysconfig
import platform
import re

# Configure logging with the required prefixes.
logging.basicConfig(level=logging.DEBUG, format='%(message)s')


def run_command(cmd, cwd):
    """
    Executes the given command in the specified working directory.

    Args:
        cmd (list): Command and arguments to execute.
        cwd (str): The working directory where the command should be executed.

    Returns:
        str: Standard output from the command.

    Raises:
        SystemExit: Exits with an error message if the command fails.
    """
    logging.info(f"[i] Running command: {' '.join(cmd)} in {cwd}")
    try:
        result = subprocess.run(
            cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
    except subprocess.CalledProcessError as e:
        logging.error(f"[x] Command {' '.join(cmd)} failed in {cwd} with error:\n{e.stderr}")
        sys.exit(1)
    logging.info(f"[i] Command output:\n{result.stdout}")
    return result.stdout


def ensure_pybind11():
    """Ensure that pybind11 is installed and return its CMake directory."""
    try:
        import pybind11  # type: ignore
    except ModuleNotFoundError:
        logging.info("[i] pybind11 not found. Installing with pip...")
        try:
            run_command([sys.executable, "-m", "pip", "install", "pybind11"], cwd=os.getcwd())
        except SystemExit:
            logging.error("[x] Failed to install pybind11")
            sys.exit(1)
        import pybind11  # type: ignore
    try:
        cmake_dir = pybind11.get_cmake_dir()  # type: ignore
        logging.debug(f"[DEBUG] Found pybind11 CMake dir: {cmake_dir}")
        return cmake_dir
    except Exception:
        logging.debug("[DEBUG] Could not determine pybind11 CMake dir; falling back to system paths.")
        return ""


def build_component(source_dir, build_dir):
    """
    Configures and builds the component using CMake in the specified build directory.

    Args:
        source_dir (str): The source directory of the component.
        build_dir (str): The directory where the build should occur.

    Raises:
        SystemExit: Exits if any build step fails.
    """
    logging.info(f"[i] Building component from source: {source_dir} into build directory: {build_dir}")

    # If the build directory exists, check for a stale CMake cache.
    cmake_cache = os.path.join(build_dir, "CMakeCache.txt")
    if os.path.exists(build_dir) and os.path.exists(cmake_cache):
        try:
            with open(cmake_cache, "r") as f:
                cache_contents = f.read()
            match = re.search(r"CMAKE_HOME_DIRECTORY:INTERNAL=(.*)", cache_contents)
            if match:
                cached_source = match.group(1).strip()
                current_source = os.path.abspath(source_dir)
                if cached_source != current_source:
                    logging.info(f"[i] Existing CMake cache source ({cached_source}) does not match current source ({current_source}). Removing build directory.")
                    shutil.rmtree(build_dir)
        except Exception as e:
            logging.error(f"[x] Failed to check CMake cache in {build_dir}: {e}")
            sys.exit(1)

    # Create the build directory if it does not exist.
    if not os.path.exists(build_dir):
        logging.info(f"[i] Build directory {build_dir} does not exist. Creating it.")
        os.makedirs(build_dir, exist_ok=True)

    # Determine the current Python executable and virtual environment root.
    python_executable = sys.executable
    python_root = sys.prefix
    logging.debug(f"[DEBUG] Using Python executable: {python_executable}")
    logging.debug(f"[DEBUG] Inferred Python root (sys.prefix): {python_root}")

    # Determine Python include directory and library using sysconfig.
    python_include_dir = sysconfig.get_path("include")
    libdir = sysconfig.get_config_var("LIBDIR")
    ldlib = sysconfig.get_config_var("LDLIBRARY")
    python_library = os.path.join(libdir, ldlib) if libdir and ldlib else ""
    logging.debug(f"[DEBUG] Python include directory: {python_include_dir}")
    logging.debug(f"[DEBUG] Python library: {python_library}")

    # Determine the site-packages directory from the virtual environment.
    site_packages = os.path.join(
        sys.prefix, "lib", f"python{sys.version_info.major}.{sys.version_info.minor}", "site-packages"
    )
    logging.debug(f"[DEBUG] Python site-packages directory: {site_packages}")

    # Ensure pybind11 is installed and get its CMake directory
    pybind11_cmake_dir = ensure_pybind11()

    # Build the CMake configuration command.
    cmake_config_cmd = [
        "cmake",
        f"-DPYTHON_EXECUTABLE={python_executable}",
        f"-DPython3_EXECUTABLE={python_executable}",
        f"-DPython3_ROOT_DIR={python_root}",
        "-DPython3_FIND_STRATEGY=LOCATION",
        f"-DPython3_INCLUDE_DIRS={python_include_dir}",
        f"-DPython3_LIBRARIES={python_library}",
        f"-DCMAKE_PREFIX_PATH={site_packages}"
    ]
    # If we got a valid pybind11 CMake directory, add it.
    if pybind11_cmake_dir:
        cmake_config_cmd.append(f"-Dpybind11_DIR={pybind11_cmake_dir}")

    # On macOS, detect the Python build architecture using sysconfig.get_platform().
    if platform.system() == "Darwin":
        # sysconfig.get_platform() returns a string like "macosx-12.0-arm64" or "macosx-12.0-x86_64"
        platform_str = sysconfig.get_platform()
        python_arch = platform_str.split('-')[-1]
        cmake_config_cmd.append(f"-DCMAKE_OSX_ARCHITECTURES={python_arch}")
        logging.debug(f"[DEBUG] Detected macOS; added CMAKE_OSX_ARCHITECTURES flag for architecture {python_arch}.")

    # Optional: force a Release build.
    cmake_config_cmd.append("-DCMAKE_BUILD_TYPE=Release")

    # The project is assumed to have a CMakeLists.txt one directory up from the build directory.
    cmake_config_cmd.append("..")

    run_command(cmake_config_cmd, cwd=build_dir)

    # Build the component.
    run_command(["cmake", "--build", "."], cwd=build_dir)

    logging.info(f"[i] Successfully built component in {build_dir}")


def main():
    """
    Main function to parse arguments and build the selected components.
    """
    parser = argparse.ArgumentParser(
        description="Build custom components (.so files) for the project."
    )
    parser.add_argument(
        "--component",
        choices=["all", "cpp_index_call", "cpp_datetime_parser"],
        default="all",
        help="Specify which component to build (default: all)"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Force a clean rebuild by deleting the existing build directory (if it exists)"
    )
    args = parser.parse_args()

    # Determine the project root (assuming this script is placed at the project root).
    project_root = os.path.abspath(os.path.dirname(__file__))
    logging.info(f"[i] Project root: {project_root}")

    # Define paths for each component.
    components = {
        "cpp_index_call": {
            "source": os.path.join(project_root, "functionality", "cpp_index_call"),
            "build": os.path.join(project_root, "functionality", "cpp_index_call", "build")
        },
        "cpp_datetime_parser": {
            "source": os.path.join(project_root, "functionality", "cpp_datetime_parser"),
            "build": os.path.join(project_root, "functionality", "cpp_datetime_parser", "build")
        }
    }

    # Determine which components to build.
    selected_components = list(components.keys()) if args.component == "all" else [args.component]

    for comp in selected_components:
        comp_info = components.get(comp)
        if comp_info is None:
            logging.error(f"[x] Component '{comp}' is not recognized.")
            sys.exit(1)
        source_dir = comp_info["source"]
        build_dir = comp_info["build"]

        # If the rebuild flag is set, remove the existing build directory.
        if args.rebuild and os.path.exists(build_dir):
            logging.info(f"[i] Removing existing build directory for component '{comp}': {build_dir}")
            try:
                shutil.rmtree(build_dir)
            except Exception as e:
                logging.error(f"[x] Failed to remove build directory {build_dir}: {e}")
                sys.exit(1)

        # Build the component.
        build_component(source_dir, build_dir)


if __name__ == "__main__":
    main()
