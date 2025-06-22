#!/bin/sh

# Determine the project root.
# This script is in <project_root>/tests/, so the project root is one directory up.
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
echo "[i] Project Root: $PROJECT_ROOT"

# Dynamically determine the current Python executable.
PYTHON_EXE=$(python3 -c "import sys; print(sys.executable)")
echo "[i] Using Python executable: $PYTHON_EXE"

# Install Python dependencies for the tests.
echo "[i] Installing requirements from requirements.txt"
"$PYTHON_EXE" -m pip install -r "$PROJECT_ROOT/requirements.txt"

# Auto-detect PYBIND11_ROOT if not set.
if [ -z "$PYBIND11_ROOT" ]; then
  echo "[i] PYBIND11_ROOT is not set. Attempting to auto-detect using $PYTHON_EXE..."
  PYBIND11_ROOT=$("$PYTHON_EXE" -c "import pybind11; print(pybind11.get_cmake_dir())" 2>/dev/null)
  if [ -z "$PYBIND11_ROOT" ]; then
    echo "[!] ERROR: Could not auto-detect pybind11 CMake directory. Please set the PYBIND11_ROOT environment variable."
    exit 1
  fi
  echo "[i] Auto-detected PYBIND11_ROOT: $PYBIND11_ROOT"
fi

###############################################################################
# Build the CPP Datetime Parser component
###############################################################################
echo "\n# Building CPP Datetime Parser component..."
DATETIME_BUILD_DIR="$PROJECT_ROOT/functionality/cpp_datetime_parser/build"
mkdir -p "$DATETIME_BUILD_DIR"
cd "$DATETIME_BUILD_DIR" || { echo "[x] ERROR: Cannot change directory to cpp_datetime_parser/build"; exit 1; }
rm -rf *
cmake -DPython3_EXECUTABLE="$PYTHON_EXE" -DCMAKE_PREFIX_PATH="$PYBIND11_ROOT" ..
echo "\n# Listing build directory (cpp_datetime_parser):"
ls -lart
cmake --build .
cd "$PROJECT_ROOT" || exit 1

###############################################################################
# Build the CPP Index Call component
###############################################################################
echo "\n# Building CPP Index Call component..."
INDEX_CALL_BUILD_DIR="$PROJECT_ROOT/functionality/cpp_index_call/build"
mkdir -p "$INDEX_CALL_BUILD_DIR"
cd "$INDEX_CALL_BUILD_DIR" || { echo "[x] ERROR: Cannot change directory to cpp_index_call/build"; exit 1; }
rm -rf *
cmake -DPython3_EXECUTABLE="$PYTHON_EXE" -DCMAKE_PREFIX_PATH="$PYBIND11_ROOT" ..
echo "\n# Listing build directory (cpp_index_call):"
ls -lart
cmake --build .
cd "$PROJECT_ROOT" || exit 1

###############################################################################
# Verify that the cpp_index_call shared library exists
###############################################################################
echo "\n# Checking for cpp_index_call shared library in build directory..."
if ls "$INDEX_CALL_BUILD_DIR"/cpp_index_call*.so 1> /dev/null 2>&1; then
    echo "[i] cpp_index_call shared library found."
else
    echo "[x] ERROR: cpp_index_call shared library not found in $INDEX_CALL_BUILD_DIR."
    echo "    This may indicate a build issue (possibly due to pybind11/Python version mismatch)."
    exit 1
fi

###############################################################################
# Run the tests from the project root
###############################################################################
# To ensure that the C++ code in cpp_index_call.cpp locates the index files in <project_root>/indexes,
# we change directory to the project root before running the tests.
echo "\n# Changing directory to project root so that indexes are correctly located..."
cd "$PROJECT_ROOT" || { echo "[x] ERROR: Cannot change directory to project root"; exit 1; }

# Now run _test.py by giving its relative path.
echo "\n[i] Displaying _test.py content (using project-root as current directory):"
cat tests/_test.py
echo "\n[i] Running tests: python3 tests/_test.py"
python3 tests/_test.py
if [ $? -ne 0 ]; then
  echo "[x] ERROR: Test script _test.py failed. This may be due to pybind11 compatibility issues or a Python version mismatch."
  exit 1
fi

python3 tests/describe_indexes.py

# Run Bandit security scan using the project's .bandit config
bandit -r .

