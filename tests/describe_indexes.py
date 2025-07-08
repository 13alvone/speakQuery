#!/usr/bin/python3
import os
import pytest
pytest.importorskip("pyarrow")
import pyarrow.parquet as pq
from subprocess import check_output
from typing import Generator


def explore_directory() -> str:
    """Run ls -lart recursively and return the output."""
    cmd = [
        "find",
        ".",
        "-type",
        "d",
        "-exec",
        "ls",
        "-lart",
        "{}",
        ";",
    ]
    return check_output(cmd).decode("utf-8")  # nosec - CLI helper for dev use only


def find_parquet_files() -> Generator[str, None, None]:
    """Find all parquet files recursively."""
    for root, _, files in os.walk('../functionality/archive'):
        for file in files:
            if file.endswith('.parquet'):
                yield os.path.join(root, file)


def read_parquet_preview(filepath: str, max_rows: int = 10) -> str:
    """Read and format parquet file contents with preview."""
    try:
        df = pq.read_table(filepath).to_pandas()
        total_rows = len(df)
        preview = df.head(max_rows)

        output = [f"\n=== PARQUET FILE: {filepath} ==="]
        output.append(str(preview))

        if total_rows > max_rows:
            output.append(f"\n[...{total_rows - max_rows} more rows truncated...]")

        return '\n'.join(output)
    except Exception as e:
        return f"Error reading {filepath}: {str(e)}"


def main():
    # Step 1: Print directory structure
    msg = "[+] Below is the layout of <project_root>/indexes/* containing source parquet files for queries. " \
          "Understanding this structure and content is crucial for validating query results in _test0.py.\n" \
          "===DIRECTORY STRUCTURE===\n"
    print(msg)
    print(explore_directory())

    # Step 2: Print parquet file contents
    for parquet_file in find_parquet_files():
        print(read_parquet_preview(parquet_file))


if __name__ == "__main__":
    main()
