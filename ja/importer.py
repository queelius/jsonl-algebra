"""Import utilities for converting various formats to JSONL data.

This module provides functions for importing data from CSV files and directory
structures, converting them to JSONL format with type inference capabilities.
"""

import csv
import json
import os
import sys


def _infer_value(value: str):
    """Infer the appropriate Python type for a string value.

    Attempts to convert string values to more appropriate types:
    integers, floats, booleans, or None for empty strings.

    Args:
        value: String value to convert.

    Returns:
        The value converted to the most appropriate type.
    """
    if not isinstance(value, str):
        return value

    if value == "":
        return None

    # Try integer
    try:
        return int(value)
    except ValueError:
        pass

    # Try float
    try:
        return float(value)
    except ValueError:
        pass

    # Try boolean
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False

    # Try null
    if value.lower() == "null":
        return None

    return value


def dir_to_jsonl_lines(dir_path):
    """
    Reads all .json and .jsonl files in a directory, yielding each as a JSONL line.
    - For .json files, the entire file is treated as a single JSON object.
    - For .jsonl files, each line is treated as a separate JSON object.
    """
    for filename in sorted(os.listdir(dir_path)):
        file_path = os.path.join(dir_path, filename)
        if filename.endswith(".json"):
            try:
                with open(file_path, "r") as f:
                    yield f.read().strip()
            except (IOError, json.JSONDecodeError) as e:
                print(f"Error reading or parsing {file_path}: {e}", file=sys.stderr)
        elif filename.endswith(".jsonl"):
            try:
                with open(file_path, "r") as f:
                    for line in f:
                        yield line.strip()
            except IOError as e:
                print(f"Error reading {file_path}: {e}", file=sys.stderr)


def csv_to_jsonl_lines(csv_input_stream, has_header: bool, infer_types: bool = False):
    """Convert a CSV stream to JSONL lines.

    Reads CSV data and yields JSON lines, with optional type inference
    and header handling.

    Args:
        csv_input_stream: Input stream containing CSV data.
        has_header: Whether the first row contains column headers.
        infer_types: Whether to infer types for values (int, float, bool, None).

    Yields:
        JSON strings representing each row.
    """

    def process_row(row):
        if not infer_types:
            return row
        return {k: _infer_value(v) for k, v in row.items()}

    if has_header:
        # Use DictReader which handles headers automatically
        reader = csv.DictReader(csv_input_stream)
        for row in reader:
            yield json.dumps(process_row(row))
    else:
        # Use the standard reader and manually create dictionaries
        reader = csv.reader(csv_input_stream)
        headers = []
        try:
            first_row = next(reader)
            # Generate headers based on the number of columns in the first row
            headers = [f"col_{i}" for i in range(len(first_row))]
            # Yield the first row which we've already consumed
            row_dict = dict(zip(headers, first_row))
            yield json.dumps(process_row(row_dict))
        except StopIteration:
            return  # Handle empty file

        # Yield the rest of the rows
        for row in reader:
            row_dict = dict(zip(headers, row))
            yield json.dumps(process_row(row_dict))
