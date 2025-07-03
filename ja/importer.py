"""Import data from other formats like CSV into the world of JSONL.

This module is the bridge that brings your existing data into the JSONL Algebra
ecosystem. It provides a collection of powerful functions for converting various
data formats—such as CSV or directories of individual JSON files—into the clean,
line-oriented JSONL format that `ja` is built to handle.
"""

import csv
import json
import os
import sys


def _infer_value(value: str):
    """Intelligently guess the data type of a string value.

    When importing from formats like CSV, everything starts as a string.
    This function is a smart helper that attempts to convert those strings
    into more useful Python types like integers, floats, booleans, or `None`.

    Args:
        value: The string value to analyze and convert.

    Returns:
        The value converted to a more specific type, or the original string
        if no other type is a good fit.
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
    """Stream a directory of .json or .jsonl files as a single JSONL stream.

    A handy utility for consolidating data. It reads all files ending in `.json`
    or `.jsonl` from a specified directory and yields each JSON object as a
    separate line. This is perfect for preparing a dataset that has been
    stored as many small files.

    - For `.json` files, the entire file is treated as a single JSON object.
    - For `.jsonl` files, each line is treated as a separate JSON object.

    Args:
        dir_path (str): The path to the directory to read.

    Yields:
        A string for each JSON object found, ready for processing.
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
    """Convert a stream of CSV data into a stream of JSONL lines.

    This function reads CSV data and transforms each row into a JSON object.
    It can automatically handle headers to use as keys and can even infer the
    data types of your values, converting them from strings to numbers or
    booleans where appropriate.

    Args:
        csv_input_stream: An input stream (like a file handle) containing CSV data.
        has_header (bool): Set to `True` if the first row of the CSV is a header
                           that should be used for JSON keys.
        infer_types (bool): If `True`, automatically convert values to `int`,
                            `float`, `bool`, or `None`. Defaults to `False`.

    Yields:
        A JSON-formatted string for each row in the CSV data.
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
