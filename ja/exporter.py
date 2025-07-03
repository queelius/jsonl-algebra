"""Export your JSONL data to other popular formats like CSV.

This module is your gateway to the wider data ecosystem. It provides powerful
and flexible tools to convert your JSONL data into formats that are easy to
use with spreadsheets, traditional databases, or other data analysis tools.

The key feature is its intelligent handling of nested JSON, which can be
"flattened" into separate columns, making complex data accessible in a simple
CSV format.
"""

import csv
import json
import sys


def _flatten_dict(d, parent_key="", sep="."):
    """Recursively flatten a nested dictionary using dot notation.

    This helper function turns a nested structure like `{"user": {"id": 1}}`
    into a flat dictionary `{"user.id": 1}`.

    Args:
        d (dict): The dictionary to flatten.
        parent_key (str): The prefix to use for the keys (used in recursion).
        sep (str): The separator to use between nested keys.

    Returns:
        A new, flattened dictionary with dot-separated keys.
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict) and v:
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Serialize lists/arrays to a JSON string as a fallback.
            # A more advanced version could offer different strategies.
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)


def jsonl_to_csv_stream(
    jsonl_stream,
    output_stream,
    flatten: bool = True,
    flatten_sep: str = ".",
    column_functions: dict = None,
):
    """Convert a stream of JSONL data into a CSV stream.

    This is a highly flexible function for exporting your data. It reads JSONL
    records, intelligently discovers all possible headers (even if they vary
    between lines), and writes to a CSV format.

    It shines when dealing with nested data. By default, it will flatten
    structures like `{"user": {"name": "X"}}` into a `user.name` column.
    You can also provide custom functions to transform data on the fly.

    Args:
        jsonl_stream: An input stream (like a file handle) yielding JSONL strings.
        output_stream: An output stream (like `sys.stdout` or a file handle)
                       where the CSV data will be written.
        flatten (bool): If `True`, nested dictionaries are flattened into columns
                        with dot-separated keys. Defaults to `True`.
        flatten_sep (str): The separator to use when flattening keys.
                           Defaults to ".".
        column_functions (dict): A dictionary mapping column names to functions
                                 that will be applied to that column's data
                                 before writing to CSV. For example,
                                 `{"price": float}`.
    """
    if column_functions is None:
        column_functions = {}

    # First pass: Discover all possible headers from the entire stream
    records = [json.loads(line) for line in jsonl_stream if line.strip()]
    if not records:
        return

    # Apply column functions before flattening
    for rec in records:
        for col, func in column_functions.items():
            if col in rec:
                try:
                    rec[col] = func(rec[col])
                except Exception as e:
                    # Optionally, log this error or handle it as needed
                    print(
                        f"Error applying function to column '{col}' for a record: {e}",
                        file=sys.stderr,
                    )

    if flatten:
        processed_records = [(_flatten_dict(rec, sep=flatten_sep)) for rec in records]
    else:
        processed_records = []
        for rec in records:
            processed_rec = {}
            for k, v in rec.items():
                if isinstance(v, (dict, list)):
                    processed_rec[k] = json.dumps(v)
                else:
                    processed_rec[k] = v
            processed_records.append(processed_rec)

    # Discover all unique keys to form the CSV header
    headers = []
    header_set = set()
    for rec in processed_records:
        for key in rec.keys():
            if key not in header_set:
                header_set.add(key)
                headers.append(key)

    # Second pass: Write to the output stream
    writer = csv.DictWriter(output_stream, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(processed_records)
