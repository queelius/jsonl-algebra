"""Export utilities for converting JSONL data to other formats.

This module provides functions for exporting JSONL data to CSV format,
with support for flattening nested structures and applying transformation
functions to columns.
"""

import csv
import json
import sys


def _flatten_dict(d, parent_key="", sep="."):
    """Flatten a nested dictionary using dot notation.

    Args:
        d: Dictionary to flatten.
        parent_key: Prefix for keys (used in recursion).
        sep: Separator for nested keys.

    Returns:
        A flattened dictionary with dot-separated keys.
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
    """Convert a JSONL stream to a CSV stream with smart flattening.

    Reads JSONL data and converts it to CSV format, optionally flattening
    nested structures and applying transformation functions to columns.

    Args:
        jsonl_stream: Input stream containing JSONL data.
        output_stream: Output stream for CSV data.
        flatten: Whether to flatten nested dictionaries.
        flatten_sep: Separator for flattened nested keys.
        column_functions: Dictionary mapping column names to transformation functions.
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
