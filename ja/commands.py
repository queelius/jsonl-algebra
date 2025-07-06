"""Command handlers for the JSONL algebra CLI.

This module connects the command-line interface to the core data processing
functions. Each `handle_*` function is responsible for reading input data,
calling the appropriate core function, and writing the results to stdout.
"""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List

import jmespath.exceptions

from .core import (
    collect,
    difference,
    distinct,
    intersection,
    join,
    product,
    project,
    rename,
    select,
    sort_by,
    union,
)
from .group import (
    groupby_agg,
    groupby_chained,
    groupby_with_metadata,
)
from .agg import (
    aggregate_grouped_data,
    aggregate_single_group,
)
from .export import json_array_to_jsonl_lines, jsonl_to_dir, jsonl_to_json_array_string
from .exporter import jsonl_to_csv_stream
from .importer import csv_to_jsonl_lines, dir_to_jsonl_lines
from .schema import infer_schema


@contextmanager
def get_input_stream(file_path):
    """
    Yield a readable file-like object.

    - If file_path is None or '-', yield sys.stdin.
    - Otherwise open the given path for reading.
    """
    if file_path is not None and file_path != "-":
        f = open(file_path, "r")
        try:
            yield f
        finally:
            f.close()
    else:
        yield sys.stdin


def read_jsonl(input_stream) -> List[Dict[str, Any]]:
    """Read JSONL data from a file-like object."""
    return [json.loads(line) for line in input_stream]


def write_jsonl(rows: List[Dict[str, Any]]) -> None:
    """Write a collection of objects as JSONL to stdout."""
    for row in rows:
        print(json.dumps(row))


def write_json_object(obj: Any) -> None:
    """Write a single object as pretty-printed JSON to stdout."""
    print(json.dumps(obj, indent=2))


def json_error(error_type: str, message: str, details: Dict[str, Any] = None) -> None:
    """Print a JSON error message to stderr and exit."""
    error_info = {
        "error": {
            "type": error_type,
            "message": message,
        }
    }
    if details:
        error_info["error"]["details"] = details
    print(json.dumps(error_info), file=sys.stderr)
    sys.exit(1)


# Command handlers
def handle_select(args):
    """Handle select command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    use_jmespath = hasattr(args, "jmespath") and args.jmespath

    try:
        result = select(data, args.expr, use_jmespath=use_jmespath)
        write_jsonl(result)
    except jmespath.exceptions.ParseError as e:
        json_error(
            "JMESPathParseError",
            f"Invalid JMESPath expression: {e}",
            {"expression": args.expr},
        )


def handle_project(args):
    """Handle project command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    use_jmespath = hasattr(args, "jmespath") and args.jmespath

    try:
        result = project(data, args.expr, use_jmespath=use_jmespath)
        write_jsonl(result)
    except jmespath.exceptions.ParseError as e:
        json_error(
            "JMESPathParseError",
            f"Invalid JMESPath expression: {e}",
            {"expression": args.expr},
        )


def handle_join(args):
    """Handle join command."""
    with get_input_stream(args.left) as f:
        left = read_jsonl(f)
    with get_input_stream(args.right) as f:
        right = read_jsonl(f)

    lcol_str, rcol_str = args.on.split("=", 1)
    lcol = lcol_str.strip()
    rcol = rcol_str.strip()

    result = join(left, right, [(lcol, rcol)])
    write_jsonl(result)


def handle_product(args):
    """Handle product command."""
    with get_input_stream(args.left) as f:
        left_data = read_jsonl(f)
    with get_input_stream(args.right) as f:
        right_data = read_jsonl(f)

    result = product(left_data, right_data)
    write_jsonl(result)


def handle_rename(args):
    """Handle rename command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    mapping_pairs = args.mapping.split(",")
    mapping = {}
    for pair_str in mapping_pairs:
        parts = pair_str.split("=", 1)
        if len(parts) == 2:
            old_name, new_name = parts
            mapping[old_name.strip()] = new_name.strip()
        else:
            print(
                f"Warning: Malformed rename pair '{pair_str.strip()}' ignored.",
                file=sys.stderr,
            )

    result = rename(data, mapping)
    write_jsonl(result)


def handle_union(args):
    """Handle union command."""
    with get_input_stream(args.left) as f:
        left_data = read_jsonl(f)
    with get_input_stream(args.right) as f:
        right_data = read_jsonl(f)

    result = union(left_data, right_data)
    write_jsonl(result)


def handle_intersection(args):
    """Handle intersection command."""
    with get_input_stream(args.left) as f:
        left_data = read_jsonl(f)
    with get_input_stream(args.right) as f:
        right_data = read_jsonl(f)

    result = intersection(left_data, right_data)
    write_jsonl(result)


def handle_difference(args):
    """Handle difference command."""
    with get_input_stream(args.left) as f:
        left_data = read_jsonl(f)
    with get_input_stream(args.right) as f:
        right_data = read_jsonl(f)

    result = difference(left_data, right_data)
    write_jsonl(result)


def handle_distinct(args):
    """Handle distinct command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    result = distinct(data)
    write_jsonl(result)


def handle_sort(args):
    """Handle sort command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    result = sort_by(data, args.keys, descending=args.desc)
    write_jsonl(result)


def handle_groupby(args):
    """Handle groupby command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    if hasattr(args, "agg") and args.agg:
        # Traditional groupby with aggregation
        result = groupby_agg(data, args.key, args.agg)
    else:
        # Check if input is already grouped - look for new format
        if data and "_groups" in data[0]:
            # This is a chained groupby
            result = groupby_chained(data, args.key)
        else:
            # First groupby
            result = groupby_with_metadata(data, args.key)

    write_jsonl(result)


def handle_agg(args):
    """Handle agg command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    if not data:
        write_jsonl([])
        return

    # Check if input has group metadata - use new format
    if "_groups" in data[0]:
        # Process grouped data
        result = aggregate_grouped_data(data, args.agg)
    else:
        # Process ungrouped data
        result = [aggregate_single_group(data, args.agg)]

    write_jsonl(result)


def handle_schema_infer(args):
    """Handle schema infer command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    schema = infer_schema(data)
    write_json_object(schema)


def handle_to_array(args):
    """Handle to-array command."""
    with get_input_stream(args.file) as input_stream:
        array_string = jsonl_to_json_array_string(input_stream)
        print(array_string)


def handle_to_jsonl(args):
    """Handle to-jsonl command."""
    with get_input_stream(args.file) as input_stream:
        try:
            for line in json_array_to_jsonl_lines(input_stream):
                print(line)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)


def handle_explode(args):
    """Handle explode command."""
    input_filename_stem = "jsonl_output"  # Default stem
    if args.file and args.file != "-":
        input_filename_stem = Path(args.file).stem

    output_directory = args.output_dir if args.output_dir else input_filename_stem

    with get_input_stream(args.file) as input_stream:
        try:
            jsonl_to_dir(input_stream, output_directory, input_filename_stem)
        except Exception as e:
            print(f"Error during explode operation: {e}", file=sys.stderr)
            sys.exit(1)


def handle_implode(args):
    """Handle implode command."""
    try:
        for line in dir_to_jsonl_lines(
            args.input_dir, args.add_filename_key, args.recursive
        ):
            print(line)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during implode: {e}", file=sys.stderr)
        sys.exit(1)


def handle_import_csv(args):
    """Handle import-csv command."""
    with get_input_stream(args.file) as input_stream:
        try:
            for line in csv_to_jsonl_lines(
                input_stream, has_header=args.has_header, infer_types=args.infer_types
            ):
                print(line)
        except Exception as e:
            print(
                f"An unexpected error occurred during CSV import: {e}", file=sys.stderr
            )
            sys.exit(1)


def handle_to_csv(args):
    """Handle to-csv command."""
    column_functions = {}
    if args.apply:
        for col, expr_str in args.apply:
            try:
                # WARNING: eval() is a security risk if the expression is not from a trusted source.
                func = eval(expr_str)
                if not callable(func):
                    raise ValueError(
                        f"Expression for column '{col}' did not evaluate to a callable function."
                    )
                column_functions[col] = func
            except Exception as e:
                print(
                    f"Error parsing --apply expression for column '{col}': {e}",
                    file=sys.stderr,
                )
                sys.exit(1)

    with get_input_stream(args.file) as input_stream:
        try:
            jsonl_to_csv_stream(
                input_stream,
                sys.stdout,
                flatten=args.flatten,
                flatten_sep=args.flatten_sep,
                column_functions=column_functions,
            )
        except Exception as e:
            print(
                f"An unexpected error occurred during CSV export: {e}", file=sys.stderr
            )
            sys.exit(1)


def handle_schema_validate(args):
    """Handle schema validate command."""
    try:
        import jsonschema
    except ImportError:
        print(
            "jsonschema is not installed. Please install it with: pip install jsonschema",
            file=sys.stderr,
        )
        sys.exit(1)

    # Can't read both from stdin
    if args.schema == "-" and (not args.file or args.file == "-"):
        print(
            "Error: When reading schema from stdin, a file argument for the data to validate must be provided.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        with get_input_stream(args.schema) as f:
            schema = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(
            f"Error reading or parsing schema file {args.schema}: {e}", file=sys.stderr
        )
        sys.exit(1)

    # If schema was from stdin, the file MUST be from a file, not stdin.
    data_source = args.file if args.schema == "-" else (args.file or "-")

    with get_input_stream(data_source) as lines:
        validation_failed = False
        for i, line in enumerate(lines, 1):
            try:
                instance = json.loads(line)
                jsonschema.validate(instance=instance, schema=schema)
                print(line.strip())
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line {i}: {e}", file=sys.stderr)
                validation_failed = True
            except jsonschema.exceptions.ValidationError as e:
                print(f"Validation error on line {i}: {e.message}", file=sys.stderr)
                validation_failed = True

    if validation_failed:
        sys.exit(1)


def handle_collect(args):
    """Handle collect command."""
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)

    if not data:
        write_jsonl([])
        return

    # Check for streaming flag
    if hasattr(args, "streaming") and args.streaming:
        json_error(
            "StreamingError",
            "Collect operation requires seeing all data and cannot be performed in streaming mode. "
            "Remove --streaming flag or use window-based processing with --window-size",
        )
        return

    # Handle window-based collection
    if hasattr(args, "window_size") and args.window_size:
        # Process data in windows
        window_size = args.window_size
        for i in range(0, len(data), window_size):
            window = data[i : i + window_size]
            result = collect(window)
            write_jsonl(result)
    else:
        # Collect all data at once
        result = collect(data)
        write_jsonl(result)
