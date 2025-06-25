import sys
import json
from pathlib import Path
from .core import *
from .groupby import groupby_agg
from .schema import infer_schema
from .export import (
    jsonl_to_json_array_string,
    json_array_to_jsonl_lines,
    jsonl_to_dir,
    dir_to_jsonl
)
from .importer import csv_to_jsonl_lines
from .exporter import jsonl_to_csv_stream

def read_jsonl(file_or_fp):
    if isinstance(file_or_fp, str) or isinstance(file_or_fp, Path):
        with open(file_or_fp) as f:
            return [json.loads(line) for line in f]
    else:
        return [json.loads(line) for line in file_or_fp]

def write_jsonl(rows):
    for row in rows:
        print(json.dumps(row))

def write_json_object(obj):
    print(json.dumps(obj, indent=2))

# Command handlers
def handle_select(args):
    data = read_jsonl(args.file or sys.stdin)
    try:
        predicate = lambda row: eval(args.expr, {}, row)
    except Exception as e:
        print(f"Invalid expression: {e}", file=sys.stderr)
        sys.exit(1)
    write_jsonl(select(data, predicate))

def handle_project(args):
    data = read_jsonl(args.file or sys.stdin)
    cols = [col.strip() for col in args.columns.split(",")]
    write_jsonl(project(data, cols))

def handle_join(args):
    left_input = sys.stdin if args.left == "-" else args.left
    left = read_jsonl(left_input or sys.stdin) # Default to stdin if left is None (not specified)
    right = read_jsonl(args.right)
    lcol_str, rcol_str = args.on.split("=", 1)
    lcol = lcol_str.strip()
    rcol = rcol_str.strip()
    write_jsonl(join(left, right, [(lcol, rcol)]))

def handle_product(args):
    a = read_jsonl(args.left)
    b = read_jsonl(args.right)
    write_jsonl(product(a, b))

def handle_rename(args):
    data = read_jsonl(args.file or sys.stdin)
    mapping_pairs = args.mapping.split(",")
    mapping = {}
    for pair_str in mapping_pairs:
        parts = pair_str.split("=", 1)
        if len(parts) == 2:
            old_name, new_name = parts
            mapping[old_name.strip()] = new_name.strip()
        else:
            # Optionally, handle malformed pairs, e.g., by printing a warning or raising an error
            print(f"Warning: Malformed rename pair '{pair_str.strip()}' ignored.", file=sys.stderr)
    write_jsonl(rename(data, mapping))

def handle_union(args):
    a = read_jsonl(args.left)
    b = read_jsonl(args.right)
    write_jsonl(union(a, b))

def handle_intersection(args):
    a = read_jsonl(args.left)
    b = read_jsonl(args.right)
    write_jsonl(intersection(a, b))

def handle_difference(args):
    a = read_jsonl(args.left)
    b = read_jsonl(args.right)
    write_jsonl(difference(a, b))

def handle_distinct(args):
    data = read_jsonl(args.file or sys.stdin)
    write_jsonl(distinct(data))

def handle_sort(args):
    data = read_jsonl(args.file or sys.stdin)
    keys = [key.strip() for key in args.columns.split(",")]
    write_jsonl(sort_by(data, keys))

def handle_groupby(args):
    data = read_jsonl(args.file or sys.stdin)
    group_by_key_cleaned = args.key.strip() # Clean the group_by key
    
    aggs = []
    for part_str in args.agg.split(","):
        stripped_part = part_str.strip() # Strip the whole "func:field" or "func" part
        if ":" in stripped_part:
            func, field = stripped_part.split(":", 1)
            aggs.append((func.strip(), field.strip()))
        else:
            # This is for aggregations like 'count' that don't take a field
            aggs.append((stripped_part.strip(), "")) 
    write_jsonl(groupby_agg(data, group_by_key_cleaned, aggs))

def handle_schema(args):
    data = read_jsonl(args.file or sys.stdin)
    schema = infer_schema(data)
    write_json_object(schema)

def handle_to_array(args):
    """Converts JSONL input to a single JSON array string."""
    input_stream = open(args.file, 'r') if args.file else sys.stdin
    try:
        array_string = jsonl_to_json_array_string(input_stream)
        print(array_string)
    finally:
        if args.file:
            input_stream.close()

def handle_to_jsonl(args):
    """Converts a JSON array input to JSONL lines."""
    input_stream = open(args.file, 'r') if args.file else sys.stdin
    try:
        for line in json_array_to_jsonl_lines(input_stream):
            print(line)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if args.file:
            input_stream.close()

def handle_explode(args):
    """Exports JSONL lines to individual JSON files in a directory."""
    input_stream = open(args.file, 'r') if args.file else sys.stdin
    
    input_filename_stem = "jsonl_output" # Default stem
    if args.file:
        input_filename_stem = Path(args.file).stem
    
    # If output_dir is not specified, use the input filename stem as the directory name in the CWD.
    # If output_dir IS specified, it's the target directory.
    output_directory = args.output_dir if args.output_dir else input_filename_stem

    try:
        jsonl_to_dir(input_stream, output_directory, input_filename_stem)
    except Exception as e:
        print(f"Error during explode operation: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if args.file:
            input_stream.close()

def handle_implode(args):
    """Converts JSON files in a directory to JSONL lines."""
    try:
        for line in dir_to_jsonl_lines(args.input_dir, args.add_filename_key, args.recursive):
            print(line)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during implode: {e}", file=sys.stderr)
        sys.exit(1)

def handle_import_csv(args):
    """Imports a CSV file and converts it to JSONL."""
    input_stream = open(args.file, 'r') if args.file else sys.stdin
    try:
        # The --no-header flag in argparse sets has_header to False if present.
        # If the flag is not present, args.has_header will be True by default.
        for line in csv_to_jsonl_lines(
            input_stream,
            has_header=args.has_header,
            infer_types=args.infer_types
        ):
            print(line)
    except Exception as e:
        print(f"An unexpected error occurred during CSV import: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if args.file:
            input_stream.close()

def handle_to_csv(args):
    """Converts a JSONL input to CSV format."""
    input_stream = open(args.file, 'r') if args.file else sys.stdin

    column_functions = {}
    if args.apply:
        for col, expr_str in args.apply:
            try:
                # WARNING: eval() is a security risk if the expression is not from a trusted source.
                func = eval(expr_str)
                if not callable(func):
                    raise ValueError(f"Expression for column '{col}' did not evaluate to a callable function.")
                column_functions[col] = func
            except Exception as e:
                print(f"Error parsing --apply expression for column '{col}': {e}", file=sys.stderr)
                sys.exit(1)

    try:
        # The output stream for the CSV writer must be a text stream.
        # sys.stdout is a text stream, so it's suitable.
        jsonl_to_csv_stream(
            input_stream,
            sys.stdout,
            flatten=args.flatten,
            flatten_sep=args.flatten_sep,
            column_functions=column_functions
        )
    except Exception as e:
        # Use stderr for error messages to not corrupt stdout if it's being piped
        print(f"An unexpected error occurred during CSV export: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if args.file:
            input_stream.close()