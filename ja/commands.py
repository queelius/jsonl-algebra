import sys
import json
import jmespath
from pathlib import Path
from contextlib import contextmanager
from .core import (
    select,
    project,
    join,
    product,
    rename,
    union,
    intersection,
    difference,
    distinct,
    sort_by,
)
from .importer import csv_to_jsonl_lines, dir_to_jsonl_lines
from .exporter import jsonl_to_csv_stream
from .export import jsonl_to_json_array_string, json_array_to_jsonl_lines, jsonl_to_dir
from .schema import infer_schema
from .groupby import groupby_agg

@contextmanager
def get_input_stream(file_path):
    """Context manager for getting an input stream from a file or stdin."""
    if file_path and file_path != '-':
        try:
            f = open(file_path, 'r')
            yield f
        finally:
            f.close()
    else:
        yield sys.stdin

def read_jsonl(input_stream):
    """Reads JSONL data from a file-like object."""
    return [json.loads(line) for line in input_stream]

def write_jsonl(rows):
    for row in rows:
        print(json.dumps(row))

def write_json_object(obj):
    print(json.dumps(obj, indent=2))

# Command handlers
def handle_select(args):
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)
    try:
        # JMESPath expects a query that filters a list (the relation).
        # A simple way to adapt row-by-row logic is to use a filter expression.
        # The expression in `args.expr` should be a valid JMESPath expression.
        expression = jmespath.compile(f"[?{args.expr}]")
        write_jsonl(select(data, expression))
    except jmespath.exceptions.ParseError as e:
        print(f"Invalid JMESPath expression: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during select: {e}", file=sys.stderr)
        sys.exit(1)

def handle_project(args):
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)
    cols = [col.strip() for col in args.columns.split(",")]
    write_jsonl(project(data, cols))

def handle_join(args):
    with get_input_stream(args.left) as f:
        left = read_jsonl(f)
    with get_input_stream(args.right) as f:
        right = read_jsonl(f)
    lcol_str, rcol_str = args.on.split("=", 1)
    lcol = lcol_str.strip()
    rcol = rcol_str.strip()
    write_jsonl(join(left, right, [(lcol, rcol)]))

def handle_product(args):
    with get_input_stream(args.left) as f:
        a = read_jsonl(f)
    with get_input_stream(args.right) as f:
        b = read_jsonl(f)
    write_jsonl(product(a, b))

def handle_rename(args):
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
            # Optionally, handle malformed pairs, e.g., by printing a warning or raising an error
            print(f"Warning: Malformed rename pair '{pair_str.strip()}' ignored.", file=sys.stderr)
    write_jsonl(rename(data, mapping))

def handle_union(args):
    with get_input_stream(args.left) as f:
        a = read_jsonl(f)
    with get_input_stream(args.right) as f:
        b = read_jsonl(f)
    write_jsonl(union(a, b))

def handle_intersection(args):
    with get_input_stream(args.left) as f:
        a = read_jsonl(f)
    with get_input_stream(args.right) as f:
        b = read_jsonl(f)
    write_jsonl(intersection(a, b))

def handle_difference(args):
    with get_input_stream(args.left) as f:
        a = read_jsonl(f)
    with get_input_stream(args.right) as f:
        b = read_jsonl(f)
    write_jsonl(difference(a, b))

def handle_distinct(args):
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)
    write_jsonl(distinct(data))

def handle_sort(args):
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)
    keys = [key.strip() for key in args.keys.split(",")]
    write_jsonl(sort_by(data, keys, reverse=args.desc))

def handle_groupby(args):
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)
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

def handle_schema_infer(args):
    with get_input_stream(args.file) as f:
        data = read_jsonl(f)
    schema = infer_schema(data)
    write_json_object(schema)

def handle_to_array(args):
    """Converts JSONL input to a single JSON array string."""
    with get_input_stream(args.file) as input_stream:
        array_string = jsonl_to_json_array_string(input_stream)
        print(array_string)

def handle_to_jsonl(args):
    """Converts a JSON array input to JSONL lines."""
    with get_input_stream(args.file) as input_stream:
        try:
            for line in json_array_to_jsonl_lines(input_stream):
                print(line)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

def handle_explode(args):
    """Exports JSONL lines to individual JSON files in a directory."""
    input_filename_stem = "jsonl_output" # Default stem
    if args.file and args.file != '-':
        input_filename_stem = Path(args.file).stem
    
    # If output_dir is not specified, use the input filename stem as the directory name in the CWD.
    # If output_dir IS specified, it's the target directory.
    output_directory = args.output_dir if args.output_dir else input_filename_stem

    with get_input_stream(args.file) as input_stream:
        try:
            jsonl_to_dir(input_stream, output_directory, input_filename_stem)
        except Exception as e:
            print(f"Error during explode operation: {e}", file=sys.stderr)
            sys.exit(1)

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
    with get_input_stream(args.file) as input_stream:
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

def handle_to_csv(args):
    """Converts a JSONL input to CSV format."""
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

    with get_input_stream(args.file) as input_stream:
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

def handle_schema_validate(args):
    try:
        import jsonschema
    except ImportError:
        print("jsonschema is not installed. Please install it with: pip install jsonschema", file=sys.stderr)
        sys.exit(1)

    # Can't read both from stdin
    if args.schema == '-' and (not args.file or args.file == '-'):
        print("Error: When reading schema from stdin, a file argument for the data to validate must be provided.", file=sys.stderr)
        sys.exit(1)

    try:
        with get_input_stream(args.schema) as f:
            schema = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing schema file {args.schema}: {e}", file=sys.stderr)
        sys.exit(1)

    # If schema was from stdin, the file MUST be from a file, not stdin.
    data_source = args.file if args.schema == '-' else (args.file or '-')

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