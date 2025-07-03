#!/usr/bin/env python3
"""Command-line interface for JSONL algebra operations.

This module provides the main CLI entry point and argument parsing for
all JSONL algebra operations including relational algebra, schema inference,
data import/export, and interactive REPL mode.
"""

import argparse
import json
import os
import signal
import sys
import traceback

from .commands import (
    handle_agg,
    handle_difference,
    handle_distinct,
    handle_explode,
    handle_groupby,
    handle_implode,
    handle_import_csv,
    handle_intersection,
    handle_join,
    handle_product,
    handle_project,
    handle_rename,
    handle_schema_infer,
    handle_schema_validate,
    handle_select,
    handle_sort,
    handle_to_array,
    handle_to_csv,
    handle_to_jsonl,
    handle_union,
)
from .repl import repl


def json_error(error_type, message, details=None, exit_code=1):
    """Output error in JSON format and exit.

    Args:
        error_type: Type of error (e.g., "ParseError", "IOError")
        message: Human-readable error message
        details: Optional dict with additional error details
        exit_code: Exit code (default: 1)
    """
    error_obj = {"error": {"type": error_type, "message": message}}
    if details:
        error_obj["error"]["details"] = details

    # If stderr is not a tty (redirected/piped), always output JSON
    # If stderr is a tty, output human-readable unless JA_JSON_ERRORS is set
    if not sys.stderr.isatty() or os.environ.get("JA_JSON_ERRORS"):
        print(json.dumps(error_obj), file=sys.stderr)
    else:
        # Human-readable format for terminal
        print(f"ja: error: {message}", file=sys.stderr)
        if details:
            for key, value in details.items():
                if value is not None and key != "traceback":
                    print(f"  {key}: {value}", file=sys.stderr)

    sys.exit(exit_code)


# Handler for the 'export' command group
def handle_export_command_group(args):
    """Handle export subcommands by delegating to appropriate handlers.

    Args:
        args: Parsed command line arguments with export_cmd attribute.
    """
    export_command_handlers = {
        "array": handle_to_array,
        "jsonl": handle_to_jsonl,
        "explode": handle_explode,
        "csv": handle_to_csv,
    }
    handler = export_command_handlers.get(args.export_cmd)
    if handler:
        handler(args)
    else:
        # This should not be reached if subcommands are handled correctly in main
        print(f"Unknown export command: {args.export_cmd}", file=sys.stderr)
        sys.exit(1)


def handle_import_command_group(args):
    """Handle import subcommands by delegating to appropriate handlers.

    Args:
        args: Parsed command line arguments with import_cmd attribute.
    """
    import_command_handlers = {
        "csv": handle_import_csv,
        "implode": handle_implode,
    }
    handler = import_command_handlers.get(args.import_cmd)
    if handler:
        handler(args)
    else:
        # This should not be reached if subcommands are handled correctly in main
        print(f"Unknown import command: {args.import_cmd}", file=sys.stderr)
        sys.exit(1)


def handle_schema_command_group(args):
    schema_command_handlers = {
        "infer": handle_schema_infer,
        "validate": handle_schema_validate,
    }
    handler = schema_command_handlers.get(args.schema_cmd)
    if handler:
        handler(args)
    else:
        # This should not be reached if subcommands are handled correctly in main
        print(f"Unknown schema command: {args.schema_cmd}", file=sys.stderr)
        sys.exit(1)


def main():
    # Handle SIGPIPE gracefully (for Unix-like systems)
    # This prevents BrokenPipeError when piping to commands like 'head' or 'jq'
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except AttributeError:
        # SIGPIPE doesn't exist on Windows
        pass

    try:
        parser = argparse.ArgumentParser(
            prog="ja",
            description="A friendly and powerful command-line tool for JSONL data manipulation, supporting relational algebra, nested data, and more.",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        subparsers = parser.add_subparsers(dest="cmd")

        # select
        sp_sel = subparsers.add_parser(
            "select", help="Filter rows with a JMESPath expression"
        )
        sp_sel.add_argument("expr", help="e.g. 'amount > `100` && user_id == `3`'")
        sp_sel.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

        # project
        sp_proj = subparsers.add_parser("project", help="Select specific fields")
        sp_proj.add_argument(
            "expr",
            help="Comma-separated dot-notation fields (e.g. 'user.id,amount') or JMESPath expression",
        )
        sp_proj.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
        sp_proj.add_argument(
            "--jmespath",
            action="store_true",
            help="Force interpretation as JMESPath expression (for complex projections)",
        )
        sp_proj.add_argument(
            "--flatten",
            action="store_true",
            help="Flatten nested fields using dot notation as keys (e.g. 'person.name': 'Alice')",
        )

        # join
        sp_join = subparsers.add_parser("join", help="Join two tables on a key")
        sp_join.add_argument("left", nargs="?", help="Left JSONL file (or - for stdin)")
        sp_join.add_argument("right", help="Right JSONL file")
        sp_join.add_argument(
            "--on",
            required=True,
            help="Join key: left_col=right_col. Supports dot notation for nested fields (e.g., user.id=customer.id).",
        )

        # product
        sp_prod = subparsers.add_parser("product", help="Cartesian product (A × B)")
        sp_prod.add_argument("left")
        sp_prod.add_argument("right")

        # rename
        sp_rename = subparsers.add_parser(
            "rename", help="Rename columns (supports nested fields)"
        )
        sp_rename.add_argument(
            "mapping",
            help="Comma-separated: old=new,old2=new2. Supports dot notation: person.name=person.full_name",
        )
        sp_rename.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

        # union
        sp_union = subparsers.add_parser("union", help="Union of two files")
        sp_union.add_argument("left")
        sp_union.add_argument("right")

        # intersection
        sp_inter = subparsers.add_parser(
            "intersection", help="A ∩ B (rows common to both)"
        )
        sp_inter.add_argument("left")
        sp_inter.add_argument("right")

        # difference
        sp_diff = subparsers.add_parser("difference", help="A - B (row-wise)")
        sp_diff.add_argument("left")
        sp_diff.add_argument("right")

        # distinct
        sp_dist = subparsers.add_parser("distinct", help="Remove duplicate rows")
        sp_dist.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

        # sort
        sp_sort = subparsers.add_parser("sort", help="Sort rows by key(s)")
        sp_sort.add_argument(
            "keys",
            help="Comma-separated key names to sort by. Supports dot notation for nested fields.",
        )
        sp_sort.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
        sp_sort.add_argument(
            "--desc", action="store_true", help="Sort in descending order"
        )

        # groupby
        sp_groupby = subparsers.add_parser(
            "groupby", 
            help="Group rows by a key",
            description="""Group rows by a key with two modes of operation:
            
1. With --agg: Perform immediate aggregation (efficient for single-level grouping)
2. Without --agg: Add grouping metadata for chaining or later aggregation
   
The metadata mode adds special fields (_group, _group_field, etc.) that allow:
- Chaining multiple groupby operations
- Filtering or transforming between groupings  
- Deferring aggregation until the end of a pipeline
- Inspecting intermediate grouped data

Examples:
  # Direct aggregation
  ja groupby region --agg "total=sum(amount)" data.jsonl
  
  # Chained grouping
  cat data.jsonl | ja groupby region | ja groupby product | ja agg "total=sum(amount)"
  
  # Filter between groups
  cat data.jsonl | ja groupby user | ja select '_group_size > 5' | ja agg count""",
            formatter_class=argparse.RawTextHelpFormatter
        )
        sp_groupby.add_argument(
            "key",
            help="Group-by key. Supports dot notation for nested fields (e.g., 'user.id').",
        )
        sp_groupby.add_argument(
            "file", nargs="?", help="Input file (defaults to stdin)"
        )
        sp_groupby.add_argument(
            "--agg",
            required=False,
            help="""Comma-separated list of aggregations.
Format: 'new_name=agg(field)' or 'agg'.
Examples: 'count,total=sum(amount),names=list(user.name)'
Available functions: count, sum, avg, min, max, list, first, last.""",
        )

        # REPL subparser
        sp_repl = subparsers.add_parser(
            "repl", help="Start an interactive REPL session to build command pipelines."
        )
        sp_repl.add_argument(
            "initial_args",
            nargs="*",
            help="Optional: initial file to load (e.g., 'data.jsonl') or a full command (e.g., \"from data.jsonl\")",
        )

        # Schema command group
        sp_schema_group = subparsers.add_parser(
            "schema", help="Infer, validate, and work with JSONL schemas."
        )
        schema_subparsers = sp_schema_group.add_subparsers(
            dest="schema_cmd", help="Schema sub-command"
        )

        # 1. schema infer
        sp_infer = schema_subparsers.add_parser(
            "infer", help="Infer and display the schema of a JSONL file."
        )
        sp_infer.add_argument(
            "file", nargs="?", help="Input JSONL file (defaults to stdin)"
        )

        # 2. schema validate
        sp_validate = schema_subparsers.add_parser(
            "validate", help="Validate JSONL data against a JSON schema."
        )
        sp_validate.add_argument(
            "schema", help="Path to the JSON schema file (use '-' for stdin)."
        )
        sp_validate.add_argument(
            "file", nargs="?", help="Input JSONL file to validate (defaults to stdin)."
        )

        # Export command group
        sp_export_group = subparsers.add_parser(
            "export", help="Export or transform data formats."
        )
        export_subparsers = sp_export_group.add_subparsers(
            dest="export_cmd", help="Export sub-command"
        )

        # 1. export array (JSONL to JSON Array)
        sp_array = export_subparsers.add_parser(
            "array", help="Convert JSONL to a single JSON array."
        )
        sp_array.add_argument(
            "file", nargs="?", help="Input JSONL file (defaults to stdin)."
        )

        # 2. export jsonl (JSON Array to JSONL)
        sp_jsonl = export_subparsers.add_parser(
            "jsonl", help="Convert a JSON array to JSONL."
        )
        sp_jsonl.add_argument(
            "file",
            nargs="?",
            help="Input JSON file containing an array (defaults to stdin).",
        )

        # 3. export explode (JSONL to Directory of JSON files)
        sp_explode = export_subparsers.add_parser(
            "explode", help="Export JSONL lines to a directory of JSON files."
        )
        sp_explode.add_argument(
            "file", nargs="?", help="Input JSONL file (defaults to stdin)."
        )
        sp_explode.add_argument(
            "-o",
            "--output-dir",
            help="Output directory name. Defaults to input filename stem.",
        )

        # 4. export csv (JSONL to CSV)
        sp_csv = export_subparsers.add_parser(
            "csv", help="Convert JSONL to CSV with flattening for nested objects."
        )
        sp_csv.add_argument(
            "file", nargs="?", help="Input JSONL file (defaults to stdin)."
        )
        sp_csv.add_argument(
            "--no-flatten",
            dest="flatten",
            action="store_false",
            help="Disable automatic flattening of nested objects.",
        )
        sp_csv.add_argument(
            "--flatten-sep",
            default=".",
            help="Separator to use for flattened keys (default: '.').",
        )
        sp_csv.add_argument(
            "--apply",
            nargs=2,
            metavar=("COLUMN", "LAMBDA_EXPR"),
            action="append",
            help="Apply a Python lambda expression to a column. Example: --apply timestamp \"lambda t: t.split('T')[0]\"",
        )
        sp_csv.set_defaults(flatten=True)

        # Import command group
        sp_import_group = subparsers.add_parser(
            "import", help="Import data from other formats into JSONL."
        )
        import_subparsers = sp_import_group.add_subparsers(
            dest="import_cmd", help="Import sub-command"
        )

        # 1. import csv
        sp_import_csv = import_subparsers.add_parser(
            "csv", help="Convert a CSV file to JSONL."
        )
        sp_import_csv.add_argument(
            "file", nargs="?", help="Input CSV file (defaults to stdin)."
        )
        sp_import_csv.add_argument(
            "--no-header",
            dest="has_header",
            action="store_false",
            help="Specify that the CSV file has no header row. Keys will be auto-generated.",
        )
        sp_import_csv.add_argument(
            "--infer-types",
            action="store_true",
            help="Automatically infer types (e.g., numeric, boolean) for CSV values.",
        )
        sp_import_csv.set_defaults(has_header=True, infer_types=False)

        # 2. import implode (Directory of JSON files to JSONL)
        sp_implode = import_subparsers.add_parser(
            "implode", help="Convert a directory of JSON files to JSONL."
        )
        sp_implode.add_argument(
            "input_dir", help="Input directory containing JSON files."
        )
        sp_implode.add_argument(
            "--add-filename-key",
            metavar="KEY_NAME",
            help="Add filename as a value with this key to each JSON object.",
        )
        sp_implode.add_argument(
            "--recursive",
            action="store_true",
            help="Recursively search for JSON files in subdirectories.",
        )

        # Aggregate command
        agg_parser = subparsers.add_parser("agg", help="Apply aggregation to data")
        agg_parser.add_argument(
            "agg", help="Aggregation expression: count, sum(field), avg(field), etc."
        )
        agg_parser.add_argument(
            "file", nargs="?", help="Input JSONL file (default: stdin)"
        )
        agg_parser.set_defaults(func=handle_agg)

        args = parser.parse_args()

        # Handle cases where a command is not provided
        if args.cmd is None:
            parser.print_help()
            sys.exit(0)

        # For command groups, if no subcommand is given, print their help
        if args.cmd == "schema" and args.schema_cmd is None:
            sp_schema_group.print_help()
            sys.exit(0)
        if args.cmd == "export" and args.export_cmd is None:
            sp_export_group.print_help()
            sys.exit(0)
        if args.cmd == "import" and args.import_cmd is None:
            sp_import_group.print_help()
            sys.exit(0)

        command_handlers = {
            "select": handle_select,
            "project": handle_project,
            "join": handle_join,
            "product": handle_product,
            "rename": handle_rename,
            "union": handle_union,
            "intersection": handle_intersection,
            "difference": handle_difference,
            "distinct": handle_distinct,
            "sort": handle_sort,
            "groupby": handle_groupby,
            "schema": handle_schema_command_group,
            "repl": lambda args: repl(args),
            "export": handle_export_command_group,
            "import": handle_import_command_group,
            "agg": handle_agg
        }

        handler = command_handlers.get(args.cmd)
        if handler:
            handler(args)
        else:
            json_error(
                "CommandError",
                f"Unknown command: {args.cmd}",
                {
                    "command": args.cmd,
                    "available_commands": list(command_handlers.keys()),
                },
            )

    except BrokenPipeError:
        # Exit cleanly when pipe is broken - this is normal behavior
        sys.exit(0)
    except KeyboardInterrupt:
        # User pressed Ctrl+C - exit cleanly without error
        sys.exit(130)  # Standard exit code for SIGINT
    except FileNotFoundError as e:
        json_error(
            "FileNotFoundError",
            str(e),
            {"filename": str(e.filename) if hasattr(e, "filename") else None},
        )
    except PermissionError as e:
        json_error(
            "PermissionError",
            str(e),
            {"filename": str(e.filename) if hasattr(e, "filename") else None},
        )
    except json.JSONDecodeError as e:
        json_error(
            "JSONDecodeError",
            f"Invalid JSON: {e.msg}",
            {"line": e.lineno, "column": e.colno, "position": e.pos},
        )
    except Exception as e:
        # For unexpected errors, include traceback in debug mode
        error_details = {
            "exception_type": type(e).__name__,
            "traceback": (
                traceback.format_exc().split("\n") if "--debug" in sys.argv else None
            ),
        }
        json_error("UnexpectedError", str(e), error_details)

    sys.exit(0)


if __name__ == "__main__":
    main()
