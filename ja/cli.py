#!/usr/bin/env python3
"""Command-line interface for JSONL algebra operations.

This module provides the main CLI entry point and argument parsing for
all JSONL algebra operations including relational algebra, schema inference,
data import/export, and interactive REPL mode.
"""

import argparse
import sys

from .commands import (
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
        # This should not be reached if 'required=True' for export_subparsers
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
        # This should not be reached if 'required=True' for schema_subparsers
        print(f"Unknown schema command: {args.schema_cmd}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(prog="ja", description="JSONL algebra")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    # select
    sp_sel = subparsers.add_parser(
        "select", help="Filter rows with a JMESPath expression"
    )
    sp_sel.add_argument("expr", help="e.g. 'amount > `100` && user_id == `3`'")
    sp_sel.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # project
    sp_proj = subparsers.add_parser("project", help="Select specific fields")
    sp_proj.add_argument("columns", help="Comma-separated column names")
    sp_proj.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # join
    sp_join = subparsers.add_parser("join", help="Join two tables on a key")
    sp_join.add_argument("left", nargs="?", help="Left JSONL file (or - for stdin)")
    sp_join.add_argument("right", help="Right JSONL file")
    sp_join.add_argument("--on", required=True, help="Join key: left_col=right_col")

    # product
    sp_prod = subparsers.add_parser("product", help="Cartesian product (A × B)")
    sp_prod.add_argument("left")
    sp_prod.add_argument("right")

    # rename
    sp_rename = subparsers.add_parser("rename", help="Rename columns")
    sp_rename.add_argument("mapping", help="Comma-separated: old=new,old2=new2")
    sp_rename.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # union
    sp_union = subparsers.add_parser("union", help="Union of two files")
    sp_union.add_argument("left")
    sp_union.add_argument("right")

    # intersection
    sp_inter = subparsers.add_parser("intersection", help="A ∩ B (rows common to both)")
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
    sp_sort.add_argument("keys", help="Comma-separated key names to sort by")
    sp_sort.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_sort.add_argument("--desc", action="store_true", help="Sort in descending order")

    # groupby
    sp_groupby = subparsers.add_parser("groupby", help="Group rows by a key")
    sp_groupby.add_argument("key", help="Group-by key")
    sp_groupby.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_groupby.add_argument(
        "--agg", required=True, help="Aggregations: count,sum:amount,max:amount,..."
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
        dest="schema_cmd", required=True, help="Schema sub-command"
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
        dest="export_cmd", required=True, help="Export sub-command"
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
    sp_csv.add_argument("file", nargs="?", help="Input JSONL file (defaults to stdin).")
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
        dest="import_cmd", required=True, help="Import sub-command"
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
    sp_implode.add_argument("input_dir", help="Input directory containing JSON files.")
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

    args = parser.parse_args()

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
        "repl": lambda args: repl(),
        "export": handle_export_command_group,
        "import": handle_import_command_group,
    }

    handler = command_handlers.get(args.cmd)
    if handler:
        handler(args)
    else:
        # This case should ideally not be reached if subparsers are 'required=True'
        # and all commands are mapped.
        print(f"Unknown command: {args.cmd}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
