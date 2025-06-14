#!/usr/bin/env python3
import sys, argparse
from .commands import (
    handle_select,
    handle_project,
    handle_join,
    handle_product,
    handle_rename,
    handle_union,
    handle_intersection,
    handle_difference,
    handle_distinct,
    handle_sort,
    handle_groupby,
    handle_schema,
    handle_to_array,    # These will be used by the new export handler
    handle_to_jsonl,
    handle_explode,
    handle_implode,
)
from .repl import repl

def handle_export_command_group(args):
    export_command_handlers = {
        "to-array": handle_to_array,
        "to-jsonl": handle_to_jsonl,
        "explode": handle_explode,
        "implode": handle_implode,
    }
    handler = export_command_handlers.get(args.export_cmd)
    if handler:
        handler(args)
    else:
        # This should not be reached if 'required=True' for export_subparsers
        print(f"Unknown export command: {args.export_cmd}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(prog="ja", description="JSONL algebra")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    # select
    sp_sel = subparsers.add_parser("select", help="Filter rows with a Python expression")
    sp_sel.add_argument("expr", help="e.g. 'amount > 100 and user_id == 3'")
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
    sp_sort = subparsers.add_parser("sort", help="Sort by column(s)")
    sp_sort.add_argument("columns", help="Comma-separated columns")
    sp_sort.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # groupby
    sp_group = subparsers.add_parser("groupby", help="Group by a key and aggregate")
    sp_group.add_argument("key", help="Group-by key")
    sp_group.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_group.add_argument("--agg", required=True, help="Aggregations: count,sum:amount,max:amount,...")

    # REPL subparser
    sp_repl = subparsers.add_parser("repl", help="Start an interactive REPL session to build command pipelines.")
    sp_repl.add_argument(
        "initial_args", 
        nargs="*",
        help="Optional: initial file to load (e.g., 'data.jsonl') or a full command (e.g., \"from data.jsonl\")"
    )
    
    # Schema subparser
    sp_schema = subparsers.add_parser("schema", help="Infer and display the schema of a JSONL file.")
    sp_schema.add_argument("file", nargs="?", help="Input JSONL file (defaults to stdin)")

    # Export command group
    sp_export_group = subparsers.add_parser("export", help="Export or transform data formats.")
    export_subparsers = sp_export_group.add_subparsers(dest="export_cmd", required=True, help="Export sub-command")

    # 1. export to-array (JSONL to JSON Array)
    sp_to_array = export_subparsers.add_parser("to-array", help="Convert JSONL to a single JSON array.")
    sp_to_array.add_argument("file", nargs="?", help="Input JSONL file (defaults to stdin).")

    # 2. export to-jsonl (JSON Array to JSONL)
    sp_to_jsonl = export_subparsers.add_parser("to-jsonl", help="Convert a JSON array to JSONL.")
    sp_to_jsonl.add_argument("file", nargs="?", help="Input JSON file containing an array (defaults to stdin).")

    # 3. export explode (JSONL to Directory of JSON files)
    sp_explode = export_subparsers.add_parser("explode", help="Export JSONL lines to a directory of JSON files.")
    sp_explode.add_argument("file", nargs="?", help="Input JSONL file (defaults to stdin).")
    sp_explode.add_argument("-o", "--output-dir", help="Output directory name. Defaults to input filename stem.")

    # 4. export implode (Directory of JSON files to JSONL)
    sp_implode = export_subparsers.add_parser("implode", help="Convert a directory of JSON files to JSONL.")
    sp_implode.add_argument("input_dir", help="Input directory containing JSON files.")
    sp_implode.add_argument("--add-filename-key", metavar="KEY_NAME", help="Add filename as a value with this key to each JSON object.")
    sp_implode.add_argument("--recursive", action="store_true", help="Recursively search for JSON files in subdirectories.")


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
        "repl": repl,
        "schema": handle_schema,
        "export": handle_export_command_group, # New top-level handler
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
