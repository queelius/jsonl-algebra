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
)

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
