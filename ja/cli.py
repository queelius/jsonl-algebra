#!/usr/bin/env python3
import sys, argparse, json
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
    # JSONPath handlers
    handle_select_path,
    handle_select_any,
    handle_select_all,
    handle_select_none,
    handle_project_template,
)


def add_window_argument(parser):
    """Add --window-size argument to parser for operations that support windowed processing."""
    parser.add_argument(
        "--window-size",
        type=int,
        default=None,
        help="Process data in windows of specified size for memory efficiency (approximate results)",
    )


def add_window_argument(parser):
    """Add --window-size argument to parser for operations that support windowed processing."""
    parser.add_argument(
        "--window-size",
        type=int,
        default=None,
        help="Process data in windows of specified size for memory efficiency (approximate results)",
    )


def main():
    parser = argparse.ArgumentParser(prog="ja", description="JSONL algebra")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    # select - supports streaming
    sp_sel = subparsers.add_parser(
        "select", help="Filter rows with a Python expression"
    )
    sp_sel.add_argument("expr", help="e.g. 'amount > 100 and user_id == 3'")
    sp_sel.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # project - streams automatically
    sp_proj = subparsers.add_parser("project", help="Select specific fields")
    sp_proj.add_argument("columns", help="Comma-separated column names")
    sp_proj.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # join - does NOT support streaming (needs memory for indexing)
    sp_join = subparsers.add_parser("join", help="Join two tables on a key")
    sp_join.add_argument("left", nargs="?", help="Left JSONL file (or - for stdin)")
    sp_join.add_argument("right", help="Right JSONL file")
    sp_join.add_argument("--on", required=True, help="Join key: left_col=right_col")
    add_window_argument(sp_join)  # Enable windowed processing

    # product - does NOT support streaming
    sp_prod = subparsers.add_parser("product", help="Cartesian product (A × B)")
    sp_prod.add_argument("left")
    sp_prod.add_argument("right")
    add_window_argument(sp_prod)  # Enable windowed processing

    # rename - supports streaming
    sp_rename = subparsers.add_parser("rename", help="Rename columns")
    sp_rename.add_argument("mapping", help="Comma-separated: old=new,old2=new2")
    sp_rename.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # union - supports streaming
    sp_union = subparsers.add_parser("union", help="Union of two files")
    sp_union.add_argument("left")
    sp_union.add_argument("right")

    # intersection - does NOT support streaming (needs set operations)
    sp_inter = subparsers.add_parser("intersection", help="A ∩ B (rows common to both)")
    sp_inter.add_argument("left")
    sp_inter.add_argument("right")
    add_window_argument(sp_inter)  # Enable windowed processing

    # difference - does NOT support streaming (needs set operations)
    sp_diff = subparsers.add_parser("difference", help="A - B (row-wise)")
    sp_diff.add_argument("left")
    sp_diff.add_argument("right")
    add_window_argument(sp_diff)  # Enable windowed processing

    # distinct - supports streaming (with memory for seen items)
    sp_dist = subparsers.add_parser("distinct", help="Remove duplicate rows")
    sp_dist.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

    # sort - does NOT support streaming (needs all data to sort)
    sp_sort = subparsers.add_parser("sort", help="Sort by column(s)")
    sp_sort.add_argument("columns", help="Comma-separated columns")
    sp_sort.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    add_window_argument(sp_sort)  # Enable windowed processing

    # groupby - does NOT support streaming (needs to accumulate groups)
    sp_group = subparsers.add_parser("groupby", help="Group by a key and aggregate")
    sp_group.add_argument("key", help="Group-by key")
    sp_group.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_group.add_argument(
        "--agg", required=True, help="Aggregations: count,sum:amount,max:amount,..."
    )
    add_window_argument(sp_group)  # Enable windowed processing

    # JSONPath extensions - all support streaming
    # select-path
    sp_selpath = subparsers.add_parser(
        "select-path", help="Filter rows using JSONPath expressions"
    )
    sp_selpath.add_argument(
        "path", help="JSONPath expression (e.g., '$.orders[*].amount')"
    )
    sp_selpath.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_selpath.add_argument(
        "--predicate",
        help="Python expression for predicate (e.g., 'lambda x: x > 100')",
    )
    sp_selpath.add_argument(
        "--quantifier",
        choices=["any", "all", "none"],
        default="any",
        help="Quantifier for multiple values (default: any)",
    )

    # select-any (convenience)
    sp_selany = subparsers.add_parser(
        "select-any", help="Filter rows where ANY path element matches"
    )
    sp_selany.add_argument("path", help="JSONPath expression")
    sp_selany.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_selany.add_argument("--predicate", help="Python expression for predicate")

    # select-all (convenience)
    sp_selall = subparsers.add_parser(
        "select-all", help="Filter rows where ALL path elements match"
    )
    sp_selall.add_argument("path", help="JSONPath expression")
    sp_selall.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_selall.add_argument("--predicate", help="Python expression for predicate")

    # select-none (convenience)
    sp_selnone = subparsers.add_parser(
        "select-none", help="Filter rows where NO path elements match"
    )
    sp_selnone.add_argument("path", help="JSONPath expression")
    sp_selnone.add_argument("file", nargs="?", help="Input file (defaults to stdin)")
    sp_selnone.add_argument("--predicate", help="Python expression for predicate")

    # project-template
    sp_projtemp = subparsers.add_parser(
        "project-template", help="Project using JSONPath templates"
    )
    sp_projtemp.add_argument("template", help="JSON template with JSONPath expressions")
    sp_projtemp.add_argument("file", nargs="?", help="Input file (defaults to stdin)")

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
        # JSONPath extensions
        "select-path": handle_select_path,
        "select-any": handle_select_any,
        "select-all": handle_select_all,
        "select-none": handle_select_none,
        "project-template": handle_project_template,
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
