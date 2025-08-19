#!/usr/bin/env python3
"""
Command-line interface for jsonl-algebra.

This module provides the argument parsing and command dispatch for the `ja` CLI tool.
It implements a Unix-philosophy tool for performing relational algebra operations
on JSONL (JSON Lines) data.

Design Philosophy:
    1. **Composability**: Each operation reads from stdin/file and writes to stdout
    2. **Streaming First**: Prefer streaming operations for memory efficiency
    3. **Simple Syntax**: Intuitive command names and argument patterns
    4. **Progressive Enhancement**: Basic operations simple, advanced features optional

Command Structure:
    ```
    ja <operation> [arguments] [options] [file]
    ```
    
    Where:
    - operation: The relational algebra operation (select, project, join, etc.)
    - arguments: Operation-specific required arguments
    - options: Optional flags and parameters
    - file: Input file (defaults to stdin for pipe compatibility)

Operation Categories:
    1. **Streaming Operations** (O(1) memory):
       - select: Filter rows with predicates
       - project: Select columns
       - rename: Rename columns
       - union: Concatenate relations
       
    2. **Stateful Streaming** (O(k) memory):
       - distinct: Remove duplicates (tracks seen values)
       
    3. **Memory Operations** (O(n) memory):
       - join: Combine relations on keys
       - sort: Order rows
       - groupby: Aggregate by groups
       - intersection: Common rows
       - difference: Set difference
       - product: Cartesian product
       
    4. **JSONPath Operations** (streaming):
       - select-path: Filter with JSONPath
       - select-any/all/none: Quantified path filters
       - project-template: Transform with templates

Memory Management:
    The CLI automatically selects the most memory-efficient processing mode:
    - Streaming when possible
    - Windowed processing with --window-size for approximations
    - Full memory loading only when necessary

Examples:
    ```bash
    # Simple pipeline
    cat users.jsonl | ja select 'age > 25' | ja project name,email
    
    # Join operation
    ja join users.jsonl orders.jsonl --on id=user_id
    
    # JSONPath filtering
    ja select-any '$.tags[*]' data.jsonl --predicate 'lambda x: x == "important"'
    
    # Windowed processing for large files
    ja sort timestamp huge.jsonl --window-size 1000
    ```

Exit Codes:
    - 0: Success
    - 1: Error (invalid arguments, parse errors, etc.)
    - 130: Interrupted (Ctrl+C)
"""

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
    """
    Add --window-size argument to parser for windowed processing support.
    
    Windowed processing enables memory-efficient handling of large datasets
    by processing data in fixed-size chunks. This trades exact results for
    bounded memory usage.
    
    Args:
        parser: ArgumentParser or subparser to add the argument to
        
    Added Argument:
        --window-size N: Process data in windows of N rows
        
    Use Cases:
        - Sort: Each window sorted independently
        - GroupBy: Groups computed within windows
        - Join: Left relation processed in windows
        
    Trade-offs:
        - Pros: Bounded memory O(w) instead of O(n)
        - Cons: Approximate results, not globally accurate
    """
    parser.add_argument(
        "--window-size",
        type=int,
        default=None,
        help="Process data in windows of specified size for memory efficiency (approximate results)",
    )


def main():
    """
    Main entry point for the jsonl-algebra CLI.
    
    Sets up argument parsing, dispatches to command handlers, and manages
    the overall CLI flow. This function:
    
    1. Creates the argument parser with subcommands
    2. Defines arguments for each operation
    3. Parses command-line arguments
    4. Dispatches to appropriate handler
    5. Handles errors and exit codes
    
    Command Registration:
        Each relational algebra operation is registered as a subcommand
        with its specific arguments and options. The registration order
        doesn't matter, but they're organized by category for clarity.
        
    Error Handling:
        - Invalid arguments: Handled by argparse (exits with error)
        - Runtime errors: Handled by command handlers
        - Unknown commands: Should not occur with required subparsers
        - Broken pipes: Clean exit when piping to head/less
        - Keyboard interrupt: Clean exit on Ctrl+C
    """
    parser = argparse.ArgumentParser(
        prog="ja",
        description="JSONL algebra - Relational operations on JSON Lines data",
        epilog="Use 'ja <command> --help' for command-specific help"
    )
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
