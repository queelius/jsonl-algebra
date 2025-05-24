import sys
import json
from pathlib import Path
from .core import *
from .groupby import groupby_agg

# Helper functions moved from cli.py
def read_jsonl(file_or_fp):
    if isinstance(file_or_fp, str) or isinstance(file_or_fp, Path):
        with open(file_or_fp) as f:
            return [json.loads(line) for line in f]
    else:
        return [json.loads(line) for line in file_or_fp]

def write_jsonl(rows):
    for row in rows:
        print(json.dumps(row))

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
    cols = args.columns.split(",")
    write_jsonl(project(data, cols))

def handle_join(args):
    left_input = sys.stdin if args.left == "-" else args.left
    left = read_jsonl(left_input or sys.stdin) # Default to stdin if left is None (not specified)
    right = read_jsonl(args.right)
    lcol, rcol = args.on.split("=")
    write_jsonl(join(left, right, [(lcol, rcol)]))

def handle_product(args):
    a = read_jsonl(args.left)
    b = read_jsonl(args.right)
    write_jsonl(product(a, b))

def handle_rename(args):
    data = read_jsonl(args.file or sys.stdin)
    mapping = dict(pair.split("=") for pair in args.mapping.split(","))
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
    keys = args.columns.split(",")
    write_jsonl(sort_by(data, keys))

def handle_groupby(args):
    data = read_jsonl(args.file or sys.stdin)
    aggs = []
    for part in args.agg.split(","):
        if ":" in part:
            func, field = part.split(":", 1)
        else:
            func, field = part, "" # Default field to empty string if not provided
        aggs.append((func, field))
    write_jsonl(groupby_agg(data, args.key, aggs))
