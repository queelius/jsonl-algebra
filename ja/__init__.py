"""
ja (JSONL Algebra)

A Python package for performing relational algebra operations on lists of
JSON objects (JSONL data).
"""

from .core import (
    Row,
    Relation,
    select,
    project,
    join,
    rename,
    union,
    difference,
    distinct,
    intersection,
    sort_by,
    product,
    # JSONPath extensions
    select_path,
    select_any,
    select_all,
    select_none,
    project_template,
)

from .groupby import groupby_agg

from .commands import read_jsonl

__all__ = [
    "Row",
    "Relation",
    "select",
    "project",
    "join",
    "rename",
    "union",
    "difference",
    "distinct",
    "intersection",
    "sort_by",
    "product",
    "groupby_agg",
    "read_jsonl",
    # JSONPath extensions
    "select_path",
    "select_any",
    "select_all",
    "select_none",
    "project_template",
]

__version__ = "0.1.0"
