"""JSONL Algebra - Relational algebra operations for JSONL data.

A Python package for performing relational algebra operations on lists of
JSON objects (JSONL data). Provides both a CLI and a library interface
for data manipulation, schema inference, and format conversion.

This package allows you to:
- Perform relational operations like select, project, join, union, etc.
- Infer and validate JSON schemas from data
- Convert between various data formats (CSV, JSON arrays, directories)
- Work with data interactively via REPL or programmatically via library functions

Example:
    >>> from ja import select, project
    >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    >>> young_people = select(data, "age < `30`")
    >>> names_only = project(young_people, ["name"])
"""

from .commands import read_jsonl
from .core import (
    Relation,
    Row,
    difference,
    distinct,
    intersection,
    join,
    product,
    project,
    rename,
    select,
    sort_by,
    union,
)
from .groupby import groupby_agg

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
]

__version__ = "0.1.0"
