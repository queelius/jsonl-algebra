"""JSONL Algebra: Your Command-Line Toolkit for JSONL Data.

Welcome to `ja`! This package provides a powerful and intuitive suite of tools
for performing relational algebra on JSONL data, right from your command line
or in your Python scripts. Think of it as a `sed` / `awk` / `grep` for structured
JSON, designed to be both powerful for complex tasks and simple for everyday use.

Whether you need to filter a massive log file, join two datasets, reshape a
complex nested JSON structure, or simply explore your data, `ja` has your back.

Core Features:

- **Relational Operations**: `select`, `project`, `join`, `union`, `difference`,
  `distinct`, and more, all supporting nested data via dot notation.
- **Powerful Aggregation**: A `groupby` command to summarize and aggregate your data.
- **Schema Inference**: Automatically generate a JSON Schema to understand your
  data's structure.
- **Format Conversion**: Easily import from CSV or directories of JSON and export
  to CSV.
- **Interactive REPL**: An interactive shell to build data pipelines step-by-step.

Get started with the `ja` command-line tool or import functions directly into
your Python code.

Example:
    >>> from ja.core import select, project
    >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    >>> young_people = select(data, "age < 30")
    >>> names_only = project(young_people, ["name"])
    >>> print(list(names_only))
    [{'name': 'Bob'}]
"""

from .commands import read_jsonl
from .core import (
    Relation,
    Row,
    collect,
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
# Import from the new modules
from .agg import aggregate_single_group, aggregate_grouped_data
from .group import groupby_agg, groupby_with_metadata, groupby_chained
# Import composable operations
from .compose import (
    Pipeline,
    pipeline,
    lazy_pipeline,
    compose,
    pipe,
    Select,
    Project,
    Sort,
    Distinct,
    Rename,
    GroupBy,
    Take,
    Skip,
    Map,
    Filter,
    Batch,
)

__all__ = [
    # Core types
    "Row",
    "Relation",
    # Core operations
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
    "collect",
    # Grouping and aggregation
    "groupby_agg",
    "groupby_with_metadata",
    "groupby_chained",
    "aggregate_single_group",
    "aggregate_grouped_data",
    # I/O
    "read_jsonl",
    # Composable operations
    "Pipeline",
    "pipeline",
    "lazy_pipeline",
    "compose",
    "pipe",
    "Select",
    "Project",
    "Sort",
    "Distinct",
    "Rename",
    "GroupBy",
    "Take",
    "Skip",
    "Map",
    "Filter",
    "Batch",
]

__version__ = "0.1.0"
