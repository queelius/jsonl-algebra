"""
ja (JSONL Algebra)

A Python package for performing relational algebra operations on lists of
JSON objects (JSONL data).

This package provides both functional and fluid APIs for data manipulation.
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

# Fluid API imports
from .relation import RelationQuery, GroupedRelation
from .fluid import query, from_jsonl, from_records, Q

__all__ = [
    # Core types
    "Row",
    "Relation",
    # Functional API
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
    # Fluid API
    "RelationQuery",
    "GroupedRelation",
    "query",
    "from_jsonl",
    "from_records",
    "Q",
]

__version__ = "0.2.0"  # Bumped for fluid API addition
