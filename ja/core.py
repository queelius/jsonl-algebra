"""Core relational operations for the JSONL algebra system.

This module implements the fundamental set and relational operations that form
the algebra for manipulating collections of JSON objects. All operations are
designed to work with lists of dictionaries, making them suitable for processing
JSONL data.
"""

from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import re

import jmespath

from .expr import ExprEval

Row = Dict[str, any]
Relation = List[Row]


def _row_to_hashable_key(row: Row) -> tuple:
    """Convert a dictionary row to a canonical hashable representation.

    Creates a tuple of (key, value) pairs, sorted by key, that can be used
    as a dictionary key or added to a set for operations like distinct.
    Handles nested dictionaries and lists by converting them to hashable tuples.

    Args:
        row: A dictionary representing a row of data.

    Returns:
        A tuple of sorted (key, value) pairs.

    Raises:
        TypeError: If any value in the row contains an unhashable type like a set.
    """

    def to_hashable(obj: Any) -> Any:
        if isinstance(obj, dict):
            return tuple(sorted((k, to_hashable(v)) for k, v in obj.items()))
        if isinstance(obj, list):
            return tuple(to_hashable(v) for v in obj)
        # Let it fail for unhashable types like sets
        hash(obj)
        return obj

    try:
        # We are converting the whole row dict into a hashable tuple of items.
        return to_hashable(row)
    except TypeError as e:
        # Find the problematic item to create a better error message
        for k, v in row.items():
            try:
                to_hashable(v)
            except TypeError:
                raise TypeError(
                    f"Row cannot be converted to a hashable key because it contains an unhashable value. "
                    f"Problem at key '{k}' with value '{v}' (type: {type(v).__name__}). "
                    f"Full row: {row}. All cell values must be hashable or be dicts/lists of hashable values."
                ) from e
        # Should not be reached if the row has an unhashable value
        raise e


def select(
    data: Relation, expr: str, use_jmespath: bool = False
) -> Relation:
    """Filter rows based on an expression.

    Args:
        data: List of dictionaries to filter
        expr: Expression to evaluate (simple expression or JMESPath)
        use_jmespath: If True, use JMESPath evaluation

    Returns:
        List of rows where the expression evaluates to true
    """
    if use_jmespath:
        compiled_expr = jmespath.compile(expr)
        return [row for row in data if compiled_expr.search(row)]

    # Use simple expression parser
    parser = ExprEval()
    result = []

    # Handle 'and' at the command level for simplicity
    if " and " in expr:
        # Multiple conditions with 'and'
        conditions = expr.split(" and ")
        for row in data:
            if all(parser.evaluate(cond.strip(), row) for cond in conditions):
                result.append(row)
    elif " or " in expr:
        # Multiple conditions with 'or'
        conditions = expr.split(" or ")
        for row in data:
            if any(parser.evaluate(cond.strip(), row) for cond in conditions):
                result.append(row)
    else:
        # Single condition
        for row in data:
            if parser.evaluate(expr, row):
                result.append(row)

    return result


def project(
    data: Relation, fields: List[str] | str, use_jmespath: bool = False
) -> Relation:
    """Project specific fields from each row.

    Args:
        data: List of dictionaries to project
        fields: Comma-separated field names or expressions
        use_jmespath: If True, use JMESPath for projection

    Returns:
        List of dictionaries with only the specified fields
    """

    if use_jmespath:
        compiled_expr = jmespath.compile(fields)
        return [compiled_expr.search(row) for row in data]

    # Parse field specifications
    result = []
    parser = ExprEval()
    field_specs = fields if isinstance(fields, list) else fields.split(",")

    for row in data:
        new_row = {}

        for spec in field_specs:
            if "=" in spec:
                # Computed field: "total=amount*1.1" or "is_adult=age>=18"
                name, expr = spec.split("=", 1)
                name = name.strip()
                expr = expr.strip()

                # Check if it's an arithmetic expression
                arith_result = parser.evaluate_arithmetic(expr, row)
                if arith_result is not None:
                    new_row[name] = arith_result
                else:
                    # Try as boolean expression
                    new_row[name] = parser.evaluate(expr, row)
            else:
                # Simple field projection
                value = parser.get_field_value(row, spec)
                if value is not None:
                    # Build nested structure
                    parser.set_field_value(new_row, spec, value)

        result.append(new_row)

    return result


# --- join --------------------------------------------------------------------
def join(left: Relation,
         right: Relation,
         on: List[Tuple[str, str]]) -> Relation:
    """Inner join with nested-key support."""
    parser = ExprEval()

    # index right side
    right_index: Dict[Tuple[Any, ...], List[Row]] = defaultdict(list)
    for r in right:
        key = tuple(parser.get_field_value(r, rk) for _, rk in on)
        if all(v is not None for v in key):
            right_index[key].append(r)

    # roots of every RHS join path (e.g. 'user.id' → 'user')
    rhs_roots = {re.split(r"[.\[]", rk, 1)[0] for _, rk in on}

    joined: Relation = []
    for l in left:
        l_key = tuple(parser.get_field_value(l, lk) for lk, _ in on)
        if not all(v is not None for v in l_key):
            continue
        for r in right_index.get(l_key, []):
            merged = r.copy()
            merged.update(l)          # left wins
            # drop right-side join columns
            for root in rhs_roots:
                merged.pop(root, None)
            joined.append(merged)
    return joined


# --- product -----------------------------------------------------------------
def product(left: Relation, right: Relation) -> Relation:
    """Cartesian product; colliding keys from *right* are prefixed with ``b_``."""
    result: Relation = []
    for l in left:
        for r in right:
            merged = l.copy()
            for k, v in r.items():
                if k in merged:
                    merged[f"b_{k}"] = v
                else:
                    merged[k] = v
            result.append(merged)
    return result


def rename(data: Relation, mapping: Dict[str, str]) -> Relation:
    """Rename fields in each row.

    Args:
        data: List of dictionaries
        mapping: Dictionary mapping old names to new names

    Returns:
        List with renamed fields
    """
    result = []
    for row in data:
        new_row = {}
        for key, value in row.items():
            new_key = mapping.get(key, key)
            new_row[new_key] = value
        result.append(new_row)
    return result


def union(
    left: Relation, right: Relation
) -> Relation:
    """Compute the union of two collections.

    Args:
        left: First collection
        right: Second collection

    Returns:
        Union of the two collections
    """
    return left + right


def intersection(
    left: Relation, right: Relation
) -> Relation:
    """Compute the intersection of two collections.

    Args:
        left: First collection
        right: Second collection

    Returns:
        Intersection of the two collections
    """
    # Convert right to a set of tuples for efficient lookup
    right_set = {tuple(sorted(row.items())) for row in right}

    result = []
    for row in left:
        if tuple(sorted(row.items())) in right_set:
            result.append(row)

    return result


def difference(
    left: Relation, right: Relation
) -> Relation:
    """Compute the difference of two collections.

    Args:
        left: First collection
        right: Second collection

    Returns:
        Elements in left but not in right
    """
    # Convert right to a set of tuples for efficient lookup
    right_set = {tuple(sorted(row.items())) for row in right}

    result = []
    for row in left:
        if tuple(sorted(row.items())) not in right_set:
            result.append(row)

    return result


def distinct(data: Relation) -> Relation:
    """Remove duplicate rows from a collection.

    Args:
        data: List of dictionaries

    Returns:
        List with duplicates removed
    """
    seen = set()
    result = []

    for row in data:
        # Convert to tuple for hashability
        row_tuple = tuple(sorted(row.items()))
        if row_tuple not in seen:
            seen.add(row_tuple)
            result.append(row)

    return result


# --- sort_by -----------------------------------------------------------------
def sort_by(data: Relation,
            keys: Union[str, List[str]],
            *,
            descending: bool = False) -> Relation:

    key_list = keys.split(",") if isinstance(keys, str) else keys
    key_list = [k.strip() for k in key_list]

    parser = ExprEval()

    def sort_val(row: Row, key: str):
        arith = parser.evaluate_arithmetic(key, row)
        if arith is not None:
            return (False, arith)
        val = parser.get_field_value(row, key)
        # None values sort first
        return (val is not None, str(val) if val is not None else "")

    return sorted(
        data,
        key=lambda r: tuple(sort_val(r, k) for k in key_list),
        reverse=descending,
    )
