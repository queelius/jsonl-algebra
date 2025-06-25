"""Core relational algebra operations for JSONL data manipulation.

This module provides fundamental relational algebra operations like select, project,
join, union, intersection, difference, and various utility functions for working
with relations (lists of dictionaries).
"""

from typing import Any, Callable, Dict, List, Tuple

import jmespath

Row = Dict[str, any]
Relation = List[Row]


def _row_to_hashable_key(row: Row) -> tuple:
    """Convert a dictionary row to a canonical hashable representation.

    Creates a tuple of (key, value) pairs, sorted by key, that can be used
    as a dictionary key or added to a set for operations like distinct.

    Args:
        row: A dictionary representing a row of data.

    Returns:
        A tuple of sorted (key, value) pairs.

    Raises:
        TypeError: If any value in the row is not hashable (e.g., contains lists or dicts).
    """
    # Check for unhashable values first to provide a more specific error.
    # The resulting tuple from sorted(row.items()) would be unhashable if it contained
    # an unhashable value (like a list), and the error would occur upon hashing (e.g., adding to a set).
    problematic_items_details = []
    for k, v in row.items():
        try:
            hash(v)
        except TypeError:
            problematic_items_details.append(
                f"key '{k}' with value '{v}' (type: {type(v).__name__})"
            )

    if problematic_items_details:
        raise TypeError(
            f"Row cannot be converted to a hashable key because it contains unhashable values: "
            f"{'; '.join(problematic_items_details)}. Full row: {row}. "
            "All cell values must be hashable (e.g., strings, numbers, booleans, or tuples of hashables)."
        )
    return tuple(sorted(row.items()))


def select(relation: Relation, expression) -> Relation:
    """Filter rows from a relation based on a JMESPath expression.

    Uses JMESPath to filter the relation, keeping only rows that match
    the given expression criteria.

    Args:
        relation: The input relation (list of dictionaries).
        expression: A JMESPath expression string or compiled JMESPath expression.

    Returns:
        A new relation containing only the rows that match the expression.

    Example:
        >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        >>> select(data, "age > `27`")
        [{"name": "Alice", "age": 30}]
    """
    if isinstance(expression, str):
        expression = jmespath.compile(f"[?{expression}]")

    # JMESPath works on the entire relation (JSON document)
    return expression.search(relation)


def project(relation: Relation, columns: List[str]) -> Relation:
    """Select specific columns from a relation.

    Creates a new relation containing only the specified columns from each row.
    If a row doesn't contain a specified column, it's omitted for that row.

    Args:
        relation: The input relation (list of dictionaries).
        columns: A list of column names to include in the result.

    Returns:
        A new relation containing only the specified columns for each row.

    Example:
        >>> data = [{"name": "Alice", "age": 30, "city": "NYC"}]
        >>> project(data, ["name", "age"])
        [{"name": "Alice", "age": 30}]
    """
    return [{col: row[col] for col in columns if col in row} for row in relation]


def join(left: Relation, right: Relation, on: List[Tuple[str, str]]) -> Relation:
    """Combine rows from two relations based on specified join conditions.

    Performs an inner join between two relations, matching rows where the
    specified columns have equal values.

    Args:
        left: The left relation (list of dictionaries).
        right: The right relation (list of dictionaries).
        on: A list of tuples, where each tuple (left_col, right_col)
            specifies the columns to join on.

    Returns:
        A new relation containing the merged rows that satisfy the join conditions.
        The resulting rows contain all columns from the left row, plus columns
        from the matching right row (excluding right columns used in join conditions).

    Example:
        >>> left = [{"id": 1, "name": "Alice"}]
        >>> right = [{"user_id": 1, "score": 95}]
        >>> join(left, right, [("id", "user_id")])
        [{"id": 1, "name": "Alice", "score": 95}]
    """
    right_index = {}
    for r_row_build_idx in right:
        key_tuple = tuple(r_row_build_idx[r_col] for _, r_col in on)
        right_index.setdefault(key_tuple, []).append(r_row_build_idx)

    result = []
    # Pre-calculate the set of right column names that are part of the join condition
    right_join_key_names = {r_col for _, r_col in on}

    for l_row in left:
        key_tuple = tuple(l_row[l_col] for l_col, _ in on)
        for r_row in right_index.get(key_tuple, []):
            merged_row = dict(l_row)  # Start with a copy of the left row

            # Add columns from the right row if they don't collide with left row's columns
            # and are not themselves right-side join keys.
            for r_key, r_val in r_row.items():
                if r_key not in merged_row and r_key not in right_join_key_names:
                    merged_row[r_key] = r_val
            result.append(merged_row)
    return result


def rename(relation: Relation, renames: Dict[str, str]) -> Relation:
    """Rename columns in a relation.

    Creates a new relation with specified columns renamed according to
    the provided mapping.

    Args:
        relation: The input relation (list of dictionaries).
        renames: A dictionary mapping old column names to new column names.

    Returns:
        A new relation with specified columns renamed.

    Example:
        >>> data = [{"old_name": "Alice", "age": 30}]
        >>> rename(data, {"old_name": "name"})
        [{"name": "Alice", "age": 30}]
    """
    return [{renames.get(k, k): v for k, v in row.items()} for row in relation]


def union(a: Relation, b: Relation) -> Relation:
    """Return all rows from two relations.

    Concatenates two relations, preserving all rows including duplicates.
    For distinct union, pipe the result through `distinct`.

    Args:
        a: The first relation (list of dictionaries).
        b: The second relation (list of dictionaries).

    Returns:
        A new relation containing all rows from both input relations.

    Example:
        >>> a = [{"name": "Alice"}]
        >>> b = [{"name": "Bob"}]
        >>> union(a, b)
        [{"name": "Alice"}, {"name": "Bob"}]
    """
    return a + b


def difference(a: Relation, b: Relation) -> Relation:
    """Return rows present in the first relation but not in the second.

    Performs set difference operation, removing from the first relation
    any rows that also appear in the second relation.

    Args:
        a: The first relation (list of dictionaries).
        b: The second relation (list of dictionaries), whose rows will be excluded from 'a'.

    Returns:
        A new relation containing rows from 'a' that are not in 'b'.

    Example:
        >>> a = [{"name": "Alice"}, {"name": "Bob"}]
        >>> b = [{"name": "Alice"}]
        >>> difference(a, b)
        [{"name": "Bob"}]
    """
    b_set = {_row_to_hashable_key(r) for r in b}
    return [r for r in a if _row_to_hashable_key(r) not in b_set]


def distinct(relation: Relation) -> Relation:
    """Remove duplicate rows from a relation.

    Creates a new relation with duplicate rows removed, preserving the
    first occurrence of each unique row.

    Args:
        relation: The input relation (list of dictionaries).

    Returns:
        A new relation with duplicate rows removed.

    Example:
        >>> data = [{"name": "Alice"}, {"name": "Alice"}, {"name": "Bob"}]
        >>> distinct(data)
        [{"name": "Alice"}, {"name": "Bob"}]
    """
    seen = set()
    out = []
    for row in relation:
        key = _row_to_hashable_key(row)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def intersection(a: Relation, b: Relation) -> Relation:
    """Return rows common to both relations.

    Creates a new relation containing only rows that are present in both
    input relations.

    Args:
        a: The first relation (list of dictionaries).
        b: The second relation (list of dictionaries).

    Returns:
        A new relation containing only rows that are present in both 'a' and 'b'.

    Example:
        >>> a = [{"name": "Alice"}, {"name": "Bob"}]
        >>> b = [{"name": "Alice"}, {"name": "Carol"}]
        >>> intersection(a, b)
        [{"name": "Alice"}]
    """
    b_set = {_row_to_hashable_key(r) for r in b}
    return [r for r in a if _row_to_hashable_key(r) in b_set]


def sort_by(relation: Relation, keys: List[str], reverse: bool = False) -> Relation:
    """Sort a relation by specified keys.

    Sorts the relation by the specified column names in order. Missing values
    (None) are sorted before non-None values.

    Args:
        relation: The input relation (list of dictionaries).
        keys: A list of column names to sort by. The sort is performed in
              the order of the columns specified.
        reverse: If True, sort in descending order.

    Returns:
        A new relation sorted by the specified keys.

    Example:
        >>> data = [{"name": "Bob", "age": 30}, {"name": "Alice", "age": 25}]
        >>> sort_by(data, ["name"])
        [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
    """

    def sort_key_func(row: Row) -> tuple:
        key_parts = []
        for k in keys:
            value = row.get(k)
            if value is None:
                # Sort None values first by using a lower first element in the tuple part
                key_parts.append((0, None))
            else:
                key_parts.append((1, value))
        return tuple(key_parts)

    return sorted(relation, key=sort_key_func, reverse=reverse)


def product(a: Relation, b: Relation) -> Relation:
    """Compute the Cartesian product of two relations.

    Creates all possible combinations of rows from the two input relations.

    Args:
        a: The first relation (list of dictionaries).
        b: The second relation (list of dictionaries).

    Returns:
        A new relation containing all combinations of rows from 'a' and 'b'.

    Example:
        >>> a = [{"x": 1}]
        >>> b = [{"y": 2}, {"y": 3}]
        >>> product(a, b)
        [{"x": 1, "y": 2}, {"x": 1, "y": 3}]
    """
    out = []
    for r1 in a:
        for r2 in b:
            merged = dict(r1)
            for k, v in r2.items():
                # avoid key collision by prefixing
                merged[f"b_{k}" if k in r1 else k] = v
            out.append(merged)
    return out
