from typing import List, Dict, Callable, Tuple
from collections import defaultdict

Row = Dict[str, any]
Relation = List[Row]

def _row_to_hashable_key(row: Row) -> tuple:
    """
    Converts a dictionary row to a canonical hashable representation:
    a tuple of (key, value) pairs, sorted by key.

    Raises:
        TypeError: If any value in the row is not hashable.
    """
    # Check for unhashable values first to provide a more specific error.
    # The resulting tuple from sorted(row.items()) would be unhashable if it contained
    # an unhashable value (like a list), and the error would occur upon hashing (e.g., adding to a set).
    problematic_items_details = []
    for k, v in row.items():
        try:
            hash(v)
        except TypeError:
            problematic_items_details.append(f"key '{k}' with value '{v}' (type: {type(v).__name__})")

    if problematic_items_details:
        raise TypeError(
            f"Row cannot be converted to a hashable key because it contains unhashable values: "
            f"{'; '.join(problematic_items_details)}. Full row: {row}. "
            "All cell values must be hashable (e.g., strings, numbers, booleans, or tuples of hashables)."
        )
    return tuple(sorted(row.items()))

def select(relation: Relation, predicate: Callable[[Row], bool]) -> Relation:
    """
    Filters rows from a relation based on a predicate.

    Args:
        relation: The input relation (list of rows).
        predicate: A function that takes a row and returns True if the row
                   should be included in the result.

    Returns:
        A new relation containing only the rows for which the predicate is True.
    """
    return [row for row in relation if predicate(row)]

def project(relation: Relation, columns: List[str]) -> Relation:
    """
    Selects specific columns from a relation.

    Args:
        relation: The input relation (list of rows).
        columns: A list of column names to include in the result.

    Returns:
        A new relation containing only the specified columns for each row.
        If a row does not contain a specified column, it's omitted for that row.
    """
    return [{col: row[col] for col in columns if col in row} for row in relation]

def join(left: Relation, right: Relation, on: List[Tuple[str, str]]) -> Relation:
    """
    Combines rows from two relations based on specified join conditions.

    Args:
        left: The left relation (list of rows).
        right: The right relation (list of rows).
        on: A list of tuples, where each tuple (left_col, right_col)
            specifies the columns to join on.

    Returns:
        A new relation containing the merged rows that satisfy the join conditions.
        Columns from the right relation that also exist in the left (and are not part of 'on')
        are not included to avoid overwriting.
    """
    right_index = {}
    for r in right:
        key = tuple(r[r_col] for _, r_col in on)
        right_index.setdefault(key, []).append(r)

    result = []
    for l in left:
        key = tuple(l[l_col] for l_col, _ in on)
        for r in right_index.get(key, []):
            row = dict(l)
            row.update({k: v for k, v in r.items() if k not in row})
            result.append(row)
    return result

def rename(relation: Relation, renames: Dict[str, str]) -> Relation:
    """
    Renames columns in a relation.

    Args:
        relation: The input relation (list of rows).
        renames: A dictionary mapping old column names to new column names.

    Returns:
        A new relation with specified columns renamed.
    """
    return [{renames.get(k, k): v for k, v in row.items()} for row in relation]

def union(a: Relation, b: Relation) -> Relation:
    """
    Returns all rows from two relations; duplicates may be present.
    This is equivalent to list concatenation. For a set union (distinct rows),
    pipe the result through `distinct`.

    Args:
        a: The first relation (list of rows).
        b: The second relation (list of rows).

    Returns:
        A new relation containing all rows from both input relations.
    """
    return a + b

def difference(a: Relation, b: Relation) -> Relation:
    """
    Returns rows present in the first relation but not in the second.
    Row comparison is based on their hashable representation.

    Args:
        a: The first relation (list of rows).
        b: The second relation (list of rows), whose rows will be excluded from 'a'.

    Returns:
        A new relation containing rows from 'a' that are not in 'b'.
    """
    b_set = {_row_to_hashable_key(r) for r in b}
    return [r for r in a if _row_to_hashable_key(r) not in b_set]

def distinct(relation: Relation) -> Relation:
    """
    Removes duplicate rows from a relation.
    Row comparison is based on their hashable representation.

    Args:
        relation: The input relation (list of rows).

    Returns:
        A new relation with duplicate rows removed. The first occurrence of
        each unique row is preserved.
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
    """
    Returns rows common to both relations.
    Row comparison is based on their hashable representation.

    Args:
        a: The first relation (list of rows).
        b: The second relation (list of rows).

    Returns:
        A new relation containing only rows that are present in both 'a' and 'b'.
    """
    b_set = {_row_to_hashable_key(r) for r in b}
    return [r for r in a if _row_to_hashable_key(r) in b_set]

def sort_by(relation: Relation, keys: List[str]) -> Relation:
    """
    Sorts a relation by specified keys.

    Args:
        relation: The input relation (list of rows).
        keys: A list of column names to sort by. The sort is performed in
              the order of the columns specified.

    Returns:
        A new relation sorted by the specified keys.
    """
    return sorted(relation, key=lambda r: tuple(r.get(k) for k in keys))

def product(a: Relation, b: Relation) -> Relation:
    """
    Computes the Cartesian product of two relations.

    Args:
        a: The first relation (list of rows).
        b: The second relation (list of rows).

    Returns:
        A new relation containing all combinations of rows from 'a' and 'b'.
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

def groupby_agg(
    relation: Relation,
    key: str,
    aggs: List[Tuple[str, str]]
) -> Relation:
    """
    Groups rows by a key and performs aggregations.

    Args:
        relation: The input relation (list of rows).
        key: The column name to group by.
        aggs: A list of tuples, each specifying an aggregation function and
              the field to aggregate. Supported functions: 'count', 'sum',
              'avg', 'min', 'max'.

    Returns:
        A new relation with one row per group, containing the group key and
        the results of the aggregations.
    """
    grouped = defaultdict(list)
    for row in relation:
        grouped[row[key]].append(row)

    out = []
    for kval, rows in grouped.items():
        result = {key: kval}
        for func, field in aggs:
            if func == "count":
                result["count"] = len(rows)
            elif func == "sum":
                values = [float(r[field]) for r in rows if field in r]
                result[f"sum_{field}"] = sum(values)
            elif func == "avg":
                values = [float(r[field]) for r in rows if field in r]
                result[f"avg_{field}"] = sum(values) / len(values) if values else None
            elif func == "min":
                values = [float(r[field]) for r in rows if field in r]
                result[f"min_{field}"] = min(values) if values else None
            elif func == "max":
                values = [float(r[field]) for r in rows if field in r]
                result[f"max_{field}"] = max(values) if values else None
            else:
                raise ValueError(f"Unsupported aggregation: {func}")
        out.append(result)
    return out

