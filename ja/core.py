from typing import List, Dict, Callable, Tuple, Any

# JSONPath extensions - integrated into core
from ja.jsonpath import (
    JSONPath,
    PathQuantifier,
    select_path as _select_path,
    select_any as _select_any,
    select_all as _select_all,
    select_none as _select_none,
    project_template as _project_template,
)

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
        The resulting row initially contains all columns from the left row.
        Then, columns from the matching right row are considered: if a right
        column's name is not present in the left row's columns AND is not one
        of the column names used on the right side of the join condition (from
        the `on` parameter), it is added to the result.
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
              the order of the columns specified. Missing values (None)
              are sorted before non-None values.

    Returns:
        A new relation sorted by the specified keys.
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

    return sorted(relation, key=sort_key_func)


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


# ==========================================
# JSONPath Extensions
# ==========================================


def select_path(
    relation: Relation,
    path_expr: str,
    predicate: Callable[[Any], bool] = None,
    quantifier: str = "any",
) -> Relation:
    """
    Select rows using JSONPath expressions with quantifiers.

    Args:
        relation: List of dictionaries to filter
        path_expr: JSONPath expression (e.g., "$.orders[*].amount")
        predicate: Optional predicate function to apply to path values
        quantifier: "any" (default), "all", or "none"

    Returns:
        Filtered list of dictionaries

    Examples:
        # Users with any expensive order
        select_path(users, "$.orders[*].amount", lambda x: x > 100)

        # Users where all scores are high
        select_path(users, "$.scores[*]", lambda x: x > 80, "all")

        # Users with no cancelled orders
        select_path(users, "$.orders[*].status", lambda x: x == "cancelled", "none")
    """
    quantifier_map = {
        "any": PathQuantifier.ANY,
        "all": PathQuantifier.ALL,
        "none": PathQuantifier.NONE,
    }

    if quantifier not in quantifier_map:
        raise ValueError(
            f"Invalid quantifier: {quantifier}. Use 'any', 'all', or 'none'."
        )

    return _select_path(relation, path_expr, predicate, quantifier_map[quantifier])


def select_any(
    relation: Relation, path_expr: str, predicate: Callable[[Any], bool] = None
) -> Relation:
    """
    Select rows where ANY element in the path matches the predicate.

    This is equivalent to select_path() with quantifier="any" (the default).

    Args:
        relation: List of dictionaries to filter
        path_expr: JSONPath expression
        predicate: Predicate function to apply to path values

    Returns:
        Rows where any path values satisfy the predicate
    """
    return _select_any(relation, path_expr, predicate)


def select_all(
    relation: Relation, path_expr: str, predicate: Callable[[Any], bool] = None
) -> Relation:
    """
    Select rows where ALL elements in the path match the predicate.

    Args:
        relation: List of dictionaries to filter
        path_expr: JSONPath expression
        predicate: Predicate function to apply to path values

    Returns:
        Rows where all path values satisfy the predicate
    """
    return _select_all(relation, path_expr, predicate)


def select_none(
    relation: Relation, path_expr: str, predicate: Callable[[Any], bool] = None
) -> Relation:
    """
    Select rows where NO elements in the path match the predicate.

    Args:
        relation: List of dictionaries to filter
        path_expr: JSONPath expression
        predicate: Predicate function to apply to path values

    Returns:
        Rows where no path values satisfy the predicate
    """
    return _select_none(relation, path_expr, predicate)


def project_template(relation: Relation, template: Dict[str, str]) -> Relation:
    """
    Project using template expressions that can include JSONPath and aggregations.

    Args:
        relation: List of dictionaries to project
        template: Dictionary mapping output field names to template expressions

    Returns:
        List of dictionaries with projected fields

    Examples:
        # Project with aggregations
        project_template(orders, {
            "customer": "$.customer.name",
            "total_spent": "sum($.items[*].price)",
            "item_count": "count($.items[*])",
            "has_orders": "exists($.items[*])"
        })
    """
    return _project_template(relation, template)
