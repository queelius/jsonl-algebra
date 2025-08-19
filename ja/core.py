"""
Core relational algebra operations for JSONL data.

This module provides the fundamental relational algebra operations for working with
JSONL (JSON Lines) data. Each operation follows the mathematical definition of
relational algebra while being optimized for JSON data structures.

Type Aliases:
    Row: A dictionary representing a single record/tuple in the relation
    Relation: A list of rows representing a table/relation
"""

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

Row = Dict[str, Any]
"""Type alias for a single row/record in a relation."""

Relation = List[Row]
"""Type alias for a relation (table) as a list of rows."""


def _row_to_hashable_key(row: Row) -> tuple:
    """
    Convert a dictionary row to a canonical hashable representation.
    
    This internal function creates a hashable key from a row by converting it to
    a sorted tuple of (key, value) pairs. Used for set operations and deduplication.
    
    Args:
        row: A dictionary representing a single record
        
    Returns:
        A tuple of sorted (key, value) pairs
        
    Raises:
        TypeError: If any value in the row is not hashable (e.g., lists, dicts)
        
    Example:
        >>> _row_to_hashable_key({"id": 1, "name": "Alice"})
        (('id', 1), ('name', 'Alice'))
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
    Select rows that satisfy a predicate (σ in relational algebra).
    
    The select operation (also known as restriction) filters rows from a relation
    based on a boolean predicate function. This is equivalent to SQL's WHERE clause.
    
    Args:
        relation: The input relation to filter
        predicate: A function that takes a row and returns True if the row
                  should be included in the result
                  
    Returns:
        A new relation containing only rows where predicate returns True
        
    Example:
        >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        >>> result = select(data, lambda row: row["age"] > 26)
        >>> result
        [{"name": "Alice", "age": 30}]
        
    Note:
        The predicate function should handle missing keys gracefully using
        row.get(key, default) rather than row[key] to avoid KeyError.
    """
    return [row for row in relation if predicate(row)]


def project(relation: Relation, columns: List[str]) -> Relation:
    """
    Project specific columns from a relation (π in relational algebra).
    
    The project operation selects a subset of columns from each row, similar to
    SQL's SELECT clause with specific column names.
    
    Args:
        relation: The input relation
        columns: List of column names to include in the result
        
    Returns:
        A new relation with only the specified columns. Rows missing a column
        will not have that column in the output.
        
    Example:
        >>> data = [{"id": 1, "name": "Alice", "age": 30}]
        >>> result = project(data, ["name", "age"])
        >>> result
        [{"name": "Alice", "age": 30}]
        
    Note:
        If a column doesn't exist in a row, it's silently omitted from that row's
        output rather than raising an error.
    """
    return [{col: row[col] for col in columns if col in row} for row in relation]


def join(left: Relation, right: Relation, on: List[Tuple[str, str]]) -> Relation:
    """
    Perform an inner join on two relations (⋈ in relational algebra).
    
    Combines rows from two relations based on matching values in specified columns.
    This is an equi-join operation similar to SQL's INNER JOIN.
    
    Args:
        left: The left relation
        right: The right relation
        on: List of tuples (left_col, right_col) specifying join conditions.
            Multiple tuples represent an AND condition.
            
    Returns:
        A new relation containing merged rows that satisfy all join conditions.
        The result includes all columns from both relations, with the right
        relation's columns taking precedence in case of name conflicts (except
        for the join columns themselves).
        
    Example:
        >>> users = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        >>> orders = [{"user_id": 1, "item": "Book"}, {"user_id": 1, "item": "Pen"}]
        >>> result = join(users, orders, [("id", "user_id")])
        >>> len(result)
        2
        
    Note:
        - This is an inner join - only matching rows are included
        - For large relations, this builds an index on the right relation for efficiency
        - Join columns from the right are excluded to avoid duplication
    """
    right_index = {}
    for r_row_build_idx in right:
        key_tuple = tuple(r_row_build_idx[r_col] for _, r_col in on)
        right_index.setdefault(key_tuple, []).append(r_row_build_idx)

    result = []
    right_join_columns = {r_col for _, r_col in on}

    for l_row in left:
        key_tuple = tuple(l_row[l_col] for l_col, _ in on)
        if key_tuple in right_index:
            for r_row in right_index[key_tuple]:
                merged_row = l_row.copy()
                for r_col, r_val in r_row.items():
                    if r_col not in right_join_columns:
                        merged_row[r_col] = r_val
                result.append(merged_row)

    return result


def rename(relation: Relation, renames: Dict[str, str]) -> Relation:
    """
    Rename columns in a relation (ρ in relational algebra).
    
    Creates a new relation with specified columns renamed according to the
    provided mapping.
    
    Args:
        relation: The input relation
        renames: Dictionary mapping old column names to new names
        
    Returns:
        A new relation with columns renamed. Columns not in renames are unchanged.
        
    Example:
        >>> data = [{"id": 1, "name": "Alice"}]
        >>> result = rename(data, {"id": "user_id"})
        >>> result
        [{"user_id": 1, "name": "Alice"}]
        
    Note:
        If an old column name doesn't exist in a row, it's ignored (no error).
    """
    result = []
    for row in relation:
        new_row = {}
        for k, v in row.items():
            new_row[renames.get(k, k)] = v
        result.append(new_row)
    return result


def union(a: Relation, b: Relation) -> Relation:
    """
    Compute the union of two relations (∪ in relational algebra).
    
    Returns all rows that appear in either relation. Unlike SQL UNION,
    this preserves duplicates (similar to SQL UNION ALL).
    
    Args:
        a: First relation
        b: Second relation
        
    Returns:
        A new relation containing all rows from both input relations
        
    Example:
        >>> r1 = [{"id": 1}, {"id": 2}]
        >>> r2 = [{"id": 2}, {"id": 3}]
        >>> result = union(r1, r2)
        >>> len(result)
        4
        
    Note:
        To remove duplicates, apply distinct() to the result.
    """
    return a + b


def difference(a: Relation, b: Relation) -> Relation:
    """
    Compute the set difference of two relations (- in relational algebra).
    
    Returns rows that appear in the first relation but not in the second.
    
    Args:
        a: The relation to subtract from
        b: The relation to subtract
        
    Returns:
        A new relation containing rows in a that are not in b
        
    Example:
        >>> r1 = [{"id": 1}, {"id": 2}, {"id": 3}]
        >>> r2 = [{"id": 2}]
        >>> result = difference(r1, r2)
        >>> len(result)
        2
        
    Note:
        Comparison is based on exact row equality (all fields must match).
    """
    b_set = set()
    for row in b:
        b_set.add(_row_to_hashable_key(row))

    result = []
    for row in a:
        if _row_to_hashable_key(row) not in b_set:
            result.append(row)

    return result


def distinct(relation: Relation) -> Relation:
    """
    Remove duplicate rows from a relation.
    
    Returns a new relation with only unique rows, preserving the order of
    first occurrence.
    
    Args:
        relation: The input relation potentially containing duplicates
        
    Returns:
        A new relation with duplicates removed
        
    Example:
        >>> data = [{"id": 1}, {"id": 2}, {"id": 1}]
        >>> result = distinct(data)
        >>> len(result)
        2
        
    Note:
        Rows are considered equal if all their fields have the same values.
        For rows with unhashable values (lists, dicts), comparison may fail.
    """
    seen = set()
    result = []

    for row in relation:
        key = _row_to_hashable_key(row)
        if key not in seen:
            seen.add(key)
            result.append(row)

    return result


def intersection(a: Relation, b: Relation) -> Relation:
    """
    Compute the intersection of two relations (∩ in relational algebra).
    
    Returns rows that appear in both relations.
    
    Args:
        a: First relation
        b: Second relation
        
    Returns:
        A new relation containing only rows present in both inputs
        
    Example:
        >>> r1 = [{"id": 1}, {"id": 2}]
        >>> r2 = [{"id": 2}, {"id": 3}]
        >>> result = intersection(r1, r2)
        >>> result
        [{"id": 2}]
        
    Note:
        Preserves the order from the first relation.
    """
    b_set = set()
    for row in b:
        b_set.add(_row_to_hashable_key(row))

    result = []
    for row in a:
        if _row_to_hashable_key(row) in b_set:
            result.append(row)

    return result


def sort_by(relation: Relation, keys: List[str]) -> Relation:
    """
    Sort a relation by specified columns.
    
    Sorts rows in ascending order by the given keys. Multi-key sorting is
    supported with precedence given to earlier keys.
    
    Args:
        relation: The relation to sort
        keys: List of column names to sort by, in order of precedence
        
    Returns:
        A new relation with rows sorted by the specified keys
        
    Example:
        >>> data = [{"name": "Bob", "age": 30}, {"name": "Alice", "age": 25}]
        >>> result = sort_by(data, ["name"])
        >>> result[0]["name"]
        'Alice'
        
    Note:
        - Missing keys are treated as None and sort before other values
        - For descending order, apply reverse() to the result
    """
    if not keys:
        return relation.copy()

    def get_sort_key(row):
        return tuple(row.get(key) for key in keys)

    return sorted(relation, key=get_sort_key)


def product(a: Relation, b: Relation) -> Relation:
    """
    Compute the Cartesian product of two relations (× in relational algebra).
    
    Returns all possible combinations of rows from both relations.
    
    Args:
        a: First relation
        b: Second relation
        
    Returns:
        A new relation where each row is a combination of a row from a
        and a row from b. Size is |a| × |b|.
        
    Example:
        >>> colors = [{"color": "red"}, {"color": "blue"}]
        >>> sizes = [{"size": "S"}, {"size": "L"}]
        >>> result = product(colors, sizes)
        >>> len(result)
        4
        
    Warning:
        The result size grows multiplicatively. Use with caution on large relations.
        
    Note:
        If column names conflict, the second relation's values overwrite the first's.
    """
    result = []
    for row_a in a:
        for row_b in b:
            merged_row = row_a.copy()
            merged_row.update(row_b)
            result.append(merged_row)
    return result


# JSONPath convenience functions

def select_path(
    relation: Relation,
    path_expr: str,
    predicate: Callable = None,
    quantifier: str = "any",
) -> Relation:
    """
    Filter rows using JSONPath expressions with optional predicates.
    
    Powerful selection using JSONPath to navigate nested structures and apply
    predicates with quantifiers for multiple matches.
    
    Args:
        relation: The input relation
        path_expr: JSONPath expression (e.g., "$.user.addresses[*].city")
        predicate: Optional function to test path values (default: check existence)
        quantifier: How to handle multiple matches: "any", "all", or "none"
        
    Returns:
        Rows where the JSONPath predicate is satisfied
        
    Example:
        >>> data = [{"user": {"age": 30}}, {"user": {"age": 20}}]
        >>> result = select_path(data, "$.user.age", lambda x: x > 25)
        >>> len(result)
        1
        
    See Also:
        - select_any: Convenience for quantifier="any"
        - select_all: Convenience for quantifier="all"
        - select_none: Convenience for quantifier="none"
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
    relation: Relation, path_expr: str, predicate: Callable = None
) -> Relation:
    """
    Select rows where ANY path match satisfies the predicate.
    
    Convenience function for select_path with quantifier="any".
    
    Args:
        relation: The input relation
        path_expr: JSONPath expression
        predicate: Optional predicate function
        
    Returns:
        Rows where at least one path match satisfies the predicate
        
    Example:
        >>> data = [{"tags": ["python", "data"]}, {"tags": ["java"]}]
        >>> result = select_any(data, "$.tags[*]", lambda x: x == "python")
        >>> len(result)
        1
    """
    return _select_any(relation, path_expr, predicate)


def select_all(
    relation: Relation, path_expr: str, predicate: Callable = None
) -> Relation:
    """
    Select rows where ALL path matches satisfy the predicate.
    
    Convenience function for select_path with quantifier="all".
    
    Args:
        relation: The input relation
        path_expr: JSONPath expression
        predicate: Optional predicate function
        
    Returns:
        Rows where all path matches satisfy the predicate
        
    Example:
        >>> data = [{"scores": [80, 90, 85]}, {"scores": [70, 75, 72]}]
        >>> result = select_all(data, "$.scores[*]", lambda x: x >= 80)
        >>> len(result)
        1
    """
    return _select_all(relation, path_expr, predicate)


def select_none(
    relation: Relation, path_expr: str, predicate: Callable = None
) -> Relation:
    """
    Select rows where NO path matches satisfy the predicate.
    
    Convenience function for select_path with quantifier="none".
    
    Args:
        relation: The input relation
        path_expr: JSONPath expression
        predicate: Optional predicate function
        
    Returns:
        Rows where no path matches satisfy the predicate
        
    Example:
        >>> data = [{"items": [{"status": "ok"}]}, {"items": [{"status": "error"}]}]
        >>> result = select_none(data, "$.items[*].status", lambda x: x == "error")
        >>> len(result)
        1
    """
    return _select_none(relation, path_expr, predicate)


def project_template(relation: Relation, template: Dict[str, str]) -> Relation:
    """
    Project using template expressions with JSONPath and aggregations.
    
    Advanced projection that can extract nested fields, perform aggregations,
    and create new structures.
    
    Args:
        relation: The input relation
        template: Dictionary mapping output field names to template expressions.
                 Expressions can be JSONPath queries or aggregation functions.
                 
    Returns:
        A new relation with fields computed according to the template
        
    Example:
        >>> data = [{"user": {"name": "Alice"}, "orders": [{"total": 10}, {"total": 20}]}]
        >>> template = {"name": "$.user.name", "total": "sum($.orders[*].total)"}
        >>> result = project_template(data, template)
        >>> result[0]["total"]
        30
        
    Supported Aggregations:
        - sum(path): Sum of numeric values
        - count(path): Count of values
        - avg(path): Average of numeric values
        - min(path): Minimum value
        - max(path): Maximum value
        - first(path): First value
        - last(path): Last value
        - list(path): All values as a list
    """
    return _project_template(relation, template)