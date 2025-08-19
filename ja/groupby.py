"""
GroupBy aggregation operations for relational algebra (γ operator).

This module implements the GroupBy operation (γ in extended relational algebra),
which partitions a relation into groups based on common attribute values and
applies aggregate functions to each group.

Mathematical Foundation:
    γ(G, F)(R) where:
    - G is a list of grouping attributes
    - F is a list of aggregate functions
    - R is the input relation
    
    Result: A relation with one tuple per group containing:
    - The grouping attribute values
    - The computed aggregate values

Architecture:
    The module uses a dispatcher pattern for extensibility:
    1. Aggregation functions are registered in AGGREGATION_DISPATCHER
    2. Each function handles the reduction of multiple values to a single value
    3. New aggregations can be added by implementing a function and registering it

Supported Aggregations:
    - count: Number of rows in group
    - sum: Sum of numeric values
    - avg: Average of numeric values
    - min: Minimum value
    - max: Maximum value
    - list: Collect all values
    - first: First value in group
    - last: Last value in group

Example:
    >>> data = [
    ...     {"dept": "Sales", "salary": 50000},
    ...     {"dept": "Sales", "salary": 60000},
    ...     {"dept": "IT", "salary": 70000}
    ... ]
    >>> result = groupby_agg(data, "dept", [("avg", "salary"), ("count", "")])
    >>> # Returns: [{"dept": "Sales", "avg_salary": 55000, "count": 2},
    >>> #           {"dept": "IT", "avg_salary": 70000, "count": 1}]

Note:
    This implements the SQL GROUP BY functionality with a focus on
    composability and extensibility for custom aggregations.
"""

from typing import List, Dict, Callable, Tuple, Any, Optional, Union
from .core import Row, Relation


def _agg_numeric_values(values_to_agg: List[Any]) -> List[float]:
    """
    Convert values to numeric format for aggregation.
    
    This helper function prepares data for numeric aggregations by:
    1. Filtering out None values
    2. Converting remaining values to float
    
    Args:
        values_to_agg: List of potentially mixed-type values
        
    Returns:
        List of float values ready for numeric aggregation
        
    Raises:
        ValueError: If a non-None value cannot be converted to float
        
    Example:
        >>> _agg_numeric_values([1, "2.5", None, 3])
        [1.0, 2.5, 3.0]
    """
    numeric_values = []
    for v_val in values_to_agg:
        if v_val is None:
            continue
        # ValueError will propagate if v_val is not convertible (e.g., "abc")
        numeric_values.append(float(v_val))
    return numeric_values


def _agg_sum_func(collected_values: List[Any]) -> float:
    """
    Sum aggregation function (Σ).
    
    Computes the sum of numeric values in the group.
    Non-numeric values and None are filtered out.
    
    Args:
        collected_values: Values from all rows in the group
        
    Returns:
        Sum of numeric values, or 0.0 if no numeric values exist
        
    Mathematical Definition:
        SUM(G) = Σ(x) for all numeric x in G
        
    Example:
        >>> _agg_sum_func([10, 20, None, 30])
        60.0
    """
    numeric_vals = _agg_numeric_values(collected_values)
    return sum(numeric_vals)  # sum of empty list is 0


def _agg_avg_func(collected_values: List[Any]) -> Optional[float]:
    """
    Average aggregation function (μ).
    
    Computes the arithmetic mean of numeric values in the group.
    
    Args:
        collected_values: Values from all rows in the group
        
    Returns:
        Average of numeric values, or None if no numeric values exist
        
    Mathematical Definition:
        AVG(G) = (1/|G|) * Σ(x) for all numeric x in G
        
    Example:
        >>> _agg_avg_func([10, 20, 30])
        20.0
        >>> _agg_avg_func([])
        None
    """
    numeric_vals = _agg_numeric_values(collected_values)
    return sum(numeric_vals) / len(numeric_vals) if numeric_vals else None


def _agg_min_func(collected_values: List[Any]) -> Optional[Any]:
    """
    Minimum aggregation function.
    
    Finds the minimum value in the group. Works with any comparable types.
    
    Args:
        collected_values: Values from all rows in the group
        
    Returns:
        Minimum value, or None if no valid values exist
        
    Note:
        For numeric comparison, values are converted to float.
        None values are filtered out before comparison.
        
    Example:
        >>> _agg_min_func([30, 10, 20, None])
        10
    """
    # First try numeric comparison
    try:
        numeric_vals = _agg_numeric_values(collected_values)
        return min(numeric_vals) if numeric_vals else None
    except (ValueError, TypeError):
        # Fall back to general comparison for non-numeric types
        valid_vals = [v for v in collected_values if v is not None]
        return min(valid_vals) if valid_vals else None


def _agg_max_func(collected_values: List[Any]) -> Optional[Any]:
    """
    Maximum aggregation function.
    
    Finds the maximum value in the group. Works with any comparable types.
    
    Args:
        collected_values: Values from all rows in the group
        
    Returns:
        Maximum value, or None if no valid values exist
        
    Note:
        For numeric comparison, values are converted to float.
        None values are filtered out before comparison.
        
    Example:
        >>> _agg_max_func([30, 10, 20, None])
        30
    """
    # First try numeric comparison
    try:
        numeric_vals = _agg_numeric_values(collected_values)
        return max(numeric_vals) if numeric_vals else None
    except (ValueError, TypeError):
        # Fall back to general comparison for non-numeric types
        valid_vals = [v for v in collected_values if v is not None]
        return max(valid_vals) if valid_vals else None


def _agg_count_func(collected_values: List[Any]) -> int:
    """
    Count aggregation function.
    
    Counts the number of non-None values in the group.
    
    Args:
        collected_values: Values from all rows in the group
        
    Returns:
        Count of non-None values
        
    Note:
        This counts rows with non-None values for the specified column.
        For counting all rows in a group, use count with empty column name.
        
    Example:
        >>> _agg_count_func([1, 2, None, 3])
        3
    """
    return sum(1 for v in collected_values if v is not None)


def _agg_list_func(collected_values: List[Any]) -> List[Any]:
    """
    List aggregation function.
    
    Collects all values into a list, preserving order.
    
    Args:
        collected_values: Values from all rows in the group
        
    Returns:
        List of all values (including None if present)
        
    Note:
        This is useful for collecting all values without reduction,
        similar to PostgreSQL's array_agg or MySQL's GROUP_CONCAT.
        
    Example:
        >>> _agg_list_func(['a', 'b', 'c'])
        ['a', 'b', 'c']
    """
    return collected_values


def _agg_first_func(collected_values: Any) -> Any:
    """
    First value aggregation function.
    
    Returns the first value encountered in the group.
    
    Args:
        collected_values: First value from the group (pre-selected)
        
    Returns:
        The first value (including None if that's what it is)
        
    Note:
        Useful for picking a representative value when all values
        in the group are expected to be the same.
        
    Example:
        >>> _agg_first_func('first_value')
        'first_value'
    """
    return collected_values


def _agg_last_func(collected_values: Any) -> Any:
    """
    Last value aggregation function.
    
    Returns the last value encountered in the group.
    
    Args:
        collected_values: Last value from the group (pre-selected)
        
    Returns:
        The last value (including None if that's what it is)
        
    Note:
        Order depends on the input order of rows.
        
    Example:
        >>> _agg_last_func('last_value')
        'last_value'
    """
    return collected_values


# Aggregation function registry
AGGREGATION_DISPATCHER: Dict[str, Callable] = {
    "sum": _agg_sum_func,
    "avg": _agg_avg_func,
    "min": _agg_min_func,
    "max": _agg_max_func,
    "count": _agg_count_func,
    "list": _agg_list_func,
    "first": _agg_first_func,
    "last": _agg_last_func,
}
"""
Registry mapping aggregation names to their implementation functions.

To add a custom aggregation:
1. Define a function that takes collected values and returns the aggregate
2. Add it to this dictionary with a descriptive name
3. Use it in groupby_agg with the registered name

Example:
    >>> def _agg_median_func(values: List[Any]) -> Optional[float]:
    ...     numeric = sorted(_agg_numeric_values(values))
    ...     if not numeric:
    ...         return None
    ...     mid = len(numeric) // 2
    ...     return numeric[mid] if len(numeric) % 2 else (numeric[mid-1] + numeric[mid]) / 2
    >>> AGGREGATION_DISPATCHER["median"] = _agg_median_func
"""


def groupby_agg(
    relation: Relation,
    group_by_key: str,
    aggregations: List[Tuple[str, str]]
) -> Relation:
    """
    Group by a key and apply aggregations (γ operator in relational algebra).
    
    This implements the GroupBy operation from extended relational algebra,
    partitioning the relation into groups based on the value of a grouping
    attribute and computing aggregates for each group.
    
    Args:
        relation: Input relation to group
        group_by_key: Column name to group by
        aggregations: List of (function, column) tuples where:
            - function: Name of aggregation function (see AGGREGATION_DISPATCHER)
            - column: Column to aggregate (empty string for count of rows)
            
    Returns:
        New relation with one row per group containing:
        - The grouping key value
        - Computed aggregates with names like "{function}_{column}"
        
    Raises:
        ValueError: If an aggregation function is not recognized
        
    Mathematical Notation:
        γ(group_by_key, F)(relation) where F is the list of aggregate functions
        
    Example:
        >>> sales = [
        ...     {"region": "North", "amount": 100, "rep": "Alice"},
        ...     {"region": "North", "amount": 150, "rep": "Bob"},
        ...     {"region": "South", "amount": 200, "rep": "Charlie"}
        ... ]
        >>> result = groupby_agg(sales, "region", [
        ...     ("sum", "amount"),
        ...     ("count", ""),
        ...     ("list", "rep")
        ... ])
        >>> # Result: [
        >>> #   {"region": "North", "sum_amount": 250, "count": 2, "list_rep": ["Alice", "Bob"]},
        >>> #   {"region": "South", "sum_amount": 200, "count": 1, "list_rep": ["Charlie"]}
        >>> # ]
        
    Note:
        - Empty column name ("") counts all rows in the group
        - Missing values are handled gracefully (typically ignored in aggregations)
        - Order of groups in output is not guaranteed
        
    See Also:
        - SQL GROUP BY clause
        - pandas DataFrame.groupby()
        - SQL window functions (for more advanced use cases)
    """
    if not relation:
        return []

    # Phase 1: Group rows by key value
    groups: Dict[Any, List[Row]] = {}
    for row in relation:
        key_value = row.get(group_by_key)
        if key_value not in groups:
            groups[key_value] = []
        groups[key_value].append(row)

    # Phase 2: Apply aggregations to each group
    result = []
    for key_value, group_rows in groups.items():
        result_row = {group_by_key: key_value}

        for agg_func_name, agg_column in aggregations:
            # Validate aggregation function
            if agg_func_name not in AGGREGATION_DISPATCHER:
                raise ValueError(
                    f"Unsupported aggregation function: {agg_func_name}. "
                    f"Supported: {list(AGGREGATION_DISPATCHER.keys())}"
                )

            # Determine output column name
            if agg_column:
                output_column = f"{agg_func_name}_{agg_column}"
            else:
                output_column = agg_func_name

            # Collect values for aggregation
            if agg_func_name in ["first", "last"]:
                # Special handling for first/last - just get single value
                if agg_column:
                    if agg_func_name == "first":
                        collected = group_rows[0].get(agg_column) if group_rows else None
                    else:  # last
                        collected = group_rows[-1].get(agg_column) if group_rows else None
                else:
                    # First/last row of group
                    collected = group_rows[0] if agg_func_name == "first" and group_rows else group_rows[-1] if group_rows else None
            else:
                # Collect all values for aggregation
                if agg_column:
                    collected = [row.get(agg_column) for row in group_rows]
                else:
                    # For count without column, just count rows
                    collected = group_rows

            # Apply aggregation function
            if agg_func_name == "count" and not agg_column:
                # Special case: count rows in group
                result_row[output_column] = len(group_rows)
            else:
                agg_func = AGGREGATION_DISPATCHER[agg_func_name]
                result_row[output_column] = agg_func(collected)

        result.append(result_row)

    return result