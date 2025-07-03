"""Aggregation engine for JSONL algebra operations.

This module provides all aggregation functionality including parsing aggregation
specifications, applying aggregations to data, and all built-in aggregation
functions (sum, avg, min, max, etc.).
"""

from typing import Any, Dict, List, Optional, Tuple

from .expr import ExprEval

# Type aliases
Row = Dict[str, Any]
Relation = List[Row]


# ============================================================================
# AGGREGATION FUNCTIONS
# ============================================================================

def _agg_numeric_values(values: List[Any]) -> List[float]:
    """Convert values to floats, skipping None."""
    numeric_values = []
    for v in values:
        if v is None:
            continue
        numeric_values.append(float(v))
    return numeric_values


def _agg_sum_func(values: List[Any]) -> float:
    """Sum of numeric values."""
    return sum(_agg_numeric_values(values))


def _agg_avg_func(values: List[Any]) -> Optional[float]:
    """Average of numeric values."""
    nums = _agg_numeric_values(values)
    return sum(nums) / len(nums) if nums else None


def _agg_min_func(values: List[Any]) -> Optional[float]:
    """Minimum of numeric values."""
    nums = _agg_numeric_values(values)
    return min(nums) if nums else None


def _agg_max_func(values: List[Any]) -> Optional[float]:
    """Maximum of numeric values."""
    nums = _agg_numeric_values(values)
    return max(nums) if nums else None


def _agg_list_func(values: List[Any]) -> List[Any]:
    """Return all values as a list."""
    return values


def _agg_first_func(value: Any) -> Any:
    """Return first value."""
    return value


def _agg_last_func(value: Any) -> Any:
    """Return last value."""
    return value


# Registry of aggregation functions
AGGREGATION_FUNCTIONS = {
    "sum": _agg_sum_func,
    "avg": _agg_avg_func,
    "min": _agg_min_func,
    "max": _agg_max_func,
    "list": _agg_list_func,
    "first": _agg_first_func,
    "last": _agg_last_func,
}


# ============================================================================
# AGGREGATION OPERATIONS
# ============================================================================

def parse_agg_specs(agg_spec: str) -> List[Tuple[str, str]]:
    """Parse aggregation specification string.

    Args:
        agg_spec: Aggregation specification (e.g., "count, avg_age=avg(age)")

    Returns:
        List of (name, expression) tuples
    """
    specs = []
    for part in agg_spec.split(","):
        part = part.strip()
        if "=" in part:
            # Named aggregation: avg_age=avg(age)
            name, expr = part.split("=", 1)
            specs.append((name.strip(), expr.strip()))
        else:
            # Simple aggregation: count
            specs.append((part, part))
    return specs


def apply_single_agg(spec: Tuple[str, str], data: Relation) -> Dict[str, Any]:
    """Apply a single aggregation to data.

    Args:
        spec: (name, expression) tuple
        data: List of dictionaries

    Returns:
        Dictionary with aggregation result
    """
    name, expr = spec
    parser = ExprEval()

    # Parse the aggregation expression
    if "(" in expr and expr.endswith(")"):
        func_name = expr[:expr.index("(")]
        field_expr = expr[expr.index("(") + 1:-1].strip()
    else:
        func_name = expr
        field_expr = ""

    # Handle conditional aggregations
    if "_if" in func_name:
        # e.g., count_if(status == active) or sum_if(amount, status == paid)
        base_func = func_name.replace("_if", "")

        if "," in field_expr:
            # sum_if(amount, status == paid)
            field, condition = field_expr.split(",", 1)
            field = field.strip()
            condition = condition.strip()

            # Filter data based on condition
            filtered_data = [row for row in data if parser.evaluate(condition, row)]

            # Apply base aggregation to filtered data
            if base_func == "sum":
                values = [parser.get_field_value(row, field) for row in filtered_data]
                return {name: sum(v for v in values if v is not None)}
            elif base_func == "avg":
                values = [parser.get_field_value(row, field) for row in filtered_data]
                values = [v for v in values if v is not None]
                return {name: sum(values) / len(values) if values else 0}
            elif base_func == "count":
                return {name: len(filtered_data)}
        else:
            # count_if(status == active)
            filtered_data = [row for row in data if parser.evaluate(field_expr, row)]
            return {name: len(filtered_data)}

    # Regular aggregations
    if func_name == "count":
        return {name: len(data)}
    
    elif func_name in AGGREGATION_FUNCTIONS:
        if func_name in ["first", "last"]:
            # Special handling for first/last
            if not data:
                return {name: None}
            row = data[0] if func_name == "first" else data[-1]
            value = parser.get_field_value(row, field_expr) if field_expr else row
            return {name: value}
        else:
            # Collect values for aggregation
            values = []
            for row in data:
                if field_expr:
                    # Try arithmetic evaluation first
                    val = parser.evaluate_arithmetic(field_expr, row)
                    if val is None:
                        val = parser.get_field_value(row, field_expr)
                else:
                    val = row
                if val is not None:
                    values.append(val)
            
            # Apply aggregation functionsum(_agg_numeric_values(values))
            result = AGGREGATION_FUNCTIONS[func_name](values)
            return {name: result}

    return {name: None}


def aggregate_single_group(data: Relation, agg_spec: str) -> Dict[str, Any]:
    """Aggregate ungrouped data as a single group.

    Args:
        data: List of dictionaries
        agg_spec: Aggregation specification

    Returns:
        Dictionary with aggregation results
    """
    agg_specs = parse_agg_specs(agg_spec)
    result = {}

    for spec in agg_specs:
        result.update(apply_single_agg(spec, data))

    return result

def aggregate_grouped_data(grouped_data: Relation, agg_spec: str) -> Relation:
    """Aggregate data that has group metadata.

    Args:
        grouped_data: Data with group metadata
        agg_spec: Aggregation specification

    Returns:
        List of aggregated results
    """
    from collections import defaultdict
    
    # Group by the combination of all grouping fields
    groups = defaultdict(list)
    group_keys = {}

    for row in grouped_data:
        # Use the _groups list to create a grouping key
        groups_list = row.get("_groups", [])
        
        # Create a tuple key for internal grouping
        group_tuple = tuple((g["field"], g["value"]) for g in groups_list)
        
        # Store the groups for this tuple
        if group_tuple not in group_keys:
            group_keys[group_tuple] = groups_list

        # Remove metadata for aggregation
        clean_row = {k: v for k, v in row.items() if not k.startswith("_group")}
        groups[group_tuple].append(clean_row)

    # Apply aggregations
    result = []

    for group_tuple, group_rows in groups.items():
        # Start with all grouping fields
        agg_result = {}
        
        # Add all grouping fields from the metadata
        for group_info in group_keys[group_tuple]:
            agg_result[group_info["field"]] = group_info["value"]

        # Parse and apply aggregations
        agg_specs = parse_agg_specs(agg_spec)
        for spec in agg_specs:
            agg_result.update(apply_single_agg(spec, group_rows))

        result.append(agg_result)

    return result