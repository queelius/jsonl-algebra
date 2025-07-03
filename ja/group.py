"""Grouping operations for JSONL algebra.

This module provides grouping functionality that supports both immediate
aggregation and metadata-based chaining for multi-level grouping.
"""

from collections import defaultdict
from typing import Dict, List, Any, Tuple

from .agg import parse_agg_specs, apply_single_agg
from .expr import ExprEval

# Type aliases
Row = Dict[str, Any]
Relation = List[Row]


def groupby_with_metadata(data: Relation, group_key: str) -> Relation:
    """Group data and add metadata fields.

    This function enables chained groupby operations by adding special
    metadata fields to each row:
    - _groups: List of {field, value} objects representing the grouping hierarchy
    - _group_size: Total number of rows in this group
    - _group_index: This row's index within its group

    Args:
        data: List of dictionaries to group
        group_key: Field to group by (supports dot notation)

    Returns:
        List with group metadata added to each row
    """
    parser = ExprEval()

    # First pass: collect groups
    groups = defaultdict(list)
    for row in data:
        key_value = parser.get_field_value(row, group_key)
        groups[key_value].append(row)

    # Second pass: add metadata and flatten
    result = []
    for group_value, group_rows in groups.items():
        group_size = len(group_rows)
        for index, row in enumerate(group_rows):
            # Create new row with metadata
            new_row = row.copy()
            new_row["_groups"] = [{"field": group_key, "value": group_value}]
            new_row["_group_size"] = group_size
            new_row["_group_index"] = index
            result.append(new_row)

    return result


def groupby_chained(grouped_data: Relation, new_group_key: str) -> Relation:
    """Apply groupby to already-grouped data.

    This function handles multi-level grouping by building on existing
    group metadata.

    Args:
        grouped_data: Data with existing group metadata
        new_group_key: Field to group by

    Returns:
        List with nested group metadata
    """
    parser = ExprEval()

    # Group within existing groups
    nested_groups = defaultdict(list)

    for row in grouped_data:
        # Get existing groups
        existing_groups = row.get("_groups", [])
        new_key_value = parser.get_field_value(row, new_group_key)
        
        # Create a tuple key for grouping (for internal use only)
        group_tuple = tuple((g["field"], g["value"]) for g in existing_groups)
        group_tuple += ((new_group_key, new_key_value),)
        
        nested_groups[group_tuple].append(row)

    # Add new metadata
    result = []
    for group_tuple, group_rows in nested_groups.items():
        group_size = len(group_rows)
        
        for index, row in enumerate(group_rows):
            new_row = row.copy()
            
            # Extend the groups list
            new_row["_groups"] = row.get("_groups", []).copy()
            new_row["_groups"].append({
                "field": new_group_key,
                "value": parser.get_field_value(row, new_group_key)
            })
            
            new_row["_group_size"] = group_size
            new_row["_group_index"] = index
            result.append(new_row)

    return result


def groupby_agg(data: Relation, group_key: str, agg_spec: str) -> Relation:
    """Group and aggregate in one operation.
    
    This function is kept for backward compatibility and for the --agg flag.
    It's more efficient for simple cases but less flexible than chaining.
    
    Args:
        data: List of dictionaries to group and aggregate
        group_key: Field to group by
        agg_spec: Aggregation specification
        
    Returns:
        List of aggregated results, one per group
    """
    parser = ExprEval()
    
    # Group data
    groups = defaultdict(list)
    for row in data:
        key = parser.get_field_value(row, group_key)
        groups[key].append(row)
    
    # Apply aggregations
    result = []
    agg_specs = parse_agg_specs(agg_spec)
    
    for key, group_rows in groups.items():
        row_result = {group_key: key}
        for spec in agg_specs:
            row_result.update(apply_single_agg(spec, group_rows))
        result.append(row_result)
    
    return result