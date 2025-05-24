"""
Provides functionality for grouping and aggregating relations (lists of dictionaries).

This module contains the `groupby_agg` function, which groups rows based on a
specified key and then applies various aggregation functions to other columns
within those groups.

The module is designed to be extensible by adding new aggregation helper functions
and registering them in the `AGGREGATION_DISPATCHER`.

Core Components:
- Helper Aggregation Functions: Private functions (e.g., `_agg_sum_func`)
  that implement specific aggregation logic.
- `AGGREGATION_DISPATCHER`: A dictionary mapping aggregation function names
  (strings) to their corresponding helper functions.
- `groupby_agg`: The main public function that performs the group-by and
  aggregation operations.
"""
from typing import List, Dict, Callable, Tuple, Any
from .core import Row, Relation

# --- Aggregation Helper Functions ---

def _agg_numeric_values(values_to_agg: List[Any]) -> List[float]:
    """
    Converts a list of values to a list of floats, skipping None values.

    Args:
        values_to_agg: A list of values, potentially mixed types or containing None.

    Returns:
        A list of float values.

    Raises:
        ValueError: If a non-None value in `values_to_agg` cannot be
                    converted to a float (e.g., a non-numeric string).
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
    Calculates the sum of numeric values in a list.
    Non-numeric values and Nones are handled by `_agg_numeric_values`.

    Args:
        collected_values: A list of values.

    Returns:
        The sum of the numeric values. Returns 0.0 if no numeric values are found.
    """
    numeric_vals = _agg_numeric_values(collected_values)
    return sum(numeric_vals) # sum of empty list is 0

def _agg_avg_func(collected_values: List[Any]) -> Any: # Can be float or None
    """
    Calculates the average of numeric values in a list.
    Non-numeric values and Nones are handled by `_agg_numeric_values`.

    Args:
        collected_values: A list of values.

    Returns:
        The average of the numeric values, or None if no numeric values are found
        (to avoid division by zero).
    """
    numeric_vals = _agg_numeric_values(collected_values)
    return sum(numeric_vals) / len(numeric_vals) if numeric_vals else None

def _agg_min_func(collected_values: List[Any]) -> Any: # Can be float/int or None
    """
    Finds the minimum of numeric values in a list.
    Non-numeric values and Nones are handled by `_agg_numeric_values`.

    Args:
        collected_values: A list of values.

    Returns:
        The minimum numeric value, or None if no numeric values are found.
    """
    numeric_vals = _agg_numeric_values(collected_values)
    return min(numeric_vals) if numeric_vals else None

def _agg_max_func(collected_values: List[Any]) -> Any: # Can be float/int or None
    """
    Finds the maximum of numeric values in a list.
    Non-numeric values and Nones are handled by `_agg_numeric_values`.

    Args:
        collected_values: A list of values.

    Returns:
        The maximum numeric value, or None if no numeric values are found.
    """
    numeric_vals = _agg_numeric_values(collected_values)
    return max(numeric_vals) if numeric_vals else None

def _agg_list_func(collected_values: List[Any]) -> List[Any]:
    """
    Returns the list of collected values as is.

    Args:
        collected_values: A list of values.

    Returns:
        The input list of values.
    """
    return collected_values

def _agg_first_func(first_value: Any) -> Any:
    """
    Returns the first encountered value for a group.

    Args:
        first_value: The first value collected for the aggregation.

    Returns:
        The `first_value`.
    """
    return first_value

def _agg_last_func(last_value: Any) -> Any:
    """
    Returns the last encountered value for a group.

    Args:
        last_value: The last value collected for the aggregation.

    Returns:
        The `last_value`.
    """
    return last_value

# --- Aggregation Dispatcher ---
# This dictionary maps aggregation function names (strings) to their
# corresponding helper functions. The helper functions are responsible for
# performing the actual aggregation logic on the data collected for each group.
# To add a new aggregation:
# 1. Implement a helper function (e.g., `_agg_new_func(data_to_aggregate, *extra_args)`).
# 2. Add an entry to this dispatcher: `"new_func_name": _agg_new_func`.
# 3. Update the data collection phase in `groupby_agg` if the new function
#    requires a different way of collecting or storing intermediate data.
AGGREGATION_DISPATCHER: Dict[str, Callable[[Any], Any]] = {
    "sum": _agg_sum_func,
    "avg": _agg_avg_func,
    "min": _agg_min_func,
    "max": _agg_max_func,
    "list": _agg_list_func,
    "first": _agg_first_func,
    "last": _agg_last_func,
    # "count" is handled as a special case directly within `groupby_agg`
    # as it doesn't operate on a collected list/value in the same way.
}

def groupby_agg(relation: Relation, group_by_key: str, aggregations: List[Tuple[str, ...]]) -> Relation:
    """
    Groups rows by a key and performs specified aggregations on other columns.

    This function works in two main passes:
    1. Data Collection Pass: Iterates through the input relation, grouping rows
       by the `group_by_key`. For each group, it collects the necessary data
       for each specified aggregation (e.g., a list of values for 'sum', the
       first value for 'first'). It also maintains a count of items in each group.
    2. Aggregation Processing Pass: Iterates through the collected grouped data.
       For each group and each specified aggregation, it retrieves the collected
       data and applies the corresponding aggregation function (obtained via
       `AGGREGATION_DISPATCHER` or special handling for 'count') to compute
       the final aggregated value.

    The resulting relation contains one row per unique value of `group_by_key`,
    with columns for the group key and each specified aggregation.

    Args:
        relation: The input relation (list of rows/dictionaries).
        group_by_key: The column name (key in the dictionaries) to group by.
        aggregations: A list of tuples, where each tuple specifies an aggregation
                      to perform. The structure of the tuple is typically:
                      `(agg_func_name: str, agg_col_name: str, *extra_args: Any)`
                      - `agg_func_name`: The name of the aggregation function
                        (e.g., "sum", "list", "count").
                      - `agg_col_name`: The name of the column on which to perform
                        the aggregation. For "count", this is often ignored or
                        can be an empty string.
                      - `*extra_args`: Optional additional arguments that might be
                        required by specific aggregation functions (e.g., for a
                        future "reduce" function, these could be expression strings).

                      Supported `agg_func_name` values are defined as keys in
                      `AGGREGATION_DISPATCHER`, plus "count".

    Returns:
        A new relation (list of rows) where each row represents a group and
        contains the group_by_key value along with the results of the
        specified aggregations. Aggregation result columns are typically named
        `f"{agg_func_name}_{agg_col_name}"` (e.g., "sum_amount") or just
        `agg_func_name` if `agg_col_name` is empty (common for "count").

    Raises:
        ValueError: If an unsupported `agg_func_name` is provided or if
                    an aggregation function encounters an issue (e.g., trying
                    to sum non-numeric data that isn't None).
    """
    grouped_data: Dict[Any, Dict[str, Any]] = {}

    # Pass 1: Collect data for aggregation
    for row in relation:
        key_value = row.get(group_by_key)
        group = grouped_data.setdefault(key_value, {group_by_key: key_value})
        # _values stores the raw data needed for each aggregation within the group
        group_values = group.setdefault("_values", {}) 
        
        # Always maintain a count for the group
        group_values.setdefault("_count", 0)
        group_values["_count"] += 1

        for agg_spec in aggregations:
            agg_func = agg_spec[0]
            # Ensure agg_col is present, default to empty string if not (e.g. for count)
            agg_col = agg_spec[1] if len(agg_spec) > 1 else "" 

            if agg_func == "count": 
                continue # Count is handled by _count increment above

            val = row.get(agg_col)
            # storage_key_for_agg is used to store the collected data for a specific (agg_func, agg_col) pair
            storage_key_for_agg = f"{agg_func}_{agg_col}" 

            if agg_func in ["sum", "avg", "min", "max", "list"]:
                # These aggregations collect all values from agg_col into a list
                group_values.setdefault(storage_key_for_agg, []).append(val)
            elif agg_func == "first":
                # Store only the first encountered value for this agg_col in the group
                if storage_key_for_agg not in group_values:
                    group_values[storage_key_for_agg] = val
            elif agg_func == "last":
                # Always store/overwrite with the latest value for this agg_col in the group
                group_values[storage_key_for_agg] = val
            elif agg_func not in AGGREGATION_DISPATCHER: # Check for unknown agg functions early
                 raise ValueError(f"Unsupported aggregation function during collection: {agg_func}")
            # Else: If agg_func is in dispatcher but not explicitly handled above,
            # it implies it doesn't need special data collection beyond what other
            # similar functions might do, or it's an error in dispatcher setup.

    # Pass 2: Process collected data to produce final aggregations
    result_relation = []
    for key_value, group_data_content in grouped_data.items():
        processed_row: Row = {group_by_key: key_value}
        collected_group_values = group_data_content.get("_values", {})

        for agg_spec in aggregations:
            agg_func_name = agg_spec[0]
            agg_col_name = agg_spec[1] if len(agg_spec) > 1 else ""
            # extra_args = agg_spec[2:] # For future use, e.g., a general reduce

            output_col_name = f"{agg_func_name}_{agg_col_name}" if agg_col_name else agg_func_name
            
            if agg_func_name == "count":
                processed_row[output_col_name] = collected_group_values.get("_count", 0)
            elif agg_func_name in AGGREGATION_DISPATCHER:
                aggregator_func = AGGREGATION_DISPATCHER[agg_func_name]
                # Key used to retrieve the raw data collected in Pass 1
                raw_data_storage_key = f"{agg_func_name}_{agg_col_name}"
                
                if agg_func_name in ["first", "last"]:
                    # For 'first'/'last', the stored data is the single value itself
                    data_to_aggregate = collected_group_values.get(raw_data_storage_key) # Defaults to None
                else: 
                    # For list-based aggregations ('sum', 'avg', 'min', 'max', 'list')
                    data_to_aggregate = collected_group_values.get(raw_data_storage_key, [])
                
                # If aggregator_func needed extra_args (e.g. for a future 'reduce'),
                # they would be passed here:
                # processed_row[output_col_name] = aggregator_func(data_to_aggregate, *extra_args)
                processed_row[output_col_name] = aggregator_func(data_to_aggregate)
            else:
                # This case should ideally not be reached if the collection phase
                # and dispatcher are correctly set up.
                raise ValueError(f"Unsupported aggregation function during processing: {agg_func_name}")
        
        result_relation.append(processed_row)
        
    return result_relation