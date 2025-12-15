"""Window functions for JSONL data analytics.

This module implements SQL-style window functions for JSONL data processing.
Window functions perform calculations across a set of rows that are related
to the current row, without collapsing rows like aggregations do.

Supported functions:
- row_number(): Sequential row number within partition
- rank(): Rank with gaps for ties
- dense_rank(): Rank without gaps for ties
- lag(field, offset=1, default=None): Value from previous row
- lead(field, offset=1, default=None): Value from next row
- first_value(field): First value in the window
- last_value(field): Last value in the window
- ntile(n): Distribute rows into n buckets
"""

from typing import Any, Dict, List, Optional, Union
from collections import defaultdict

from .expr import ExprEval

Row = Dict[str, Any]
Relation = List[Row]


def _partition_data(
    data: Relation,
    partition_by: Optional[Union[str, List[str]]] = None
) -> Dict[tuple, List[tuple]]:
    """Partition data into groups, preserving original indices.

    Args:
        data: List of dictionaries
        partition_by: Field(s) to partition by, or None for single partition

    Returns:
        Dict mapping partition keys to list of (index, row) tuples
    """
    parser = ExprEval()
    partitions: Dict[tuple, List[tuple]] = defaultdict(list)

    if partition_by is None:
        # Single partition containing all rows
        partitions[()] = [(i, row) for i, row in enumerate(data)]
    else:
        # Parse partition keys
        if isinstance(partition_by, str):
            keys = [k.strip() for k in partition_by.split(",")]
        else:
            keys = partition_by

        for i, row in enumerate(data):
            partition_key = tuple(parser.get_field_value(row, k) for k in keys)
            partitions[partition_key].append((i, row))

    return partitions


def _sort_partition(
    partition: List[tuple],
    order_by: Optional[Union[str, List[str]]] = None,
    descending: bool = False
) -> List[tuple]:
    """Sort a partition by order_by keys.

    Args:
        partition: List of (index, row) tuples
        order_by: Field(s) to order by
        descending: Sort in descending order

    Returns:
        Sorted list of (index, row) tuples
    """
    if order_by is None:
        return partition

    parser = ExprEval()

    if isinstance(order_by, str):
        keys = [k.strip() for k in order_by.split(",")]
    else:
        keys = order_by

    def sort_key(item):
        _, row = item
        values = []
        for k in keys:
            val = parser.get_field_value(row, k)
            # Handle None values (sort first)
            if val is None:
                values.append((True, ""))
            else:
                values.append((False, val))
        return values

    return sorted(partition, key=sort_key, reverse=descending)


def row_number(
    data: Relation,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: str = "_row_number"
) -> Relation:
    """Add row number to each row within partitions.

    Args:
        data: List of dictionaries
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field

    Returns:
        Data with row numbers added

    Example:
        >>> data = [{"dept": "A", "name": "Alice"}, {"dept": "A", "name": "Bob"}]
        >>> row_number(data, partition_by="dept", order_by="name")
        [{"dept": "A", "name": "Alice", "_row_number": 1},
         {"dept": "A", "name": "Bob", "_row_number": 2}]
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)
        for num, (orig_idx, _) in enumerate(sorted_partition, start=1):
            result[orig_idx][output_field] = num

    return result


def rank(
    data: Relation,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: str = "_rank"
) -> Relation:
    """Add rank to each row within partitions (with gaps for ties).

    Rows with equal order_by values get the same rank.
    The next rank after ties skips numbers (e.g., 1, 1, 3 for two ties).

    Args:
        data: List of dictionaries
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field

    Returns:
        Data with ranks added
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)
    parser = ExprEval()

    # Parse order keys
    if order_by is None:
        order_keys = []
    elif isinstance(order_by, str):
        order_keys = [k.strip() for k in order_by.split(",")]
    else:
        order_keys = order_by

    def get_order_value(row):
        return tuple(parser.get_field_value(row, k) for k in order_keys)

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)

        current_rank = 1
        prev_value = None

        for position, (orig_idx, row) in enumerate(sorted_partition, start=1):
            order_value = get_order_value(row)

            if prev_value is not None and order_value != prev_value:
                current_rank = position

            result[orig_idx][output_field] = current_rank
            prev_value = order_value

    return result


def dense_rank(
    data: Relation,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: str = "_dense_rank"
) -> Relation:
    """Add dense rank to each row within partitions (no gaps for ties).

    Rows with equal order_by values get the same rank.
    The next rank after ties continues sequentially (e.g., 1, 1, 2 for two ties).

    Args:
        data: List of dictionaries
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field

    Returns:
        Data with dense ranks added
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)
    parser = ExprEval()

    # Parse order keys
    if order_by is None:
        order_keys = []
    elif isinstance(order_by, str):
        order_keys = [k.strip() for k in order_by.split(",")]
    else:
        order_keys = order_by

    def get_order_value(row):
        return tuple(parser.get_field_value(row, k) for k in order_keys)

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)

        current_rank = 0
        prev_value = None

        for orig_idx, row in sorted_partition:
            order_value = get_order_value(row)

            if prev_value is None or order_value != prev_value:
                current_rank += 1

            result[orig_idx][output_field] = current_rank
            prev_value = order_value

    return result


def lag(
    data: Relation,
    field: str,
    offset: int = 1,
    default: Any = None,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: Optional[str] = None
) -> Relation:
    """Add value from a previous row within the partition.

    Args:
        data: List of dictionaries
        field: Field to get value from
        offset: Number of rows back (default 1)
        default: Value to use when no previous row exists
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field (default: _lag_{field})

    Returns:
        Data with lagged values added
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)
    parser = ExprEval()

    if output_field is None:
        output_field = f"_lag_{field.replace('.', '_')}"

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)

        for i, (orig_idx, _) in enumerate(sorted_partition):
            if i >= offset:
                prev_idx, prev_row = sorted_partition[i - offset]
                value = parser.get_field_value(prev_row, field)
                result[orig_idx][output_field] = value if value is not None else default
            else:
                result[orig_idx][output_field] = default

    return result


def lead(
    data: Relation,
    field: str,
    offset: int = 1,
    default: Any = None,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: Optional[str] = None
) -> Relation:
    """Add value from a following row within the partition.

    Args:
        data: List of dictionaries
        field: Field to get value from
        offset: Number of rows forward (default 1)
        default: Value to use when no following row exists
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field (default: _lead_{field})

    Returns:
        Data with lead values added
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)
    parser = ExprEval()

    if output_field is None:
        output_field = f"_lead_{field.replace('.', '_')}"

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)
        n = len(sorted_partition)

        for i, (orig_idx, _) in enumerate(sorted_partition):
            if i + offset < n:
                next_idx, next_row = sorted_partition[i + offset]
                value = parser.get_field_value(next_row, field)
                result[orig_idx][output_field] = value if value is not None else default
            else:
                result[orig_idx][output_field] = default

    return result


def first_value(
    data: Relation,
    field: str,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: Optional[str] = None
) -> Relation:
    """Add the first value in the partition to each row.

    Args:
        data: List of dictionaries
        field: Field to get first value from
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field (default: _first_{field})

    Returns:
        Data with first values added
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)
    parser = ExprEval()

    if output_field is None:
        output_field = f"_first_{field.replace('.', '_')}"

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)

        if sorted_partition:
            _, first_row = sorted_partition[0]
            first_val = parser.get_field_value(first_row, field)

            for orig_idx, _ in sorted_partition:
                result[orig_idx][output_field] = first_val

    return result


def last_value(
    data: Relation,
    field: str,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: Optional[str] = None
) -> Relation:
    """Add the last value in the partition to each row.

    Args:
        data: List of dictionaries
        field: Field to get last value from
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field (default: _last_{field})

    Returns:
        Data with last values added
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)
    parser = ExprEval()

    if output_field is None:
        output_field = f"_last_{field.replace('.', '_')}"

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)

        if sorted_partition:
            _, last_row = sorted_partition[-1]
            last_val = parser.get_field_value(last_row, field)

            for orig_idx, _ in sorted_partition:
                result[orig_idx][output_field] = last_val

    return result


def ntile(
    data: Relation,
    n: int,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: str = "_ntile"
) -> Relation:
    """Distribute rows into n roughly equal buckets.

    Args:
        data: List of dictionaries
        n: Number of buckets
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field

    Returns:
        Data with bucket numbers (1 to n) added
    """
    if n < 1:
        raise ValueError("n must be at least 1")

    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)
        total = len(sorted_partition)

        for i, (orig_idx, _) in enumerate(sorted_partition):
            # Calculate bucket: floor((i * n) / total) + 1
            bucket = (i * n) // total + 1
            result[orig_idx][output_field] = bucket

    return result


def percent_rank(
    data: Relation,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: str = "_percent_rank"
) -> Relation:
    """Calculate relative rank as percentage (0 to 1).

    Formula: (rank - 1) / (total_rows - 1)

    Args:
        data: List of dictionaries
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field

    Returns:
        Data with percent ranks added
    """
    # First get regular ranks
    ranked = rank(data, partition_by, order_by, "_temp_rank")
    result = [row.copy() for row in ranked]
    partitions = _partition_data(data, partition_by)

    for partition_key, partition in partitions.items():
        total = len(partition)
        if total == 1:
            # Single row gets 0
            for orig_idx, _ in partition:
                result[orig_idx][output_field] = 0.0
        else:
            for orig_idx, _ in partition:
                rank_val = result[orig_idx]["_temp_rank"]
                result[orig_idx][output_field] = (rank_val - 1) / (total - 1)

        # Clean up temp field
        for orig_idx, _ in partition:
            del result[orig_idx]["_temp_rank"]

    return result


def cume_dist(
    data: Relation,
    partition_by: Optional[Union[str, List[str]]] = None,
    order_by: Optional[Union[str, List[str]]] = None,
    output_field: str = "_cume_dist"
) -> Relation:
    """Calculate cumulative distribution (0 to 1).

    Formula: (number of rows <= current row) / total_rows

    Args:
        data: List of dictionaries
        partition_by: Field(s) to partition by
        order_by: Field(s) to order by within partition
        output_field: Name of the output field

    Returns:
        Data with cumulative distribution added
    """
    result = [row.copy() for row in data]
    partitions = _partition_data(data, partition_by)
    parser = ExprEval()

    # Parse order keys
    if order_by is None:
        order_keys = []
    elif isinstance(order_by, str):
        order_keys = [k.strip() for k in order_by.split(",")]
    else:
        order_keys = order_by

    def get_order_value(row):
        return tuple(parser.get_field_value(row, k) for k in order_keys)

    for partition in partitions.values():
        sorted_partition = _sort_partition(partition, order_by)
        total = len(sorted_partition)

        # Count rows with each order value
        value_counts = defaultdict(int)
        for _, row in sorted_partition:
            value_counts[get_order_value(row)] += 1

        # Calculate cumulative counts
        cumulative = {}
        running_count = 0
        prev_value = None

        for _, row in sorted_partition:
            order_value = get_order_value(row)
            if order_value != prev_value:
                running_count += value_counts[order_value]
                cumulative[order_value] = running_count
            prev_value = order_value

        # Apply to result
        for orig_idx, row in sorted_partition:
            order_value = get_order_value(row)
            result[orig_idx][output_field] = cumulative[order_value] / total

    return result
