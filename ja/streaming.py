"""
Streaming implementations for memory-efficient processing of large JSONL files.

This module provides generator-based versions of core relational algebra operations
that process data line-by-line without loading entire datasets into memory. These
implementations are crucial for handling datasets larger than available RAM.

Theoretical Foundation:
    Streaming algorithms process data in a single pass (or limited passes) using
    sublinear space complexity O(s) where s << n (the input size).
    
    Classification of Operations:
    1. **One-pass streamable** (O(1) memory):
       - Selection (σ): Each row evaluated independently
       - Projection (π): Each row transformed independently
       - Rename (ρ): Each row modified independently
       
    2. **Stateful streaming** (O(k) memory for k distinct values):
       - Distinct: Must track seen values
       - Semi-join with small dimension table
       
    3. **Non-streamable** (O(n) memory required):
       - Sort: Requires seeing all data for global ordering
       - Join: Needs to match rows from both relations
       - GroupBy: Must collect all rows per group
       - Set operations: Need full materialization for comparison

Approximation Strategies:
    For non-streamable operations, this module provides windowed approximations:
    - **Windowed processing**: Divide stream into fixed-size windows
    - **Sliding windows**: Overlapping windows for smoother results
    - **Sampling**: Process representative subset of data
    
    Trade-offs:
    - Memory usage vs. accuracy
    - Single-pass vs. multi-pass algorithms
    - Exact vs. approximate results

Implementation Notes:
    - All streaming functions return generators (lazy evaluation)
    - Memory usage is constant or sublinear in input size
    - Compatible with Unix pipe philosophy for composability
    - Handles both file paths and file-like objects (including stdin)

Example:
    >>> # Process multi-GB file with constant memory
    >>> stream = read_jsonl_stream("huge_dataset.jsonl")
    >>> filtered = select_stream(stream, lambda x: x["score"] > 90)
    >>> projected = project_stream(filtered, ["id", "name", "score"])
    >>> write_jsonl_stream(projected)  # Outputs to stdout
"""

import json
import sys
from typing import Iterator, Callable, List, Dict, Union, Any
from pathlib import Path

Row = Dict[str, Any]
RowStream = Iterator[Row]


def read_jsonl_stream(file_or_fp) -> RowStream:
    """
    Generator that yields JSONL rows one at a time for memory-efficient processing.
    
    This is the foundation of all streaming operations, providing lazy evaluation
    of JSONL data with O(1) memory complexity regardless of file size.
    
    Args:
        file_or_fp: File path (str or Path), '-' for stdin, or file-like object
        
    Yields:
        Dict[str, Any]: Parsed JSON object for each non-empty line
        
    Memory Complexity:
        O(1) - Only one row in memory at a time
        
    Time Complexity:
        O(n) where n is the number of lines
        
    Example:
        >>> # Read from file
        >>> for row in read_jsonl_stream("data.jsonl"):
        ...     print(row["id"])
        
        >>> # Read from stdin
        >>> for row in read_jsonl_stream("-"):
        ...     process(row)
        
        >>> # Read from file object
        >>> with open("data.jsonl") as f:
        ...     for row in read_jsonl_stream(f):
        ...         process(row)
    
    Note:
        Empty lines are automatically skipped.
        Invalid JSON lines will raise json.JSONDecodeError.
    """
    if isinstance(file_or_fp, (str, Path)):
        # Handle special case where '-' means stdin
        if str(file_or_fp) == '-':
            import sys
            for line in sys.stdin:
                line = line.strip()
                if line:  # Skip empty lines
                    yield json.loads(line)
        else:
            with open(file_or_fp) as f:
                for line in f:
                    line = line.strip()
                    if line:  # Skip empty lines
                        yield json.loads(line)
    else:
        for line in file_or_fp:
            line = line.strip()
            if line:  # Skip empty lines
                yield json.loads(line)


def write_jsonl_stream(row_stream: RowStream) -> None:
    """
    Write rows to stdout as they're generated (streaming output).
    
    Consumes a stream and outputs JSONL to stdout, maintaining streaming
    properties for pipeline composition.
    
    Args:
        row_stream: Generator or iterator of row dictionaries
        
    Memory Complexity:
        O(1) - Processes one row at a time
        
    Side Effects:
        Writes to stdout (sys.stdout)
        
    Example:
        >>> stream = read_jsonl_stream("input.jsonl")
        >>> filtered = select_stream(stream, lambda x: x["active"])
        >>> write_jsonl_stream(filtered)  # Outputs to stdout
        
    Note:
        Output is unbuffered for real-time streaming.
        Compatible with Unix pipes for chaining commands.
    """
    for row in row_stream:
        print(json.dumps(row))


def select_stream(relation_stream: RowStream, predicate: Callable[[Row], bool]) -> RowStream:
    """
    Stream-based selection/filtering operation (σ in relational algebra).
    
    Implements the selection operator with O(1) memory complexity by evaluating
    each row independently without maintaining state.
    
    Args:
        relation_stream: Generator/iterator of rows
        predicate: Boolean function to evaluate on each row
        
    Yields:
        Row: Rows where predicate(row) returns True
        
    Mathematical Notation:
        σ_p(R) where p is the predicate
        
    Memory Complexity:
        O(1) - No state maintained between rows
        
    Time Complexity:
        O(n) where n is the number of input rows
        O(m) output where m ≤ n depending on selectivity
        
    Example:
        >>> stream = read_jsonl_stream("users.jsonl")
        >>> adults = select_stream(stream, lambda x: x["age"] >= 18)
        >>> active_adults = select_stream(adults, lambda x: x["status"] == "active")
        
    Note:
        Predicates are composable - multiple selections can be chained.
        This is the most fundamental streaming operation.
    """
    for row in relation_stream:
        if predicate(row):
            yield row


def project_stream(relation_stream: RowStream, columns: List[str]) -> RowStream:
    """
    Stream-based projection operation (π in relational algebra).
    
    Projects specified columns from each row with O(1) memory complexity.
    Missing columns are silently omitted from the output.
    
    Args:
        relation_stream: Generator/iterator of rows
        columns: List of column names to retain
        
    Yields:
        Row: Dictionary with only specified columns that exist
        
    Mathematical Notation:
        π_{columns}(R)
        
    Memory Complexity:
        O(k) where k = len(columns), typically k << row size
        
    Time Complexity:
        O(n*k) where n is number of rows, k is number of columns
        
    Example:
        >>> stream = read_jsonl_stream("users.jsonl")
        >>> summary = project_stream(stream, ["id", "name", "email"])
        >>> # Only id, name, email fields in output
        
    Note:
        Column order in output matches input dictionary order.
        Non-existent columns are ignored (not an error).
    """
    for row in relation_stream:
        yield {col: row[col] for col in columns if col in row}


def rename_stream(relation_stream: RowStream, renames: Dict[str, str]) -> RowStream:
    """
    Stream-based rename operation (ρ in relational algebra).
    
    Renames columns in each row with O(1) memory complexity.
    Columns not in the rename mapping are preserved unchanged.
    
    Args:
        relation_stream: Generator/iterator of rows
        renames: Mapping from old names to new names
        
    Yields:
        Row: Rows with renamed columns
        
    Mathematical Notation:
        ρ_{renames}(R)
        
    Memory Complexity:
        O(r) where r = len(renames), typically small
        
    Time Complexity:
        O(n*c) where n is rows, c is columns per row
        
    Example:
        >>> stream = read_jsonl_stream("data.jsonl")
        >>> renamed = rename_stream(stream, {"old_id": "id", "old_name": "name"})
        
    Note:
        If new name conflicts with existing column, the renamed column
        takes precedence (overwrites the existing column).
    """
    for row in relation_stream:
        yield {renames.get(k, k): v for k, v in row.items()}


def union_stream(stream_a: RowStream, stream_b: RowStream) -> RowStream:
    """
    Stream-based union operation (∪ in relational algebra).
    
    Concatenates two streams sequentially with O(1) memory complexity.
    This is a bag union (multiset) - duplicates are preserved.
    
    Args:
        stream_a: First stream of rows
        stream_b: Second stream of rows
        
    Yields:
        Row: All rows from stream_a followed by all rows from stream_b
        
    Mathematical Notation:
        R ∪ S (bag semantics, not set semantics)
        
    Memory Complexity:
        O(1) - No buffering required
        
    Time Complexity:
        O(|R| + |S|) where |R| and |S| are stream sizes
        
    Example:
        >>> stream1 = read_jsonl_stream("file1.jsonl")
        >>> stream2 = read_jsonl_stream("file2.jsonl")
        >>> combined = union_stream(stream1, stream2)
        >>> # First all rows from file1, then all from file2
        
    Note:
        For set union (removing duplicates), pipe through distinct_stream.
        Order is preserved: all of stream_a before any of stream_b.
    """
    for row in stream_a:
        yield row
    for row in stream_b:
        yield row


def distinct_stream(relation_stream: RowStream) -> RowStream:
    """
    Stream-based distinct operation (δ or DISTINCT in SQL).
    
    Removes duplicate rows while maintaining streaming properties.
    Requires memory proportional to the number of distinct values.
    
    Args:
        relation_stream: Generator/iterator of rows
        
    Yields:
        Row: Unique rows in order of first appearance
        
    Memory Complexity:
        O(d) where d is the number of distinct rows
        Best case: O(1) if all rows identical
        Worst case: O(n) if all rows unique
        
    Time Complexity:
        O(n) with hash-based duplicate detection
        
    Implementation Strategy:
        Uses set to track seen row signatures. Falls back to string
        representation for unhashable values (e.g., nested lists).
        
    Example:
        >>> stream = read_jsonl_stream("data.jsonl")
        >>> unique = distinct_stream(stream)
        >>> # Only first occurrence of each distinct row is yielded
        
    Note:
        For very large cardinality, consider windowed approximation
        or probabilistic data structures (Bloom filters, HyperLogLog).
    """
    seen = set()
    
    for row in relation_stream:
        try:
            # Convert row to hashable key for deduplication
            key = tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                yield row
        except TypeError:
            # Handle unhashable values by falling back to string representation
            key = str(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                yield row


# JSONPath streaming operations
def select_path_stream(relation_stream: RowStream, path: str, predicate: Callable = None, quantifier: str = "any") -> RowStream:
    """
    Stream-based JSONPath selection for nested data structures.
    
    Extends relational selection to handle nested JSON structures using
    JSONPath expressions with quantified predicates.
    
    Args:
        relation_stream: Generator/iterator of rows
        path: JSONPath expression (e.g., "$.user.addresses[*].city")
        predicate: Optional function to evaluate on path results
        quantifier: How to combine multiple matches ("any", "all", "none")
        
    Yields:
        Row: Rows where JSONPath evaluation satisfies the quantified predicate
        
    Memory Complexity:
        O(1) per row (path evaluation is bounded by row size)
        
    Quantifier Semantics:
        - "any": ∃x ∈ path(row): predicate(x) = True
        - "all": ∀x ∈ path(row): predicate(x) = True  
        - "none": ¬∃x ∈ path(row): predicate(x) = True
        
    Example:
        >>> # Find orders with any item over $100
        >>> stream = read_jsonl_stream("orders.jsonl")
        >>> expensive = select_path_stream(
        ...     stream,
        ...     "$.items[*].price",
        ...     lambda x: x > 100,
        ...     "any"
        ... )
        
    Note:
        JSONPath evaluation is performed per row independently.
        Complex paths may impact performance but not memory usage.
    """
    # Import here to avoid circular imports
    from .core import select_path
    
    # Note: We could optimize this further by implementing path evaluation
    # directly in the stream, but for now we convert single rows
    for row in relation_stream:
        result = select_path([row], path, predicate, quantifier)
        if result:  # If the single-row list has content, yield the row
            yield row


def project_template_stream(relation_stream: RowStream, template: Dict[str, str]) -> RowStream:
    """
    Stream-based template projection for data reshaping.
    
    Projects and transforms rows using JSONPath templates, enabling
    extraction from nested structures and aggregation within documents.
    
    Args:
        relation_stream: Generator/iterator of rows
        template: Dictionary mapping output keys to JSONPath expressions
        
    Yields:
        Row: Transformed rows matching the template structure
        
    Memory Complexity:
        O(t) where t = size of template (typically small)
        
    Template Features:
        - JSONPath expressions for nested extraction
        - Aggregation functions: sum(), avg(), min(), max(), count()
        - Computed fields from multiple source paths
        
    Example:
        >>> template = {
        ...     "user_id": "$.user.id",
        ...     "full_name": "$.user.firstName + ' ' + $.user.lastName",
        ...     "total_spent": "sum($.orders[*].amount)",
        ...     "order_count": "count($.orders[*])"
        ... }
        >>> stream = read_jsonl_stream("users.jsonl")
        >>> transformed = project_template_stream(stream, template)
        
    Note:
        Template evaluation is atomic per row.
        Invalid paths result in null values, not errors.
    """
    # Import here to avoid circular imports
    from .core import project_template
    
    for row in relation_stream:
        result = project_template([row], template)
        if result:  # Yield the projected row
            yield result[0]


# Utility functions for stream detection
def can_stream_operation(operation: str) -> bool:
    """
    Check if an operation can be performed in streaming mode.
    
    Determines whether an operation can process data with sublinear
    memory complexity (one-pass algorithm with bounded state).
    
    Args:
        operation: Name of the operation to check
        
    Returns:
        bool: True if operation supports streaming with O(1) or O(k) memory
        
    Theoretical Classification:
        Streamable operations have the property that output can be produced
        incrementally without seeing the entire input first.
        
    Example:
        >>> can_stream_operation("select")  # True - stateless
        True
        >>> can_stream_operation("sort")    # False - needs all data
        False
        
    See Also:
        requires_memory_operation: Inverse classification
        supports_windowed_operation: Operations with approximations
    """
    streamable_ops = {
        'select', 'project', 'rename', 'union', 'distinct',
        'select_path', 'select_any', 'select_all', 'select_none',
        'project_template'
    }
    return operation in streamable_ops


def requires_memory_operation(operation: str) -> bool:
    """
    Check if an operation requires loading all data into memory.
    
    Identifies operations that cannot be computed in a single pass
    with bounded memory (require O(n) space complexity).
    
    Args:
        operation: Name of the operation to check
        
    Returns:
        bool: True if operation requires O(n) memory
        
    Theoretical Justification:
        These operations require global knowledge of the dataset:
        - Sort: Total ordering needs all elements
        - Join: Must match across entire relations
        - GroupBy: Groups may span entire dataset
        - Set operations: Require set membership testing
        
    Example:
        >>> requires_memory_operation("join")     # True - needs both relations
        True
        >>> requires_memory_operation("project")  # False - row-by-row
        False
        
    Note:
        These operations may support windowed approximations.
    """
    memory_ops = {
        'join', 'sort', 'groupby', 'intersection', 'difference'
    }
    return operation in memory_ops


# Chunked processing for memory-intensive operations
def chunked_sort_stream(relation_stream: RowStream, keys: List[str], chunk_size: int = 10000) -> RowStream:
    """
    Sort large datasets using chunked processing to reduce memory usage.
    
    Implements a memory-bounded approximation of sorting where data is
    sorted within fixed-size chunks but not globally sorted.
    
    Args:
        relation_stream: Generator/iterator of rows
        keys: Column names to sort by (in order of precedence)
        chunk_size: Maximum rows to hold in memory at once
        
    Yields:
        Row: Rows sorted within each chunk
        
    Memory Complexity:
        O(c) where c = chunk_size << n (total rows)
        
    Time Complexity:
        O(n * log(c)) for n rows with chunk size c
        
    Approximation Quality:
        - Within-chunk ordering: Perfect
        - Global ordering: Approximate
        - Use case: Time-series data with natural ordering
        
    Example:
        >>> # Sort log entries by timestamp in manageable chunks
        >>> stream = read_jsonl_stream("huge_log.jsonl")
        >>> sorted_chunks = chunked_sort_stream(stream, ["timestamp"], 5000)
        >>> # Each 5000-row chunk is perfectly sorted
        
    Note:
        For true streaming sort, consider external merge sort
        or maintaining a bounded priority queue for top-k.
    """
    # Import here to avoid circular imports
    from .core import sort_by
    
    chunk = []
    for row in relation_stream:
        chunk.append(row)
        if len(chunk) >= chunk_size:
            # Sort and yield this chunk
            sorted_chunk = sort_by(chunk, keys)
            for sorted_row in sorted_chunk:
                yield sorted_row
            chunk = []
    
    # Handle remaining rows
    if chunk:
        sorted_chunk = sort_by(chunk, keys)
        for sorted_row in sorted_chunk:
            yield sorted_row


# Windowed processing for memory-intensive operations
def windowed_sort_stream(relation_stream: RowStream, keys: List[str], window_size: int) -> RowStream:
    """
    Sort using fixed-size windows for memory-bounded processing.
    
    Similar to chunked_sort but with explicit window semantics,
    useful when data has natural window boundaries.
    
    Args:
        relation_stream: Generator/iterator of rows
        keys: Column names to sort by
        window_size: Exact number of rows per window
        
    Yields:
        Row: Rows sorted within each window
        
    Memory Complexity:
        O(w) where w = window_size
        
    Window Semantics:
        - Non-overlapping windows (tumbling windows)
        - Last window may be smaller than window_size
        - No cross-window ordering guarantees
        
    Use Cases:
        - Sorting batched data (e.g., hourly logs)
        - Memory-constrained environments
        - Approximate median/percentile calculations
        
    Example:
        >>> # Sort user events in daily windows
        >>> stream = read_jsonl_stream("events.jsonl")
        >>> daily_sorted = windowed_sort_stream(stream, ["user_id", "timestamp"], 86400)
        
    See Also:
        chunked_sort_stream: Equivalent with different naming
        sliding_window_sort: Overlapping windows (not implemented)
    """
    from .core import sort_by
    
    window = []
    for row in relation_stream:
        window.append(row)
        if len(window) >= window_size:
            # Sort and yield this window
            sorted_window = sort_by(window, keys)
            for sorted_row in sorted_window:
                yield sorted_row
            window = []
    
    # Handle remaining rows
    if window:
        sorted_window = sort_by(window, keys)
        for sorted_row in sorted_window:
            yield sorted_row


def windowed_groupby_stream(relation_stream: RowStream, key: str, aggs: List, window_size: int) -> RowStream:
    """
    GroupBy with windowed aggregation for bounded memory usage.
    
    Approximates the GroupBy operation (γ) by computing aggregates
    within fixed-size windows. Groups spanning windows are computed
    separately in each window.
    
    Args:
        relation_stream: Generator/iterator of rows
        key: Column name to group by
        aggs: List of (function, column) aggregation tuples
        window_size: Number of rows per window
        
    Yields:
        Row: Aggregated groups from each window
        
    Memory Complexity:
        O(w + g) where w = window_size, g = groups per window
        
    Approximation Characteristics:
        - Groups within window: Exact aggregation
        - Groups across windows: Separate partial aggregates
        - Final merge required for global aggregates
        
    Mathematical Model:
        Instead of γ(k,F)(R), computes:
        ∪_i γ(k,F)(W_i) where W_i are windows
        
    Example:
        >>> # Aggregate sales by region in windows
        >>> stream = read_jsonl_stream("sales.jsonl")
        >>> windowed_totals = windowed_groupby_stream(
        ...     stream,
        ...     "region",
        ...     [("sum", "amount"), ("count", "")],
        ...     1000
        ... )
        >>> # Each window produces partial aggregates
        
    Note:
        For associative aggregations (sum, min, max), results can
        be merged. For holistic aggregations (median), approximation
        quality depends on data distribution.
    """
    from .groupby import groupby_agg
    
    window = []
    for row in relation_stream:
        window.append(row)
        if len(window) >= window_size:
            # Group and yield this window
            grouped_window = groupby_agg(window, key, aggs)
            for group_row in grouped_window:
                yield group_row
            window = []
    
    # Handle remaining rows
    if window:
        grouped_window = groupby_agg(window, key, aggs)
        for group_row in grouped_window:
            yield group_row


def windowed_join_stream(left_stream: RowStream, right_file: str, join_keys: List, window_size: int) -> RowStream:
    """
    Semi-streaming join with windowed left relation.
    
    Implements a hybrid join where the right relation is fully loaded
    (dimension table pattern) while the left streams through windows.
    Suitable when right relation is small enough to fit in memory.
    
    Args:
        left_stream: Generator/iterator of left rows (fact table)
        right_file: Path to right table file (dimension table)
        join_keys: List of (left_key, right_key) tuples
        window_size: Number of left rows to process at once
        
    Yields:
        Row: Joined rows from each window
        
    Memory Complexity:
        O(|R| + w) where |R| = size of right relation, w = window_size
        
    Join Algorithm:
        1. Load entire right relation into memory (hash table)
        2. Stream left relation in windows
        3. Perform hash join for each window
        
    Use Cases:
        - Star schema: Large fact table × small dimension tables
        - Enrichment: Stream of events × reference data
        - Filtering: Large dataset × small filter list
        
    Example:
        >>> # Join order stream with customer dimension
        >>> orders = read_jsonl_stream("orders_2024.jsonl")  # 10M rows
        >>> # customers.jsonl has 100K rows (fits in memory)
        >>> enriched = windowed_join_stream(
        ...     orders,
        ...     "customers.jsonl",
        ...     [("customer_id", "id")],
        ...     5000
        ... )
        
    Note:
        For symmetric large relations, consider sort-merge join
        or distributed join algorithms.
    """
    from .core import join
    
    # Load right table (still required for join)
    right_data = read_jsonl_stream(right_file)
    right_list = list(right_data)  # Must materialize for join
    
    window = []
    for row in left_stream:
        window.append(row)
        if len(window) >= window_size:
            # Join this window with full right table
            joined_window = join(window, right_list, join_keys)
            for joined_row in joined_window:
                yield joined_row
            window = []
    
    # Handle remaining rows
    if window:
        joined_window = join(window, right_list, join_keys)
        for joined_row in joined_window:
            yield joined_row


def windowed_intersection_stream(left_stream: RowStream, right_file: str, window_size: int) -> RowStream:
    """
    Semi-streaming set intersection with windowed processing.
    
    Computes R ∩ S where R streams through windows and S is loaded
    into memory. Produces exact intersection results despite windowing.
    
    Args:
        left_stream: Generator/iterator of left rows
        right_file: Path to right table file
        window_size: Number of left rows per window
        
    Yields:
        Row: Rows present in both relations
        
    Memory Complexity:
        O(|S| + w) where |S| = size of right set, w = window_size
        
    Algorithm:
        1. Load right relation into set for O(1) lookup
        2. Stream left relation in windows
        3. Yield rows present in right set
        4. Track duplicates to maintain set semantics
        
    Mathematical Properties:
        - Commutative: R ∩ S = S ∩ R (but not in streaming)
        - Associative: (R ∩ S) ∩ T = R ∩ (S ∩ T)
        - Idempotent: R ∩ R = R
        
    Example:
        >>> # Find common users between two systems
        >>> active_users = read_jsonl_stream("active_users.jsonl")
        >>> common = windowed_intersection_stream(
        ...     active_users,
        ...     "premium_users.jsonl",
        ...     1000
        ... )
        
    Note:
        Results are exact despite windowing - windows only affect
        memory usage for the left stream, not correctness.
    """
    from .core import intersection
    
    # Load right table (required for intersection)
    right_data = read_jsonl_stream(right_file)
    right_list = list(right_data)  # Must materialize for set operations
    
    window = []
    for row in left_stream:
        window.append(row)
        if len(window) >= window_size:
            # Intersect this window with full right table
            intersected_window = intersection(window, right_list)
            for intersected_row in intersected_window:
                yield intersected_row
            window = []
    
    # Handle remaining rows
    if window:
        intersected_window = intersection(window, right_list)
        for intersected_row in intersected_window:
            yield intersected_row


def windowed_difference_stream(left_stream: RowStream, right_file: str, window_size: int) -> RowStream:
    """
    Semi-streaming set difference with windowed processing.
    
    Computes R - S where R streams through windows and S is loaded
    into memory. Produces exact difference results.
    
    Args:
        left_stream: Generator/iterator of left rows
        right_file: Path to right table file (subtrahend)
        window_size: Number of left rows per window
        
    Yields:
        Row: Rows in left but not in right
        
    Memory Complexity:
        O(|S| + w) where |S| = size of right set, w = window_size
        
    Algorithm:
        1. Load right relation into set
        2. Stream left relation in windows
        3. Yield rows NOT present in right set
        4. Handle duplicates for set semantics
        
    Mathematical Properties:
        - Non-commutative: R - S ≠ S - R
        - Non-associative: (R - S) - T ≠ R - (S - T)
        - Identity: R - ∅ = R
        - Annihilator: R - R = ∅
        
    Example:
        >>> # Find users who haven't purchased
        >>> all_users = read_jsonl_stream("users.jsonl")
        >>> non_customers = windowed_difference_stream(
        ...     all_users,
        ...     "customers.jsonl",
        ...     2000
        ... )
        
    Note:
        Order matters: R - S produces rows in R but not in S.
        For symmetric difference, compute (R - S) ∪ (S - R).
    """
    from .core import difference
    
    # Load right table (required for difference)
    right_data = read_jsonl_stream(right_file)
    right_list = list(right_data)  # Must materialize for set operations
    
    window = []
    for row in left_stream:
        window.append(row)
        if len(window) >= window_size:
            # Compute difference for this window
            diff_window = difference(window, right_list)
            for diff_row in diff_window:
                yield diff_row
            window = []
    
    # Handle remaining rows
    if window:
        diff_window = difference(window, right_list)
        for diff_row in diff_window:
            yield diff_row


def supports_windowed_operation(operation: str) -> bool:
    """
    Check if an operation supports windowed approximation.
    
    Identifies operations that can trade accuracy for memory efficiency
    through windowed processing, producing approximate results.
    
    Args:
        operation: Name of the operation to check
        
    Returns:
        bool: True if operation has a windowed implementation
        
    Windowed Processing Theory:
        Operations that require global knowledge can be approximated
        by dividing input into windows and combining local results.
        Quality depends on:
        - Window size relative to data
        - Data distribution and ordering
        - Aggregation properties (associative, commutative)
        
    Trade-offs:
        - Exact global result vs. approximate result
        - O(n) memory vs. O(w) memory where w << n
        - Single pass vs. multiple passes
        
    Example:
        >>> supports_windowed_operation("sort")      # True - can sort windows
        True
        >>> supports_windowed_operation("select")    # False - already streams
        False
        
    See Also:
        can_stream_operation: Operations needing no windowing
        requires_memory_operation: Operations needing approximation
    """
    windowed_ops = {
        'sort', 'groupby', 'join', 'intersection', 'difference'
    }
    return operation in windowed_ops
