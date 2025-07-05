"""
Streaming implementations for memory-efficient processing of large JSONL files.

This module provides generator-based versions of core operations that can process
data line-by-line without loading entire datasets into memory.
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
    
    Args:
        file_or_fp: File path, Path object, or file-like object
        
    Yields:
        Dict: JSON object for each line
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
    
    Args:
        row_stream: Generator or iterator of row dictionaries
    """
    for row in row_stream:
        print(json.dumps(row))


def select_stream(relation_stream: RowStream, predicate: Callable[[Row], bool]) -> RowStream:
    """
    Stream-based filtering that processes rows one at a time.
    
    Args:
        relation_stream: Generator/iterator of rows
        predicate: Function that takes a row and returns True if row should be included
        
    Yields:
        Row: Rows that satisfy the predicate
    """
    for row in relation_stream:
        if predicate(row):
            yield row


def project_stream(relation_stream: RowStream, columns: List[str]) -> RowStream:
    """
    Stream-based column projection that processes rows one at a time.
    
    Args:
        relation_stream: Generator/iterator of rows
        columns: List of column names to include
        
    Yields:
        Row: Projected rows with only specified columns
    """
    for row in relation_stream:
        yield {col: row[col] for col in columns if col in row}


def rename_stream(relation_stream: RowStream, renames: Dict[str, str]) -> RowStream:
    """
    Stream-based column renaming that processes rows one at a time.
    
    Args:
        relation_stream: Generator/iterator of rows
        renames: Dictionary mapping old column names to new names
        
    Yields:
        Row: Rows with renamed columns
    """
    for row in relation_stream:
        yield {renames.get(k, k): v for k, v in row.items()}


def union_stream(stream_a: RowStream, stream_b: RowStream) -> RowStream:
    """
    Stream-based union that yields rows from both streams.
    
    Args:
        stream_a: First stream of rows
        stream_b: Second stream of rows
        
    Yields:
        Row: All rows from both streams
    """
    for row in stream_a:
        yield row
    for row in stream_b:
        yield row


def distinct_stream(relation_stream: RowStream) -> RowStream:
    """
    Stream-based distinct that removes duplicates.
    
    Note: This still requires memory to track seen rows, but processes
    input one row at a time and outputs as soon as duplicates are detected.
    
    Args:
        relation_stream: Generator/iterator of rows
        
    Yields:
        Row: Unique rows in order of first appearance
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
    Stream-based JSONPath selection.
    
    Args:
        relation_stream: Generator/iterator of rows
        path: JSONPath expression
        predicate: Optional predicate function
        quantifier: Quantifier for path evaluation ("any", "all", "none")
        
    Yields:
        Row: Rows that match the JSONPath criteria
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
    Stream-based template projection.
    
    Args:
        relation_stream: Generator/iterator of rows
        template: Template dictionary with JSONPath expressions as values
        
    Yields:
        Row: Projected rows based on template
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
    
    Args:
        operation: Name of the operation
        
    Returns:
        bool: True if operation supports streaming
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
    
    Args:
        operation: Name of the operation
        
    Returns:
        bool: True if operation requires full dataset in memory
    """
    memory_ops = {
        'join', 'sort', 'groupby', 'intersection', 'difference'
    }
    return operation in memory_ops


# Chunked processing for memory-intensive operations
def chunked_sort_stream(relation_stream: RowStream, keys: List[str], chunk_size: int = 10000) -> RowStream:
    """
    Sort large datasets using chunked processing to reduce memory usage.
    
    Note: This is a compromise - true streaming sort is impossible,
    but chunked processing can handle larger datasets than full in-memory sort.
    
    Args:
        relation_stream: Generator/iterator of rows
        keys: Column names to sort by
        chunk_size: Number of rows to process at once
        
    Yields:
        Row: Sorted rows (note: only sorted within chunks)
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
    Sort large datasets using windowed processing to reduce memory usage.
    
    Note: This sorts within each window of the specified size. The overall result
    is not globally sorted, but each window is individually sorted.
    
    Args:
        relation_stream: Generator/iterator of rows
        keys: Column names to sort by
        window_size: Number of rows per window
        
    Yields:
        Row: Sorted rows (sorted within each window)
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
    Group by key using windowed processing to reduce memory usage.
    
    Note: This groups within each window of the specified size. Groups may span
    across windows, so this is an approximation of true groupby.
    
    Args:
        relation_stream: Generator/iterator of rows
        key: Group-by key
        aggs: List of aggregation functions
        window_size: Number of rows per window
        
    Yields:
        Row: Grouped and aggregated rows (within each window)
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
    Join streams using windowed processing to reduce memory usage.
    
    Note: This loads the right table fully but processes the left stream in windows.
    This reduces memory usage for the left stream but still requires full right table.
    
    Args:
        left_stream: Generator/iterator of left rows
        right_file: Path to right table file
        join_keys: List of (left_key, right_key) tuples
        window_size: Number of left rows per window
        
    Yields:
        Row: Joined rows
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
    Compute intersection using windowed processing.
    
    Note: This finds intersection within each window against the full right set.
    Results may differ from true intersection depending on data distribution.
    
    Args:
        left_stream: Generator/iterator of left rows
        right_file: Path to right table file
        window_size: Number of left rows per window
        
    Yields:
        Row: Intersected rows
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
    Compute difference using windowed processing.
    
    Note: This finds difference within each window against the full right set.
    
    Args:
        left_stream: Generator/iterator of left rows
        right_file: Path to right table file
        window_size: Number of left rows per window
        
    Yields:
        Row: Difference rows
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
    Check if an operation supports windowed processing.
    
    Args:
        operation: Name of the operation
        
    Returns:
        bool: True if operation supports windowed processing
    """
    windowed_ops = {
        'sort', 'groupby', 'join', 'intersection', 'difference'
    }
    return operation in windowed_ops
