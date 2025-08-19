"""
Command handlers for the jsonl-algebra CLI.

This module provides the implementation layer between the CLI argument parser
and the core relational algebra operations. It handles:

1. **Mode Selection**: Choosing between streaming, windowed, or in-memory processing
2. **Data I/O**: Reading from files or stdin, writing to stdout
3. **Warning Management**: Informing users about performance implications
4. **Error Handling**: Graceful degradation and informative error messages

Architecture:
    The module follows a command pattern where each CLI command has a corresponding
    handler function. Handlers are responsible for:
    - Parsing command-specific arguments
    - Selecting appropriate processing mode (streaming vs memory)
    - Invoking core operations
    - Managing I/O streams

Processing Modes:
    1. **Streaming Mode**: Process data row-by-row with O(1) memory
       - Used for: select, project, rename, union, distinct
       - Automatic when operation supports it
       
    2. **Windowed Mode**: Process data in fixed-size chunks
       - Used for: sort, groupby, join (with --window-size)
       - Trades accuracy for memory efficiency
       
    3. **In-Memory Mode**: Load entire dataset into memory
       - Used for: operations requiring global view
       - Default for non-streamable operations

Design Principles:
    - **Automatic Optimization**: Choose best mode based on operation
    - **User Control**: Allow override with --window-size parameter
    - **Transparency**: Warn about performance implications
    - **Composability**: Support Unix pipe philosophy

Example Flow:
    ```
    CLI Args -> Handler -> Mode Selection -> Operation -> Output
                   |            |               |
                   v            v               v
              Parse Args   Check Stream    Execute Op
                          Capability      (stream/memory)
    ```
"""

import sys
import json
import warnings
from pathlib import Path
from .core import *
from .groupby import groupby_agg
from .streaming import (
    read_jsonl_stream,
    write_jsonl_stream,
    select_stream,
    project_stream,
    rename_stream,
    union_stream,
    distinct_stream,
    can_stream_operation,
    requires_memory_operation,
    supports_windowed_operation,
    windowed_sort_stream,
    windowed_groupby_stream,
    windowed_join_stream,
    windowed_intersection_stream,
    windowed_difference_stream,
)


def _warn_streaming_not_supported(operation_name):
    """
    Warn user that streaming is not supported for this operation.
    
    Provides actionable advice for handling large datasets when
    streaming is not available.
    
    Args:
        operation_name: Name of the operation that doesn't support streaming
        
    Suggested Alternatives:
        1. Use --window-size for approximate results
        2. Pre-filter data with streaming operations
        3. Increase available memory
    """
    warnings.warn(
        f"Operation '{operation_name}' requires loading data into memory for correct results. "
        f"For large datasets, consider: (1) using --window-size N for approximate results, "
        f"or (2) filtering data first with: ja select 'condition' --stream | ja {operation_name}",
        UserWarning,
        stacklevel=3
    )


def _warn_memory_intensive(operation_name):
    """
    Warn user about memory-intensive operations.
    
    Alerts users when an operation requires O(n) memory,
    suggesting alternatives for large datasets.
    
    Args:
        operation_name: Name of the memory-intensive operation
        
    Warning Conditions:
        - Operation requires full dataset in memory
        - No streaming alternative available
        - Window size not specified
    """
    if requires_memory_operation(operation_name):
        warnings.warn(
            f"Operation '{operation_name}' requires loading data into memory for optimal results. "
            f"For large datasets, consider using --window-size N for memory-efficient approximate processing, "
            f"or use streaming operations (select, project, rename) to reduce data size first.",
            UserWarning,
            stacklevel=3
        )


def _warn_windowed_approximation(operation_name):
    """
    Warn user that windowed processing provides approximate results.
    
    Informs users about the trade-off between memory usage and accuracy
    when using windowed processing.
    
    Args:
        operation_name: Name of the operation being windowed
        
    Approximation Impact:
        - Sort: Only sorted within windows
        - GroupBy: Groups may be split across windows
        - Join: May miss matches across window boundaries
    """
    warnings.warn(
        f"Windowed processing for '{operation_name}' provides approximate results. "
        f"The operation is applied within each window independently, which may differ "
        f"from applying the operation to the entire dataset.",
        UserWarning,
        stacklevel=3
    )


# Helper functions moved from cli.py
def read_jsonl(file_or_fp):
    """
    Read JSONL data into memory from file or file-like object.
    
    Legacy function for in-memory processing. Loads entire dataset
    at once.
    
    Args:
        file_or_fp: File path, Path object, or file-like object
        
    Returns:
        List of dictionaries representing all rows
        
    Memory Complexity:
        O(n) where n is the total data size
        
    Note:
        For large files, consider using read_jsonl_stream instead.
    """
    if isinstance(file_or_fp, str) or isinstance(file_or_fp, Path):
        with open(file_or_fp) as f:
            return [json.loads(line) for line in f]
    else:
        return [json.loads(line) for line in file_or_fp]


def write_jsonl(rows):
    """
    Write rows to stdout as JSONL.
    
    Legacy function for in-memory data output.
    
    Args:
        rows: List of dictionaries to output
        
    Side Effects:
        Writes to stdout
        
    Note:
        For streaming output, use write_jsonl_stream instead.
    """
    for row in rows:
        print(json.dumps(row))


def _should_stream(args, operation_name):
    """
    Determine if streaming mode should be used.
    
    Makes intelligent decision about processing mode based on
    operation capabilities.
    
    Args:
        args: Parsed command-line arguments
        operation_name: Name of the operation to perform
        
    Returns:
        bool: True if streaming should be used
        
    Decision Logic:
        - Always stream if operation supports it (O(1) memory)
        - Never stream if operation requires global view
        - User cannot override (automatic optimization)
    """
    # Always stream when the operation supports it
    return can_stream_operation(operation_name)


def _should_use_windowed(args, operation_name):
    """
    Determine if windowed processing should be used.
    
    Checks if user requested windowed mode and if operation supports it.
    
    Args:
        args: Parsed command-line arguments
        operation_name: Name of the operation to perform
        
    Returns:
        tuple: (should_use_windowed: bool, window_size: int or None)
        
    Windowing Conditions:
        1. User specified --window-size
        2. Operation supports windowed processing
        3. Window size is positive integer
        
    Side Effects:
        Issues warning about approximation if windowing enabled
    """
    # Convert argparse attribute name (window-size becomes window_size)
    window_size = getattr(args, 'window_size', None)
    if window_size is not None and isinstance(window_size, int) and window_size > 0:
        if supports_windowed_operation(operation_name):
            _warn_windowed_approximation(operation_name)
            return True, window_size
        else:
            warnings.warn(
                f"Windowed processing is not supported for '{operation_name}' operation.",
                UserWarning,
                stacklevel=3
            )
    return False, None


# Command handlers
def handle_select(args):
    """
    Handle the select command (σ operator).
    
    Filters rows based on a Python expression predicate.
    Automatically uses streaming mode for memory efficiency.
    
    Args:
        args: Namespace with attributes:
            - expr: Python expression to evaluate
            - file: Input file path or None for stdin
            
    Expression Context:
        The expression is evaluated with each row as local variables.
        Example: "age > 25 and status == 'active'"
        
    Streaming:
        Always uses streaming mode (O(1) memory) since selection
        is inherently streamable.
    """
    try:
        predicate = lambda row: eval(args.expr, {}, row)
        
        if _should_stream(args, 'select'):
            # Streaming mode
            data_stream = read_jsonl_stream(args.file or sys.stdin)
            result_stream = select_stream(data_stream, predicate)
            write_jsonl_stream(result_stream)
        else:
            # Memory mode (backward compatibility)
            data = read_jsonl(args.file or sys.stdin)
            write_jsonl(select(data, predicate))
    except Exception as e:
        print(f"Invalid expression: {e}", file=sys.stderr)
        sys.exit(1)


def handle_project(args):
    """
    Handle the project command (π operator).
    
    Projects specified columns from each row.
    Automatically uses streaming mode.
    
    Args:
        args: Namespace with attributes:
            - columns: Comma-separated column names
            - file: Input file path or None for stdin
            
    Column Handling:
        - Non-existent columns are silently ignored
        - Empty result if no columns match
    """
    cols = args.columns.split(",")
    
    if _should_stream(args, 'project'):
        # Streaming mode
        data_stream = read_jsonl_stream(args.file or sys.stdin)
        result_stream = project_stream(data_stream, cols)
        write_jsonl_stream(result_stream)
    else:
        # Memory mode (backward compatibility)
        data = read_jsonl(args.file or sys.stdin)
        write_jsonl(project(data, cols))


def handle_join(args):
    """
    Handle the join command (⋈ operator).
    
    Performs inner join on specified columns.
    Supports windowed mode for memory efficiency.
    
    Args:
        args: Namespace with attributes:
            - left: Left relation file or "-" for stdin
            - right: Right relation file
            - on: Join condition "left_col=right_col"
            - window_size: Optional window size for approximation
            
    Processing Modes:
        - Normal: Loads both relations into memory O(|L| + |R|)
        - Windowed: Processes left in windows, right in memory O(w + |R|)
    """
    _should_stream(args, 'join')  # This will warn if streaming is requested
    
    # Check for windowed processing
    use_windowed, window_size = _should_use_windowed(args, 'join')
    
    lcol, rcol = args.on.split("=")
    
    if use_windowed:
        # Windowed processing - memory efficient but approximate
        left_input = sys.stdin if args.left == "-" else args.left
        left_stream = read_jsonl_stream(left_input or sys.stdin)
        result_stream = windowed_join_stream(left_stream, args.right, [(lcol, rcol)], window_size)
        write_jsonl_stream(result_stream)
    else:
        # Traditional processing - accurate but memory intensive
        _warn_memory_intensive('join')
        left_input = sys.stdin if args.left == "-" else args.left
        left = read_jsonl(
            left_input or sys.stdin
        )  # Default to stdin if left is None (not specified)
        right = read_jsonl(args.right)
        write_jsonl(join(left, right, [(lcol, rcol)]))


def handle_product(args):
    """
    Handle the product command (× operator).
    
    Computes Cartesian product of two relations.
    Memory-intensive operation.
    
    Args:
        args: Namespace with attributes:
            - left: Left relation file
            - right: Right relation file
            
    Warning:
        Result size is |L| × |R|, can be very large.
        No streaming or windowed mode available.
    """
    a = read_jsonl(args.left)
    b = read_jsonl(args.right)
    write_jsonl(product(a, b))


def handle_rename(args):
    """
    Handle the rename command (ρ operator).
    
    Renames columns according to mapping.
    Automatically uses streaming mode.
    
    Args:
        args: Namespace with attributes:
            - mapping: Rename spec "old1=new1,old2=new2"
            - file: Input file path or None for stdin
    """
    mapping = dict(pair.split("=") for pair in args.mapping.split(","))
    
    if _should_stream(args, 'rename'):
        # Streaming mode
        data_stream = read_jsonl_stream(args.file or sys.stdin)
        result_stream = rename_stream(data_stream, mapping)
        write_jsonl_stream(result_stream)
    else:
        # Memory mode (backward compatibility)
        data = read_jsonl(args.file or sys.stdin)
        write_jsonl(rename(data, mapping))


def handle_union(args):
    """
    Handle the union command (∪ operator).
    
    Concatenates two relations (bag union).
    Automatically uses streaming mode.
    
    Args:
        args: Namespace with attributes:
            - left: First relation file
            - right: Second relation file
            
    Note:
        This is bag union (preserves duplicates).
        For set union, pipe through distinct.
    """
    if _should_stream(args, 'union'):
        # Streaming mode
        left_stream = read_jsonl_stream(args.left)
        right_stream = read_jsonl_stream(args.right)
        result_stream = union_stream(left_stream, right_stream)
        write_jsonl_stream(result_stream)
    else:
        # Memory mode (backward compatibility)
        a = read_jsonl(args.left)
        b = read_jsonl(args.right)
        write_jsonl(union(a, b))


def handle_intersection(args):
    """
    Handle the intersection command (∩ operator).
    
    Finds common rows between two relations.
    Supports windowed mode for large datasets.
    
    Args:
        args: Namespace with attributes:
            - left: First relation file
            - right: Second relation file
            - window_size: Optional window size
    """
    # Check for windowed processing
    use_windowed, window_size = _should_use_windowed(args, 'intersection')
    
    if use_windowed:
        # Windowed processing - memory efficient but approximate
        left_stream = read_jsonl_stream(args.left)
        result_stream = windowed_intersection_stream(left_stream, args.right, window_size)
        write_jsonl_stream(result_stream)
    else:
        # Traditional processing - accurate but memory intensive
        _warn_memory_intensive('intersection')
        a = read_jsonl(args.left)
        b = read_jsonl(args.right)
        write_jsonl(intersection(a, b))


def handle_difference(args):
    """
    Handle the difference command (- operator).
    
    Finds rows in left but not in right.
    Supports windowed mode for large datasets.
    
    Args:
        args: Namespace with attributes:
            - left: First relation file
            - right: Second relation file  
            - window_size: Optional window size
    """
    # Check for windowed processing
    use_windowed, window_size = _should_use_windowed(args, 'difference')
    
    if use_windowed:
        # Windowed processing - memory efficient but approximate
        left_stream = read_jsonl_stream(args.left)
        result_stream = windowed_difference_stream(left_stream, args.right, window_size)
        write_jsonl_stream(result_stream)
    else:
        # Traditional processing - accurate but memory intensive
        _warn_memory_intensive('difference')
        a = read_jsonl(args.left)
        b = read_jsonl(args.right)
        write_jsonl(difference(a, b))


def handle_distinct(args):
    """
    Handle the distinct command (δ operator).
    
    Removes duplicate rows.
    Uses streaming with O(d) memory for d distinct values.
    
    Args:
        args: Namespace with attributes:
            - file: Input file path or None for stdin
    """
    if _should_stream(args, 'distinct'):
        # Streaming mode (still uses memory for seen items, but processes input stream)
        data_stream = read_jsonl_stream(args.file or sys.stdin)
        result_stream = distinct_stream(data_stream)
        write_jsonl_stream(result_stream)
    else:
        # Memory mode (backward compatibility)
        data = read_jsonl(args.file or sys.stdin)
        write_jsonl(distinct(data))


def handle_sort(args):
    """
    Handle the sort command.
    
    Sorts rows by specified columns.
    Supports windowed mode for approximate sorting.
    
    Args:
        args: Namespace with attributes:
            - columns: Comma-separated sort keys
            - file: Input file path or None for stdin
            - window_size: Optional window size
            
    Windowed Behavior:
        Sorts within each window independently.
        Not globally sorted but locally sorted.
    """
    _should_stream(args, 'sort')  # This will warn if streaming is requested
    
    # Check for windowed processing
    use_windowed, window_size = _should_use_windowed(args, 'sort')
    
    if use_windowed:
        # Windowed processing - memory efficient but approximate
        data_stream = read_jsonl_stream(args.file or sys.stdin)
        keys = args.columns.split(",")
        result_stream = windowed_sort_stream(data_stream, keys, window_size)
        write_jsonl_stream(result_stream)
    else:
        # Traditional processing - accurate but memory intensive
        _warn_memory_intensive('sort')
        data = read_jsonl(args.file or sys.stdin)
        keys = args.columns.split(",")
        write_jsonl(sort_by(data, keys))


def handle_groupby(args):
    """
    Handle the groupby command (γ operator).
    
    Groups rows and applies aggregations.
    Supports windowed mode for memory efficiency.
    
    Args:
        args: Namespace with attributes:
            - key: Grouping column
            - agg: Aggregations "func1:col1,func2:col2"
            - file: Input file path or None for stdin
            - window_size: Optional window size
            
    Aggregation Functions:
        sum, avg, min, max, count, list, first, last
    """
    _should_stream(args, 'groupby')  # This will warn if streaming is requested
    
    # Check for windowed processing
    use_windowed, window_size = _should_use_windowed(args, 'groupby')
    
    aggs = []
    for part in args.agg.split(","):
        if ":" in part:
            func, field = part.split(":", 1)
        else:
            func, field = part, ""  # Default field to empty string if not provided
        aggs.append((func, field))
    
    if use_windowed:
        # Windowed processing - memory efficient but approximate
        data_stream = read_jsonl_stream(args.file or sys.stdin)
        result_stream = windowed_groupby_stream(data_stream, args.key, aggs, window_size)
        write_jsonl_stream(result_stream)
    else:
        # Traditional processing - accurate but memory intensive
        _warn_memory_intensive('groupby')
        data = read_jsonl(args.file or sys.stdin)
        write_jsonl(groupby_agg(data, args.key, aggs))


# ==========================================
# JSONPath Command Handlers
# ==========================================


def _parse_predicate(predicate_str):
    """
    Parse a predicate string into a callable function.
    
    Converts string predicates to Python functions for JSONPath operations.
    
    Args:
        predicate_str: String representation of predicate
        
    Returns:
        Callable that takes a value and returns bool
        
    Supported Formats:
        - Lambda: "lambda x: x > 10"
        - Expression: "x > 10" (auto-wrapped in lambda)
        - None/empty: Returns None (check existence)
        
    Error Handling:
        Exits with error message if predicate is invalid.
    """
    if not predicate_str:
        return None

    try:
        # Support both lambda syntax and simple expressions
        if predicate_str.startswith("lambda"):
            return eval(predicate_str)
        else:
            # Convert simple expressions to lambda
            return eval(f"lambda x: {predicate_str}")
    except Exception as e:
        print(f"Invalid predicate: {e}", file=sys.stderr)
        sys.exit(1)


def handle_select_path(args):
    """
    Handle select-path command with JSONPath.
    
    Filters rows based on JSONPath expressions with quantified predicates.
    
    Args:
        args: Namespace with attributes:
            - path: JSONPath expression
            - predicate: Optional predicate string
            - quantifier: "any", "all", or "none"
            - file: Input file path or None for stdin
    """
    predicate = _parse_predicate(args.predicate)

    try:
        if _should_stream(args, 'select_path'):
            # Streaming mode - import streaming version
            from .streaming import select_path_stream
            data_stream = read_jsonl_stream(args.file or sys.stdin)
            result_stream = select_path_stream(data_stream, args.path, predicate, args.quantifier)
            write_jsonl_stream(result_stream)
        else:
            # Memory mode (backward compatibility)
            data = read_jsonl(args.file or sys.stdin)
            result = select_path(data, args.path, predicate, args.quantifier)
            write_jsonl(result)
    except Exception as e:
        print(f"JSONPath error: {e}", file=sys.stderr)
        sys.exit(1)


def handle_select_any(args):
    """
    Handle select-any command (existential JSONPath).
    
    Filters rows where any path value matches predicate.
    
    Args:
        args: Namespace with attributes:
            - path: JSONPath expression
            - predicate: Optional predicate string
            - file: Input file path or None for stdin
    """
    predicate = _parse_predicate(args.predicate)

    try:
        if _should_stream(args, 'select_any'):
            # Streaming mode
            from .streaming import select_path_stream
            data_stream = read_jsonl_stream(args.file or sys.stdin)
            result_stream = select_path_stream(data_stream, args.path, predicate, "any")
            write_jsonl_stream(result_stream)
        else:
            # Memory mode (backward compatibility)
            data = read_jsonl(args.file or sys.stdin)
            result = select_any(data, args.path, predicate)
            write_jsonl(result)
    except Exception as e:
        print(f"JSONPath error: {e}", file=sys.stderr)
        sys.exit(1)


def handle_select_all(args):
    """
    Handle select-all command (universal JSONPath).
    
    Filters rows where all path values match predicate.
    
    Args:
        args: Namespace with attributes:
            - path: JSONPath expression
            - predicate: Optional predicate string
            - file: Input file path or None for stdin
    """
    predicate = _parse_predicate(args.predicate)

    try:
        if _should_stream(args, 'select_all'):
            # Streaming mode
            from .streaming import select_path_stream
            data_stream = read_jsonl_stream(args.file or sys.stdin)
            result_stream = select_path_stream(data_stream, args.path, predicate, "all")
            write_jsonl_stream(result_stream)
        else:
            # Memory mode (backward compatibility)
            data = read_jsonl(args.file or sys.stdin)
            result = select_all(data, args.path, predicate)
            write_jsonl(result)
    except Exception as e:
        print(f"JSONPath error: {e}", file=sys.stderr)
        sys.exit(1)


def handle_select_none(args):
    """
    Handle select-none command (negated JSONPath).
    
    Filters rows where no path values match predicate.
    
    Args:
        args: Namespace with attributes:
            - path: JSONPath expression
            - predicate: Optional predicate string  
            - file: Input file path or None for stdin
    """
    predicate = _parse_predicate(args.predicate)

    try:
        if _should_stream(args, 'select_none'):
            # Streaming mode
            from .streaming import select_path_stream
            data_stream = read_jsonl_stream(args.file or sys.stdin)
            result_stream = select_path_stream(data_stream, args.path, predicate, "none")
            write_jsonl_stream(result_stream)
        else:
            # Memory mode (backward compatibility)
            data = read_jsonl(args.file or sys.stdin)
            result = select_none(data, args.path, predicate)
            write_jsonl(result)
    except Exception as e:
        print(f"JSONPath error: {e}", file=sys.stderr)
        sys.exit(1)


def handle_project_template(args):
    """
    Handle project-template command.
    
    Projects and transforms rows using JSONPath templates.
    
    Args:
        args: Namespace with attributes:
            - template: JSON string defining output structure
            - file: Input file path or None for stdin
            
    Template Format:
        JSON object mapping output fields to expressions:
        {"field1": "$.path", "field2": "sum($.array[*])"}
    """
    try:
        # Parse template JSON
        template = json.loads(args.template)
        
        if _should_stream(args, 'project_template'):
            # Streaming mode
            from .streaming import project_template_stream
            data_stream = read_jsonl_stream(args.file or sys.stdin)
            result_stream = project_template_stream(data_stream, template)
            write_jsonl_stream(result_stream)
        else:
            # Memory mode (backward compatibility)
            data = read_jsonl(args.file or sys.stdin)
            result = project_template(data, template)
            write_jsonl(result)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON template: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Template error: {e}", file=sys.stderr)
        sys.exit(1)
