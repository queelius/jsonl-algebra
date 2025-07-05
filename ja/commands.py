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
    """Warn user that streaming is not supported for this operation."""
    warnings.warn(
        f"Operation '{operation_name}' requires loading data into memory for correct results. "
        f"For large datasets, consider: (1) using --window-size N for approximate results, "
        f"or (2) filtering data first with: ja select 'condition' --stream | ja {operation_name}",
        UserWarning,
        stacklevel=3
    )


def _warn_memory_intensive(operation_name):
    """Warn user about memory-intensive operations."""
    if requires_memory_operation(operation_name):
        warnings.warn(
            f"Operation '{operation_name}' requires loading data into memory for optimal results. "
            f"For large datasets, consider using --window-size N for memory-efficient approximate processing, "
            f"or use streaming operations (select, project, rename) to reduce data size first.",
            UserWarning,
            stacklevel=3
        )


def _warn_windowed_approximation(operation_name):
    """Warn user that windowed processing provides approximate results."""
    warnings.warn(
        f"Windowed processing for '{operation_name}' provides approximate results. "
        f"The operation is applied within each window independently, which may differ "
        f"from applying the operation to the entire dataset.",
        UserWarning,
        stacklevel=3
    )


# Helper functions moved from cli.py
def read_jsonl(file_or_fp):
    if isinstance(file_or_fp, str) or isinstance(file_or_fp, Path):
        with open(file_or_fp) as f:
            return [json.loads(line) for line in f]
    else:
        return [json.loads(line) for line in file_or_fp]


def write_jsonl(rows):
    for row in rows:
        print(json.dumps(row))


def _should_stream(args, operation_name):
    """Determine if we should use streaming mode for this operation."""
    # Always stream when the operation supports it
    return can_stream_operation(operation_name)


def _should_use_windowed(args, operation_name):
    """Determine if we should use windowed processing for this operation."""
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
    a = read_jsonl(args.left)
    b = read_jsonl(args.right)
    write_jsonl(product(a, b))


def handle_rename(args):
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
    """Parse a predicate string into a callable function."""
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
    """Handle select-path command."""
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
    """Handle select-any command."""
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
    """Handle select-all command."""
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
    """Handle select-none command."""
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
    """Handle project-template command."""
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
