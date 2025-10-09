"""Functional composition utilities for building complex data pipelines.

This module provides composable operators and pipeline builders that follow
functional programming principles for elegant data transformations.
"""

from typing import Callable, TypeVar, Iterator, Any, List, Optional, Union
from functools import reduce, partial, wraps
import itertools

from .core import Row, Relation
from .core import select, project, rename, distinct, sort_by
from .group import groupby_agg, groupby_with_metadata
from .expr import ExprEval

T = TypeVar('T')


class Pipeline:
    """Composable pipeline for chaining operations.

    Supports both eager (list-based) and lazy (generator-based) evaluation.
    Uses the pipe operator (|) for intuitive chaining.

    Examples:
        >>> # Eager evaluation (default)
        >>> pipeline = (
        ...     Pipeline()
        ...     | Select("age > 30")
        ...     | Project(["name", "email"])
        ...     | Sort("name")
        ... )
        >>> result = pipeline(data)

        >>> # Lazy evaluation for large datasets
        >>> lazy_pipeline = (
        ...     Pipeline(lazy=True)
        ...     | Select("status == 'active'")
        ...     | Take(1000)
        ... )
        >>> for row in lazy_pipeline(huge_data):
        ...     process(row)
    """

    def __init__(self, *ops: Callable, lazy: bool = False):
        """Initialize pipeline with optional operations.

        Args:
            *ops: Initial operations to add to pipeline
            lazy: If True, use lazy (generator-based) evaluation
        """
        self.ops = list(ops)
        self.lazy = lazy

    def __or__(self, operation: Union[Callable, 'Pipeline']) -> 'Pipeline':
        """Pipe operator for chaining operations.

        Args:
            operation: Operation to add to pipeline

        Returns:
            New Pipeline with operation added
        """
        if isinstance(operation, Pipeline):
            return Pipeline(*self.ops, *operation.ops, lazy=self.lazy)
        return Pipeline(*self.ops, operation, lazy=self.lazy)

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        """Execute the pipeline on data.

        Args:
            data: Input data (list or iterator)

        Returns:
            Transformed data (list or iterator based on lazy flag)
        """
        if self.lazy:
            # Lazy evaluation - return generator
            result = iter(data) if not hasattr(data, '__iter__') else data
            for op in self.ops:
                result = op(result)
            return result
        else:
            # Eager evaluation - return list
            result = list(data) if hasattr(data, '__iter__') else data
            for op in self.ops:
                result = op(result)
            return result

    def __repr__(self) -> str:
        """String representation of pipeline."""
        op_names = [op.__class__.__name__ if hasattr(op, '__class__') else str(op)
                    for op in self.ops]
        mode = "lazy" if self.lazy else "eager"
        return f"Pipeline({mode}, ops={' | '.join(op_names)})"


# Composable operation classes
class Operation:
    """Base class for composable operations."""

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        """Execute operation on data."""
        raise NotImplementedError

    def __or__(self, other: Union['Operation', Pipeline]) -> Pipeline:
        """Allow operations to be piped together."""
        if isinstance(other, Pipeline):
            return Pipeline(self, *other.ops, lazy=other.lazy)
        return Pipeline(self, other)


class Select(Operation):
    """Composable select operation."""

    def __init__(self, expr: str, use_jmespath: bool = False):
        self.expr = expr
        self.use_jmespath = use_jmespath

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        if hasattr(data, '__iter__') and not isinstance(data, list):
            # Lazy evaluation for iterators
            return self._lazy_select(data)
        return select(list(data), self.expr, self.use_jmespath)

    def _lazy_select(self, data: Iterator[Row]) -> Iterator[Row]:
        """Lazy evaluation of select."""
        parser = ExprEval()
        for row in data:
            if self.use_jmespath:
                import jmespath
                compiled_expr = jmespath.compile(self.expr)
                if compiled_expr.search(row):
                    yield row
            elif parser.evaluate(self.expr, row):
                yield row

    def __repr__(self) -> str:
        return f"Select('{self.expr}')"


class Project(Operation):
    """Composable project operation."""

    def __init__(self, fields: Union[List[str], str], use_jmespath: bool = False):
        self.fields = fields
        self.use_jmespath = use_jmespath

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        if hasattr(data, '__iter__') and not isinstance(data, list):
            # Lazy evaluation for iterators
            return self._lazy_project(data)
        return project(list(data), self.fields, self.use_jmespath)

    def _lazy_project(self, data: Iterator[Row]) -> Iterator[Row]:
        """Lazy evaluation of project."""
        parser = ExprEval()
        field_specs = self.fields if isinstance(self.fields, list) else self.fields.split(",")

        for row in data:
            new_row = {}
            for spec in field_specs:
                if "=" in spec:
                    # Computed field
                    name, expr = spec.split("=", 1)
                    name = name.strip()
                    expr = expr.strip()
                    arith_result = parser.evaluate_arithmetic(expr, row)
                    if arith_result is not None:
                        new_row[name] = arith_result
                    else:
                        new_row[name] = parser.evaluate(expr, row)
                else:
                    # Simple field projection
                    value = parser.get_field_value(row, spec)
                    if value is not None:
                        parser.set_field_value(new_row, spec, value)
            yield new_row

    def __repr__(self) -> str:
        fields_str = self.fields if isinstance(self.fields, str) else ",".join(self.fields)
        return f"Project([{fields_str}])"


class Sort(Operation):
    """Composable sort operation."""

    def __init__(self, keys: Union[str, List[str]], descending: bool = False):
        self.keys = keys
        self.descending = descending

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Relation:
        # Sorting requires materializing the entire dataset
        return sort_by(list(data), self.keys, descending=self.descending)

    def __repr__(self) -> str:
        keys_str = self.keys if isinstance(self.keys, str) else ",".join(self.keys)
        desc = " desc" if self.descending else ""
        return f"Sort({keys_str}{desc})"


class Distinct(Operation):
    """Composable distinct operation."""

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Relation:
        # Distinct requires materializing to check uniqueness
        return distinct(list(data))

    def __repr__(self) -> str:
        return "Distinct()"


class Rename(Operation):
    """Composable rename operation."""

    def __init__(self, mapping: dict):
        self.mapping = mapping

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        if hasattr(data, '__iter__') and not isinstance(data, list):
            return self._lazy_rename(data)
        return rename(list(data), self.mapping)

    def _lazy_rename(self, data: Iterator[Row]) -> Iterator[Row]:
        """Lazy evaluation of rename."""
        for row in data:
            new_row = {}
            for key, value in row.items():
                new_key = self.mapping.get(key, key)
                new_row[new_key] = value
            yield new_row

    def __repr__(self) -> str:
        return f"Rename({self.mapping})"


class GroupBy(Operation):
    """Composable groupby operation."""

    def __init__(self, key: str, agg: Optional[str] = None):
        self.key = key
        self.agg = agg

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Relation:
        # Grouping requires materializing the entire dataset
        data_list = list(data)
        if self.agg:
            return groupby_agg(data_list, self.key, self.agg)
        else:
            return groupby_with_metadata(data_list, self.key)

    def __repr__(self) -> str:
        if self.agg:
            return f"GroupBy('{self.key}', agg='{self.agg}')"
        return f"GroupBy('{self.key}')"


class Take(Operation):
    """Take first n elements (useful for lazy pipelines)."""

    def __init__(self, n: int):
        self.n = n

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        if hasattr(data, '__iter__') and not isinstance(data, list):
            return itertools.islice(data, self.n)
        return list(itertools.islice(iter(data), self.n))

    def __repr__(self) -> str:
        return f"Take({self.n})"


class Skip(Operation):
    """Skip first n elements."""

    def __init__(self, n: int):
        self.n = n

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        if hasattr(data, '__iter__') and not isinstance(data, list):
            return itertools.islice(data, self.n, None)
        return list(itertools.islice(iter(data), self.n, None))

    def __repr__(self) -> str:
        return f"Skip({self.n})"


class Map(Operation):
    """Apply a function to each row."""

    def __init__(self, func: Callable[[Row], Row]):
        self.func = func

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        if hasattr(data, '__iter__') and not isinstance(data, list):
            return map(self.func, data)
        return list(map(self.func, data))

    def __repr__(self) -> str:
        return f"Map({self.func.__name__ if hasattr(self.func, '__name__') else 'lambda'})"


class Filter(Operation):
    """Filter rows using a Python function."""

    def __init__(self, predicate: Callable[[Row], bool]):
        self.predicate = predicate

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Union[Relation, Iterator[Row]]:
        if hasattr(data, '__iter__') and not isinstance(data, list):
            return filter(self.predicate, data)
        return list(filter(self.predicate, data))

    def __repr__(self) -> str:
        return f"Filter({self.predicate.__name__ if hasattr(self.predicate, '__name__') else 'lambda'})"


class Batch(Operation):
    """Batch rows into groups of n."""

    def __init__(self, size: int):
        self.size = size

    def __call__(self, data: Union[Relation, Iterator[Row]]) -> Iterator[List[Row]]:
        """Returns an iterator of batches."""
        iterator = iter(data)
        while True:
            batch = list(itertools.islice(iterator, self.size))
            if not batch:
                break
            yield batch

    def __repr__(self) -> str:
        return f"Batch({self.size})"


# Convenience functions for building pipelines
def pipeline(*ops: Union[Operation, Callable], lazy: bool = False) -> Pipeline:
    """Create a pipeline from operations.

    Args:
        *ops: Operations to chain
        lazy: If True, use lazy evaluation

    Returns:
        Pipeline ready to execute

    Example:
        >>> p = pipeline(
        ...     Select("age > 25"),
        ...     Project(["name", "email"]),
        ...     Sort("name")
        ... )
        >>> result = p(data)
    """
    return Pipeline(*ops, lazy=lazy)


def lazy_pipeline(*ops: Union[Operation, Callable]) -> Pipeline:
    """Create a lazy pipeline from operations.

    Args:
        *ops: Operations to chain

    Returns:
        Lazy pipeline ready to execute

    Example:
        >>> p = lazy_pipeline(
        ...     Select("status == 'active'"),
        ...     Map(lambda x: {...x, 'processed': True}),
        ...     Take(100)
        ... )
        >>> for row in p(huge_dataset):
        ...     print(row)
    """
    return Pipeline(*ops, lazy=True)


# Functional helpers
def compose(*funcs: Callable) -> Callable:
    """Compose functions right to left.

    Args:
        *funcs: Functions to compose

    Returns:
        Composed function

    Example:
        >>> f = compose(
        ...     partial(select, expr="age > 25"),
        ...     partial(project, fields=["name"]),
        ...     distinct
        ... )
        >>> result = f(data)
    """
    def composed(data):
        return reduce(lambda d, f: f(d), reversed(funcs), data)
    return composed


def pipe(data: Any, *funcs: Callable) -> Any:
    """Pipe data through functions left to right.

    Args:
        data: Initial data
        *funcs: Functions to apply in sequence

    Returns:
        Transformed data

    Example:
        >>> result = pipe(
        ...     data,
        ...     partial(select, expr="age > 25"),
        ...     partial(project, fields=["name"]),
        ...     distinct
        ... )
    """
    return reduce(lambda d, f: f(d), funcs, data)