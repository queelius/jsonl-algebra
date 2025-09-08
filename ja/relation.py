"""
Unified Relation abstraction with fluid API for JSONL algebra operations.

This module provides the RelationQuery class which enables chaining of operations
in a fluent interface style, supporting both eager and lazy evaluation.
"""

from typing import (
    Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union, TYPE_CHECKING
)
import json
from pathlib import Path
from collections import defaultdict

# Import core operations
from .core import (
    Row,
    select as core_select,
    project as core_project,
    join as core_join,
    rename as core_rename,
    union as core_union,
    difference as core_difference,
    distinct as core_distinct,
    intersection as core_intersection,
    sort_by as core_sort_by,
    product as core_product,
    # JSONPath operations
    select_path as core_select_path,
    select_any as core_select_any,
    select_all as core_select_all,
    select_none as core_select_none,
    project_template as core_project_template,
)
from .groupby import groupby_agg as core_groupby_agg
from .streaming import (
    read_jsonl_stream,
    select_stream,
    project_stream,
    rename_stream,
    union_stream,
    distinct_stream,
)

if TYPE_CHECKING:
    import pandas as pd


class RelationQuery:
    """
    A fluent interface for chaining relational algebra operations on JSONL data.
    
    This class wraps an iterable of rows and provides chainable methods for
    data manipulation. It supports both eager evaluation (with lists) and
    lazy evaluation (with generators/iterators).
    
    Example:
        >>> result = (RelationQuery(data)
        ...     .select(lambda r: r["age"] > 25)
        ...     .project(["name", "email"])
        ...     .sort("name")
        ...     .collect())
    """
    
    def __init__(self, data: Union[Iterable[Row], str, Path]):
        """
        Initialize a RelationQuery with data.
        
        Args:
            data: Can be:
                - An iterable of Row dictionaries
                - A string path to a JSONL file
                - A Path object to a JSONL file
                - The string "-" for stdin
        """
        if isinstance(data, (str, Path)):
            # Handle file paths and stdin
            self._data = read_jsonl_stream(data)
            self._is_stream = True
        else:
            self._data = data
            # Check if it's already a generator/iterator
            self._is_stream = hasattr(data, '__next__') or hasattr(data, '__iter__') and not isinstance(data, (list, tuple))
        
        self._operations = []  # Track operations for explain()
    
    @classmethod
    def from_jsonl(cls, file_path: Union[str, Path]) -> 'RelationQuery':
        """
        Create a RelationQuery from a JSONL file.
        
        Args:
            file_path: Path to the JSONL file or "-" for stdin
            
        Returns:
            RelationQuery instance with streaming data
        """
        return cls(file_path)
    
    @classmethod
    def from_records(cls, records: Iterable[Row]) -> 'RelationQuery':
        """
        Create a RelationQuery from an iterable of records.
        
        Args:
            records: Iterable of row dictionaries
            
        Returns:
            RelationQuery instance
        """
        return cls(records)
    
    def select(self, predicate: Union[Callable[[Row], bool], str]) -> 'RelationQuery':
        """
        Filter rows based on a predicate.
        
        Args:
            predicate: Either a function that returns True for rows to keep,
                      or a string expression (for CLI compatibility)
                      
        Returns:
            New RelationQuery with filtered data
        """
        if isinstance(predicate, str):
            # Handle string expressions for CLI compatibility
            pred_func = lambda row: eval(predicate, {"row": row})
        else:
            pred_func = predicate
        
        if self._is_stream:
            new_data = select_stream(self._data, pred_func)
        else:
            new_data = core_select(self._data, pred_func)
        
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"select({predicate})"]
        return result
    
    def where(self, predicate: Union[Callable[[Row], bool], str]) -> 'RelationQuery':
        """Alias for select() for SQL-like syntax."""
        return self.select(predicate)
    
    def project(self, columns: Union[List[str], Dict[str, str]]) -> 'RelationQuery':
        """
        Select specific columns or apply projection template.
        
        Args:
            columns: Either a list of column names to keep,
                    or a dict mapping new names to JSONPath expressions
                    
        Returns:
            New RelationQuery with projected data
        """
        if isinstance(columns, dict):
            # Handle template projection
            new_data = core_project_template(list(self._data), columns)
        elif self._is_stream:
            new_data = project_stream(self._data, columns)
        else:
            new_data = core_project(self._data, columns)
        
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"project({columns})"]
        return result
    
    def rename(self, renames: Dict[str, str]) -> 'RelationQuery':
        """
        Rename columns.
        
        Args:
            renames: Dictionary mapping old column names to new names
            
        Returns:
            New RelationQuery with renamed columns
        """
        if self._is_stream:
            new_data = rename_stream(self._data, renames)
        else:
            new_data = core_rename(self._data, renames)
        
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"rename({renames})"]
        return result
    
    def join(self, other: Union['RelationQuery', Iterable[Row]], 
             on: Union[str, List[Tuple[str, str]]]) -> 'RelationQuery':
        """
        Join with another relation.
        
        Args:
            other: Another RelationQuery or iterable of rows
            on: Either a single column name (for natural join) or
                list of (left_col, right_col) tuples
                
        Returns:
            New RelationQuery with joined data
        """
        # Convert other to list if needed (joins require materialization)
        if isinstance(other, RelationQuery):
            other_data = list(other._data)
        else:
            other_data = list(other)
        
        # Handle single column natural join
        if isinstance(on, str):
            on_tuples = [(on, on)]
        else:
            on_tuples = on
        
        # Joins require materialization of left side too
        left_data = list(self._data)
        new_data = core_join(left_data, other_data, on_tuples)
        
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"join(on={on})"]
        return result
    
    def union(self, other: Union['RelationQuery', Iterable[Row]]) -> 'RelationQuery':
        """
        Union with another relation (preserves duplicates).
        
        Args:
            other: Another RelationQuery or iterable of rows
            
        Returns:
            New RelationQuery with combined data
        """
        if isinstance(other, RelationQuery):
            other_data = other._data
        else:
            other_data = other
        
        if self._is_stream:
            new_data = union_stream(self._data, other_data)
        else:
            new_data = core_union(self._data, other_data)
        
        result = RelationQuery(new_data)
        result._operations = self._operations + ["union()"]
        return result
    
    def distinct(self) -> 'RelationQuery':
        """
        Remove duplicate rows.
        
        Returns:
            New RelationQuery with unique rows
        """
        if self._is_stream:
            new_data = distinct_stream(self._data)
        else:
            new_data = core_distinct(self._data)
        
        result = RelationQuery(new_data)
        result._operations = self._operations + ["distinct()"]
        return result
    
    def sort(self, *keys: str, desc: bool = False) -> 'RelationQuery':
        """
        Sort by specified keys.
        
        Args:
            *keys: Column names to sort by
            desc: If True, sort in descending order
            
        Returns:
            New RelationQuery with sorted data
        """
        # Sorting requires materialization
        data_list = list(self._data)
        new_data = core_sort_by(data_list, list(keys))
        
        if desc:
            new_data = list(reversed(new_data))
        
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"sort({keys}, desc={desc})"]
        return result
    
    def sort_by(self, keys: List[str]) -> 'RelationQuery':
        """Alternative sort method matching core API."""
        return self.sort(*keys)
    
    def groupby(self, key: Union[str, List[str]]) -> 'GroupedRelation':
        """
        Group by specified key(s).
        
        Args:
            key: Column name or list of column names to group by
            
        Returns:
            GroupedRelation for applying aggregations
        """
        if isinstance(key, str):
            key = [key]
        return GroupedRelation(self._data, key, self._operations)
    
    def limit(self, n: int) -> 'RelationQuery':
        """
        Limit to first n rows.
        
        Args:
            n: Maximum number of rows to return
            
        Returns:
            New RelationQuery with limited data
        """
        def limited_generator():
            count = 0
            for row in self._data:
                if count >= n:
                    break
                yield row
                count += 1
        
        result = RelationQuery(limited_generator())
        result._operations = self._operations + [f"limit({n})"]
        return result
    
    def skip(self, n: int) -> 'RelationQuery':
        """
        Skip first n rows.
        
        Args:
            n: Number of rows to skip
            
        Returns:
            New RelationQuery with remaining data
        """
        def skipped_generator():
            count = 0
            for row in self._data:
                if count >= n:
                    yield row
                else:
                    count += 1
        
        result = RelationQuery(skipped_generator())
        result._operations = self._operations + [f"skip({n})"]
        return result
    
    def map(self, func: Callable[[Row], Row]) -> 'RelationQuery':
        """
        Apply a transformation function to each row.
        
        Args:
            func: Function that transforms a row
            
        Returns:
            New RelationQuery with transformed data
        """
        def mapped_generator():
            for row in self._data:
                yield func(row)
        
        result = RelationQuery(mapped_generator())
        result._operations = self._operations + [f"map({func.__name__})"]
        return result
    
    # JSONPath operations
    def select_path(self, path: str, predicate: Callable[[Any], bool]) -> 'RelationQuery':
        """Filter using JSONPath expressions."""
        new_data = core_select_path(list(self._data), path, predicate)
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"select_path({path})"]
        return result
    
    def select_any(self, path: str, predicate: Callable[[Any], bool]) -> 'RelationQuery':
        """Select rows where any value at path matches predicate."""
        new_data = core_select_any(list(self._data), path, predicate)
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"select_any({path})"]
        return result
    
    def select_all(self, path: str, predicate: Callable[[Any], bool]) -> 'RelationQuery':
        """Select rows where all values at path match predicate."""
        new_data = core_select_all(list(self._data), path, predicate)
        result = RelationQuery(new_data)
        result._operations = self._operations + [f"select_all({path})"]
        return result
    
    # Execution methods
    def collect(self) -> List[Row]:
        """
        Execute the query and collect results into a list.
        
        Returns:
            List of row dictionaries
        """
        return list(self._data)
    
    def stream(self) -> Iterator[Row]:
        """
        Return a generator for streaming results.
        
        Returns:
            Iterator of row dictionaries
        """
        return iter(self._data)
    
    def first(self) -> Optional[Row]:
        """
        Get the first row or None if empty.
        
        Returns:
            First row dictionary or None
        """
        try:
            return next(iter(self._data))
        except StopIteration:
            return None
    
    def count(self) -> int:
        """
        Count the number of rows.
        
        Returns:
            Number of rows
        """
        return sum(1 for _ in self._data)
    
    def to_jsonl(self, file_path: Union[str, Path]) -> None:
        """
        Write results to a JSONL file.
        
        Args:
            file_path: Path to output file or "-" for stdout
        """
        if str(file_path) == "-":
            import sys
            for row in self._data:
                print(json.dumps(row))
        else:
            with open(file_path, 'w') as f:
                for row in self._data:
                    f.write(json.dumps(row) + '\n')
    
    def to_pandas(self) -> 'pd.DataFrame':
        """
        Convert to a pandas DataFrame.
        
        Returns:
            pandas DataFrame
            
        Raises:
            ImportError: If pandas is not installed
        """
        try:
            import pandas as pd
            return pd.DataFrame(self.collect())
        except ImportError:
            raise ImportError("pandas is required for to_pandas(). Install with: pip install pandas")
    
    def explain(self) -> str:
        """
        Get a string representation of the query execution plan.
        
        Returns:
            String describing the operations
        """
        if not self._operations:
            return "RelationQuery(source_data)"
        return "RelationQuery\n  " + "\n  ".join(f"→ {op}" for op in self._operations)
    
    def __iter__(self) -> Iterator[Row]:
        """Allow iteration over the query results."""
        return iter(self._data)
    
    def __repr__(self) -> str:
        """String representation of the query."""
        return self.explain()


class GroupedRelation:
    """
    Represents a grouped relation for applying aggregations.
    
    This class is returned by RelationQuery.groupby() and provides
    methods for aggregating grouped data.
    """
    
    def __init__(self, data: Iterable[Row], keys: List[str], operations: List[str]):
        """
        Initialize a grouped relation.
        
        Args:
            data: The underlying data
            keys: Column names to group by
            operations: List of operations for tracking
        """
        self._data = data
        self._keys = keys
        self._operations = operations
    
    def agg(self, *args, **kwargs) -> RelationQuery:
        """
        Apply aggregations to grouped data.
        
        Args:
            *args: Aggregation names (e.g., "count", "sum", "avg")
            **kwargs: Named aggregations mapping result name to "agg:column"
                     e.g., total="sum:amount", average="avg:score"
                     
        Returns:
            RelationQuery with aggregated results
        """
        # Build aggregation list
        aggregations = []
        result_names = {}  # Track custom names for aggregations
        
        # Handle positional arguments (simple aggregations)
        for arg in args:
            if arg == "count":
                aggregations.append(("count", ""))
            else:
                # Assume it's in format "agg:column"
                if ":" in arg:
                    agg_type, column = arg.split(":", 1)
                    aggregations.append((agg_type, column))
                else:
                    raise ValueError(f"Invalid aggregation format: {arg}")
        
        # Handle keyword arguments (named aggregations)
        for name, spec in kwargs.items():
            if ":" in spec:
                agg_type, column = spec.split(":", 1)
                aggregations.append((agg_type, column))
                # Store the custom name mapping
                result_names[f"{agg_type}_{column}"] = name
            elif spec in ["count", "list"]:
                aggregations.append((spec, ""))
                result_names[spec] = name
            else:
                # Assume it's a column name for a default aggregation
                aggregations.append(("sum", spec))
                result_names[f"sum_{spec}"] = name
        
        # Convert single key to string if needed
        group_key = self._keys[0] if len(self._keys) == 1 else self._keys
        
        # Apply aggregations
        result_data = core_groupby_agg(list(self._data), group_key, aggregations)
        
        # Rename aggregation result columns if custom names were provided
        if result_names:
            renamed_data = []
            for row in result_data:
                new_row = {}
                for key, value in row.items():
                    if key in result_names:
                        new_row[result_names[key]] = value
                    else:
                        new_row[key] = value
                renamed_data.append(new_row)
            result_data = renamed_data
        
        result = RelationQuery(result_data)
        result._operations = self._operations + [f"groupby({self._keys}).agg({aggregations})"]
        return result
    
    def count(self) -> RelationQuery:
        """Count rows in each group."""
        return self.agg("count")
    
    def sum(self, column: str) -> RelationQuery:
        """Sum values in each group."""
        return self.agg(f"sum:{column}")
    
    def avg(self, column: str) -> RelationQuery:
        """Average values in each group."""
        return self.agg(f"avg:{column}")
    
    def min(self, column: str) -> RelationQuery:
        """Find minimum value in each group."""
        return self.agg(f"min:{column}")
    
    def max(self, column: str) -> RelationQuery:
        """Find maximum value in each group."""
        return self.agg(f"max:{column}")