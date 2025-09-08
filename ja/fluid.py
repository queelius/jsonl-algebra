"""
Fluid API entry points for JSONL algebra operations.

This module provides convenient factory functions for creating RelationQuery
instances and enables the fluid API style of data manipulation.
"""

from typing import Union, Iterable, List
from pathlib import Path
from .relation import RelationQuery, Row


def from_jsonl(file_path: Union[str, Path]) -> RelationQuery:
    """
    Create a RelationQuery from a JSONL file.
    
    Args:
        file_path: Path to the JSONL file or "-" for stdin
        
    Returns:
        RelationQuery instance ready for chaining operations
        
    Example:
        >>> import ja.fluid as jaf
        >>> result = (jaf.from_jsonl("data.jsonl")
        ...     .select(lambda r: r["age"] > 25)
        ...     .project(["name", "email"])
        ...     .collect())
    """
    return RelationQuery.from_jsonl(file_path)


def from_records(records: Iterable[Row]) -> RelationQuery:
    """
    Create a RelationQuery from an iterable of records.
    
    Args:
        records: Iterable of row dictionaries
        
    Returns:
        RelationQuery instance ready for chaining operations
        
    Example:
        >>> import ja.fluid as jaf
        >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        >>> result = (jaf.from_records(data)
        ...     .select(lambda r: r["age"] > 25)
        ...     .collect())
    """
    return RelationQuery.from_records(records)


def query(data: Union[Iterable[Row], str, Path]) -> RelationQuery:
    """
    Create a RelationQuery from various data sources.
    
    This is the most flexible entry point that accepts:
    - An iterable of row dictionaries
    - A string path to a JSONL file  
    - A Path object to a JSONL file
    - The string "-" for stdin
    
    Args:
        data: Data source
        
    Returns:
        RelationQuery instance ready for chaining operations
        
    Example:
        >>> import ja.fluid as jaf
        >>> # From file
        >>> result1 = jaf.query("data.jsonl").select(lambda r: r["active"]).collect()
        >>> # From list
        >>> result2 = jaf.query([{"a": 1}, {"a": 2}]).project(["a"]).collect()
    """
    return RelationQuery(data)


# Convenience aliases
Q = query  # Short alias for quick interactive use
load = from_jsonl  # Alternative name for file loading


def example_usage():
    """
    Demonstrate the fluid API with examples.
    
    This function shows various ways to use the fluid API for
    data manipulation tasks.
    """
    # Example data
    users = [
        {"id": 1, "name": "Alice", "age": 30, "city": "NYC"},
        {"id": 2, "name": "Bob", "age": 25, "city": "LA"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "NYC"},
        {"id": 4, "name": "Diana", "age": 28, "city": "Chicago"},
    ]
    
    orders = [
        {"user_id": 1, "product": "Book", "price": 20},
        {"user_id": 1, "product": "Pen", "price": 5},
        {"user_id": 2, "product": "Notebook", "price": 15},
        {"user_id": 3, "product": "Book", "price": 20},
    ]
    
    print("Example 1: Simple filtering and projection")
    result1 = (query(users)
        .select(lambda r: r["age"] > 25)
        .project(["name", "age"])
        .sort("age")
        .collect())
    print(result1)
    
    print("\nExample 2: Grouping and aggregation")
    result2 = (query(users)
        .groupby("city")
        .agg("count", avg_age="avg:age")
        .sort("count", desc=True)
        .collect())
    print(result2)
    
    print("\nExample 3: Join operations")
    result3 = (query(users)
        .join(orders, on=[("id", "user_id")])
        .project(["name", "product", "price"])
        .collect())
    print(result3)
    
    print("\nExample 4: Complex pipeline")
    result4 = (query(users)
        .select(lambda r: r["city"] == "NYC")
        .join(orders, on=[("id", "user_id")])
        .groupby("name")
        .agg(total="sum:price", items="count")
        .sort("total", desc=True)
        .limit(5)
        .collect())
    print(result4)
    
    print("\nExample 5: Streaming from file (mock)")
    # This would work with actual JSONL files:
    # result5 = (from_jsonl("large_dataset.jsonl")
    #     .select(lambda r: r["status"] == "active")
    #     .project(["id", "timestamp"])
    #     .limit(1000)
    #     .to_jsonl("filtered_output.jsonl"))
    
    return "Examples completed!"


if __name__ == "__main__":
    # Run examples when module is executed directly
    example_usage()