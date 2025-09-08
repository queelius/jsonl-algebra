# ja - JSONL Algebra

**ja** is a lightweight command-line tool and Python library for performing relational algebra operations on JSONL (JSON Lines) data. It's designed to be a simple, dependency-free alternative for common data manipulation tasks, inspired by tools like `jq` and traditional SQL.

**New in v0.2**: Fluid API for intuitive method chaining!

## Features

- **Two powerful APIs**: Traditional functional API and new fluid/chainable API for intuitive data manipulation
- Perform common relational algebra operations: select, project, join, union, intersection, difference, distinct, sort, product, and group by with aggregations.

- `groupby`: A powerful feature that allows you to group data by one or more keys and perform various aggregations on the grouped data.
  - Includes `sum`, `avg`, `min`, `max`, `count`, `list`, `first`, `last`, `median`, `mode`, `std`, `unique`, `concat` aggregations
  - Can be extended with custom aggregation functions. See "Extending Group By Aggregations" section.
- Works with JSONL files or piped data from stdin/stdout.
- Can be used as a CLI tool or as a Python library (functional or fluid style).
- No external dependencies.

## Installation

There are two main ways to install `ja`:

### For users (from PyPI):

You can install the package directly from PyPI (Python Package Index) using pip. We'll assume the package is published under the name `jsonl-algebra` (as `ja` is likely taken):

```bash
pip install jsonl-algebra
```

### For developers (from local repository):

If you have cloned this repository and want to install it for development or from local sources:

```bash
pip install .
```

To install in editable mode for development:

```bash
pip install -e .
```

## CLI Usage

The `ja` command-line tool provides several subcommands for different operations.

**General Syntax:**
`ja <command> [options] [file(s)]`

If `file` is omitted for commands that expect a single input, `ja` reads from stdin.

### Examples

- **Select rows where 'amount' is greater than 100:**

    ```bash
    cat data.jsonl | ja select 'amount > 100'
    ```

    ```bash
    ja select 'amount > 100' data.jsonl
    ```

- **Project 'id' and 'name' columns:**

    ```bash
    cat data.jsonl | ja project id,name
    ```

- **Join two files on a common key:**

    ```bash
    ja join users.jsonl orders.jsonl --on user_id=customer_id
    ```

- **Group by 'category' and count items:**

    ```bash
    cat products.jsonl | ja groupby category --agg count
    ```

- **Group by 'category', count items, and list all product names:**

    ```bash
    cat products.jsonl | ja groupby category --agg count --agg list:name
    ```

    This will produce output like: `{"category": "electronics", "count": 5, "list_name": ["laptop", "mouse", ...]}`

- **Group by 'user_id' and get the first action:**

    ```bash
    cat user_actions.jsonl | ja groupby user_id --agg first:action
    ```

- **Sort data by 'timestamp':**

    ```bash
    cat logs.jsonl | ja sort timestamp
    ```

### Available Commands

- `select`: Filter rows based on a Python expression.
- `project`: Select specific columns.
- `join`: Join two relations on specified keys.
- `rename`: Rename columns.
- `union`: Combine two relations (all rows).
- `difference`: Rows in the first relation but not the second.
- `distinct`: Remove duplicate rows.
- `intersection`: Rows common to both relations.
- `sort` (maps to `sort_by`): Sort a relation by specified keys.
- `product`: Cartesian product of two relations.
- `groupby` (maps to `groupby_agg`): Group rows by a key and perform aggregations.

Use `ja <command> --help` for more details on specific commands.

## Programmatic API Usage

You can use `ja` as a Python library with two different API styles:

### Fluid API (New in v0.2!)

The fluid API provides an intuitive, chainable interface for data manipulation:

```python
import ja

# Create a query from various sources
result = (ja.query(data)  # or ja.from_jsonl("file.jsonl")
    .select(lambda r: r["age"] > 25)
    .project(["name", "email", "age"])
    .join(orders, on=[("id", "user_id")])
    .groupby("category")
    .agg(total="sum:amount", avg_rating="avg:rating")
    .sort("total", desc=True)
    .limit(10)
    .collect())  # Execute and get results

# Alternative entry points
result = ja.from_jsonl("data.jsonl")  # Load from file
result = ja.from_records(list_of_dicts)  # From Python list
result = ja.Q(data)  # Short alias for quick use

# Execution modes
result.collect()  # Get results as list
result.stream()   # Get lazy iterator
result.first()    # Get first row
result.count()    # Count rows
result.to_jsonl("output.jsonl")  # Write to file
result.to_pandas()  # Convert to DataFrame (requires pandas)
result.explain()  # Show query plan
```

### Traditional Functional API

The original functional API is still fully supported:

```python
import ja
from ja import Row, Relation # For type hinting if needed

# Load data from JSONL files
# users_data = ja.read_jsonl("users.jsonl")
# orders_data = ja.read_jsonl("orders.jsonl")

# Example data (replace with ja.read_jsonl for actual files)
users_data: Relation = [
    {"user_id": 1, "name": "Alice", "status": "active", "email": "alice@example.com"},
    {"user_id": 2, "name": "Bob", "status": "inactive", "email": "bob@example.com"},
    {"user_id": 1, "name": "Alice", "status": "active", "email": "alice@example.com"} # Duplicate for distinct example
]
orders_data: Relation = [
    {"order_id": 101, "customer_id": 1, "item": "Book", "quantity": 1},
    {"order_id": 102, "customer_id": 2, "item": "Pen", "quantity": 5},
    {"order_id": 103, "customer_id": 1, "item": "Notebook", "quantity": 2},
    {"order_id": 104, "customer_id": 1, "item": "Book", "quantity": 3}
]


# Example: Select active users
active_users = ja.select(users_data, lambda row: row.get("status") == "active")
# active_users will be:
# [{'user_id': 1, 'name': 'Alice', 'status': 'active', 'email': 'alice@example.com'},
#  {'user_id': 1, 'name': 'Alice', 'status': 'active', 'email': 'alice@example.com'}]

# Example: Project name and email from distinct active users
distinct_active_users = ja.distinct(active_users)
user_info = ja.project(distinct_active_users, ["name", "email"])
# user_info will be:
# [{'name': 'Alice', 'email': 'alice@example.com'}]

# Example: Join distinct active users with their orders
# Ensure the join key 'user_id' is present in the projected active_users relation
active_users_with_id = ja.project(distinct_active_users, ["user_id", "name", "email"])
joined_data = ja.join(active_users_with_id, orders_data, on=[("user_id", "customer_id")])
# joined_data will be:
# [{'user_id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'order_id': 101, 'item': 'Book', 'quantity': 1},
#  {'user_id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'order_id': 103, 'item': 'Notebook', 'quantity': 2},
#  {'user_id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'order_id': 104, 'item': 'Book', 'quantity': 3}]

# Example: Group joined data by user and sum quantities, list items
grouped_orders = ja.groupby_agg(
    joined_data,
    group_by_key="user_id",
    aggregations=[
        ("sum", "quantity"),
        ("list", "item"),
        ("count", "") # Count groups
    ]
)
# grouped_orders might be (depending on Alice's orders):
# [{'user_id': 1, 'sum_quantity': 6, 'list_item': ['Book', 'Notebook', 'Book'], 'count': 3}]


# Print results (example)
for row in grouped_orders:
    print(row)

# Available functions mirror the CLI commands:
# ja.select, ja.project, ja.join, ja.rename, ja.union,
# ja.difference, ja.distinct, ja.intersection, ja.sort_by,
# ja.product, ja.groupby_agg
```

### Fluid API Examples

```python
import ja

# Example 1: Complex data pipeline
users = ja.from_jsonl("users.jsonl")
orders = ja.from_jsonl("orders.jsonl")

top_customers = (users
    .select(lambda r: r["status"] == "active")
    .join(orders, on="user_id")
    .groupby("user_id")
    .agg(
        total_spent="sum:amount",
        order_count="count",
        avg_order="avg:amount",
        products="list:product_id"
    )
    .sort("total_spent", desc=True)
    .limit(10)
    .collect())

# Example 2: Data analysis with new aggregations
sales_analysis = (ja.from_jsonl("sales.jsonl")
    .select(lambda r: r["year"] == 2024)
    .groupby("region")
    .agg(
        revenue="sum:amount",
        median_sale="median:amount",
        std_dev="std:amount",
        top_product="mode:product",
        unique_customers="unique:customer_id"
    )
    .sort("revenue", desc=True)
    .to_pandas())  # Convert to DataFrame for further analysis

# Example 3: Streaming large datasets
(ja.from_jsonl("huge_log_file.jsonl")
    .select(lambda r: r["level"] == "ERROR")
    .map(lambda r: {**r, "date": r["timestamp"][:10]})
    .project(["date", "error_code", "message"])
    .limit(1000)
    .to_jsonl("errors_sample.jsonl"))

# Example 4: Method chaining flexibility
query = ja.from_jsonl("data.jsonl")

# Build query conditionally
if filter_active:
    query = query.select(lambda r: r["active"])

if group_by_category:
    query = query.groupby("category").count()

results = query.collect()

# Example 5: Explain query plan
query = (ja.from_jsonl("data.jsonl")
    .select(lambda r: r["value"] > 100)
    .groupby("category")
    .agg("sum:value")
    .sort("sum_value"))

print(query.explain())
# Output:
# RelationQuery
#   → select(<lambda>)
#   → groupby(['category']).agg([('sum', 'value')])
#   → sort(('sum_value',), desc=False)
```

## Extending Group By Aggregations

By default, `ja` supports several built-in aggregation functions for the `groupby_agg` operation. These include: `count`, `sum`, `avg`, `min`, `max`, `list`, `first`, and `last`. Syntax for aggregations: `agg_name` (for count) or `agg_name:column_name` (e.g., `sum:price`, `list:product_id`).

The `groupby_agg` functionality is designed to be extensible. The core logic resides in the `ja.groupby` module, which uses a dispatcher pattern.

### Define an aggregation helper function

This function will take the collected data for a group (typically a list of values, or a single value for aggregations like `first`/`last`) and return the aggregated result.

For instance, if you want to add a custom aggregation function for calculating the median, you would define a function that takes a list of values and returns the median.

```python
def _my_custom_median_agg(collected_values: list) -> float | None:
    numeric_vals = sorted([v for v in collected_values if isinstance(v, (int, float))])
    if not numeric_vals:
        return None
    n = len(numeric_vals)
    mid = n // 2
    if n % 2 == 0:
        return (numeric_vals[mid - 1] + numeric_vals[mid]) / 2
    else:
        return numeric_vals[mid]
```

Now, register it (if modifying `ja` directly or for illustration):
  
```python
# In ja/groupby.py
AGGREGATION_DISPATCHER = {
    # ...
    "median": _my_custom_median_agg,
}
```

For programmatic use with your own `ja` instance or a forked version, you could potentially expose a way to register custom aggregators or pass them directly if the API supported it.

If your aggregation requires a specific way of collecting data during the first pass of `groupby_agg` (different from how `list`, `first`, or `last` collect data), you would need to modify the data collection logic in `ja.groupby.groupby_agg`.

This structure allows for significant flexibility. For instance, one could implement a general `reduce` aggregation that takes Python expressions for an initial value and a step function, operating on the list of values collected for a group.

## Memory-Efficient Processing

**ja** automatically uses streaming mode for memory-efficient processing of large JSONL files. Operations that can be streamed process data line-by-line without loading entire datasets into memory.

### Automatic Streaming

Most operations stream by default - no flags needed:

```bash
# These automatically stream for constant memory usage
cat huge_logs.jsonl | ja select 'status == "error"'
cat large_data.jsonl | ja select 'amount > 1000' | ja project id,amount,timestamp
```

### Operations That Stream Automatically

These operations process data with O(1) memory usage:
- `select` - Filter rows  
- `project` - Select columns
- `rename` - Rename columns
- `union` - Combine datasets (preserves duplicates per relational algebra)
- JSONPath operations: `select-path`, `select-any`, `select-all`, `select-none`, `project-template`

**Note**: `distinct` streams input but requires O(unique_items) memory to track seen records for exact deduplication.

### Memory-Intensive Operations (Relational Algebra)

**Important**: The core relational/set operations require loading data into memory for correctness, but offer alternatives:

- `join` - Requires indexing both tables for correct results
- `sort` - Requires all data to sort correctly  
- `groupby` - Requires accumulating all groups
- `intersection` - Requires set operations on full datasets
- `difference` - Requires set operations on full datasets

**💡 Solution: Windowed Processing**

For large datasets, use `--window-size N` to process data in chunks, providing approximate results with much lower memory usage:

```bash
# Sort in windows of 1000 records each (approximate global sort)
cat large_data.jsonl | ja sort timestamp --window-size 1000

# Group by category in windows of 500 records (approximate aggregation)
cat large_data.jsonl | ja groupby category --agg sum:amount --window-size 500
```

**Note**: Windowed processing provides approximate results since operations are applied within each window independently. This is useful for analysis where perfect accuracy isn't required but memory efficiency is critical.

**💡 Strategy: Filter First, Then Process**

For best results with large datasets:

```bash
# Reduce data size first with streaming operations, then use memory-intensive operations
cat huge_dataset.jsonl | ja select 'category == "important"' | ja sort timestamp
cat huge_dataset.jsonl | ja project id,timestamp,value | ja groupby id --agg sum:value
```

### Performance Benefits

**Memory Usage:**
- **Standard operations**: O(n) memory usage (loads entire dataset) 
- **Automatic streaming**: O(1) memory usage (constant memory, line-by-line)
- **Windowed processing**: O(window_size) memory usage (approximate results)

**When Each Mode is Used:**
- **Streaming (automatic)**: `select`, `project`, `rename`, `union`, `distinct`, JSONPath operations
- **Memory-intensive**: `join`, `sort`, `groupby`, `intersection`, `difference` 
- **Windowed**: Any memory-intensive operation with `--window-size N`

**Example Performance Comparison:**

```bash
# These are equivalent and both automatically stream:
cat logs_1gb.jsonl | ja select 'severity == "ERROR"' | ja project timestamp,message

# Memory-intensive operation (will warn for large files):
cat logs_1gb.jsonl | ja sort timestamp

# Memory-efficient approximation:
cat logs_1gb.jsonl | ja sort timestamp --window-size 10000
```

### Smart Warnings

**ja** will automatically inform you about memory usage and provide helpful suggestions:

```bash
# Memory-intensive operations will suggest alternatives for large datasets:
cat large_data.jsonl | ja join other_data.jsonl --on id=user_id
# Warning: Operation 'join' requires loading data into memory for optimal results. 
# For large datasets, consider using --window-size N for memory-efficient approximate processing, 
# or use streaming operations (select, project, rename) to reduce data size first.

# Windowed processing will warn about approximate results:
cat data.jsonl | ja sort value --window-size 100
### Processing Modes Summary

| Mode | Memory Usage | Accuracy | When Used |
|------|-------------|----------|----------|
| **Auto-Streaming** | O(1) | Perfect | `select`, `project`, `rename`, `union`, `distinct`, JSONPath ops |
| **Memory-Intensive** | O(dataset_size) | Perfect | `join`, `sort`, `groupby`, `intersection`, `difference` |
| **Windowed** | O(window_size) | Approximate | Any memory-intensive operation + `--window-size N` |

**Recommendation**: Use streaming operations to filter/reduce data size, then apply memory-intensive operations to smaller datasets.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

## License

This project is licensed under the MIT License.
