# ja - JSONL Algebra

**ja** is a lightweight command-line tool and Python library for performing relational algebra operations on JSONL (JSON Lines) data. It's designed to be a simple, dependency-free alternative for common data manipulation tasks, inspired by tools like `jq` and traditional SQL.

## Features

* Perform common relational algebra operations: select, project, join, union, intersection, difference, distinct, sort, product, and group by with aggregations.

* `groupby`: A powerful feature that allows you to group data by one or more keys and perform various aggregations on the grouped data.
  * By default, includes `sum`, `avg`, `min`, `max`, `count`, `list` (collect all values), `first` (first value in group), `last` (last value in group) aggregations.
  * Can be extended with custom aggregation functions. See "Extending Group By Aggregations" section.
* Works with JSONL files or piped data from stdin/stdout.
* Can be used as a CLI tool or as a Python library.
* No external dependencies.

## Installation

There are two main ways to install `ja`:

* **For users (from PyPI):**

    You can install the package directly from PyPI (Python Package Index) using pip. We'll assume the package is published under the name `jsonl-algebra` (as `ja` is likely taken):

    ```bash
    pip install jsonl-algebra
    ```

* **For developers (from local repository):**

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

**Examples:**

* **Select rows where 'amount' is greater than 100:**

    ```bash
    cat data.jsonl | ja select 'amount > 100'
    ```

    ```bash
    ja select 'amount > 100' data.jsonl
    ```

* **Project 'id' and 'name' columns:**

    ```bash
    cat data.jsonl | ja project id,name
    ```

* **Join two files on a common key:**

    ```bash
    ja join users.jsonl orders.jsonl --on user_id=customer_id
    ```

* **Group by 'category' and count items:**

    ```bash
    cat products.jsonl | ja groupby category --agg count
    ```

* **Group by 'category', count items, and list all product names:**

    ```bash
    cat products.jsonl | ja groupby category --agg count --agg list:name\
    ```

    This will produce output like: `{"category": "electronics", "count": 5, "list_name": ["laptop", "mouse", ...]}`

* **Group by 'user_id' and get the first action:**

    ```bash
    cat user_actions.jsonl | ja groupby user_id --agg first:action
    ```

* **Sort data by 'timestamp':**

    ```bash
    cat logs.jsonl | ja sort timestamp
    ```

**Available Commands:**

* `select`: Filter rows based on a Python expression.
* `project`: Select specific columns.
* `join`: Join two relations on specified keys.
* `rename`: Rename columns.
* `union`: Combine two relations (all rows).
* `difference`: Rows in the first relation but not the second.
* `distinct`: Remove duplicate rows.
* `intersection`: Rows common to both relations.
* `sort` (maps to `sort_by`): Sort a relation by specified keys.
* `product`: Cartesian product of two relations.
* `groupby` (maps to `groupby_agg`): Group rows by a key and perform aggregations.

  * Supported aggregations: `count`, `sum`, `avg`, `min`, `max`, `list`, `first`, `last`.
  
  * Syntax for aggregations: `agg_name` (for count) or `agg_name:column_name` (e.g., `sum:price`, `list:product_id`).

Use `ja <command> --help` for more details on specific commands.

## Programmatic API Usage

You can also use `ja` as a Python library:

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

## Extending Group By Aggregations

The `groupby_agg` functionality is designed to be extensible. The core logic resides in the `ja.groupby` module, which uses a dispatcher pattern.

To add your own custom aggregation function:

* **Define an aggregation helper function:** This function will take the collected data for a group (typically a list of values, or a single value for aggregations like `first`/`last`) and return the aggregated result.

    ```python
    # Example: A custom function to calculate the median
    # (This would typically be added to ja/groupby.py or your own extensions module)
    # from ja.groupby import _agg_numeric_values # If you need numeric conversion

    def _my_custom_median_agg(collected_values: list) -> float | None:
        # Simplified median, assumes _agg_numeric_values handles non-numeric/None
        # numeric_vals = sorted(_agg_numeric_values(collected_values)) # Example
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

* **Register it (if modifying `ja` directly or for illustration):**
    If you were modifying `ja` itself, you would add your function to the `AGGREGATION_DISPATCHER` dictionary in `ja/groupby.py`:

    ```python
    # In ja/groupby.py
    # AGGREGATION_DISPATCHER = {
    #     ...
    #     "median": _my_custom_median_agg,
    # }
    ```

    For programmatic use with your own `ja` instance or a forked version, you could potentially expose a way to register custom aggregators or pass them directly if the API supported it.

* **Adjust Data Collection (if needed):**
    If your aggregation requires a specific way of collecting data during the first pass of `groupby_agg` (different from how `list`, `first`, or `last` collect data), you would need to modify the data collection logic in `ja.groupby.groupby_agg`.

This structure allows for significant flexibility. For instance, one could implement a general `reduce` aggregation that takes Python expressions for an initial value and a step function, operating on the list of values collected for a group.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

## License

This project is licensed under the MIT License.
