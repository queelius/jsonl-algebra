# ja - JSONL Algebra

**ja** is a lightweight command-line tool and Python library for performing relational algebra operations on JSONL (JSON Lines) data. It's designed to be a simple, dependency-free alternative for common data manipulation tasks, inspired by tools like `jq` and traditional SQL.

## Features

*   Perform common relational algebra operations: select, project, join, union, intersection, difference, distinct, sort, product, and group by with aggregations.
*   Works with JSONL files or piped data from stdin/stdout.
*   Can be used as a CLI tool or as a Python library.
*   No external dependencies.

## Installation

There are two main ways to install `ja`:

1.  **For users (from PyPI):**

    You can install the package directly from PyPI (Python Package Index) using pip. We'll assume the package is published under the name `jsonl-algebra` (as `ja` is likely taken):

    ```bash
    pip install jsonl-algebra
    ```

2.  **For developers (from local repository):**

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

*   **Select rows where 'amount' is greater than 100:**
    ```bash
    cat data.jsonl | ja select 'amount > 100'
    ```
    ```bash
    ja select 'amount > 100' data.jsonl
    ```

*   **Project 'id' and 'name' columns:**
    ```bash
    cat data.jsonl | ja project id,name
    ```

*   **Join two files on a common key:**
    ```bash
    ja join users.jsonl orders.jsonl --on user_id=customer_id
    ```

*   **Group by 'category' and count items:**
    ```bash
    cat products.jsonl | ja groupby category --agg count
    ```

*   **Sort data by 'timestamp':**
    ```bash
    cat logs.jsonl | ja sort timestamp
    ```

**Available Commands:**

*   `select`: Filter rows based on a Python expression.
*   `project`: Select specific columns.
*   `join`: Join two relations on specified keys.
*   `rename`: Rename columns.
*   `union`: Combine two relations (all rows).
*   `difference`: Rows in the first relation but not the second.
*   `distinct`: Remove duplicate rows.
*   `intersection`: Rows common to both relations.
*   `sort` (maps to `sort_by`): Sort a relation by specified keys.
*   `product`: Cartesian product of two relations.
*   `groupby` (maps to `groupby_agg`): Group rows by a key and perform aggregations.

Use `ja <command> --help` for more details on specific commands.

## Programmatic API Usage

You can also use `ja` as a Python library:

```python
import ja

# Load data from JSONL files
# users_data = ja.read_jsonl("users.jsonl")
# orders_data = ja.read_jsonl("orders.jsonl")

# Example data (replace with ja.read_jsonl for actual files)
users_data = [
    {"user_id": 1, "name": "Alice", "status": "active", "email": "alice@example.com"},
    {"user_id": 2, "name": "Bob", "status": "inactive", "email": "bob@example.com"}
]
orders_data = [
    {"order_id": 101, "customer_id": 1, "item": "Book"},
    {"order_id": 102, "customer_id": 2, "item": "Pen"},
    {"order_id": 103, "customer_id": 1, "item": "Notebook"}
]


# Example: Select active users
active_users = ja.select(users_data, lambda row: row.get("status") == "active")
# active_users will be:
# [{'user_id': 1, 'name': 'Alice', 'status': 'active', 'email': 'alice@example.com'}]

# Example: Project name and email from active users
user_info = ja.project(active_users, ["name", "email"])
# user_info will be:
# [{'name': 'Alice', 'email': 'alice@example.com'}]

# Example: Join active users with their orders
# Ensure the join key 'user_id' is present in the projected active_users relation
active_users_with_id = ja.project(active_users, ["user_id", "name", "email"])
joined_data = ja.join(active_users_with_id, orders_data, on=[("user_id", "customer_id")])
# joined_data will be:
# [{'user_id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'order_id': 101, 'item': 'Book'},
#  {'user_id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'order_id': 103, 'item': 'Notebook'}]


# Print results (example)
for row in joined_data:
    print(row)

# Available functions mirror the CLI commands:
# ja.select, ja.project, ja.join, ja.rename, ja.union,
# ja.difference, ja.distinct, ja.intersection, ja.sort_by,
# ja.product, ja.groupby_agg
```

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

## License

This project is licensed under the MIT License.
