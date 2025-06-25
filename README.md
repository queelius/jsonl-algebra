# ja - JSONL Algebra

**ja** is a powerful command-line tool and Python library for performing relational algebra operations on JSONL (JSON Lines) data. It's designed to be a robust, feature-rich alternative for data manipulation tasks, inspired by tools like `jq` and traditional SQL.

## Features

* **Relational Operations**: Perform common relational algebra operations: select, project, join, union, intersection, difference, distinct, sort, product, and group by with aggregations.

* **Advanced Filtering**: Uses JMESPath expressions for safe and expressive row filtering (instead of eval).

* **Schema Management**: 
  * `schema infer`: Automatically infer JSON Schema from JSONL data
  * `schema validate`: Validate JSONL data against JSON Schema files

* **Interactive REPL**: Build and test data pipelines interactively with auto-completion and pipeline compilation.

* **Import/Export**: Comprehensive data format conversion capabilities:
  * CSV import/export with type inference and flattening
  * JSON array conversion
  * Directory explode/implode for individual JSON files
  * Custom column transformations for CSV export

* **Aggregations**: Powerful `groupby` feature with built-in aggregations:
  * `sum`, `avg`, `min`, `max`, `count`, `list`, `first`, `last`
  * Extensible aggregation system for custom functions

* **Fully Pipeable**: Consistent support for stdin/stdout with `-` notation for maximum composability.

* **Type-Safe**: Optional type inference for CSV imports and robust schema validation.

## Installation

### Dependencies

`ja` now includes optional dependencies for enhanced functionality:

- **jmespath**: For safe and expressive filtering (replaces eval)
- **jsonschema**: For schema validation features
- All other features work without external dependencies

### For users (from PyPI)

You can install the package directly from PyPI (Python Package Index) using pip.

```bash
pip install jsonl-algebra
```

This will automatically install the required dependencies (`jmespath` and `jsonschema`).

### For developers (from local repository)

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

**Select rows where 'amount' is greater than 100 (using JMESPath):**

```bash
cat data.jsonl | ja select 'amount > `100`'
ja select 'amount > `100`' data.jsonl
```

**Project 'id' and 'name' columns:**

```bash
cat data.jsonl | ja project id,name
```

**Join two files on a common key:**

```bash
ja join users.jsonl orders.jsonl --on user_id=customer_id
```

**Group by 'category' and count items:**

```bash
cat products.jsonl | ja groupby category --agg count
```

**Sort data by 'timestamp' in descending order:**

```bash
cat logs.jsonl | ja sort timestamp --desc
```

**Schema Operations:**

```bash
# Infer schema from JSONL data
ja schema infer data.jsonl

# Validate JSONL data against a schema
ja schema validate schema.json data.jsonl

# Pipeline: infer schema from filtered data, then validate original file
ja select 'active == `true`' users.jsonl | ja schema infer | ja schema validate - users.jsonl
```

**Interactive REPL session:**

```bash
ja repl
```

Inside the REPL:

```text
ja> from data.jsonl
ja> select 'age > `30`'
ja> project name,email
ja> execute --lines=5 
ja> compile
```

**Export/Import Operations:**

```bash
# Convert JSONL to JSON array
ja export array data.jsonl > data.json

# Convert JSON array back to JSONL
ja export jsonl data.json > data.jsonl

# Explode JSONL into individual JSON files
ja export explode data.jsonl -o data_exploded

# Implode directory back to JSONL
ja import implode data_exploded --add-filename-key source_file > combined.jsonl

# Export to CSV with flattening and custom transformations
ja export csv data.jsonl --apply timestamp "lambda t: t.split('T')[0]" > data.csv

# Import CSV with type inference
ja import csv data.csv --infer-types > data.jsonl
```

### Available Commands

**Core Operations:**
* `select`: Filter rows using JMESPath expressions (safe alternative to eval)
* `project`: Select specific columns
* `join`: Join two relations on specified keys
* `rename`: Rename columns
* `union`: Combine two relations (all rows)
* `difference`: Rows in the first relation but not the second
* `distinct`: Remove duplicate rows
* `intersection`: Rows common to both relations
* `sort`: Sort a relation by specified keys (supports `--desc` for descending order)
* `product`: Cartesian product of two relations
* `groupby`: Group rows by a key and perform aggregations

**Schema Operations:**
* `schema infer`: Infer and display JSON Schema from JSONL data
* `schema validate`: Validate JSONL data against a JSON Schema file (supports piping schemas)

**Interactive Tools:**
* `repl`: Interactive REPL for building and testing data pipelines

**Export Operations:**
* `export array`: Convert JSONL to a single JSON array
* `export jsonl`: Convert a JSON array to JSONL
* `export explode`: Export each JSONL line to a separate JSON file in a directory
* `export csv`: Convert JSONL to CSV with automatic flattening and custom transformations

**Import Operations:**
* `import csv`: Convert CSV to JSONL with optional type inference
* `import implode`: Combine JSON files from a directory into JSONL

**Pipeline-Friendly Design:**
All commands support stdin/stdout and the `-` notation for maximum composability in shell pipelines.

Use `ja <command> --help` or `ja export <subcommand> --help` for more details on specific commands.

## Programmatic API Usage

You can also use `ja` as a Python library:

```python
import ja
import jmespath
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

# Example: Select active users using JMESPath
expression = jmespath.compile("[?status == 'active']")
active_users = ja.select(users_data, expression)
# active_users will be:
# [{'user_id': 1, 'name': 'Alice', 'status': 'active', 'email': 'alice@example.com'},
#  {'user_id': 1, 'name': 'Alice', 'status': 'active', 'email': 'alice@example.com'}]

# Example: Project name and email from distinct active users
distinct_active_users = ja.distinct(active_users)
user_info = ja.project(distinct_active_users, ["name", "email"])
# user_info will be:
# [{'name': 'Alice', 'email': 'alice@example.com'}]

# Example: Schema inference
from ja.schema import infer_schema
schema = infer_schema(users_data)
# Returns a valid JSON Schema with inferred types and required fields

# Example: Join distinct active users with their orders
active_users_with_id = ja.project(distinct_active_users, ["user_id", "name", "email"])
joined_data = ja.join(active_users_with_id, orders_data, on=[("user_id", "customer_id")])

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

# Available functions mirror the CLI commands:
# ja.select, ja.project, ja.join, ja.rename, ja.union,
# ja.difference, ja.distinct, ja.intersection, ja.sort_by,
# ja.product, ja.groupby_agg
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

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

## License

This project is licensed under the MIT License.
