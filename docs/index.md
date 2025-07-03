# Welcome to ja - Your Friendly JSONL Toolkit

Tired of wrestling with complex JSON data in the command line? **ja** (JSONL Algebra) is a powerful and user-friendly command-line tool for slicing, dicing, and transforming JSONL data. Think of it as `SQL` or `pandas` for JSON, but living right in your shell.

It provides a suite of relational algebra operations (`join`, `select`, `project`, etc.), powerful `groupby` aggregations, and seamless support for nested data structures using simple dot notation.

## Getting Started: A 5-Minute Tour

Let's say you have two files: `users.jsonl` and `orders.jsonl`.

**`users.jsonl`**:

```json
{"user": {"id": 1, "name": "Alice"}}
{"user": {"id": 2, "name": "Bob"}}
```

**`orders.jsonl`**:

```json
{"order_id": 101, "customer_id": 1, "amount": 50}
{"order_id": 102, "customer_id": 1, "amount": 75}
{"order_id": 103, "customer_id": 2, "amount": 120}
```

Our goal is to find the total amount spent by each user.

### Step 1: Join the files

We can join these two files on the user ID. Notice how `ja` can access the nested `user.id` field directly with dot notation.

```bash
ja join users.jsonl orders.jsonl --on user.id=customer_id
```

This gives us:

```json
{"user": {"id": 1, "name": "Alice"}, "order_id": 101, "amount": 50}
{"user": {"id": 1, "name": "Alice"}, "order_id": 102, "amount": 75}
{"user": {"id": 2, "name": "Bob"}, "order_id": 103, "amount": 120}
```

### Step 2: Group and Sum

Now, we can pipe this result into `groupby` to sum the amounts for each user name.

```bash
ja join users.jsonl orders.jsonl --on user.id=customer_id \
  | ja groupby user.name --agg "total=sum(amount)"
```

**Result:**

```json
{"user.name": "Alice", "total": 125}
{"user.name": "Bob", "total": 120}
```

And just like that, you've performed a complex data analysis task directly in your terminal!

## Key Features

* **Relational Operations**: `select`, `project`, `join`, `union`, `intersection`, `difference`, `distinct`, and more.
* **Seamless Nested Data Support**: Access and manipulate nested fields using intuitive **dot notation** everywhere.
* **Powerful Aggregations**: A `groupby` command with `sum`, `avg`, `min`, `max`, `count`, `list`, and custom aggregations.
* **Safe Filtering**: Uses JMESPath for expressive and safe filtering, no more risky `eval()`.
* **Schema Management**: Infer and validate data schemas with `schema infer` and `schema validate`.
* **Interactive REPL**: An interactive shell (`ja repl`) for building data pipelines step-by-step.
* **Broad Format Support**: Convert to/from CSV, JSON arrays, and directories of JSON files.
* **Fully Pipeable**: Designed from the ground up to work with standard Unix pipes (`|`).

## Working with Nested Data

`ja` makes working with nested JSON objects feel effortless.

**Projecting Nested Fields:**

```bash
# Given: {"user": {"id": 1, "name": "Alice"}, "status": "active"}
ja project user.name,status data.jsonl
# Output: {"user": {"name": "Alice"}, "status": "active"}
```

You can also flatten the result:

```bash
ja project user.name,status --flatten data.jsonl
# Output: {"user.name": "Alice", "status": "active"}
```

**Renaming Nested Fields:**

```bash
# Given: {"person": {"name": "Alice"}}
ja rename person.name=person.fullName data.jsonl
# Output: {"person": {"fullName": "Alice"}}
```

## Installation

### Dependencies

`ja` now includes optional dependencies for enhanced functionality:

* **jmespath**: For safe and expressive filtering (replaces eval)
* **jsonschema**: For schema validation features
* All other features work without external dependencies

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
