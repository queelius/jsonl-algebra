# `ja`: The JSONL Algebra Toolkit 🚀

**Your friendly, powerful command-line tool for wrangling JSONL data.**

[![CI](https://github.com/your-repo/ja/actions/workflows/ci.yml/badge.svg)](https://github.com/your-repo/ja/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/jsonl-algebra.svg)](https://badge.fury.io/py/jsonl-algebra)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Tired of wrestling with complex JSON in the command line? **`ja`** is here to help. It brings the power of relational algebra to your terminal, making it effortless to slice, dice, and transform streams of JSONL data. Think of it as `sed`, `awk`, and `SQL` rolled into one, but designed specifically for the structure and complexity of modern JSON.

## Key Features

* **Intuitive Relational Operations**: All the classics are here: `select`, `project`, `join`, `union`, `difference`, `distinct`, and more.
* **Seamless Nested Data Support**: Access and manipulate nested fields using simple, intuitive **dot notation** (e.g., `user.address.city`). This is a core design principle.
* **Powerful Aggregations**: Group your data with `groupby` and calculate `sum`, `avg`, `min`, `max`, `count`, or see all values in a `list`.
* **Interactive REPL**: Jump into an interactive session with `ja repl` to build and test complex pipelines step-by-step.
* **Schema Inference & Validation**: Automatically discover the schema of your data with `schema infer` and validate it against a known structure.
* **Format Conversion**: Easily convert data to and from CSV, including intelligent flattening of nested structures.
* **Built for Pipes**: `ja` is a good Unix citizen. It reads from `stdin`, writes to `stdout`, and is designed to be a component in larger shell pipelines.

## Getting Started: A 5-Minute Tour

Let's see how `ja` can solve a real-world problem. Imagine you have two data files: `users.jsonl` and `orders.jsonl`.

**`users.jsonl`**

```json
{"user": {"id": 1, "name": "Alice"}}
{"user": {"id": 2, "name": "Bob"}}
```

**`orders.jsonl`**

```json
{"order_id": 101, "customer_id": 1, "amount": 50}
{"order_id": 102, "customer_id": 1, "amount": 75}
{"order_id": 103, "customer_id": 2, "amount": 120}
```

**Goal**: Find the total amount spent by each user.

### Step 1: Join the Datasets

First, we'll join the two files. Notice how we can reach into the nested `user` object with `user.id` to connect it to the `customer_id` in the orders file.

```bash
ja join users.jsonl orders.jsonl --on user.id=customer_id
```

This command produces a stream of the combined data:

```json
{"user": {"id": 1, "name": "Alice"}, "order_id": 101, "customer_id": 1, "amount": 50}
{"user": {"id": 1, "name": "Alice"}, "order_id": 102, "customer_id": 1, "amount": 75}
{"user": {"id": 2, "name": "Bob"}, "order_id": 103, "customer_id": 2, "amount": 120}
```

### Step 2: Group and Sum

Now, we can pipe (`|`) the result of our join directly into the `groupby` command. We'll group by the user's name and ask for the `sum` of the `amount` field.

```bash
ja join users.jsonl orders.jsonl --on user.id=customer_id \
  | ja groupby user.name --agg sum:amount
```

**Final Result:**

```json
{"user.name": "Alice", "sum_amount": 125}
{"user.name": "Bob", "sum_amount": 120}
```

And there you have it! A sophisticated data transformation performed with a clear, readable command-line pipeline.

## Installation

Installing `ja` is as simple as a single `pip` command.

```bash
pip install jsonl-algebra
```

This gives you the `ja` command-line tool and the Python library.

## Core Concepts

### Dot Notation for Nested Data

You can access nested values in any command by separating keys with a dot.

> **`project`**: `ja project user.name,user.address.city ...`
>
> **`select`**: `ja select 'user.age > 30' ...`
>
> **`join`**: `ja join ... --on user.id=order.user_id`
>
> **`groupby`**: `ja groupby user.location ...`

### Streaming and Piping

Every `ja` command that processes a single stream of data can read from `stdin` and writes to `stdout`. This lets you build powerful workflows by chaining commands together with the standard Unix pipe (`|`).

```bash
cat data.jsonl | ja select ... | ja groupby ... | ja sort ...
```

## CLI Command Examples

Here are a few quick examples of common commands.

### **Filtering Data**

```bash
# Select rows where the 'status' field is 'active'
ja select 'status == `"active"`' data.jsonl

# Select rows where a nested 'age' is over 30
ja select 'user.age > 30' data.jsonl
```

### **Reshaping Data**

```bash
# Pick only the 'id' and 'name' fields
ja project id,name data.jsonl

# Rename 'id' to 'user_id' and a nested 'loc' to 'location'
ja rename id=user_id,user.loc=user.location data.jsonl
```

### **Grouping and Aggregating**

```bash
# Group by category and get the count for each
ja groupby category --agg count data.jsonl

# Group by location and find the average score
ja groupby location --agg avg:score data.jsonl
```

### **Working with Schemas**

```bash
# Automatically infer a JSON Schema from your data
ja schema infer my_data.jsonl > my_schema.json

# Validate that a file conforms to a schema
ja schema validate my_schema.json my_data.jsonl
```

## Programmatic API Usage

You can also use `ja` as a Python library to bring its power into your scripts.

```python
from ja.core import read_jsonl, join, groupby_agg
from ja.schema import infer_schema

# Load data from JSONL files
users = read_jsonl("users.jsonl")
orders = read_jsonl("orders.jsonl")

# Join the data on the user ID
joined_data = join(users, orders, on=[("user.id", "customer_id")])

# Group by user name and sum the order amounts
# The result is an iterator, so we wrap it in list() to print.
result = groupby_agg(
    joined_data,
    group_by_key="user.name",
    aggregations=[("sum", "amount")]
)

print(list(result))
# Output:
# [{'user.name': 'Alice', 'sum_amount': 125},
#  {'user.name': 'Bob', 'sum_amount': 120}]

# You can also infer a schema directly
users_schema = infer_schema(users)
print(users_schema)
```

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

## License

This project is licensed under the MIT License.
