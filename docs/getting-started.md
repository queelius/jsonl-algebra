# Getting Started with `ja`

Welcome to `ja`, the JSONL Algebra toolkit! This guide will walk you through the basics of using `ja` to manipulate JSONL data with the power of relational algebra.

## What is JSONL?

JSONL (JSON Lines) is a convenient format for storing structured data where each line is a valid JSON object. It's perfect for streaming and processing large datasets because you can read it line by line.

Example JSONL file:
```json
{"name": "Alice", "age": 30, "city": "New York"}
{"name": "Bob", "age": 25, "city": "San Francisco"}
{"name": "Charlie", "age": 35, "city": "New York"}
```

## Installation

```bash
pip install jsonl-algebra
```

This installs the `ja` command-line tool and the Python library.

## Your First Commands

### 1. Viewing Data

The simplest operation is to view your data:

```bash
cat data.jsonl
```

### 2. Selecting Rows (Filtering)

Use `select` to filter rows based on conditions:

```bash
# Select people over 30
ja select 'age > `30`' data.jsonl

# Select people from New York
ja select 'city == `"New York"`' data.jsonl
```

### 3. Projecting Columns

Use `project` to select specific fields:

```bash
# Get just names and ages
ja project name,age data.jsonl

# Project nested fields
ja project user.name,user.email users.jsonl
```

### 4. Working with Nested Data

`ja` excels at handling nested JSON structures:

```bash
# Given nested data like:
# {"person": {"name": {"first": "Alice", "last": "Smith"}, "age": 30}}

# Project nested fields
ja project person.name.first data.jsonl

# Flatten nested structures
ja project person.name.first,person.age --flatten data.jsonl
# Output: {"person.name.first": "Alice", "person.age": 30}
```

## A Complete Example

Let's walk through a real-world scenario. You have two files:

**users.jsonl**:
```json
{"id": 1, "name": "Alice", "department": "Sales"}
{"id": 2, "name": "Bob", "department": "Engineering"}
{"id": 3, "name": "Charlie", "department": "Sales"}
```

**sales.jsonl**:
```json
{"user_id": 1, "amount": 1000, "date": "2024-01-01"}
{"user_id": 1, "amount": 1500, "date": "2024-01-02"}
{"user_id": 2, "amount": 2000, "date": "2024-01-01"}
{"user_id": 3, "amount": 1200, "date": "2024-01-01"}
```

**Goal**: Find total sales by department.

```bash
# Step 1: Join users with their sales
ja join users.jsonl sales.jsonl --on id=user_id > joined.jsonl

# Step 2: Group by department and sum amounts
ja groupby department --agg total=sum(amount) joined.jsonl

# Or do it all in one pipeline:
ja join users.jsonl sales.jsonl --on id=user_id | \
  ja groupby department --agg total=sum(amount)
```

Output:
```json
{"department": "Sales", "total": 3700}
{"department": "Engineering", "total": 2000}
```

## Command Chaining

One of `ja`'s most powerful features is the ability to chain commands using Unix pipes:

```bash
# Complex data pipeline
cat data.jsonl | \
  ja select 'status == `"active"`' | \
  ja project user.name,amount | \
  ja sort amount --desc | \
  head -10
```

## Common Patterns

### 1. Finding Unique Values

```bash
ja project category data.jsonl | ja distinct
```

### 2. Data Validation

```bash
# Find records with missing fields
ja select '!email' users.jsonl
```

### 3. Computing Statistics

```bash
# Average order amount by customer
ja groupby customer_id --agg avg_amount=avg(amount) orders.jsonl
```

### 4. Data Transformation

```bash
# Rename fields
ja rename old_name=new_name,price=cost data.jsonl

# Flatten nested structures for export
ja project user.name,user.email,order.total --flatten data.jsonl | \
  ja export csv > output.csv
```

## Next Steps

- Learn about [Advanced Operations](docs/advanced.md)
- Explore the [Interactive REPL](docs/repl.md)
- Read about [Schema Management](docs/schema.md)
- Check out the [API Reference](docs/reference.md)

## Getting Help

```bash
# General help
ja --help

# Command-specific help
ja select --help
ja groupby --help
```

Remember: `ja` is designed to be intuitive. If you know SQL or have used Unix tools like `awk` or `sed`, you'll feel right at home!