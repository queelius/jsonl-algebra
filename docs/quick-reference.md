# Quick Reference

## Basic Operations

### Select (Filter)
```bash
# Filter with Python expression
ja select 'age > 25' data.jsonl

# Complex conditions
ja select 'age > 25 and status == "active"' data.jsonl
```

### Project (Select Columns)
```bash
# Select specific fields
ja project name,age,email data.jsonl

# From stdin
cat data.jsonl | ja project id,name
```

### Join
```bash
# Inner join on matching columns
ja join users.jsonl orders.jsonl --on id=user_id

# Multi-column join
ja join left.jsonl right.jsonl --on "id=id,date=order_date"
```

## Set Operations

### Union
```bash
# Combine two datasets (keeps duplicates)
ja union file1.jsonl file2.jsonl
```

### Intersection
```bash
# Find common rows
ja intersection file1.jsonl file2.jsonl
```

### Difference
```bash
# Rows in first but not second
ja difference file1.jsonl file2.jsonl
```

### Distinct
```bash
# Remove duplicates
ja distinct data.jsonl
```

## Aggregations

### GroupBy
```bash
# Count by category
ja groupby category --agg count data.jsonl

# Multiple aggregations
ja groupby department --agg count --agg sum:salary --agg avg:age data.jsonl

# Available aggregations: count, sum, avg, min, max, list, first, last
```

### Sort
```bash
# Sort by single column
ja sort age data.jsonl

# Sort by multiple columns
ja sort department,salary data.jsonl
```

## JSONPath Operations

### Select with JSONPath
```bash
# Filter by nested field
ja select-path '$.user.age' data.jsonl --predicate 'lambda x: x > 25'

# Check if any item matches
ja select-any '$.items[*].inStock' data.jsonl --predicate 'lambda x: x == True'

# Check if all items match
ja select-all '$.scores[*]' data.jsonl --predicate 'lambda x: x >= 80'

# Check if no items match
ja select-none '$.errors[*]' data.jsonl --predicate 'lambda x: x is not None'
```

### Project with Templates
```bash
# Extract and reshape data
ja project-template '{"name": "$.user.name", "total": "sum($.orders[*].amount)"}' data.jsonl
```

## Streaming & Performance

### Automatic Streaming
These operations automatically stream (constant memory):
- select
- project
- rename
- union
- distinct (tracks seen items)

### Memory-Intensive Operations
These require loading data into memory:
- join
- sort
- groupby
- intersection
- difference

### Windowed Processing
For large datasets, use `--window-size` for approximate results:
```bash
# Sort in windows
ja sort timestamp --window-size 1000 huge.jsonl

# Windowed groupby
ja groupby category --agg sum:amount --window-size 500 huge.jsonl
```

## Piping & Chaining

```bash
# Chain operations with pipes
cat data.jsonl | \
  ja select 'age > 25' | \
  ja project name,age,department | \
  ja sort age | \
  ja distinct

# Complex pipeline
ja select 'status == "active"' users.jsonl | \
  ja join - orders.jsonl --on id=user_id | \
  ja groupby user_id --agg count --agg sum:amount | \
  ja sort sum_amount
```

## Python API

```python
import ja

# Load data
data = ja.read_jsonl("data.jsonl")

# Basic operations
filtered = ja.select(data, lambda row: row["age"] > 25)
projected = ja.project(filtered, ["name", "age"])
sorted_data = ja.sort_by(projected, ["age"])

# Joins
users = ja.read_jsonl("users.jsonl")
orders = ja.read_jsonl("orders.jsonl")
joined = ja.join(users, orders, on=[("id", "user_id")])

# GroupBy
grouped = ja.groupby_agg(
    joined,
    group_by_key="user_id",
    aggregations=[
        ("count", ""),
        ("sum", "amount"),
        ("list", "product_id")
    ]
)

# JSONPath
active_users = ja.select_path(data, "$.status", lambda x: x == "active")
high_scorers = ja.select_all(data, "$.scores[*]", lambda x: x >= 90)

# Write results
ja.write_jsonl(grouped, "output.jsonl")
```