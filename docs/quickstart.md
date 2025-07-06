# Quickstart: Your First 5 Minutes with ja

## Installation

```bash
pip install jsonl-algebra
```

Verify it's working:

```bash
ja --version
```

## Your First Pipeline

Let's analyze some order data. Create `orders.jsonl`:

```json
{"order_id": 1, "customer": "Alice", "amount": 99.99, "status": "shipped"}
{"order_id": 2, "customer": "Bob", "amount": 149.99, "status": "pending"}
{"order_id": 3, "customer": "Alice", "amount": 79.99, "status": "shipped"}
{"order_id": 4, "customer": "Charlie", "amount": 199.99, "status": "shipped"}
{"order_id": 5, "customer": "Bob", "amount": 59.99, "status": "cancelled"}
```

### 1. Filter Orders

Get only shipped orders:

```bash
ja select 'status == "shipped"' orders.jsonl
```

### 2. Calculate Totals

Total revenue from shipped orders:

```bash
ja select 'status == "shipped"' orders.jsonl | ja agg total=sum(amount)
```

Output:

```json
{"total": 379.97}
```

### 3. Group by Customer

Revenue per customer (shipped only):

```bash
ja select 'status == "shipped"' orders.jsonl \
  | ja groupby customer \
  | ja agg revenue=sum(amount),orders=count
```

Output:

```json
{"customer": "Alice", "revenue": 179.98, "orders": 2}
{"customer": "Charlie", "revenue": 199.99, "orders": 1}
```

### 4. Join with Customer Data

Create `customers.jsonl`:

```json
{"name": "Alice", "tier": "gold", "region": "west"}
{"name": "Bob", "tier": "silver", "region": "east"}
{"name": "Charlie", "tier": "gold", "region": "west"}
```

Join and analyze:

```bash
ja join customers.jsonl orders.jsonl --on name=customer \
  | ja select 'status == "shipped"' \
  | ja groupby region \
  | ja agg revenue=sum(amount)
```

Output:

```json
{"region": "west", "revenue": 379.97}
```

### 5. Multi-Level Grouping

Showcase the power of chained groupby:

```bash
ja select 'status == "shipped"' orders.jsonl \
  | ja groupby customer \
  | ja groupby amount \
  | ja agg count
```

Output:

```json
{"customer": "Alice", "amount": 99.99, "count": 1}
{"customer": "Alice", "amount": 79.99, "count": 1}
{"customer": "Charlie", "amount": 199.99, "count": 1}
```

## Key Concepts Demonstrated

1. **Filtering**: Use `select` with expressions
2. **Aggregation**: Use `agg` for calculations
3. **Grouping**: Use `groupby` to segment data
4. **Joining**: Combine data from multiple files
5. **Chaining**: Use pipes to build complex pipelines
6. **Multi-level Grouping**: Chain groupby operations for hierarchical analysis

## Interactive Mode

Want to explore? Try the REPL:

```bash
ja repl

ja> from orders.jsonl
Input source set to: orders.jsonl
ja> select status == "shipped"
Added: select status == "shipped"
ja> groupby customer
Added: groupby customer
ja> agg revenue=sum(amount)
Added: agg revenue=sum(amount)
ja> execute
Executing: ja select 'status == "shipped"' orders.jsonl | ja groupby customer - | ja agg revenue=sum(amount) -

--- Output ---
{"customer": "Alice", "revenue": 179.98}
{"customer": "Charlie", "revenue": 199.99}
--------------
```

## Common Patterns

### Data Exploration

```bash
# See the structure
ja project customer,amount orders.jsonl | head -5

# Find unique values
ja project status orders.jsonl | ja distinct

# Quick statistics
ja agg count,avg_amount=avg(amount),total=sum(amount) orders.jsonl
```

### Filtering and Aggregation

```bash
# Conditional aggregation
ja agg shipped_revenue=sum_if(amount,status=="shipped") orders.jsonl

# Top customers
ja groupby customer orders.jsonl \
  | ja agg total=sum(amount) \
  | ja sort total --desc \
  | head -5
```

### Working with Nested Data

```bash
# Assuming nested structure like {"user": {"id": 1, "name": "Alice"}}
ja project user.name,user.id nested.jsonl
ja groupby user.region nested.jsonl | ja agg count
ja select 'user.age > 30' nested.jsonl
```

## What's Next?

- [Learn all operations →](../operations/overview.md)
- [Work with nested data →](../tutorials/nested-data.md)
- [Understand the theory →](../concepts/jsonl-algebra.md)
- [See real examples →](../cookbook/log-analysis.md)

## Getting Help

```bash
# General help
ja --help

# Operation help
ja select --help
ja groupby --help
ja agg --help

# Interactive help
ja repl
ja> help
```

Welcome to the world of JSONL algebra! 🎉
