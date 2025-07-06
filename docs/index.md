# ja: JSONL Algebra

> **Relational algebra meets JSON streaming.** Transform your data with the power of mathematical principles and the simplicity of Unix pipes.

## What is ja?

`ja` (JSONL Algebra) is a command-line tool that brings the elegance of relational algebra to JSON data processing. It treats JSONL files as relations (tables) and provides operations that can be composed into powerful data pipelines.

```bash
# A taste of ja
cat orders.jsonl \
  | ja select 'status == "shipped"' \
  | ja join customers.jsonl --on customer_id=id \
  | ja groupby region \
  | ja agg revenue=sum(amount),orders=count
```

## Why ja?

- **🧮 Algebraic Foundation**: Based on mathematical principles that guarantee composability
- **🔗 Unix Philosophy**: Small, focused tools that do one thing well
- **📊 Streaming Architecture**: Process gigabytes without loading into memory
- **🎯 Nested Data Support**: First-class support for real-world JSON structures
- **⚡ Zero Dependencies**: Pure Python implementation (with optional enhancements)

## Quick Links

- [**Quickstart →**](quickstart.md) Get running in 5 minutes
- [**Concepts →**](concepts/jsonl-algebra.md) Understand the theory
- [**Operations →**](operations/overview.md) Learn each operation
- [**Cookbook →**](cookbook/log-analysis.md) Real-world examples

## At a Glance

### The Operations

| Operation | Symbol | Purpose | Example |
|-----------|---------|---------|---------|
| **select** | σ | Filter rows | `ja select 'age > 30'` |
| **project** | π | Select columns | `ja project name,email` |
| **join** | ⋈ | Combine relations | `ja join users.jsonl orders.jsonl --on id=user_id` |
| **groupby** | γ | Group rows | `ja groupby department` |
| **union** | ∪ | Combine all rows | `ja union file1.jsonl file2.jsonl` |
| **distinct** | δ | Remove duplicates | `ja distinct` |

### The Philosophy

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌────────┐
│  Data   │ --> │ Filter  │ --> │  Join   │ --> │ Result │
│ (JSONL) │     │(select) │     │ (join)  │     │(JSONL) │
└─────────┘     └─────────┘     └─────────┘     └────────┘
     ↓               ↓               ↓               ↓
  Relation  -->  Relation  -->  Relation  -->  Relation
```

Every operation takes relations and produces relations. This closure property enables infinite composability.

## Installation

```bash
pip install jsonl-algebra
```

That's it! You now have the `ja` command available.

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

### 4. Multi-Level Grouping

Our innovative chained groupby enables complex analytics:

```bash
cat sales.jsonl \
  | ja groupby region \      # First level grouping
  | ja groupby product \     # Second level grouping  
  | ja agg total=sum(amount) # Final aggregation
```

This produces results like:
```json
{"region": "North", "product": "Widget", "total": 1250}
{"region": "North", "product": "Gadget", "total": 850}
{"region": "South", "product": "Widget", "total": 900}
```

## Key Features

- **Relational Operations**: select, project, join, union, intersection, difference, distinct, and more
- **Chained Grouping**: Multi-level grouping that preserves composability
- **Nested Data Support**: Access and manipulate nested fields using intuitive dot notation
- **Streaming Architecture**: Process large datasets without loading into memory
- **Expression Language**: Safe and expressive filtering with ExprEval
- **Interactive REPL**: Build data pipelines step-by-step interactively
- **Format Conversion**: Import/export CSV, JSON arrays, and directory structures
- **Unix Philosophy**: Designed for pipes and command composition

## Working with Nested Data

`ja` makes working with nested JSON objects effortless:

```bash
# Project nested fields
ja project user.name,user.email,order.total data.jsonl

# Group by nested values
ja groupby user.region orders.jsonl | ja agg revenue=sum(amount)

# Filter on nested conditions
ja select 'user.age > 30 and order.status == "shipped"' data.jsonl
```

## Interactive Mode

Want to explore? Try the REPL:

```bash
ja repl

ja> from orders.jsonl
ja> select amount > 100
ja> groupby customer
ja> agg total=sum(amount)
ja> execute
```

## Next Steps

1. **[Read the Quickstart](quickstart.md)** - Get hands-on in 5 minutes
2. **[Explore the Concepts](concepts/jsonl-algebra.md)** - Understand the theory
3. **[Browse the Cookbook](cookbook/log-analysis.md)** - See real examples
4. **[Join the Community](https://github.com/queelius/jsonl-algebra)** - Contribute and get help

## Dependencies and Setup

`ja` includes optional dependencies for enhanced functionality:

- **jmespath**: For safe and expressive filtering (replaces eval)
- **jsonschema**: For schema validation features
- All other features work without external dependencies

### For users (from PyPI)

```bash
pip install jsonl-algebra
```

This automatically installs the required dependencies.

### For developers (from local repository)

```bash
# Standard installation
pip install .

# Editable mode for development
pip install -e .
```

---

**Ready to transform your JSON data?** Start with the [quickstart guide](quickstart.md) or dive into the [concepts](concepts/jsonl-algebra.md) to understand the theory behind the tool.
