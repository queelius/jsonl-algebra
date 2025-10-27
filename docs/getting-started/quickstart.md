# Quick Start Guide

Get up and running with **jsonl-algebra** in just 5 minutes! This hands-on tutorial will teach you the essentials through practical examples.

## What You'll Learn

By the end of this guide, you'll be able to:

- Filter JSON data with `select`
- Choose specific fields with `project`
- Combine datasets with `join`
- Calculate aggregates with `groupby`
- Build powerful data pipelines

**Time required:** 5-10 minutes

!!! tip "Follow Along"
    Copy and paste the commands into your terminal to see them in action!

## Setup: Create Sample Data

First, let's create some sample data files to work with:

```bash
# Create a users file
cat > users.jsonl << 'EOF'
{"user_id": 1, "name": "Alice", "age": 30, "city": "NYC", "role": "engineer"}
{"user_id": 2, "name": "Bob", "age": 25, "city": "SF", "role": "designer"}
{"user_id": 3, "name": "Charlie", "age": 35, "city": "NYC", "role": "manager"}
{"user_id": 4, "name": "Diana", "age": 28, "city": "LA", "role": "engineer"}
{"user_id": 5, "name": "Eve", "age": 32, "city": "SF", "role": "engineer"}
EOF

# Create an orders file
cat > orders.jsonl << 'EOF'
{"order_id": 101, "user_id": 1, "amount": 250, "status": "shipped"}
{"order_id": 102, "user_id": 1, "amount": 175, "status": "shipped"}
{"order_id": 103, "user_id": 2, "amount": 420, "status": "pending"}
{"order_id": 104, "user_id": 3, "amount": 890, "status": "shipped"}
{"order_id": 105, "user_id": 4, "amount": 325, "status": "cancelled"}
{"order_id": 106, "user_id": 5, "amount": 560, "status": "shipped"}
EOF
```

Great! Now you have two JSONL files to practice with.

## Lesson 1: Filtering Data with `select`

The `select` operation filters rows based on a condition.

### Example 1: Find Engineers

```bash
ja select 'role == "engineer"' users.jsonl
```

**Output:**
```json
{"user_id": 1, "name": "Alice", "age": 30, "city": "NYC", "role": "engineer"}
{"user_id": 4, "name": "Diana", "age": 28, "city": "LA", "role": "engineer"}
{"user_id": 5, "name": "Eve", "age": 32, "city": "SF", "role": "engineer"}
```

### Example 2: Find Users Over 30

```bash
ja select 'age > 30' users.jsonl
```

**Output:**
```json
{"user_id": 3, "name": "Charlie", "age": 35, "city": "NYC", "role": "manager"}
{"user_id": 5, "name": "Eve", "age": 32, "city": "SF", "role": "engineer"}
```

### Example 3: Combine Conditions

```bash
ja select 'role == "engineer" and city == "NYC"' users.jsonl
```

**Output:**
```json
{"user_id": 1, "name": "Alice", "age": 30, "city": "NYC", "role": "engineer"}
```

!!! tip "Expression Syntax"
    Use `==` for equality, `>`, `<`, `>=`, `<=` for comparisons, and `and`/`or` for logic.

## Lesson 2: Choosing Fields with `project`

The `project` operation selects specific fields from each record.

### Example 1: Get Names and Ages

```bash
ja project name,age users.jsonl
```

**Output:**
```json
{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}
{"name": "Diana", "age": 28}
{"name": "Eve", "age": 32}
```

### Example 2: Single Field

```bash
ja project name users.jsonl
```

**Output:**
```json
{"name": "Alice"}
{"name": "Bob"}
{"name": "Charlie"}
{"name": "Diana"}
{"name": "Eve"}
```

## Lesson 3: Building Pipelines

The real power comes from chaining operations together with pipes (`|`).

### Example: Engineers' Names in NYC

```bash
ja select 'city == "NYC"' users.jsonl | ja project name,role
```

**Output:**
```json
{"name": "Alice", "role": "engineer"}
{"name": "Charlie", "role": "manager"}
```

### Example: Filter and Count

```bash
ja select 'status == "shipped"' orders.jsonl | wc -l
```

**Output:** `4` (four shipped orders)

## Lesson 4: Joining Datasets

Combine data from multiple files with `join`.

### Example: Users with Their Orders

```bash
ja join users.jsonl orders.jsonl --on user_id=user_id
```

**Output:**
```json
{"user_id": 1, "name": "Alice", "age": 30, "city": "NYC", "role": "engineer", "order_id": 101, "amount": 250, "status": "shipped"}
{"user_id": 1, "name": "Alice", "age": 30, "city": "NYC", "role": "engineer", "order_id": 102, "amount": 175, "status": "shipped"}
{"user_id": 2, "name": "Bob", "age": 25, "city": "SF", "role": "designer", "order_id": 103, "amount": 420, "status": "pending"}
...
```

### Example: Join and Filter

Get only shipped orders with user info:

```bash
ja join users.jsonl orders.jsonl --on user_id=user_id \
  | ja select 'status == "shipped"' \
  | ja project name,amount,city
```

**Output:**
```json
{"name": "Alice", "amount": 250, "city": "NYC"}
{"name": "Alice", "amount": 175, "city": "NYC"}
{"name": "Charlie", "amount": 890, "city": "NYC"}
{"name": "Eve", "amount": 560, "city": "SF"}
```

## Lesson 5: Grouping and Aggregating

Calculate statistics by grouping data.

### Example 1: Count by City

```bash
ja groupby city --agg count users.jsonl
```

**Output:**
```json
{"city": "NYC", "count": 2}
{"city": "SF", "count": 2}
{"city": "LA", "count": 1}
```

### Example 2: Sum Order Amounts by User

```bash
ja groupby user_id --agg total=sum:amount orders.jsonl
```

**Output:**
```json
{"user_id": 1, "total": 425}
{"user_id": 2, "total": 420}
{"user_id": 3, "total": 890}
{"user_id": 4, "total": 325}
{"user_id": 5, "total": 560}
```

### Example 3: Multiple Aggregates

```bash
ja groupby city --agg count,avg_age=avg:age users.jsonl
```

**Output:**
```json
{"city": "NYC", "count": 2, "avg_age": 32.5}
{"city": "SF", "count": 2, "avg_age": 28.5}
{"city": "LA", "count": 1, "avg_age": 28.0}
```

## Putting It All Together

Let's solve a real-world problem: **Find the total revenue per city from shipped orders**.

### Step-by-Step Solution

1. **Join** users and orders
2. **Filter** for shipped orders only
3. **Group** by city
4. **Sum** the amounts

```bash
ja join users.jsonl orders.jsonl --on user_id=user_id \
  | ja select 'status == "shipped"' \
  | ja groupby city --agg revenue=sum:amount
```

**Output:**
```json
{"city": "NYC", "revenue": 1315}
{"city": "SF", "revenue": 560}
```

!!! success "You Did It!"
    You just performed a multi-step analysis combining filtering, joining, and aggregation!

## Working with Nested Data

jsonl-algebra excels at handling nested JSON structures using **dot notation**.

### Create Nested Data

```bash
cat > nested.jsonl << 'EOF'
{"id": 1, "user": {"name": "Alice", "profile": {"age": 30, "city": "NYC"}}}
{"id": 2, "user": {"name": "Bob", "profile": {"age": 25, "city": "SF"}}}
{"id": 3, "user": {"name": "Charlie", "profile": {"age": 35, "city": "NYC"}}}
EOF
```

### Access Nested Fields

```bash
# Project nested fields
ja project user.name,user.profile.city nested.jsonl
```

**Output:**
```json
{"user.name": "Alice", "user.profile.city": "NYC"}
{"user.name": "Bob", "user.profile.city": "SF"}
{"user.name": "Charlie", "user.profile.city": "NYC"}
```

```bash
# Filter on nested fields
ja select 'user.profile.age > 28' nested.jsonl
```

**Output:**
```json
{"id": 1, "user": {"name": "Alice", "profile": {"age": 30, "city": "NYC"}}}
{"id": 3, "user": {"name": "Charlie", "profile": {"age": 35, "city": "NYC"}}}
```

## Common Patterns

### Pattern 1: Top N Results

```bash
# Top 3 highest amounts
ja select 'status == "shipped"' orders.jsonl \
  | ja sort amount --desc \
  | head -3
```

### Pattern 2: Unique Values

```bash
# List all cities (unique)
ja project city users.jsonl | ja distinct
```

**Output:**
```json
{"city": "NYC"}
{"city": "SF"}
{"city": "LA"}
```

### Pattern 3: Data Validation

```bash
# Find users without required fields
ja select 'name == null or age == null' users.jsonl
```

### Pattern 4: Converting Formats

```bash
# Convert to CSV
ja project name,age,city users.jsonl | ja export csv > users.csv
```

## Command Cheat Sheet

| Task | Command |
|------|---------|
| Filter rows | `ja select 'condition' file.jsonl` |
| Select fields | `ja project field1,field2 file.jsonl` |
| Join files | `ja join left.jsonl right.jsonl --on key1=key2` |
| Group & count | `ja groupby field --agg count file.jsonl` |
| Sort | `ja sort field [--desc] file.jsonl` |
| Remove dupes | `ja distinct file.jsonl` |
| Combine files | `ja union file1.jsonl file2.jsonl` |
| Get first N | `ja ... | head -N` |
| Count lines | `ja ... | wc -l` |

## Next Steps

Now that you've mastered the basics, explore more:

### Learn Core Concepts
- [Relational Algebra](../concepts/relational-algebra.md) - Mathematical foundation
- [Dot Notation](../concepts/dotnotation.md) - Working with nested data
- [Streaming & Piping](../concepts/streaming.md) - Processing large files

### Explore Advanced Features
- [All CLI Commands](../cli/commands.md) - Complete command reference
- [ja-shell](../shell/introduction.md) - Interactive data exploration
- [Integrations](../integrations/overview.md) - MCP server, log analyzer, etc.

### Try Real-World Tutorials
- [Analyzing Log Files](../tutorials/data-analysis.md)
- [Building ETL Pipelines](../tutorials/etl.md)
- [Data Quality Checks](../tutorials/quality.md)

### Use the REPL
For interactive exploration:

```bash
ja repl users.jsonl
```

Then try commands like:
```
ja> select 'age > 28' filtered
ja> info filtered
ja> ls filtered --limit 3
```

## Practice Exercises

Try these challenges on your own:

1. Find all engineers in NYC over age 28
2. Calculate average order amount by status
3. List users who have never placed an order
4. Find the city with the highest total revenue

??? example "Solutions"
    ```bash
    # 1. Engineers in NYC over 28
    ja select 'role == "engineer" and city == "NYC" and age > 28' users.jsonl

    # 2. Average order amount by status
    ja groupby status --agg avg_amount=avg:amount orders.jsonl

    # 3. Users with no orders (left join, filter nulls)
    ja join users.jsonl orders.jsonl --on user_id=user_id --left \
      | ja select 'order_id == null'

    # 4. City with highest revenue
    ja join users.jsonl orders.jsonl --on user_id=user_id \
      | ja select 'status == "shipped"' \
      | ja groupby city --agg revenue=sum:amount \
      | ja sort revenue --desc \
      | head -1
    ```

## Getting Help

- Run `ja --help` for command overview
- Run `ja <command> --help` for specific command help
- Check the [FAQ](../faq.md) for common questions
- Visit the [Troubleshooting Guide](../troubleshooting.md) if stuck

!!! success "You're Ready!"
    You now know the essential operations of jsonl-algebra. Start using it on your own data!
