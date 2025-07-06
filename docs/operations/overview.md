# Operations Overview

`ja` provides a complete set of relational algebra operations designed for JSON data. Each operation follows the principle of taking relations (JSONL data) and producing relations, enabling infinite composability.

## Core Operations

### Selection and Filtering

| Operation | Symbol | Purpose | Example |
|-----------|---------|---------|---------|
| **select** | σ | Filter rows based on conditions | `ja select 'age > 30'` |
| **distinct** | δ | Remove duplicate rows | `ja distinct` |

### Projection and Transformation

| Operation | Symbol | Purpose | Example |
|-----------|---------|---------|---------|
| **project** | π | Select/compute columns | `ja project name,total=price*qty` |
| **rename** | ρ | Rename fields | `ja rename old_name=new_name` |

### Set Operations

| Operation | Symbol | Purpose | Example |
|-----------|---------|---------|---------|
| **union** | ∪ | Combine all rows from relations | `ja union file1.jsonl file2.jsonl` |
| **intersection** | ∩ | Keep only rows in both relations | `ja intersection file1.jsonl file2.jsonl` |
| **difference** | - | Keep rows in left but not right | `ja difference file1.jsonl file2.jsonl` |

### Join Operations

| Operation | Symbol | Purpose | Example |
|-----------|---------|---------|---------|
| **join** | ⋈ | Inner join on condition | `ja join users.jsonl orders.jsonl --on id=user_id` |
| **product** | × | Cartesian product | `ja product features.jsonl options.jsonl` |

### Grouping and Aggregation

| Operation | Symbol | Purpose | Example |
|-----------|---------|---------|---------|
| **groupby** | γ | Group rows (chainable) | `ja groupby department` |
| **agg** | γ | Aggregate data | `ja agg count,total=sum(amount)` |

### Ordering

| Operation | Symbol | Purpose | Example |
|-----------|---------|---------|---------|
| **sort** | τ | Sort rows | `ja sort name,age --desc` |

## Quick Reference

### Selection (Filter Rows)

```bash
# Basic filtering
ja select 'age > 30' people.jsonl

# Complex conditions
ja select 'status == "active" and score >= 80' users.jsonl

# Nested field filtering
ja select 'user.location.country == "US"' data.jsonl

# Using group metadata
ja groupby department data.jsonl | ja select '_group_size > 10'
```

### Projection (Select/Transform Columns)

```bash
# Select specific fields
ja project name,email people.jsonl

# Compute new fields
ja project name,total=price*quantity orders.jsonl

# Nested field selection
ja project user.name,user.email,order.total data.jsonl

# Flatten nested structures
ja project user.name,user.email --flatten data.jsonl
```

### Aggregation

```bash
# Simple aggregation
ja agg count,total=sum(amount) orders.jsonl

# After grouping
ja groupby department employees.jsonl | ja agg avg_salary=avg(salary)

# Chained grouping
ja groupby region sales.jsonl | ja groupby product | ja agg revenue=sum(amount)

# Conditional aggregation
ja agg shipped=count_if(status=="shipped"),total=sum(amount) orders.jsonl
```

### Joins

```bash
# Inner join
ja join users.jsonl orders.jsonl --on id=user_id

# Join with nested fields
ja join customers.jsonl orders.jsonl --on customer.id=customer_id

# Multiple files in pipeline
cat orders.jsonl | ja join customers.jsonl --on customer_id=id
```

### Set Operations

```bash
# Combine files
ja union current.jsonl historical.jsonl

# Find common records
ja intersection whitelist.jsonl data.jsonl

# Find differences
ja difference all_users.jsonl inactive_users.jsonl
```

## Advanced Features

### Nested Data Support

All operations support dot notation for nested fields:

```bash
# Works with any level of nesting
ja select 'user.profile.preferences.notifications == true'
ja project user.profile.name,user.profile.email
ja groupby user.location.country
ja join file1.jsonl file2.jsonl --on user.id=customer.id
```

### Expression Language

Rich expression support in `select` and `project`:

```bash
# Arithmetic
ja project name,annual_salary=monthly_salary*12

# String operations  
ja select 'name.startswith("A")'

# Comparisons
ja select 'created_date > "2024-01-01"'

# Logical operations
ja select 'age >= 18 and status == "active"'
```

### Aggregation Functions

| Function | Purpose | Example |
|----------|---------|---------|
| `count` | Count rows | `count` |
| `sum(field)` | Sum numeric values | `sum(amount)` |
| `avg(field)` | Average of values | `avg(score)` |
| `min(field)` | Minimum value | `min(date)` |
| `max(field)` | Maximum value | `max(price)` |
| `list(field)` | Collect values in list | `list(tag)` |
| `first(field)` | First value | `first(name)` |
| `last(field)` | Last value | `last(status)` |

### Conditional Aggregations

| Function | Purpose | Example |
|----------|---------|---------|
| `count_if(condition)` | Count matching rows | `count_if(status=="paid")` |
| `sum_if(field, condition)` | Sum matching values | `sum_if(amount, region=="North")` |
| `avg_if(field, condition)` | Average matching values | `avg_if(score, active==true)` |

## Composition Examples

### Building Complex Pipelines

```bash
# E-commerce analysis pipeline
cat orders.jsonl \
  | ja select 'status == "completed"' \
  | ja join products.jsonl --on product_id=id \
  | ja join customers.jsonl --on customer_id=id \
  | ja project \
      customer.name, \
      product.category, \
      total=quantity*price, \
      month=date[0:7] \
  | ja groupby customer.tier \
  | ja groupby product.category \
  | ja agg \
      revenue=sum(total), \
      orders=count, \
      avg_order=avg(total)
```

### Data Quality Pipeline

```bash
# Clean and validate data
cat raw_data.jsonl \
  | ja select 'email != null and age > 0' \
  | ja project \
      name, \
      email=email.lower(), \
      age, \
      valid=email.contains("@") \
  | ja select 'valid == true' \
  | ja distinct \
  | ja sort name
```

### Time Series Analysis

```bash
# Analyze user engagement over time
cat events.jsonl \
  | ja project \
      user_id, \
      event_type, \
      date=timestamp[0:10] \
  | ja groupby date \
  | ja groupby event_type \
  | ja agg \
      events=count, \
      unique_users=count_distinct(user_id) \
  | ja sort date
```

## Performance Considerations

### Optimization Tips

1. **Filter Early**: Use `select` before expensive operations
2. **Project Only Needed Fields**: Reduce data size with `project`
3. **Use Direct Aggregation**: Use `--agg` for simple grouping
4. **Order Operations**: Put most selective filters first

```bash
# Optimized pipeline
cat large_file.jsonl \
  | ja select 'timestamp > "2024-01-01"' \
  | ja project user_id,amount,category \
  | ja groupby category \
  | ja agg total=sum(amount)
```

### Memory Efficiency

All operations are streaming by design:

- Process arbitrarily large files
- Constant memory usage (except for operations requiring sorting/grouping)
- Natural integration with Unix pipes

## Error Handling

### Common Issues

**Invalid Expressions**:

```bash
# Check expression syntax
ja select 'age > thirty'  # Error: 'thirty' not defined
ja select 'age > 30'      # Correct
```

**Missing Fields**:

```bash
# Handle missing fields gracefully
ja select 'field != null'  # Only rows where field exists
```

**Type Mismatches**:

```bash
# Ensure consistent types
ja select 'amount > 0'     # Assumes amount is numeric
```

## Next Steps

- [Deep Dive: Selection](selection.md) - Advanced filtering techniques
- [Deep Dive: Grouping](grouping.md) - Master chained groupby
- [Deep Dive: Joins](joins.md) - Complex join patterns
- [Cookbook](../cookbook/log-analysis.md) - Real-world examples
