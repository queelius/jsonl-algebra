# Getting Started with ja

This guide walks you through jsonl-algebra's core concepts using realistic example data.

## Installation

```bash
# Basic installation
pip install jsonl-algebra

# With dataset generation tools
pip install "jsonl-algebra[dataset]"
```

## Generate Example Data

Let's start by creating some example data to work with:

```bash
# Generate sample datasets
ja-generate-dataset --num-companies 8 --num-people 30 --output-dir examples/

# This creates:
# - examples/companies.jsonl (8 companies with nested headquarters data)
# - examples/people.jsonl (30 people with nested personal info, jobs, and household relationships)
```

## Explore the Data

### Look at the Structure

```bash
# See the first few records
ja examples/people.jsonl --head 2 --pretty

# Check companies structure  
ja examples/companies.jsonl --head 2 --pretty
```

### Basic Filtering

```bash
# Find people over 30
ja examples/people.jsonl --where 'person.age > 30' --select person.name,person.age

# Find tech companies
ja examples/companies.jsonl --where 'industry == "Technology"' --select name,size
```

### Working with Nested Data

```bash
# Extract names and locations
ja examples/people.jsonl --select person.name,person.location.state,person.location.city

# Group by location
ja examples/people.jsonl --group-by person.location.state --count
```

## Core Operations

### Selection and Projection

```bash
# Select high earners in California
ja examples/people.jsonl \\
  --where 'person.job.salary >= 80000 and person.location.state == "CA"' \\
  --select person.name,person.job.title,person.job.salary

# Project just essential job info
ja examples/people.jsonl \\
  --select person.name.first,person.name.last,person.job.title,person.job.company_name
```

### Grouping and Aggregation

```bash
# Count employees by company
ja examples/people.jsonl --group-by person.job.company_name --count

# Average salary by company
ja examples/people.jsonl \\
  --group-by person.job.company_name \\
  --agg 'avg_salary=avg(person.job.salary),count=count(*)' \\
  --sort-by avg_salary --reverse

# Salary statistics by state
ja examples/people.jsonl \\
  --group-by person.location.state \\
  --agg 'avg_salary=avg(person.job.salary),min_salary=min(person.job.salary),max_salary=max(person.job.salary),count=count(*)'
```

### Joins

```bash
# Join people with their companies
ja examples/people.jsonl \\
  --join examples/companies.jsonl \\
  --on 'person.job.company_name = name' \\
  --select person.name,person.job.title,name,industry,headquarters.city

# Find people working for large tech companies
ja examples/people.jsonl \\
  --join examples/companies.jsonl \\
  --on 'person.job.company_name = name' \\
  --where 'industry == "Technology" and size > 1000' \\
  --select person.name,person.job.title,name,size
```

## Advanced Examples

### Household Analysis

```bash
# Find households with multiple people
ja examples/people.jsonl \\
  --group-by household_id \\
  --agg 'count=count(*),members=collect(person.name.first)' \\
  --where 'count > 1' \\
  --select household_id,count,members

# Average age by household
ja examples/people.jsonl \\
  --group-by household_id \\
  --agg 'avg_age=avg(person.age),family_name=first(person.name.last)' \\
  --select family_name,avg_age \\
  --sort-by avg_age --reverse
```

### Multi-step Pipelines

```bash
# Complex analysis: High earners by industry and location
ja examples/people.jsonl \\
  --join examples/companies.jsonl --on 'person.job.company_name = name' \\
  --where 'person.job.salary >= 70000' \\
  --group-by 'industry,person.location.state' \\
  --agg 'count=count(*),avg_salary=avg(person.job.salary)' \\
  --where 'count >= 2' \\
  --sort-by avg_salary --reverse
```

### Chained Grouping

```bash
# Group by industry, then by job title within each industry
ja examples/people.jsonl \\
  --join examples/companies.jsonl --on 'person.job.company_name = name' \\
  --group-by industry \\
  --group-by person.job.title \\
  --agg 'count=count(*),avg_salary=avg(person.job.salary)' \\
  --sort-by avg_salary --reverse
```

## Key Concepts

### Nested JSON Navigation

ja uses JSONPath-like syntax to navigate nested structures:

- `person.name.first` - Access nested fields with dots
- `person.location.state` - Navigate deep into objects
- `headquarters.city` - Works with any level of nesting

### Algebraic Operations

ja operations are composable and can be chained:

```bash
# Each operation produces a new relation
ja data.jsonl --where 'age > 25' | ja --group-by department | ja --agg 'count=count(*)'
```

### Streaming Processing

ja processes data in a streaming fashion, making it efficient for large datasets:

```bash
# Generate larger dataset for testing
ja-generate-dataset --num-companies 100 --num-people 10000 --output-dir large/

# Still processes efficiently
ja large/people.jsonl --group-by person.location.state --count
```

## Next Steps

- Read the [Concepts Guide](concepts/jsonl-algebra.md) for deeper understanding
- Explore [Chained Groups](concepts/chained-groups.md) for advanced grouping
- Check out the [Cookbook](cookbook/log-analysis.md) for real-world examples
- Try the [Interactive REPL](advanced/repl.md) for experimentation

## Generated Data Reference

The `ja-generate-dataset` command creates two related files:

### companies.jsonl Structure

```json
{
  "id": "uuid",
  "name": "Company Name",
  "industry": "Technology",
  "headquarters": {"city": "San Francisco", "state": "CA", "country": "USA"},
  "size": 1500,
  "founded": 2010
}
```

### people.jsonl Structure

```json
{
  "id": "uuid",
  "created_at": "2023-05-15T10:30:00Z",
  "status": "active",
  "household_id": "household-uuid",
  "person": {
    "name": {"first": "Sarah", "last": "Johnson"},
    "age": 32,
    "gender": "female",
    "email": "sarah.johnson@gmail.com",
    "phone": "555-123-4567",
    "location": {"city": "San Francisco", "state": "CA", "country": "USA"},
    "interests": ["hiking", "photography"],
    "job": {
      "title": "Software Engineer",
      "company_name": "Tech Solutions Inc", 
      "salary": 95000.0
    }
  }
}
```

The data includes realistic relationships:

- **Employment**: `person.job.company_name` matches company `name`
- **Households**: People with the same `household_id` share last names and locations
- **Geographic**: Hierarchical city/state/country structure with `ja`

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