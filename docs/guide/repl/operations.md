# REPL Operations

All data transformation operations in the REPL require an output name and create a new dataset.

## Unary Operations

Unary operations transform the current dataset.

### `select '<expression>' <output_name>`

Filter rows based on a condition:

```
ja> select 'age > 30' adults
Created: adults (current)

ja> select 'status == `"active"` && score > 80' high_performers
Created: high_performers (current)
```

**Expression Syntax:**
- JMESPath expressions
- Use backticks for string literals: `\`"value"\``
- Supports nested fields with dot notation: `user.age > 25`

### `project <fields> <output_name>`

Select specific fields:

```
ja> project id,name,email user_contact
Created: user_contact (current)

ja> project user.name,user.location.city user_info
Created: user_info (current)
```

**Field Syntax:**
- Comma-separated field names
- Supports dot notation for nested fields
- No spaces around commas

### `rename <old=new,...> <output_name>`

Rename fields:

```
ja> rename id=user_id,name=full_name renamed
Created: renamed (current)

ja> rename user.loc=user.location updated
Created: updated (current)
```

### `distinct <output_name>`

Remove duplicate rows:

```
ja> distinct unique
Created: unique (current)
```

### `sort <keys> [--desc] <output_name>`

Sort by one or more keys:

```
# Ascending order
ja> sort age sorted_by_age
Created: sorted_by_age (current)

# Descending order
ja> sort amount --desc top_amounts
Created: top_amounts (current)

# Multiple keys
ja> sort city,age sorted_multi
Created: sorted_multi (current)
```

### `groupby <key> [--agg <spec>] <output_name>`

Group rows by a key:

```
# Group without aggregation (adds metadata)
ja> groupby category grouped
Created: grouped (current)

# Group with aggregation
ja> groupby region --agg count,sum(amount) summary
Created: summary (current)

# Multiple aggregations
ja> groupby product --agg count,avg(price),max(price) stats
Created: stats (current)
```

**Aggregation Functions:**
- `count` - Count rows
- `sum(field)` - Sum values
- `avg(field)` - Average
- `min(field)` - Minimum
- `max(field)` - Maximum
- `list(field)` - Collect all values
- `first(field)` - First value
- `last(field)` - Last value

## Binary Operations

Binary operations combine the current dataset with another named dataset.

### `join <dataset> --on <mapping> <output_name>`

Join two datasets:

```
ja> cd users
Current dataset: users

ja> join orders --on id=user_id user_orders
Created: user_orders (current)

# Join on nested fields
ja> join companies --on user.company_id=id user_companies
Created: user_companies (current)
```

**Notes:**
- Current dataset is the left side
- Specified dataset is the right side
- Mapping format: `left_field=right_field`
- Supports dot notation in field names

### `union <dataset> <output_name>`

Combine rows from two datasets (deduplicated):

```
ja> cd jan_sales
ja> union feb_sales q1_sales
Created: q1_sales (current)
```

### `intersection <dataset> <output_name>`

Keep only rows present in both datasets:

```
ja> cd active_users
ja> intersection premium_users premium_active
Created: premium_active (current)
```

### `difference <dataset> <output_name>`

Remove rows present in another dataset:

```
ja> cd all_users
ja> difference inactive_users active_only
Created: active_only (current)
```

### `product <dataset> <output_name>`

Cartesian product of two datasets:

```
ja> cd colors
ja> product sizes combinations
Created: combinations (current)
```

## Operation Chaining

Operations automatically switch to the newly created dataset, enabling natural chaining:

```
ja> load users.jsonl
Loaded: users (current)

ja> select 'age > 25' adults
Created: adults (current)

ja> project name,email,city contact_info
Created: contact_info (current)

ja> sort name sorted
Created: sorted (current)

ja> ls --limit 3
{"name": "Alice", "email": "alice@example.com", "city": "NYC"}
...
```

## Working with Multiple Datasets

You can work with multiple datasets by switching between them:

```
ja> load users.jsonl
ja> load orders.jsonl

# Work with users
ja> cd users
ja> select 'status == `"active"`' active_users
Created: active_users (current)

# Work with orders
ja> cd orders
ja> select 'amount > 100' large_orders
Created: large_orders (current)

# Join them
ja> cd active_users
ja> join large_orders --on id=user_id result
Created: result (current)
```

## Examples

### Data Cleaning Pipeline

```
ja> load raw_data.jsonl
Loaded: raw_data (current)

# Remove nulls
ja> select 'name != `null`' has_name
Created: has_name (current)

# Remove duplicates
ja> distinct unique
Created: unique (current)

# Select specific fields
ja> project id,name,email,created_at clean
Created: clean (current)

# Sort by creation date
ja> sort created_at sorted
Created: sorted (current)

ja> save cleaned_data.jsonl
Saved sorted to: cleaned_data.jsonl
```

### Aggregation Workflow

```
ja> load sales.jsonl
Loaded: sales (current)

# Filter to this year
ja> select 'year == `2024`' sales_2024
Created: sales_2024 (current)

# Group by region and calculate totals
ja> groupby region --agg count,sum(amount),avg(amount) regional_summary
Created: regional_summary (current)

# Sort by total amount
ja> sort sum_amount --desc top_regions
Created: top_regions (current)

ja> ls
{"region": "West", "count": 1234, "sum_amount": 456789, "avg_amount": 370}
{"region": "East", "count": 1100, "sum_amount": 398000, "avg_amount": 362}
...
```

### Multi-Dataset Join

```
ja> load users.jsonl
ja> load orders.jsonl
ja> load products.jsonl

# Join users with orders
ja> cd users
ja> join orders --on id=user_id user_orders
Created: user_orders (current)

# Join with products
ja> join products --on product_id=id full_data
Created: full_data (current)

# Aggregate by user
ja> groupby name --agg count,sum(price) user_spending
Created: user_spending (current)

# Get top spenders
ja> sort sum_price --desc top_spenders
Created: top_spenders (current)

ja> ls --limit 10
...
```

### Filtering and Comparison

```
ja> load all_transactions.jsonl
Loaded: all_transactions (current)

# Get high-value transactions
ja> select 'amount > 1000' high_value
Created: high_value (current)

# Get fraud flags
ja> cd all_transactions
ja> select 'flagged == `true`' flagged
Created: flagged (current)

# Find intersection (high-value AND flagged)
ja> cd high_value
ja> intersection flagged investigate
Created: investigate (current)

ja> info
Dataset: investigate
Rows: 23
Size: 5.6 KB
...
```

## Tips

1. **Name your outputs descriptively** to track transformations:
   ```
   ja> select 'age > 65' seniors
   ja> select 'income < 30000' low_income
   ja> intersection low_income seniors low_income_seniors
   ```

2. **Use `info` after operations** to verify results:
   ```
   ja> select 'status == `"active"`' active
   Created: active (current)

   ja> info
   Dataset: active
   Rows: 523  # Down from 1000, looks right!
   ```

3. **Preview before saving**:
   ```
   ja> ls --limit 10  # Check it looks good
   ja> save final_output.jsonl
   ```

4. **Keep intermediate results** for debugging:
   ```
   ja> select ... step1
   ja> project ... step2
   ja> join ... step3
   # Now you can go back and check each step!
   ```
