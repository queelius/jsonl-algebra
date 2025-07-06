# Chained Grouping: Deep Dive

Chained groupby is one of `ja`'s most innovative features. It allows you to build multi-level aggregations while maintaining the JSONL format throughout the pipeline.

## The Problem with Traditional Grouping

Most data tools force you to specify all grouping levels upfront:

```sql
-- SQL requires specifying all levels at once
SELECT region, product, SUM(amount)
FROM sales
GROUP BY region, product
```

This approach has limitations:

- Can't filter between grouping levels
- Can't inspect intermediate groupings
- Hard to build complex hierarchies progressively
- Doesn't compose well with other operations

## The ja Solution: Metadata-Preserving Grouping

Instead of immediately aggregating, `ja groupby` adds metadata to preserve grouping information:

```bash
# First level: group by region
ja groupby region sales.jsonl
```

This adds metadata to each row:

```json
{
  "sale_id": 1,
  "region": "North",
  "product": "Widget",
  "amount": 100,
  "_groups": [{"field": "region", "value": "North"}],
  "_group_size": 3,
  "_group_index": 0
}
```

### Key Metadata Fields

- `_groups`: Array of grouping levels with field names and values
- `_group_size`: Number of rows in this group
- `_group_index`: This row's position within its group

## Building Multi-Level Hierarchies

### Two-Level Grouping

```bash
ja groupby region sales.jsonl | ja groupby product
```

The second groupby extends the metadata:

```json
{
  "sale_id": 1,
  "region": "North", 
  "product": "Widget",
  "amount": 100,
  "_groups": [
    {"field": "region", "value": "North"},
    {"field": "product", "value": "Widget"}
  ],
  "_group_size": 2,
  "_group_index": 0
}
```

### Three-Level and Beyond

```bash
ja groupby year sales.jsonl \
  | ja groupby region \
  | ja groupby product \
  | ja groupby month
```

Each level adds to the `_groups` array, creating a complete hierarchy.

## The Power of Composition

### Filter Between Levels

```bash
# Group by user, filter active groups, then group by product
ja groupby user_id transactions.jsonl \
  | ja select '_group_size > 5' \
  | ja groupby product_id \
  | ja agg total=sum(amount)
```

This finds users with more than 5 transactions, then analyzes their product preferences.

### Transform Between Levels

```bash
# Add computed fields between groupings
ja groupby customer orders.jsonl \
  | ja project customer,order_id,amount,is_large=amount>100 \
  | ja groupby is_large \
  | ja agg count,avg_amount=avg(amount)
```

### Inspect Intermediate Results

```bash
# See the grouping structure
ja groupby region sales.jsonl | ja groupby product | head -3
```

You can examine the data at any stage to understand the grouping.

## Aggregation: The Final Step

When you're ready to aggregate, `ja agg` uses the metadata to produce clean results:

```bash
ja groupby region sales.jsonl \
  | ja groupby product \
  | ja agg total=sum(amount),count,avg=avg(amount)
```

Output:

```json
{"region": "North", "product": "Widget", "total": 500, "count": 3, "avg": 166.67}
{"region": "North", "product": "Gadget", "total": 300, "count": 2, "avg": 150.00}
{"region": "South", "product": "Widget", "total": 400, "count": 2, "avg": 200.00}
```

Notice how all grouping fields are preserved in the output.

## Advanced Patterns

### Conditional Aggregation Within Groups

```bash
ja groupby department employees.jsonl \
  | ja agg \
      total_salary=sum(salary), \
      senior_count=count_if(level=="senior"), \
      avg_senior_salary=avg_if(salary,level=="senior")
```

### Nested Field Grouping

```bash
ja groupby user.region sales.jsonl \
  | ja groupby product.category \
  | ja agg revenue=sum(amount)
```

### Time-Series Grouping

```bash
ja groupby date sales.jsonl \
  | ja project date,customer,amount,month=date[0:7] \
  | ja groupby month \
  | ja agg daily_avg=avg(amount)
```

## Implementation Details

### Metadata Structure

The `_groups` array maintains the complete grouping hierarchy:

```json
"_groups": [
  {"field": "region", "value": "North"},
  {"field": "product", "value": "Widget"},
  {"field": "size", "value": "Large"}
]
```

### Efficient Grouping

Internally, `ja` uses Python's `defaultdict` and tuple keys for efficient grouping:

```python
# Conceptual implementation
group_key = tuple((g["field"], g["value"]) for g in row["_groups"])
groups[group_key].append(clean_row)
```

### Memory Efficiency

Because `ja` processes data streaming, even complex multi-level groupings don't require loading entire datasets into memory.

## Comparison with Direct Aggregation

### When to Use Chained Grouping

Use chained grouping when you need:

- **Multi-level hierarchies**: More than one grouping dimension
- **Intermediate filtering**: Filtering between grouping levels  
- **Progressive building**: Building up complex queries step by step
- **Inspection**: Examining intermediate grouping states
- **Composition**: Integrating with other operations

```bash
# Complex pipeline with chained grouping
cat transactions.jsonl \
  | ja select 'amount > 10' \
  | ja join customers.jsonl --on customer_id=id \
  | ja groupby tier \
  | ja select '_group_size > 100' \
  | ja groupby month \
  | ja agg revenue=sum(amount),customers=count_distinct(customer_id)
```

### When to Use Direct Aggregation

Use the `--agg` flag for simple, single-level aggregations:

```bash
# Simple aggregation - more efficient
ja groupby category --agg 'total=sum(amount),count' sales.jsonl
```

## Real-World Examples

### E-commerce Analysis

```bash
# Multi-dimensional sales analysis
cat orders.jsonl \
  | ja select 'status == "completed"' \
  | ja join products.jsonl --on product_id=id \
  | ja groupby category \
  | ja groupby price_tier \
  | ja groupby month \
  | ja agg \
      revenue=sum(total), \
      orders=count, \
      unique_customers=count_distinct(customer_id), \
      avg_order=avg(total)
```

### Log Analysis

```bash
# Server log analysis by endpoint and status
cat access.log.jsonl \
  | ja select 'timestamp > "2024-01-01"' \
  | ja groupby endpoint \
  | ja select '_group_size > 1000' \
  | ja groupby status_code \
  | ja agg \
      requests=count, \
      avg_response_time=avg(response_time), \
      error_rate=count_if(status_code>=400)/count
```

### User Behavior Analysis

```bash
# User engagement analysis
cat events.jsonl \
  | ja groupby user_id \
  | ja select '_group_size >= 10' \
  | ja groupby event_type \
  | ja groupby date \
  | ja agg \
      daily_events=count, \
      unique_users=count_distinct(user_id)
```

## Best Practices

1. **Start Simple**: Begin with single-level grouping, then add levels
2. **Filter Early**: Use `select` before grouping to reduce data size  
3. **Inspect Intermediate Results**: Use `head` or `--lines` to check grouping structure
4. **Use Meaningful Names**: Choose descriptive names for aggregated fields
5. **Leverage Metadata**: Use `_group_size` for filtering and analysis

## Troubleshooting

### Common Issues

**Empty Results After Chaining**:

```bash
# Check each step
ja groupby region data.jsonl | ja agg count
ja groupby region data.jsonl | ja groupby product | ja agg count
```

**Unexpected Grouping Values**:

```bash
# Inspect the grouping metadata
ja groupby region data.jsonl | ja project region,_groups | head -5
```

**Performance Issues**:

```bash
# Consider filtering early
ja select 'relevant_condition' data.jsonl | ja groupby region | ja groupby product
```

## What's Next?

- [Expression Language](expression-language.md) - Learn advanced filtering
- [Performance Guide](../advanced/performance.md) - Optimize complex pipelines
- [Cookbook Examples](../cookbook/log-analysis.md) - See real-world usage
