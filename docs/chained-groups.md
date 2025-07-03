# Chained GroupBy Operations

`ja` supports a powerful pattern of chaining multiple `groupby` operations, allowing you to create multi-level aggregations while maintaining the JSONL format throughout the pipeline.

## How It Works

When you use `groupby` without the `--agg` flag, `ja` adds special metadata fields to each row:

- `_group`: The value of the grouping key for this row
- `_group_field`: The field name used for grouping
- `_group_size`: Total number of rows in this group
- `_group_index`: This row's index within its group

These fields allow subsequent operations to understand the grouping structure while keeping the data in JSONL format.

## Basic Example

```bash
# Group sales by region, then by product within each region
cat sales.jsonl | ja groupby region | ja groupby product | ja agg "total=sum(amount),count"
```

## Representation

### Original Data
```json
{"region": "North", "product": "Widget", "amount": 100}
{"region": "North", "product": "Gadget", "amount": 150}
{"region": "North", "product": "Widget", "amount": 200}
{"region": "South", "product": "Widget", "amount": 250}
```

### After First GroupBy
```json
{"region": "North", "product": "Widget", "amount": 100, "_group": "North", "_group_field": "region", "_group_size": 3, "_group_index": 0}
{"region": "North", "product": "Gadget", "amount": 150, "_group": "North", "_group_field": "region", "_group_size": 3, "_group_index": 1}
{"region": "North", "product": "Widget", "amount": 200, "_group": "North", "_group_field": "region", "_group_size": 3, "_group_index": 2}
{"region": "South", "product": "Widget", "amount": 250, "_group": "South", "_group_field": "region", "_group_size": 1, "_group_index": 0}
```

### After Second GroupBy
```json
{"region": "North", "product": "Widget", "amount": 100, "_group": "North.Widget", "_group_field": "product", "_group_size": 2, "_group_index": 0, "_parent_group": "North", "_group_trail": ["region", "product"]}
{"region": "North", "product": "Widget", "amount": 200, "_group": "North.Widget", "_group_field": "product", "_group_size": 2, "_group_index": 1, "_parent_group": "North", "_group_trail": ["region", "product"]}
{"region": "North", "product": "Gadget", "amount": 150, "_group": "North.Gadget", "_group_field": "product", "_group_size": 1, "_group_index": 0, "_parent_group": "North", "_group_trail": ["region", "product"]}
{"region": "South", "product": "Widget", "amount": 250, "_group": "South.Widget", "_group_field": "product", "_group_size": 1, "_group_index": 0, "_parent_group": "South", "_group_trail": ["region", "product"]}
```

### After Aggregation
```json
{"region": "North", "product": "Widget", "total": 300, "count": 2}
{"region": "North", "product": "Gadget", "total": 150, "count": 1}
{"region": "South", "product": "Widget", "total": 250, "count": 1}
```

## Advanced Examples

### Three-Level Grouping
```bash
# Group by year, then month, then category
cat transactions.jsonl \
  | ja groupby year \
  | ja groupby month \
  | ja groupby category \
  | ja agg revenue=sum(amount),transactions=count,avg_transaction=avg(amount)
```

### Filtering Between Groups
```bash
# Group by user, filter to active users, then group by product
cat purchases.jsonl \
  | ja groupby user_id \
  | ja select '_group_size > 5' \
  | ja groupby product_id \
  | ja agg total=sum(price)
```

### Inspecting Intermediate Results
```bash
# Use jq to examine the grouping structure
cat data.jsonl | ja groupby category | jq '._group_field, ._group_size' | sort | uniq -c
```

## Comparison with Direct Aggregation

You can still use the `--agg` flag for more efficient single-level aggregations:

```bash
# Direct aggregation (more efficient for simple cases)
ja groupby region --agg total=sum(amount),count sales.jsonl

# Equivalent chained operation
cat sales.jsonl | ja groupby region | ja agg total=sum(amount),count
```

The chained approach is more flexible but may be slightly less efficient for simple aggregations. Use it when you need:
- Multi-level grouping
- Intermediate filtering or transformation
- Exploratory analysis with inspection of groups
- Integration with other tools in a pipeline