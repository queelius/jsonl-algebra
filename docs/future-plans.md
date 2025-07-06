# Future Plans for ja

## Lazy Evaluation and JSON Query Language

### The Core Idea

What if we could represent JSONL algebra pipelines as JSON? Instead of executing each step immediately, we'd build a query plan that could be inspected, modified, and optimized before execution.

### Simple Example

```bash
# Current eager mode (executes immediately)
cat orders.jsonl | ja select 'amount > 100' | ja groupby customer | ja agg total=sum(amount)

# Future lazy mode (builds query plan)
cat orders.jsonl | ja query --build | ja select 'amount > 100' | ja groupby customer | ja agg total=sum(amount)
```

This would output a JSON query plan:
```json
{
  "source": "stdin",
  "operations": [
    {"op": "select", "expr": "amount > 100"},
    {"op": "groupby", "key": "customer"},
    {"op": "agg", "spec": {"total": "sum(amount)"}}
  ]
}
```

### Why This Matters

1. **Inspection**: See what will happen before it happens
2. **Optimization**: Rearrange operations for better performance
3. **Reusability**: Save and share query definitions
4. **Tooling**: Other tools could generate or consume these plans

### Multi-Source Example

```bash
# A join pipeline
cat transactions.jsonl | ja query --build | ja join users.jsonl --on user_id | ja select 'user.active == true'
```

Query plan:
```json
{
  "source": "stdin",
  "operations": [
    {
      "op": "join",
      "with": "users.jsonl",
      "on": ["user_id", "id"]
    },
    {
      "op": "select",
      "expr": "user.active == true"
    }
  ]
}
```

### Execution Options

```bash
# Build and save a query plan
ja query --build < orders.jsonl > plan.json

# Execute a saved plan
ja query --execute plan.json < orders.jsonl

# Show what would happen (dry run)
ja query --explain plan.json
```

### stdin Handling

For stdin sources, we keep it simple:

```bash
# If stdin is too large for lazy mode
$ cat huge_file.jsonl | ja query --build
Error: stdin too large for query building (>10MB)

Options:
1. Save to a file first: cat huge_file.jsonl > data.jsonl
2. Use eager mode: ja select ... (without --build)
3. Use existing tools: cat huge_file.jsonl | head -10000 | ja query --build
```

### Integration with Unix Tools

The beauty is that JSON query plans work well with existing tools:

```bash
# Use jq to modify query plans
ja query --build < data.jsonl | jq '.operations += [{"op": "limit", "n": 100}]' | ja query --execute

# Version control your queries
git add queries/monthly_report.json

# Generate queries programmatically
python generate_query.py | ja query --execute < data.jsonl
```

### Potential Benefits

1. **Query Optimization**
   ```json
   {
     "original": [
       {"op": "join", "with": "huge_file.jsonl"},
       {"op": "select", "expr": "amount > 1000"}
     ],
     "optimized": [
       {"op": "select", "expr": "amount > 1000"},
       {"op": "join", "with": "huge_file.jsonl"}
     ]
   }
   ```

2. **Debugging**
   ```bash
   ja query --explain plan.json
   # Step 1: Load stdin (est. 10,000 rows)
   # Step 2: Filter where amount > 100 (est. 2,000 rows)
   # Step 3: Group by customer (est. 500 groups)
   # Step 4: Aggregate sum(amount)
   ```

3. **Alternative Execution Engines**
   ```bash
   # Future: Convert to SQL
   ja query --to-sql plan.json
   # SELECT customer, SUM(amount) as total
   # FROM stdin
   # WHERE amount > 100
   # GROUP BY customer
   ```

### Open Questions

1. Should this be part of `ja` or a separate tool?
2. How much optimization is worth the complexity?
3. What's the right balance between lazy and eager execution?

### Next Steps

Start simple:
1. Add `--dry-run` to show what an operation would do
2. Add `--explain` to show row count estimates
3. Gather feedback on whether full lazy evaluation is needed

The goal is to enhance `ja`'s power while maintaining its simplicity. JSON query plans could be the bridge between simple command-line usage and more complex data processing needs.

## Streaming Mode and Window Processing

### The --streaming Flag

Add a `--streaming` flag that enforces streaming constraints:

```bash
# This would error
ja sort --streaming data.jsonl
Error: sort operation requires seeing all data and cannot be performed in streaming mode

# This would work
ja select 'amount > 100' --streaming data.jsonl
```

Benefits:
- Explicit about memory usage expectations
- Fail fast when streaming isn't possible
- Good for production pipelines with memory constraints

### Window-Based Processing

For operations that normally require seeing all data, add `--window-size` support:

```bash
# Sort within 1000-row windows
ja sort amount --window-size 1000 huge.jsonl

# Collect groups within windows
ja groupby region huge.jsonl | ja collect --window-size 5000

# Remove duplicates within windows
ja distinct --window-size 10000 huge.jsonl
```

This provides a middle ground:
- Process arbitrarily large files
- Trade completeness for memory efficiency
- Useful for approximate results on huge datasets

### Operations by Streaming Capability

#### Always Streaming
- `select` - Row-by-row filtering
- `project` - Row-by-row transformation
- `rename` - Row-by-row field renaming
- `groupby` (without --agg) - Adds metadata only

#### Never Streaming (Need Full Data)
- `sort` - Must compare all rows
- `distinct` - Must track all seen values
- `groupby --agg` - Must see all groups
- `collect` - Must gather all group members
- `join` (currently) - Needs to load right side

#### Could Be Streaming
- `join` with pre-sorted data and merge join
- `union` with duplicate handling disabled
- `intersection/difference` with bloom filters

### Implementation Plan

1. **Phase 1**: Add `--streaming` flag to enforce constraints
2. **Phase 2**: Implement `--window-size` for sort, distinct, collect
3. **Phase 3**: Document streaming characteristics in help text
4. **Phase 4**: Add approximate algorithms for streaming versions

### Example: Memory-Conscious Pipeline

```bash
# Process 1TB log file with memory constraints
cat huge_log.jsonl \
  | ja select 'level == "ERROR"' --streaming \
  | ja project timestamp,message,host \
  | ja groupby host \
  | ja collect --window-size 10000 \
  | ja agg errors=count --window-size 10000
```

This processes the entire file while never holding more than 10,000 rows in memory.


## Path-like Matching

We provide dot notation. We will extend this to support more complex matching.

The path `field1.*.field2[<condition-predicate>].field4` can point to many values at `field4`. Maybe the value is
a simple string or integer, or maybe it is arbititrarily complex JSON values. When we group by
such a field, *many* values may be returned for a single JSONL line (JSON).

When we perform a group-by operation, we group by the value at the end of that path. If two JSON lines have say the  same value associated to `field4` in the above example, then they placed in the same group. Right?

Not so fast.

Suppose we have a JSONL file with 3 entries:

```jsonl
{ "a": { "key" : "value" } } 
{ "b": { "key" : "value" } } 
{ "c": { "key" : "other-value" } } 
```

If we group by `a.key`, we get one group with the value `value` and the other gru