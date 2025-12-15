# CLI Overview

The **ja** command-line tool is your gateway to powerful JSONL data manipulation. This page provides an overview of the CLI architecture, usage patterns, and core principles.

## Command Structure

All ja commands follow a consistent pattern:

```bash
ja <command> [options] [files...]
```

### Anatomy of a Command

```bash
ja select 'age > 30' --output filtered.jsonl users.jsonl
│  │      │          │                    │
│  │      │          │                    └─ Input file(s)
│  │      │          └─ Optional flags
│  │      └─ Command-specific arguments
│  └─ Command name
└─ Program name
```

## Core Commands

### Data Filtering & Selection

| Command | Purpose | Example |
|---------|---------|---------|
| **select** | Filter rows by condition | `ja select 'age > 30' users.jsonl` |
| **distinct** | Remove duplicate rows | `ja distinct users.jsonl` |

### Data Transformation

| Command | Purpose | Example |
|---------|---------|---------|
| **project** | Select specific fields | `ja project name,email users.jsonl` |
| **rename** | Rename fields | `ja rename old=new users.jsonl` |
| **explode** | Flatten arrays | `ja explode tags users.jsonl` |
| **implode** | Group values into arrays | `ja implode --key user_id --field tag` |

### Data Combination

| Command | Purpose | Example |
|---------|---------|---------|
| **join** | Combine datasets | `ja join users.jsonl orders.jsonl --on id=user_id` |
| **union** | Merge all rows | `ja union file1.jsonl file2.jsonl` |
| **intersection** | Find common rows | `ja intersection file1.jsonl file2.jsonl` |
| **difference** | Find unique rows | `ja difference file1.jsonl file2.jsonl` |
| **product** | Cartesian product | `ja product file1.jsonl file2.jsonl` |

#### Join Types

The `join` command supports multiple join types via the `--how` flag:

| Join Type | Description | Example |
|-----------|-------------|---------|
| **inner** (default) | Only matching rows from both sides | `ja join a.jsonl b.jsonl --on id` |
| **left** | All rows from left, matching from right | `ja join a.jsonl b.jsonl --on id --how left` |
| **right** | All rows from right, matching from left | `ja join a.jsonl b.jsonl --on id --how right` |
| **outer** | All rows from both sides | `ja join a.jsonl b.jsonl --on id --how outer` |
| **cross** | Cartesian product (no key needed) | `ja join a.jsonl b.jsonl --how cross` |

### Window Functions

| Command | Purpose | Example |
|---------|---------|---------|
| **window** | Apply SQL-style window functions | `ja window row_number --partition-by dept file.jsonl` |

#### Available Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| **row_number** | Sequential row number within partition | `ja window row_number --partition-by dept` |
| **rank** | Rank with gaps for ties | `ja window rank --partition-by dept --order-by salary` |
| **dense_rank** | Rank without gaps | `ja window dense_rank --partition-by dept --order-by salary` |
| **lag** | Value from previous row | `ja window lag --field price --offset 1` |
| **lead** | Value from next row | `ja window lead --field price --offset 1` |
| **first_value** | First value in partition | `ja window first_value --field name --partition-by dept` |
| **last_value** | Last value in partition | `ja window last_value --field name --partition-by dept` |
| **ntile** | Divide into N buckets | `ja window ntile --n 4 --partition-by region` |
| **percent_rank** | Percentile rank (0-1) | `ja window percent_rank --order-by score` |
| **cume_dist** | Cumulative distribution | `ja window cume_dist --order-by score` |

Window function options:
- `--partition-by`, `-p`: Field(s) to partition by (comma-separated)
- `--order-by`, `-o`: Field(s) to order by within partition
- `--field`, `-f`: Field to operate on (for lag, lead, first_value, last_value)
- `--offset`: Offset for lag/lead (default: 1)
- `--default`: Default value when no row exists (lag/lead)
- `--n`: Number of buckets (ntile)
- `--output-field`: Custom name for output field

### Aggregation & Grouping

| Command | Purpose | Example |
|---------|---------|---------|
| **groupby** | Group by fields | `ja groupby city users.jsonl` |
| **agg** | Aggregate grouped data | `ja agg "total=sum(amount)"` |

### Data Organization

| Command | Purpose | Example |
|---------|---------|---------|
| **sort** | Sort by field | `ja sort age --desc users.jsonl` |
| **collect** | Collect stream into array | `ja collect users.jsonl` |

### Import & Export

| Command | Purpose | Example |
|---------|---------|---------|
| **import csv** | Convert CSV to JSONL | `ja import csv data.csv` |
| **export csv** | Convert JSONL to CSV | `ja export csv data.jsonl` |
| **export json** | Convert to JSON array | `ja export json data.jsonl` |

### Schema & Validation

| Command | Purpose | Example |
|---------|---------|---------|
| **schema infer** | Infer JSON schema | `ja schema infer users.jsonl` |
| **schema validate** | Validate against schema | `ja schema validate schema.json users.jsonl` |

### Interactive

| Command | Purpose | Example |
|---------|---------|---------|
| **repl** | Interactive mode | `ja repl users.jsonl` |

## Input/Output Patterns

### Reading Input

ja commands accept input in three ways:

=== "From File"

    ```bash
    ja select 'age > 30' users.jsonl
    ```

=== "From stdin"

    ```bash
    cat users.jsonl | ja select 'age > 30'
    ```

=== "From Multiple Files"

    ```bash
    ja union file1.jsonl file2.jsonl file3.jsonl
    ```

### Writing Output

By default, output goes to stdout:

=== "To stdout (default)"

    ```bash
    ja select 'age > 30' users.jsonl
    # Output appears on screen
    ```

=== "To File (redirect)"

    ```bash
    ja select 'age > 30' users.jsonl > filtered.jsonl
    ```

=== "To File (--output flag)"

    ```bash
    ja select 'age > 30' --output filtered.jsonl users.jsonl
    ```

=== "To Pipeline"

    ```bash
    ja select 'age > 30' users.jsonl | ja project name,email
    ```

## Common Patterns

### Pattern 1: Filter-Transform-Save

```bash
cat input.jsonl \
  | ja select 'status == "active"' \
  | ja project id,name,email \
  | ja sort name \
  > output.jsonl
```

### Pattern 2: Join-Aggregate-Report

```bash
ja join users.jsonl orders.jsonl --on user_id=user_id \
  | ja groupby user.name --agg total=sum:amount,count \
  | ja sort total --desc \
  | head -10
```

### Pattern 3: Multi-Source Union

```bash
ja union logs-*.jsonl \
  | ja select 'level == "ERROR"' \
  | ja project timestamp,message,service
```

### Pattern 4: Schema-First Validation

```bash
# 1. Infer schema from good data
ja schema infer good_data.jsonl > schema.json

# 2. Validate new data
ja schema validate schema.json new_data.jsonl

# 3. Process if valid
ja select 'verified == true' new_data.jsonl | ...
```

## Global Options

These flags work with most commands:

| Flag | Description | Example |
|------|-------------|---------|
| `--help`, `-h` | Show help | `ja select --help` |
| `--version` | Show version | `ja --version` |
| `--output <file>`, `-o` | Output file | `ja select ... -o result.jsonl` |
| `--verbose`, `-v` | Verbose output | `ja -v select ...` |

## Expression Syntax

Many commands use expressions for filtering or calculations:

### Basic Expressions

```bash
# Equality
ja select 'status == "active"'

# Comparison
ja select 'age > 30'
ja select 'price <= 100'

# Logical operators
ja select 'age > 18 and status == "active"'
ja select 'role == "admin" or role == "owner"'

# Null checks
ja select 'email != null'
```

### Nested Field Access

```bash
# Dot notation
ja select 'user.profile.age > 25'
ja project user.name,user.email
```

### Value Types

```bash
# Strings (quoted)
'name == "Alice"'

# Numbers (unquoted)
'age > 30'

# Booleans
'is_active == true'
'verified == false'

# Null
'value == null'
'field != null'
```

## Working with Nested Data

jsonl-algebra excels at nested JSON structures:

### Accessing Nested Fields

```json
{"user": {"profile": {"email": "alice@example.com"}}}
```

```bash
# Select nested field
ja project user.profile.email data.jsonl

# Filter on nested field
ja select 'user.profile.age > 25' data.jsonl

# Join on nested field
ja join users.jsonl orders.jsonl --on user.id=customer.id
```

### Flattening Arrays

```json
{"id": 1, "tags": ["admin", "premium"]}
```

```bash
# Explode creates one row per tag
ja explode tags data.jsonl
# Output:
# {"id": 1, "tags": "admin"}
# {"id": 1, "tags": "premium"}
```

## Performance Considerations

### Streaming Operations

These commands stream data (constant memory):

- `select`
- `project`
- `rename`
- `explode`

```bash
# Efficient even for huge files
cat huge.jsonl | ja select 'x > 0' | head -10
```

### Buffering Operations

These commands buffer data (memory grows with input):

- `sort`
- `distinct`
- `groupby`
- `join` (right side)

```bash
# May use significant memory
ja sort name huge.jsonl
```

!!! tip "Optimization Strategy"
    Filter data early in the pipeline to reduce memory usage:
    ```bash
    # Good: Filter first
    ja select 'status == "active"' huge.jsonl | ja sort name

    # Bad: Sort everything first
    ja sort name huge.jsonl | ja select 'status == "active"'
    ```

## Error Handling

### Common Error Messages

**File Not Found**
```bash
$ ja select 'x > 0' missing.jsonl
Error: File not found: missing.jsonl
```

**Invalid Expression**
```bash
$ ja select 'invalid syntax' data.jsonl
Error: Invalid expression: invalid syntax
```

**Missing Required Argument**
```bash
$ ja join users.jsonl orders.jsonl
Error: --on flag required for join operation
```

### Error Output

Errors go to stderr, allowing proper piping:

```bash
# Errors visible, output can be piped
ja select 'age > 30' users.jsonl 2> errors.log | ja project name
```

## Exit Codes

ja uses standard exit codes:

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | General error |
| `2` | Command line syntax error |
| `3` | File I/O error |
| `4` | Invalid data/expression |

Use in scripts:

```bash
if ja select 'status == "active"' users.jsonl > active.jsonl; then
    echo "Filtering successful"
else
    echo "Filtering failed" >&2
    exit 1
fi
```

## Environment Variables

Customize behavior with environment variables:

| Variable | Effect | Example |
|----------|--------|---------|
| `JA_JSON_ERRORS` | Output errors as JSON | `export JA_JSON_ERRORS=1` |
| `JA_COLOR` | Enable/disable colors | `export JA_COLOR=never` |
| `JA_CACHE_SIZE` | REPL cache size | `export JA_CACHE_SIZE=1000` |

## Combining with Unix Tools

ja plays well with standard Unix utilities:

### With grep

```bash
# Find records containing "error"
ja project message logs.jsonl | grep -i error
```

### With wc

```bash
# Count filtered records
ja select 'status == "active"' users.jsonl | wc -l
```

### With head/tail

```bash
# First 10 results
ja sort score --desc scores.jsonl | head -10

# Last 5 results
ja sort timestamp scores.jsonl | tail -5
```

### With parallel

```bash
# Process multiple files in parallel
ls *.jsonl | parallel 'ja select "status == \"active\"" {} > {.}_active.jsonl'
```

## Shell Completion

Enable tab completion for ja commands:

=== "Bash"

    ```bash
    # Add to ~/.bashrc
    eval "$(ja --completion bash)"
    ```

=== "Zsh"

    ```bash
    # Add to ~/.zshrc
    eval "$(ja --completion zsh)"
    ```

=== "Fish"

    ```bash
    # Add to ~/.config/fish/config.fish
    ja --completion fish | source
    ```

## Getting Help

### Built-in Help

```bash
# General help
ja --help

# Command-specific help
ja select --help
ja join --help
ja groupby --help
```

### Online Resources

- [Command Reference](commands.md) - Detailed command documentation
- [Examples](examples.md) - Real-world usage patterns
- [Tutorials](../tutorials/data-analysis.md) - Step-by-step guides
- [FAQ](../faq.md) - Common questions

## Best Practices

1. **Filter Early** - Reduce data size before expensive operations
2. **Use Pipes** - Build complex transformations incrementally
3. **Test Incrementally** - Run each stage separately first
4. **Save Intermediate Results** - For complex pipelines
5. **Validate Input** - Use schema validation for critical data
6. **Handle Errors** - Check exit codes in scripts

## Next Steps

- [Commands Reference](commands.md) - Learn each command in detail
- [Examples & Patterns](examples.md) - See real-world usage
- [Quick Start Tutorial](../getting-started/quickstart.md) - Hands-on learning
- [REPL Mode](../repl/introduction.md) - Interactive exploration
