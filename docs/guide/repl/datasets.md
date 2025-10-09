# Dataset Management

The REPL provides comprehensive dataset management commands to load, organize, and inspect your data.

## Loading Datasets

### `load <file> [name]`

Load a JSONL file into the workspace:

```
# Load with default name (filename without extension)
ja> load users.jsonl
Loaded: users (current)
  Path: /home/user/users.jsonl

# Load with custom name
ja> load /data/customers.jsonl clients
Loaded: clients (current)
  Path: /data/customers.jsonl
```

**Behavior:**
- The dataset becomes the current dataset
- Default name is the filename stem (without `.jsonl`)
- Names must be unique (loading fails if name already exists)
- File paths are stored, not data (streaming model preserved)

## Switching Between Datasets

### `cd <name>`

Change the current dataset:

```
ja> load users.jsonl
ja> load orders.jsonl
ja> cd users
Current dataset: users
```

### `pwd` / `current`

Show the current dataset:

```
ja> pwd
Current dataset: users
  Path: /home/user/users.jsonl
```

## Listing Datasets

### `datasets`

List all registered datasets:

```
ja> datasets
Registered datasets:
  orders
    /home/user/orders.jsonl
  users (current)
    /home/user/users.jsonl
  filtered
    /tmp/ja_repl_abc123/filtered_1.jsonl
```

**Output shows:**
- Dataset names (alphabetically sorted)
- Current dataset marked with `(current)`
- File paths (temp files for derived datasets)

## Inspecting Datasets

### `info [name]`

Show detailed statistics about a dataset:

```
# Show info for current dataset
ja> info

Dataset: users
Path: /home/user/users.jsonl
Rows: 1,234
Size: 456.7 KB
Fields: id, name, age, email, location.city, location.state

Sample (first row):
  {
    "id": 1,
    "name": "Alice",
    "age": 30,
    ...
  }

# Show info for specific dataset
ja> info filtered

Dataset: filtered
Path: /tmp/ja_repl_abc123/filtered_1.jsonl
Rows: 523
Size: 198.4 KB
...
```

**Information displayed:**
- Dataset name
- File path
- Row count (with comma formatting)
- File size (in B, KB, or MB)
- Field names (with dot notation for nested fields)
- Sample of first row

### `ls [name] [--limit N]`

Preview dataset contents:

```
# Preview current dataset (default: window-size lines)
ja> ls
{"id": 1, "name": "Alice", ...}
{"id": 2, "name": "Bob", ...}
...

# Preview with custom limit
ja> ls --limit 3
{"id": 1, "name": "Alice", ...}
{"id": 2, "name": "Bob", ...}
{"id": 3, "name": "Charlie", ...}

# Preview specific dataset
ja> ls users --limit 5
...
```

## Saving Datasets

### `save <file>`

Persist the current dataset to a file:

```
ja> save output.jsonl
Saved users to: output.jsonl

# Save to a different location
ja> save /tmp/backup.jsonl
Saved users to: /tmp/backup.jsonl
```

**Notes:**
- Only saves the current dataset
- Does NOT register the saved file as a new dataset
- Overwrites existing files without warning

## Dataset Lifecycle

### Original Files vs. Derived Datasets

- **Original files**: Loaded with `load`, stored at their original paths
- **Derived datasets**: Created by operations, stored in temp directory

```
ja> load users.jsonl
# users -> /home/user/users.jsonl (original)

ja> select 'age > 30' adults
# adults -> /tmp/ja_repl_xyz/adults_1.jsonl (derived)
```

### Temporary Files

Derived datasets are automatically stored in a temporary directory:

```
/tmp/ja_repl_<session_id>/
  ├── adults_1.jsonl
  ├── filtered_2.jsonl
  └── joined_3.jsonl
```

**Cleanup:**
- Temp files persist for the session duration
- Automatically cleaned up when REPL exits
- Use `save` to persist important results

## Name Conflict Prevention

Dataset names must be unique:

```
ja> load users.jsonl
Loaded: users (current)

ja> load users.jsonl
Error: Dataset 'users' already exists. Use a different name.

# Solution: Use custom name
ja> load users.jsonl users2
Loaded: users2 (current)
```

This applies to both loading and operations:

```
ja> select 'age > 30' filtered
Created: filtered (current)

ja> select 'city == "NYC"' filtered
Error: Dataset 'filtered' already exists. Use a different name.
```

## Best Practices

1. **Use descriptive names** for derived datasets:
   ```
   ja> select 'status == "active"' active_users
   ja> select 'age > 65' seniors
   ```

2. **Check `info` before operations** to understand your data:
   ```
   ja> load data.jsonl
   ja> info  # Check structure first
   ja> select '...' filtered
   ```

3. **Use `datasets` to track your workspace**:
   ```
   ja> datasets  # See what's loaded
   ja> info users
   ja> info processed
   ```

4. **Save important results** before continuing:
   ```
   ja> select ... important
   ja> save important_data.jsonl  # Persist it
   ja> select ... more_work
   ```

## Examples

### Loading Multiple Files

```
ja> load users.jsonl
Loaded: users (current)

ja> load orders.jsonl
Loaded: orders (current)

ja> load products.jsonl
Loaded: products (current)

ja> datasets
Registered datasets:
  orders
  products (current)
  users
```

### Working with Custom Names

```
ja> load jan_sales.jsonl sales_jan
Loaded: sales_jan (current)

ja> load feb_sales.jsonl sales_feb
Loaded: sales_feb (current)

ja> cd sales_jan
Current dataset: sales_jan

ja> union sales_feb q1_sales
Created: q1_sales (current)

ja> info q1_sales
Dataset: q1_sales
Rows: 2,456
...
```

### Exploring Unknown Data

```
ja> load mystery.jsonl
Loaded: mystery (current)

ja> info
Dataset: mystery
Rows: 10,234
Size: 2.3 MB
Fields (15 total): id, timestamp, user.name, user.email, ...

ja> ls --limit 2
{"id": 1, "timestamp": "2024-01-01", ...}
{"id": 2, "timestamp": "2024-01-02", ...}
```
