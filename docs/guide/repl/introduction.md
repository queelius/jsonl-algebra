# Interactive REPL

The JSONL Algebra REPL (Read-Eval-Print Loop) provides a powerful interactive environment for exploring and transforming JSONL data.

## Overview

The REPL allows you to:

- **Load and manage multiple datasets** by name
- **Execute operations immediately** and see results
- **Chain transformations** interactively
- **Explore data** with preview and statistics commands
- **Save results** when you're satisfied with the transformation

Unlike traditional pipeline-based tools, the REPL maintains a workspace of named datasets that you can switch between, combine, and transform without leaving your session.

## Starting the REPL

```bash
# Start an empty REPL session
ja repl

# Start with a file already loaded
ja repl mydata.jsonl
```

## Key Design Principles

### Named Datasets

Every dataset in the REPL has a unique name:

- Original files keep their filename (without extension) as the default name
- Operations create new named datasets
- You can have multiple datasets loaded simultaneously

### Non-Destructive Operations

All operations create **new datasets** rather than modifying existing ones:

```
ja> load users.jsonl
Loaded: users (current)

ja> select 'age > 30' adults
Created: adults (current)

# 'users' still exists unchanged!
ja> datasets
  users
  adults (current)
```

### Immediate Execution

Operations execute immediately and show results:

```
ja> select 'age > 30' filtered
Created: filtered (current)

ja> ls --limit 3
{"id": 1, "name": "Alice", "age": 35}
{"id": 2, "name": "Bob", "age": 42}
{"id": 3, "name": "Charlie", "age": 38}
```

### Current Dataset Context

The REPL tracks a "current" dataset that operations use by default:

```
ja> load users.jsonl
Loaded: users (current)

ja> pwd
Current dataset: users
  Path: /home/user/users.jsonl
```

## Basic Workflow

Here's a typical REPL workflow:

```
# 1. Load data
ja> load users.jsonl
Loaded: users (current)

# 2. Explore the data
ja> info
Dataset: users
Rows: 100
Size: 15.2 KB
Fields: id, name, age, email, city

# 3. Preview some rows
ja> ls --limit 5
...

# 4. Transform the data
ja> select 'age > 25' adults
Created: adults (current)

# 5. Check the results
ja> info
Dataset: adults
Rows: 75
Size: 11.4 KB

# 6. Save when satisfied
ja> save output.jsonl
Saved adults to: output.jsonl
```

## Command Categories

The REPL commands fall into several categories:

### Dataset Management
- `load`, `cd`, `pwd`, `datasets`, `info`, `save`

### Data Operations
- Unary: `select`, `project`, `rename`, `distinct`, `sort`, `groupby`
- Binary: `join`, `union`, `intersection`, `difference`, `product`

### Exploration
- `ls`, `info`

### Utilities
- `!<command>` - Shell commands
- `window-size` - Settings
- `help` - Show help
- `exit` - Quit

## Next Steps

- [Dataset Management](datasets.md) - Learn about loading, switching, and managing datasets
- [Operations](operations.md) - Explore all available data transformations
- [Tips & Tricks](tips.md) - Advanced REPL usage patterns
