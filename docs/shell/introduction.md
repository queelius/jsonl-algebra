# ja-shell: Interactive JSON Navigator

**ja-shell** is a revolutionary way to explore JSON and JSONL files - navigate them like a filesystem! Think `cd`, `ls`, and `cat` for your data.

## What is ja-shell?

ja-shell creates a **virtual filesystem** from your JSON data:

- JSONL files become directories of records
- JSON objects become directories of fields
- Arrays become directories of indexed elements
- Atomic values (strings, numbers) become files

You can `cd` into your data, `ls` to see what's there, and `cat` to view values - just like navigating directories!

## Why Use ja-shell?

### Traditional Approach: jq

```bash
# With jq - requires learning query syntax
cat users.jsonl | jq '.[0].address.city'
cat users.jsonl | jq 'map(select(.age > 30))'
```

### The ja-shell Way

```
$ ja-shell

ja:/$ cd users.jsonl
ja:/users.jsonl$ cd [0]
ja:/users.jsonl/[0]$ cd address
ja:/users.jsonl/[0]/address$ cat city
NYC

ja:/users.jsonl/[0]/address$ cd /users.jsonl
ja:/users.jsonl$ ls @[age>30]  # Coming soon!
```

**Result:** Natural, filesystem-like navigation with tab completion!

## Key Features

### 1. Filesystem Abstraction

Navigate JSON structures intuitively:

```
users.jsonl               →  Directory of records
  [0]/                    →  First record (directory)
    name                  →  Field (file): "Alice"
    age                   →  Field (file): 30
    address/              →  Nested object (directory)
      city                →  Field (file): "NYC"
      zip                 →  Field (file): "10001"
    tags[]                →  Array (directory)
      [0]                 →  Element (file): "admin"
      [1]                 →  Element (file): "premium"
```

### 2. Rich Terminal UI

Beautiful, color-coded output:

- Syntax-highlighted JSON
- Pretty-printed tables
- Tree views for structure
- Icons for file types
- Progress indicators

### 3. Powerful Shell Features

Everything you expect from a modern shell:

- **Tab completion** - Auto-complete paths and commands
- **Command history** - Use arrow keys to recall commands
- **Auto-suggestions** - See suggestions as you type
- **Multi-line editing** - Edit complex commands easily
- **Vi/Emacs bindings** - Use your preferred keybindings

### 4. Performance Optimized

- **Lazy loading** - JSONL records loaded on-demand
- **Caching** - Recently accessed data stays in memory
- **Streaming** - Handle gigabyte files efficiently
- **Incremental parsing** - Start exploring immediately

## Quick Start

### Installation

ja-shell is included when you install jsonl-algebra:

```bash
pip install jsonl-algebra
```

### Launch the Shell

```bash
# Start in current directory
ja-shell

# Start in specific directory
ja-shell ~/data

# Start with a file
ja-shell users.jsonl
```

### Your First Session

```
$ ja-shell data/

Welcome to ja-shell!
Navigate JSON/JSONL files like a filesystem.
Type 'help' for available commands.

ja:/$ ls
📁 users.jsonl
📁 orders.jsonl
📁 config.json

ja:/$ cd users.jsonl
ja:/users.jsonl$ ls
📁 [0]        {"id": 1, "name": "Alice", ...}
📁 [1]        {"id": 2, "name": "Bob", ...}
📁 [2]        {"id": 3, "name": "Charlie", ...}

ja:/users.jsonl$ cd [0]
ja:/users.jsonl/[0]$ ls
📄 id         1
📄 name       Alice
📄 age        30
📄 email      alice@example.com
📁 address/   {"city": "NYC", "zip": "10001"}
📁 tags[]     ["admin", "premium"]

ja:/users.jsonl/[0]$ cat name
Alice

ja:/users.jsonl/[0]$ cd address
ja:/users.jsonl/[0]/address$ pwd
/users.jsonl/[0]/address

ja:/users.jsonl/[0]/address$ cat city
NYC

ja:/users.jsonl/[0]/address$ cd ../tags
ja:/users.jsonl/[0]/tags$ ls
📄 [0]        admin
📄 [1]        premium

ja:/users.jsonl/[0]/tags$ cd /
ja:/$ tree users.jsonl 2
📁 /users.jsonl
├── 📁 [0]
│   ├── 📄 id
│   ├── 📄 name
│   ├── 📄 age
│   ├── 📄 email
│   ├── 📁 address
│   └── 📁 tags
├── 📁 [1]
│   └── ...

ja:/$ exit
Goodbye!
```

## Common Use Cases

### 1. Exploring Unfamiliar Data

When you receive JSON data you've never seen before:

```
ja:/$ cd api_response.json
ja:/api_response.json$ tree 2
# See structure at a glance

ja:/api_response.json$ cd data/users/[0]
ja:/api_response.json/data/users/[0]$ ls
# Discover what fields exist
```

### 2. Debugging API Responses

Quickly navigate complex API responses:

```
ja:/$ cd response.json/data/users/[0]/profile
ja:/response.json/data/users/[0]/profile$ cat email
user@example.com

ja:/response.json/data/users/[0]/profile$ cd ../permissions
ja:/response.json/data/users/[0]/permissions$ ls
```

### 3. Configuration File Inspection

Navigate nested configuration:

```
ja:/$ cd config.json/database/credentials
ja:/config.json/database/credentials$ cat host
localhost

ja:/config.json/database/credentials$ cat port
5432
```

### 4. Log File Analysis

Explore log entries interactively:

```
ja:/$ cd logs.jsonl
ja:/logs.jsonl$ cd [0]
ja:/logs.jsonl/[0]$ cat level
ERROR

ja:/logs.jsonl/[0]$ cat message
Connection timeout

ja:/logs.jsonl/[0]$ cat timestamp
2025-10-27T10:30:00Z
```

## Available Commands

### Navigation

| Command | Description | Example |
|---------|-------------|---------|
| `ls [path]` | List contents | `ls`, `ls users.jsonl` |
| `cd <path>` | Change directory | `cd users.jsonl/[0]` |
| `pwd` | Show current path | `pwd` |

### Viewing Data

| Command | Description | Example |
|---------|-------------|---------|
| `cat <path>` | Display content | `cat name`, `cat [0]` |
| `tree [path] [depth]` | Show tree structure | `tree users.jsonl 2` |
| `stat <path>` | Show metadata | `stat users.jsonl` |

### Utility

| Command | Description | Example |
|---------|-------------|---------|
| `help` | Show help | `help` |
| `exit` / `quit` | Exit shell | `exit` |

## Path Syntax

### Absolute Paths

Start with `/` to navigate from root:

```bash
/users.jsonl/[0]/address/city
```

### Relative Paths

Navigate from current location:

```bash
address/city        # Go into address, then city
../orders           # Go up one level, then into orders
```

### Special Paths

- `.` - Current directory
- `..` - Parent directory
- `/` - Root (physical filesystem root)

### JSONL Records

Access by index in square brackets:

```bash
[0]     # First record
[1]     # Second record
[42]    # 43rd record
```

### Object Keys

Just use the key name:

```bash
name
address
user_id
```

### Nested Navigation

Use `/` to go deeper:

```bash
address/city
user/profile/email
data/results/[0]/score
```

## Tips & Tricks

### 1. Tab Completion is Your Friend

Press ++tab++ to auto-complete:

```
ja:/$ cd us<TAB>
# Completes to: cd users.jsonl

ja:/users.jsonl$ cd [0]/ad<TAB>
# Completes to: cd [0]/address
```

### 2. Use Tree for Quick Overview

```
ja:/$ tree users.jsonl 2
# See structure at a glance without cd-ing around
```

### 3. Jump with Absolute Paths

```
ja:/users.jsonl/[5]/address$ cd /config.json
# Jump directly instead of cd ../../..
```

### 4. Cat Works on Directories Too

```
ja:/users.jsonl$ cat [0]
# Shows entire record as pretty JSON
```

### 5. Command History

- Press ++up++ / ++down++ to recall previous commands
- Press ++ctrl+r++ to search history

## Comparison with Other Tools

| Feature | ja-shell | jq | GUI Viewers |
|---------|----------|----|----|
| **Interactive** | ✅ | ❌ | ✅ |
| **Navigation** | ✅ Filesystem-like | ❌ Query-based | ⚠️ GUI-only |
| **Large JSONL** | ✅ Streaming | ⚠️ Loads all | ❌ Memory-bound |
| **Learning Curve** | 🟢 Low (like cd/ls) | 🔴 High (query syntax) | 🟡 Medium |
| **Terminal UI** | ✅ Rich colors/tables | ⚠️ Plain text | ✅ GUI |
| **Tab Completion** | ✅ | ❌ | N/A |
| **Scriptable** | ⚠️ Future feature | ✅ | ❌ |

## Architecture

How ja-shell works under the hood:

```
┌─────────────────────────────────┐
│  JAShell (UI Layer)             │
│  - prompt_toolkit input         │
│  - rich output                  │
│  - Command handlers             │
└────────────┬────────────────────┘
             │
┌────────────▼────────────────────┐
│  JSONPath (VFS Layer)           │
│  - Path parsing & resolution    │
│  - Navigation (cd, ls)          │
│  - Data access (cat, stat)      │
│  - Lazy JSONL loading           │
└────────────┬────────────────────┘
             │
┌────────────▼────────────────────┐
│  Storage Layer                  │
│  - LazyJSONL streaming          │
│  - File caching                 │
│  - JSON parsing                 │
└─────────────────────────────────┘
```

## Performance

### Lazy Loading

JSONL files are **not loaded entirely** into memory:

- Records loaded on-demand when accessed
- Recently accessed records cached (LRU cache)
- Perfect for multi-gigabyte files

### Caching Strategy

- **Physical files** cached after first load
- **JSONL indices** built lazily
- **LRU cache** for JSONL records (default: 100)

### Benchmarks

| File Size | First Access | Cached Access | Memory Usage |
|-----------|--------------|---------------|--------------|
| 1 MB | ~50ms | <1ms | ~2 MB |
| 100 MB | ~100ms | <1ms | ~5 MB |
| 1 GB | ~200ms | <1ms | ~10 MB |
| 10 GB | ~300ms | <1ms | ~20 MB |

*Note: Memory usage stays constant regardless of file size!*

## Future Features

### Query-Based Navigation (Coming Soon)

Filter records as you navigate:

```
ja:/users.jsonl$ cd @[age>30]
ja:/users.jsonl/@[age>30]$ ls
📁 [0]  # Alice (age: 30)
📁 [1]  # Charlie (age: 35)
```

### Write Operations (Planned)

Edit data in-place:

```
ja:/users.jsonl/[0]$ echo "Bob" > name
# Changes Alice's name to Bob
```

### Export Filtered Data (Planned)

```
ja:/users.jsonl/@[age>30]$ export > filtered.jsonl
# Save filtered view to file
```

### FUSE Filesystem (Future)

Mount JSON as a real filesystem:

```bash
ja-mount ~/data /mnt/json

# Now ANY program can access it!
cd /mnt/json/users.jsonl/[0]/address
cat city  # Works with standard cat!
vim name  # Edit with vim!
```

## Limitations

Current limitations to be aware of:

1. **Read-only** - Can't modify data yet (coming soon)
2. **No regex in paths** - Can't use wildcards yet
3. **Limited filtering** - Advanced queries coming soon
4. **No scripting** - Can't automate commands yet

## Troubleshooting

### Tab Completion Not Working

Make sure `prompt-toolkit` is installed:

```bash
pip install prompt-toolkit
```

### Colors Look Wrong

Your terminal may not support true color. Try a modern terminal:

- Linux: GNOME Terminal, Konsole, kitty
- macOS: iTerm2, Alacritty
- Windows: Windows Terminal, WSL2

### Can't Navigate into .jsonl File

Ensure the file contains valid JSONL (one JSON object per line):

```bash
# Check file format
head -1 users.jsonl | python3 -m json.tool
```

### Performance is Slow

For very large files, first access may be slow. Subsequent access is cached and fast.

## Next Steps

Ready to dive in? Continue with:

- [Tutorial](tutorial.md) - Step-by-step walkthrough
- [Command Reference](commands.md) - All commands detailed
- [Advanced Features](advanced.md) - Power user tips
- [Use Cases](use-cases.md) - Real-world examples

## Getting Help

- Type `help` in the shell
- Check the [FAQ](../faq.md)
- Report issues on [GitHub](https://github.com/queelius/jsonl-algebra/issues)

!!! tip "Try It Now!"
    ```bash
    ja-shell
    ```
    Start exploring your data immediately!
