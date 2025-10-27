# ja-shell: Navigate JSON/JSONL Files Like a Filesystem

**ja-shell** is a revolutionary interactive shell that lets you navigate JSON and JSONL files as if they were directories and files. Think of it as `cd`-ing into your data!

## Features

✨ **Filesystem Abstraction**
- JSONL files appear as directories of records
- JSON objects become directories of key-value pairs
- Arrays are directories of indexed elements
- Atomic values (strings, numbers) are files you can `cat`

🎨 **Rich Terminal UI**
- Beautiful syntax-highlighted output
- Pretty tables and tree views
- Color-coded file types
- Preview panes

⌨️ **Powerful Shell Features**
- Command history (↑↓ arrows)
- Tab completion for paths and commands
- Auto-suggestions
- Multi-line editing
- Vi/Emacs keybindings

🔍 **Advanced Navigation**
- Navigate nested structures with ease
- Filter records with query expressions (`@[age>25]`)
- Lazy loading for massive JSONL files
- Path normalization (`.` and `..`)

## Installation

```bash
pip install jsonl-algebra
```

Dependencies are installed automatically:
- `prompt-toolkit` - Rich terminal input
- `rich` - Beautiful output formatting

## Quick Start

```bash
# Launch the shell in current directory
ja-shell

# Or specify a data directory
ja-shell ~/my-data
```

## Example Session

```
Welcome to ja-shell!
Navigate JSON/JSONL files like a filesystem.

ja:/$ ls
📁 users.jsonl
📁 config.json

ja:/$ cd users.jsonl
ja:/users.jsonl$ ls
📁 [0]        {"id": 1, "name": "Alice", "age": 30, ...}
📁 [1]        {"id": 2, "name": "Bob", "age": 25, ...}
📁 [2]        {"id": 3, "name": "Charlie", "age": 35, ...}

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
ja:/users.jsonl/[0]/address$ ls
📄 city       NYC
📄 zip        10001

ja:/users.jsonl/[0]/address$ cat city
NYC

ja:/users.jsonl/[0]/address$ cd /config.json
ja:/config.json$ tree 2
📁 /config.json
├── 📁 database
│   ├── 📄 host
│   ├── 📄 port
│   └── 📁 credentials
├── 📁 features
│   ├── 📄 [0]
│   ├── 📄 [1]
│   └── 📄 [2]
└── 📄 debug
```

## Available Commands

### Navigation

| Command | Description | Example |
|---------|-------------|---------|
| `ls [path]` | List directory contents | `ls`, `ls users.jsonl` |
| `cd <path>` | Change directory | `cd users.jsonl/[0]/address` |
| `pwd` | Print working directory | `pwd` |

### Viewing Data

| Command | Description | Example |
|---------|-------------|---------|
| `cat <path>` | Display file contents | `cat name`, `cat [0]` |
| `tree [path] [depth]` | Show directory tree | `tree users.jsonl 2` |
| `stat <path>` | Show detailed info | `stat users.jsonl` |

### Utility

| Command | Description | Example |
|---------|-------------|---------|
| `help` | Show help | `help` |
| `exit` / `quit` | Exit shell | `exit` |

## Path Syntax

### Basic Paths

```bash
/                   # Root (physical directory)
users.jsonl         # JSONL file
config.json         # JSON file
```

### Navigating Structures

```bash
[0]                 # Array/JSONL index (first element)
[42]                # 43rd element
name                # Object key
address/city        # Nested navigation
```

### Special Paths

```bash
.                   # Current directory
..                  # Parent directory
/absolute/path      # Absolute from root
relative/path       # Relative to current directory
```

### Filters (Coming Soon)

```bash
@[age>25]           # Filter records where age > 25
@[status=='active'] # Filter by equality
@[*]                # All elements (explicit)
```

## Use Cases

### 1. Exploring Unfamiliar Data

```bash
ja-shell data/
ja:/$ ls
ja:/$ cd logs.jsonl
ja:/logs.jsonl$ ls | head -5  # See structure
ja:/logs.jsonl$ cd [0]
ja:/logs.jsonl/[0]$ ls        # Discover fields
ja:/logs.jsonl/[0]$ cat       # See sample record
```

### 2. Debugging API Responses

```bash
ja-shell api_responses/
ja:/$ cd response.json/data/users/[0]
ja:/response.json/data/users/[0]$ ls
ja:/response.json/data/users/[0]$ cat email
```

### 3. Data Quality Checking

```bash
ja:/users.jsonl$ cd [0]
ja:/users.jsonl/[0]$ stat
# Check record structure
ja:/users.jsonl/[0]$ cat
# View full record in pretty JSON
```

### 4. Navigating Nested Configs

```bash
ja:/$ cd config.json/database/credentials
ja:/config.json/database/credentials$ cat user
admin
ja:/config.json/database/credentials$ cat password
secret123
```

## Tips & Tricks

### 1. Tab Completion is Your Friend

```bash
ja:/$ cd us<TAB>         # Completes to users.jsonl
ja:/users.jsonl$ cd [0]/ad<TAB>  # Completes to address/
```

### 2. Use Tree for Quick Overview

```bash
ja:/$ tree users.jsonl 2
# See structure at a glance
```

###3. Navigate Quickly with Absolute Paths

```bash
ja:/users.jsonl/[0]/address$ cd /config.json
# Jump directly to config
```

### 4. Command History

```bash
# Press ↑ to recall previous commands
# Press Ctrl+R to search history
```

### 5. Cat Works on Directories Too

```bash
ja:/users.jsonl$ cat [0]
# Shows the entire record as pretty JSON
```

## Architecture

### How It Works

```
┌─────────────────────────────────────┐
│  JAShell (UI Layer)                 │
│  - prompt_toolkit for rich input    │
│  - rich for pretty output           │
│  - Command handlers                 │
└─────────────────┬───────────────────┘
                  │
┌─────────────────▼───────────────────┐
│  JSONPath (VFS Layer)               │
│  - Path parsing & resolution        │
│  - Navigation (cd, ls)              │
│  - Data access (cat, stat)          │
│  - Lazy loading for JSONL           │
└─────────────────┬───────────────────┘
                  │
┌─────────────────▼───────────────────┐
│  Storage Layer                      │
│  - LazyJSONL: Stream large files   │
│  - File caching                     │
│  - JSON parsing                     │
└─────────────────────────────────────┘
```

### Virtual Filesystem Mapping

| JSON/JSONL Element | Appears As | Example |
|-------------------|------------|---------|
| JSONL file | Directory | `users.jsonl/` |
| JSON file | Directory | `config.json/` |
| JSONL record | Directory | `[0]/`, `[1]/` |
| JSON object | Directory | `address/` |
| JSON array | Directory | `tags/` |
| Atomic value | File | `name`, `age` |

## Performance

### Lazy Loading

ja-shell uses lazy loading for JSONL files:
- Files are not loaded entirely into memory
- Records are loaded on-demand
- Recently accessed records are cached
- Perfect for gigabyte-sized datasets

### Caching

- Physical files are cached after first load
- JSONL indices are built lazily
- LRU cache for JSONL records (default: 100 records)

## Comparison with Other Tools

| Feature | ja-shell | jq | JSON viewers |
|---------|----------|----|--------------|
| Interactive | ✅ | ❌ | ✅ |
| Navigation | ✅ Filesystem-like | ❌ Query-based | ⚠️ GUI-based |
| Large JSONL | ✅ Streaming | ⚠️ Loads all | ❌ Memory-bound |
| Pretty output | ✅ Tables & trees | ⚠️ JSON only | ✅ |
| Learning curve | 🟢 Low (like `cd`/`ls`) | 🔴 High (query language) | 🟡 Medium |
| Scriptable | ⚠️ (Future) | ✅ | ❌ |

## Future Features

### Query-Based Navigation (Coming Soon)

```bash
ja:/users.jsonl$ cd @[age>25]
# Navigate into filtered view
ja:/users.jsonl/@[age>25]$ ls
📁 [0]  # Alice (age: 30)
📁 [1]  # Charlie (age: 35)
```

### Write Operations

```bash
ja:/users.jsonl/[0]$ echo "Bob" > name
# Edit Alice's name to Bob
```

### FUSE Filesystem (Future)

```bash
# Mount as real filesystem
ja-mount ~/data /mnt/json

# Now ANY program can use it!
cd /mnt/json/users.jsonl/[0]/address
cat city  # Works with standard cat!
vim name  # Edit with vim!
```

## Troubleshooting

### Q: Tab completion doesn't work
**A:** Make sure `prompt-toolkit` is installed: `pip install prompt-toolkit`

### Q: Colors look wrong
**A:** Your terminal may not support true color. Try a modern terminal (iTerm2, Windows Terminal, etc.)

### Q: Can't navigate into `.jsonl` file
**A:** Make sure the file contains valid JSONL (one JSON object per line)

### Q: Arrow keys show weird characters
**A:** Your terminal may not support ANSI escape codes. Use a modern terminal.

### Q: Performance is slow with huge JSONL files
**A:** This is expected - we lazy-load records. The first access to each record may be slow, but subsequent access is cached.

## Contributing

Ideas for new commands or features? Issues with the shell?

File an issue at: https://github.com/queelius/jsonl-algebra/issues

## License

MIT License - Same as jsonl-algebra

## See Also

- [jsonl-algebra README](README.md) - Core library documentation
- [ja CLI documentation](docs/cli.md) - Command-line tools
- [JSONL specification](http://jsonlines.org/) - JSONL format
