# Frequently Asked Questions (FAQ)

Common questions about **jsonl-algebra** and their answers.

## General Questions

### What is jsonl-algebra?

jsonl-algebra (command: `ja`) is a command-line tool and Python library for manipulating JSONL (JSON Lines) data using relational algebra operations like select, project, join, and groupby.

### What is JSONL?

JSONL (JSON Lines) is a format where each line is a valid JSON object:

```json
{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
```

Unlike JSON arrays, JSONL files can be processed line-by-line, making them ideal for streaming and large datasets.

### Do I need to know relational algebra to use ja?

No! While ja is based on relational algebra principles, you don't need mathematical knowledge to use it. The commands are intuitive:

- `select` = filter rows
- `project` = choose columns
- `join` = combine datasets
- `groupby` = aggregate data

### What's the difference between ja and jq?

| Feature | ja | jq |
|---------|----|----|
| **Data format** | JSONL (one object per line) | JSON (any structure) |
| **Operations** | Relational algebra | Query language |
| **Learning curve** | Low (SQL-like) | Medium (custom syntax) |
| **Streaming** | Built-in | Partial |
| **Joins** | Native support | Complex |
| **Best for** | Tabular data, logs, datasets | Tree transformations |

**Use ja for:** Filtering, joining, and aggregating structured data
**Use jq for:** Complex JSON transformations and restructuring

## Installation & Setup

### How do I install jsonl-algebra?

```bash
pip install jsonl-algebra
```

See the [Installation Guide](getting-started/installation.md) for detailed instructions.

### What Python version do I need?

Python 3.8 or higher is required.

### Can I use ja without installing Python?

No, ja is a Python-based tool and requires Python to be installed. However, once Python is installed, setup is just one command: `pip install jsonl-algebra`.

### How do I upgrade to the latest version?

```bash
pip install --upgrade jsonl-algebra
```

### Is ja available for Windows?

Yes! ja works on Windows, macOS, and Linux. On Windows, we recommend using WSL2 for the best experience, but it also works in PowerShell and Command Prompt.

## Usage Questions

### How do I filter rows in a JSONL file?

Use the `select` command:

```bash
ja select 'age > 30' users.jsonl
```

### How do I choose specific fields?

Use the `project` command:

```bash
ja project name,email users.jsonl
```

### How do I join two JSONL files?

Use the `join` command:

```bash
ja join users.jsonl orders.jsonl --on user_id=customer_id
```

### Can I pipe ja commands together?

Yes! That's the recommended way to build complex operations:

```bash
cat data.jsonl | ja select 'active == true' | ja project id,name | ja sort name
```

### How do I save output to a file?

Use shell redirection:

```bash
ja select 'age > 30' users.jsonl > filtered.jsonl
```

Or use the `--output` flag:

```bash
ja select 'age > 30' --output filtered.jsonl users.jsonl
```

### How do I work with nested fields?

Use dot notation:

```bash
# Access nested field
ja project user.profile.email data.jsonl

# Filter on nested field
ja select 'user.age > 30' data.jsonl
```

### Can I use ja with stdin?

Yes! ja reads from stdin when no file is specified:

```bash
cat data.jsonl | ja select 'x > 0'
echo '{"name": "Alice"}' | ja project name
curl https://api.example.com/data | ja select 'status == "active"'
```

## Data Format Questions

### What's the difference between .json and .jsonl files?

**JSON (.json):**
```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

**JSONL (.jsonl):**
```json
{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
```

JSONL is better for:
- Streaming (process line-by-line)
- Appending (just add new lines)
- Large files (constant memory usage)
- Log files

### Can ja work with regular JSON files?

ja is designed for JSONL, but you can convert:

```bash
# JSON array to JSONL
cat array.json | jq -c '.[]' > data.jsonl

# JSONL to JSON array
ja collect data.jsonl > array.json
```

### How do I convert CSV to JSONL?

Use the import command:

```bash
ja import csv data.csv > data.jsonl
```

### How do I convert JSONL to CSV?

Use the export command:

```bash
ja export csv data.jsonl > data.csv
```

### Can ja handle nested JSON structures?

Yes! Use dot notation to access nested fields:

```json
{"user": {"profile": {"email": "alice@example.com"}}}
```

```bash
ja project user.profile.email data.jsonl
```

## Performance Questions

### Can ja handle large files?

Yes! ja uses streaming, so it can process files larger than available RAM. Memory usage is constant per operation.

### How do I make operations faster?

1. **Filter early** - Reduce data size before expensive operations
   ```bash
   ja select 'status == "active"' huge.jsonl | ja groupby category
   ```

2. **Use specific commands** - Don't use pipes unnecessarily
   ```bash
   # Good (one pass)
   ja select 'a > 10 and b < 20' data.jsonl

   # Bad (two passes)
   ja select 'a > 10' data.jsonl | ja select 'b < 20'
   ```

3. **Limit output** - Use `head` for sampling
   ```bash
   ja sort score --desc huge.jsonl | head -100
   ```

### Why is sort/groupby slower than select?

Some operations require seeing all data:

- **Streaming** (fast, constant memory): `select`, `project`, `rename`
- **Buffering** (slower, grows with data): `sort`, `distinct`, `groupby`, `join`

### Can I process multiple files in parallel?

Yes, using GNU parallel or xargs:

```bash
ls *.jsonl | parallel 'ja select "status == \"active\"" {} > {.}_active.jsonl'
```

## Expression & Syntax Questions

### What operators can I use in expressions?

| Operator | Purpose | Example |
|----------|---------|---------|
| `==` | Equal | `status == "active"` |
| `!=` | Not equal | `role != "admin"` |
| `>` | Greater than | `age > 30` |
| `<` | Less than | `price < 100` |
| `>=` | Greater or equal | `score >= 90` |
| `<=` | Less or equal | `count <= 10` |
| `and` | Logical AND | `age > 18 and status == "active"` |
| `or` | Logical OR | `role == "admin" or role == "owner"` |

### How do I check for null values?

```bash
ja select 'field == null' data.jsonl    # Is null
ja select 'field != null' data.jsonl    # Is not null
```

### How do I use quotes in expressions?

Use different quote types:

```bash
# Outer single quotes, inner double quotes
ja select 'name == "Alice"' data.jsonl

# Or escape
ja select "name == \"Alice\"" data.jsonl
```

### Can I use regular expressions?

Not directly in expressions, but you can use `grep`:

```bash
ja project message logs.jsonl | grep -E "error|warning"
```

### How do I compare strings?

Strings use lexicographic (dictionary) ordering:

```bash
ja select 'name > "M"' users.jsonl  # Names starting with N-Z
```

## Feature Questions

### Does ja support aggregations?

Yes! Use `groupby` with `--agg`:

```bash
ja groupby category --agg count,total=sum:amount data.jsonl
```

Available aggregations:
- `count` - Count rows
- `sum:field` - Sum values
- `avg:field` - Average
- `min:field` - Minimum
- `max:field` - Maximum
- `list:field` - Collect into array

### Can I do left/right/outer joins?

Yes! Use the `--left`, `--right`, or `--outer` flags:

```bash
ja join users.jsonl orders.jsonl --on id=user_id --left
```

### Is there an interactive mode?

Yes! Use the REPL:

```bash
ja repl users.jsonl
```

Or try ja-shell for filesystem-like navigation:

```bash
ja-shell
```

### Can I validate data schemas?

Yes! Infer and validate JSON schemas:

```bash
# Infer schema
ja schema infer data.jsonl > schema.json

# Validate data
ja schema validate schema.json new_data.jsonl
```

## Troubleshooting

### Command not found: ja

The installation directory may not be in your PATH. Try:

```bash
# Check if installed
pip show jsonl-algebra

# Add to PATH (Linux/Mac)
export PATH="$HOME/.local/bin:$PATH"

# Or use full path
python -m ja.cli select 'x > 0' data.jsonl
```

### Invalid expression error

Check your expression syntax:

```bash
# Wrong - missing quotes around strings
ja select 'name == Alice' data.jsonl

# Right - strings need quotes
ja select 'name == "Alice"' data.jsonl
```

### Memory error with large files

Some operations buffer data. Solutions:

1. Filter first to reduce size
2. Use sampling with `head`
3. Split file into chunks
4. Increase system memory

### JSON decode error

Check that your file is valid JSONL:

```bash
# Validate each line
cat data.jsonl | python -m json.tool

# Find problematic lines
awk 'NR==1 || !system("echo " $0 " | python -m json.tool > /dev/null 2>&1")' data.jsonl
```

### Output looks wrong

ja outputs JSONL by default (one object per line). For pretty printing:

```bash
# Pretty print with jq
ja select 'x > 0' data.jsonl | jq '.'

# Or convert to JSON array
ja collect data.jsonl | jq '.'
```

## Advanced Usage

### Can I use ja in scripts?

Yes! ja is designed for scripting:

```bash
#!/bin/bash
if ja select 'status == "active"' users.jsonl > active.jsonl; then
    echo "Found $(wc -l < active.jsonl) active users"
else
    echo "Error filtering users" >&2
    exit 1
fi
```

### How do I use ja programmatically in Python?

Import the library:

```python
from ja.core import read_jsonl, select, project, join

users = read_jsonl("users.jsonl")
filtered = select(users, "age > 30")
projected = project(filtered, ["name", "email"])

for record in projected:
    print(record)
```

### Can I extend ja with custom operations?

Yes! You can:

1. Use the Python API to create custom functions
2. Build integrations (see [Integrations](integrations/overview.md))
3. Contribute to the project

### How do I process streaming data?

ja works with streaming inputs:

```bash
# Process logs in real-time
tail -f /var/log/app.log | ja select 'level == "ERROR"'

# From API stream
curl -N https://api.example.com/stream | ja project id,timestamp
```

## Integration Questions

### What is the MCP server?

The Model Context Protocol server lets AI assistants use ja operations. See [MCP Integration](integrations/mcp.md).

### Can I use ja with other tools?

Yes! ja works great with:

- **jq** - For complex JSON transformations
- **awk/sed** - For text processing
- **grep** - For pattern matching
- **parallel** - For parallel processing
- **curl** - For API data
- **pandas** - For data analysis

### Does ja work with databases?

Not directly, but you can:

1. Export database to JSONL
2. Process with ja
3. Import results back

Many databases support JSON export.

## Contributing & Development

### How can I contribute?

See the [Contributing Guide](contributing.md) for details:

- Report bugs
- Suggest features
- Submit pull requests
- Improve documentation
- Share use cases

### Where is the source code?

On GitHub: [github.com/queelius/jsonl-algebra](https://github.com/queelius/jsonl-algebra)

### How do I run tests?

```bash
pytest tests/
```

### Is there a roadmap?

Check the GitHub issues and project boards for planned features.

## Getting Help

### Where can I get help?

1. Read the [documentation](index.md)
2. Check this FAQ
3. Search [GitHub issues](https://github.com/queelius/jsonl-algebra/issues)
4. Open a [new issue](https://github.com/queelius/jsonl-algebra/issues/new)

### How do I report a bug?

Open an issue on GitHub with:

- ja version (`ja --version`)
- Python version
- Operating system
- Minimal example to reproduce
- Expected vs actual behavior

### Can I request a feature?

Yes! Open a feature request on GitHub. Include:

- Use case description
- Example of desired behavior
- Why existing features don't work

## Still Have Questions?

- Check the [Tutorials](tutorials/data-analysis.md) for examples
- Read the [CLI Reference](cli/overview.md) for all commands
- Visit the [GitHub Discussions](https://github.com/queelius/jsonl-algebra/discussions)
- Open an [issue](https://github.com/queelius/jsonl-algebra/issues/new) if you found a bug
