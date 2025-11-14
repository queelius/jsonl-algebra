# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**jsonl-algebra** is a Python toolkit for manipulating JSONL (JSON Lines) data using relational algebra operations. It provides:

1. **Core Library** (`ja/` directory) - Relational operations on JSONL data
2. **CLI Tool** (`ja` command) - Command-line interface for data processing
3. **Interactive Shell** (`ja-shell` command) - Navigate JSON/JSONL like a filesystem
4. **Integrations** (`integrations/` directory) - MCP server, log analyzer, data explorer, ML pipeline

## Essential Commands

### Development Setup
```bash
# Install in development mode with all dependencies
pip install -e ".[dev]"

# Install optional integrations
pip install -e ".[dataset]"  # For dataset generation
pip install mcp              # For MCP server testing
```

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=ja --cov=integrations --cov-report=html

# Run specific test file
pytest tests/test_compose.py

# Run single test
pytest tests/test_compose.py::test_pipeline_with_select_and_project

# Run tests matching pattern
pytest -k "test_select"
```

### Code Quality
```bash
# Format code (Black + isort configured to work together)
black .
isort .

# Type checking
mypy ja/

# Linting
flake8 ja/
```

### Documentation
```bash
# Serve documentation locally (with live reload)
mkdocs serve
# Visit http://127.0.0.1:8000

# Build static documentation
mkdocs build

# Deploy to GitHub Pages
mkdocs gh-deploy
```

### CLI Usage Examples
```bash
# Test the CLI tools
ja select 'age > 25' tests/data/users.jsonl
ja-shell tests/data/
```

## Architecture

### Core Data Flow

```
JSONL File → ja.commands → ja.core operations → Output
                ↓
           ja.expr (expression evaluation)
           ja.group (groupby operations)
           ja.agg (aggregation functions)
```

### Key Modules

**`ja/core.py`** - Fundamental relational algebra operations
- `select()` - Filter rows using JMESPath or simple expressions
- `project()` - Select specific fields (dot notation for nested data)
- `join()` - Join two datasets on common keys
- `union()`, `difference()`, `intersection()` - Set operations
- `distinct()` - Remove duplicates
- `sort_by()` - Sort by field

**`ja/expr.py`** - Expression evaluation engine
- Parses simple expressions like `age > 25` or `status == "active"`
- Supports dot notation for nested field access
- Falls back to JMESPath for complex queries

**`ja/group.py`** - Grouping and aggregation
- `groupby_agg()` - Single-pass grouping with aggregation
- `groupby_with_metadata()` - Adds metadata for chained grouping
- Supports chained groupby operations with metadata propagation

**`ja/compose.py`** - Functional composition patterns
- `Pipeline` class with Unix pipe operator (`|`) support
- Lazy evaluation for large datasets
- Operation classes: `Select`, `Project`, `Sort`, `GroupBy`, `Take`, `Skip`, `Map`, `Filter`, `Batch`

**`ja/vfs.py`** - Virtual filesystem abstraction (NEW)
- Treat JSONL files as directories of records
- Navigate JSON objects like directories
- `LazyJSONL` class for streaming large files
- Path parsing with indices `[0]`, filters `@[expr]`, and keys

**`ja/shell.py`** - Interactive shell (NEW)
- Rich terminal UI with `prompt_toolkit` and `rich`
- Commands: `ls`, `cd`, `pwd`, `cat`, `tree`, `stat`
- Tab completion and command history

### Command Structure

The CLI (`ja/cli.py`) dispatches to handlers in `ja/commands.py`:
- Each command (select, project, etc.) has a `handle_*()` function
- Handlers read input, call core operations, write output
- All operations are streaming-compatible

### Expression Language

Two expression evaluators:
1. **Simple expressions** (`ja/expr.py`) - Fast, for basic comparisons
2. **JMESPath** - Full JSON query language for complex operations

Use `--jmespath` flag or set `use_jmespath=True` to force JMESPath mode.

### Dot Notation

Access nested data with dots: `user.address.city`
- Implemented in `ja/expr.py:get_field_value()`
- Works consistently across all operations
- Handles arrays with numeric indices

## Testing Architecture

### Test Organization

```
tests/
├── test_core.py          # Core operations (81% coverage)
├── test_compose.py       # Composability (81% coverage) - 65 tests
├── test_mcp_server.py    # MCP server (58% coverage) - 36 tests
└── test_expr_eval.py     # Expression evaluation (89% coverage)
```

### Test Principles

All tests follow TDD best practices (see `TESTING_STRATEGY.md`):
- Test behavior, not implementation
- Given-When-Then structure
- Meaningful names describing behavior and outcome
- Independent, repeatable tests

### Known Issues

See `TEST_FINDINGS.md` for documented bugs:
1. MCP Join Tool - incorrect function signature
2. MCP Aggregate Tool - incorrect parameter format

Tests for these are in `tests/test_mcp_server.py` marked with `@pytest.mark.skip`.

## Integration Development

### MCP Server (`integrations/mcp_server.py`)

Model Context Protocol server for AI assistants:
- 9 tools: query, select, project, aggregate, join, sort, sample, stats, transform
- Uses `ja/vfs.py` for file access
- Returns data in multiple formats: JSONL, JSON, table, summary

**Testing**: Run `integrations/test_mcp_minimal.py` for core logic tests (doesn't require MCP SDK).

### Other Integrations

See `integrations/README.md` for:
- Log Analyzer - streaming log analysis
- Data Explorer - interactive REPL
- ML Pipeline - scikit-learn integration

## Important Design Patterns

### Streaming by Default

All core operations return generators/iterators, enabling:
```python
# Process gigabyte files without loading into memory
for record in select(huge_file, "status == 'active'"):
    process(record)
```

### Lazy Loading for JSONL

`LazyJSONL` class (in `ja/vfs.py`):
- Builds index of record positions on first access
- Loads records on-demand
- Caches recently accessed records (LRU, max 100)

### Composability

Two ways to compose operations:

**1. Unix Pipes** (in shell):
```bash
ja select 'age > 25' data.jsonl | ja project name,email
```

**2. Python Pipelines** (in code):
```python
from ja.compose import Pipeline, Select, Project

pipeline = Pipeline() | Select("age > 25") | Project(["name", "email"])
results = pipeline.run("data.jsonl")
```

## Common Gotchas

### Import Differences

- Core operations: `from ja import select, project, join`
- Commands (CLI handlers): `from ja.commands import handle_select`
- Composability: `from ja.compose import Pipeline, Select`

### File vs Data

Core operations in `ja/core.py` expect data (lists/iterators):
```python
select(data, "expr")  # ✅ Pass data
select("file.jsonl", "expr")  # ❌ Won't work
```

Commands in `ja/commands.py` handle file I/O:
```python
handle_select(args)  # ✅ Reads file from args
```

### Expression Syntax

Simple expressions use Python-like syntax:
```bash
ja select "age > 25 and status == 'active'"  # Simple
ja select "age > \`25\` && status == \`'active'\`" --jmespath  # JMESPath
```

### Nested Field Access

Always use dots, never brackets:
```bash
ja project user.address.city  # ✅ Correct
ja project user[address][city]  # ❌ Wrong
```

## ja-shell Specific

### Path Syntax

- `/` - Root (physical directory)
- `users.jsonl/` - JSONL file (directory of records)
- `[0]/` - Array index or record number
- `@[expr]` - Filter (future feature, partially implemented)
- `name` - Object key

### Virtual Filesystem Mapping

| JSON/JSONL Element | Appears As |
|-------------------|------------|
| JSONL file | Directory |
| JSON object | Directory |
| JSON array | Directory with `[0]`, `[1]`, ... |
| Atomic value | File |

### Example Navigation

```bash
ja-shell
ja:/$ cd users.jsonl/[0]/address
ja:/users.jsonl/[0]/address$ cat city
NYC
```

## Special Notes

### Remember to Run Tests

After any changes, especially to core operations or new features, run the full test suite with coverage to ensure nothing breaks.

### MCP Server Requires SDK

Full MCP server testing requires `pip install mcp`. Basic functionality tests work without it (see `integrations/test_mcp_minimal.py`).

### Documentation Structure

- User-facing docs in `docs/` (MkDocs with Material theme)
- Integration guides in `integrations/*_README.md`
- Testing strategy in `TESTING_STRATEGY.md`
- This file for development guidance
