# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**jsonl-algebra** (v1.04) is a Python toolkit for manipulating JSONL data using relational algebra operations. Entry points: `ja` (CLI), `ja-shell` (interactive filesystem navigator), `ja repl` (interactive REPL). Published on PyPI as `jsonl-algebra`.

## Essential Commands

```bash
# Dev install
pip install -e ".[dev]"

# Tests
pytest                                              # all tests (~300 tests)
pytest --cov=ja --cov=integrations --cov-report=html  # with coverage
pytest tests/test_compose.py                        # single file
pytest tests/test_compose.py::test_pipeline_with_select_and_project  # single test
pytest -k "test_select"                             # pattern match

# Code quality
black . && isort .   # format (both configured for line-length 88)
mypy ja/             # type check
flake8 ja/           # lint

# Docs
mkdocs serve         # local dev server at http://127.0.0.1:8000
mkdocs gh-deploy     # deploy to GitHub Pages
```

## Architecture

### Core Data Flow

```
CLI (ja/cli.py) → argparse dispatches to ja/commands.py handle_*() functions
                                    ↓
                        ja/core.py (relational ops)
                        ja/expr.py (expression eval)
                        ja/group.py + ja/agg.py (groupby/aggregation)
                        ja/window.py (window functions)
                        ja/schema.py (schema infer/validate)
                                    ↓
                              stdout (JSONL)
```

### Module Map

| Module | Role |
|--------|------|
| `cli.py` | argparse CLI with 18 top-level subcommands (schema/export/import have nested sub-subcommands), dispatches to `commands.py` |
| `commands.py` | `handle_*()` functions that bridge CLI args → core operations → output |
| `core.py` | `select`, `project`, `join` (inner/left/right/outer/cross), `union`, `difference`, `intersection`, `distinct`, `sort_by`, `rename`, `product`, `collect` |
| `expr.py` | Two-mode expression evaluation: simple Python-like expressions (fast path) with JMESPath fallback. `get_field_value()` handles dot notation everywhere. |
| `group.py` | `groupby_agg()` for single-pass grouping, `groupby_with_metadata()` for chained groupby |
| `agg.py` | Aggregation functions: sum, avg, min, max, count, list, first, last |
| `window.py` | SQL-style window functions: `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, `ntile`, `percent_rank`, `cume_dist`. Partition-by and order-by support. |
| `compose.py` | `Pipeline` class with `|` operator. Operation classes: `Select`, `Project`, `Sort`, `Distinct`, `Rename`, `GroupBy`, `Take`, `Skip`, `Map`, `Filter`, `Batch` |
| `schema.py` | JSON Schema inference from JSONL data, schema merging. Validation handled by `jsonschema` library in `commands.py`. |
| `vfs.py` | Virtual filesystem: JSONL files as directories, `LazyJSONL` with LRU cache (max 100 records), path parsing with `[0]` indices and `@[expr]` filters |
| `shell.py` | `ja-shell` entry point: `prompt_toolkit` + `rich` UI, 13 commands (ls/cd/pwd/cat/tree/stat/head/tail/count/grep/select/help/exit), tab completion |
| `repl.py` | `ja repl` entry point: named datasets, load/save/cd/datasets/info/ls, all core operations as REPL commands |
| `export.py` | JSONL ↔ JSON array, JSONL → directory of JSON files |
| `exporter.py` | JSONL → CSV (handles nested field flattening) |
| `importer.py` | CSV → JSONL, directory of JSON → JSONL |

### CLI Commands (18 top-level, 23 leaf commands)

Core: `select`, `project`, `join`, `product`, `rename`, `union`, `intersection`, `difference`, `distinct`, `sort`
Grouping: `groupby` (with `--agg` for immediate aggregation), `agg`, `collect`
Window: `window <function>` (positional arg, with `--partition-by`, `--order-by`)
Schema: `schema infer`, `schema validate`
Format: `export array|jsonl|explode|csv`, `import csv|implode`
Interactive: `repl`

## Key Design Patterns

### Memory Model

Core operations (`ja/core.py`) work on and return `List[Row]` — data must fit in memory. The CLI (`commands.py`) loads entire files via `read_jsonl()` before processing. For large datasets, use `Pipeline(lazy=True)` from `ja/compose.py` which provides generator-based streaming via lazy operation variants.

### File vs Data Boundary

Core operations (`ja/core.py`) expect **data** (lists/iterators of dicts). Commands (`ja/commands.py`) handle file I/O. Don't pass filenames to core functions.

```python
# Core: expects data
from ja.core import select
results = select(data_iterator, "age > 25")

# Commands: handle files
from ja.commands import handle_select
handle_select(args)  # reads from args.file
```

### Expression Language

Two modes with automatic fallback:
1. **Simple expressions** (`ja/expr.py`) — Python-like: `age > 25 and status == 'active'`
2. **JMESPath** — `--jmespath` flag on `project` command only

Dot notation everywhere: `user.address.city` (never brackets).

### Composability

```python
from ja.compose import Pipeline, Select, Project
pipeline = Pipeline() | Select("age > 25") | Project(["name", "email"])
results = pipeline(data)  # pass data (list/iterator), not filename
```

Or via Unix pipes: `ja select 'age > 25' data.jsonl | ja project name,email`

## Testing

~300 tests across 12 active test files (`test_cli.py` and `test_utils.py` exist but have no collected tests). Key test files:

| File | Focus |
|------|-------|
| `test_core.py` + `test_core_enhanced.py` | Core relational operations |
| `test_compose.py` | Pipeline, composable operations (65 tests) |
| `test_window.py` | Window functions (27 tests) |
| `test_mcp_server.py` | MCP server integration (34 tests) |
| `test_groupby.py` | Grouping operations |
| `test_schema.py` | Schema inference/validation |
| `test_export.py` + `test_exporter.py` + `test_importer.py` | Format conversion |
| `test_expr_eval.py` | Expression evaluation |
| `test_dataset_integration.py` | End-to-end dataset integration |

Test data lives in `tests/data/`.

## Integrations

In `integrations/` directory (not installed as a package — standalone scripts):

- **MCP Server** (`mcp_server.py`) — Model Context Protocol for AI assistants. 9 tools. Requires `pip install mcp`. Basic tests via `test_mcp_minimal.py` don't need the SDK.
- **Log Analyzer** (`log_analyzer.py`) — Streaming log processing with sliding windows
- **Data Explorer** (`data_explorer.py`) — Interactive REPL with SQL-like syntax
- **ML Pipeline** (`ml_pipeline.py`) — scikit-learn integration

## Common Gotchas

- `ja/core.py:join()` supports 5 join types (`inner`, `left`, `right`, `outer`, `cross`) — the `--type` CLI flag
- `groupby` has two modes: immediate aggregation with `--agg sum:amount` vs. metadata-based for chaining with downstream `agg` command
- Window functions require `--order-by` to be meaningful (rank, lag, etc.)
- `LazyJSONL` caches up to 100 records — watch for stale data if underlying file changes
- The `integrations/` directory is **not** in `[tool.setuptools] packages` — it's standalone scripts, not importable as a package
- `select` expression parsing splits on ` and ` / ` or ` via string splitting — no parenthesized grouping, no mixing `and`+`or` in a single expression
- `export csv --apply` uses dynamic code execution on user-provided expressions — security boundary; don't expose via MCP or untrusted input
- `TestDataGenerator` in `tests/test_utils.py` triggers `PytestCollectionWarning` due to `Test` prefix — it's a utility, not a test class

## VFS / ja-shell Path Syntax

| JSON/JSONL Element | Appears As |
|-------------------|------------|
| JSONL file | Directory of records |
| JSON object | Directory of keys |
| JSON array | Directory with `[0]`, `[1]`, ... entries |
| Atomic value | File (leaf) |

Navigation: `cd users.jsonl/[0]/address` then `cat city`
