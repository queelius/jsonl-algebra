# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**jsonl-algebra** (ja) is a lightweight command-line tool and Python library for performing relational algebra operations on JSONL (JSON Lines) data. It provides SQL-like operations without dependencies, supporting both streaming and memory-intensive operations.

## Key Commands

### Development & Testing
```bash
# Install development environment
make setup              # Complete dev setup with virtual environment
make install-dev        # Install with dev dependencies

# Run tests
make test              # Run all tests with pytest
make test-coverage     # Run tests with coverage report
make test-verbose      # Run tests with verbose output (-v -s)
make test-jsonpath     # Test JSONPath functionality only
make test-core         # Test core functionality only

# Code quality
make lint              # Run flake8 linting
make format            # Format code with black (88 char line length)
make typecheck         # Run mypy type checking
make check             # Run all quality checks (lint + typecheck)

# Documentation
make docs              # Build MkDocs documentation
make docs-serve        # Serve docs locally at http://127.0.0.1:8000
make docs-deploy       # Deploy to GitHub Pages

# Building & Distribution
make build             # Build distribution packages
make upload-test       # Upload to TestPyPI
make upload            # Upload to PyPI (requires confirmation)
```

### Testing Individual Components
```bash
pytest tests/test_core.py -v          # Core algebra operations
pytest tests/test_streaming.py -v     # Streaming functionality
pytest tests/test_jsonpath.py -v      # JSONPath operations
pytest tests/test_commands.py -v      # CLI command tests
pytest tests/test_groupby_extended.py -v  # GroupBy functionality
```

## Code Architecture

### Module Structure

- **ja/core.py**: Core relational algebra operations (select, project, join, union, etc.)
  - Type definitions: `Row = Dict[str, any]`, `Relation = List[Row]`
  - Functions for set operations and transforms
  
- **ja/streaming.py**: Memory-efficient streaming operations
  - `StreamingOperator` base class for line-by-line processing
  - Automatic streaming for select, project, rename, union operations
  - Windowed processing support for memory-intensive operations

- **ja/cli.py**: Main CLI entry point and argument parsing
  - Handles command routing and file I/O
  - Manages streaming vs memory modes
  - Warning system for large datasets

- **ja/commands.py**: CLI command implementations
  - Maps CLI arguments to core functions
  - Handles expression parsing for select operations
  - Manages JSONPath operations

- **ja/groupby.py**: GroupBy aggregation functionality
  - Extensible aggregation system with dispatcher pattern
  - Built-in aggregations: sum, avg, min, max, count, list, first, last
  - Support for custom aggregation functions

- **ja/jsonpath.py**: JSONPath query support
  - Path parsing and evaluation
  - Quantifiers: any, all, none
  - Template-based projection

### Key Design Patterns

1. **Streaming vs Memory Processing**:
   - Operations automatically stream when possible (O(1) memory)
   - Memory-intensive operations (join, sort, groupby) require full dataset
   - Windowed processing provides approximate results with bounded memory

2. **Aggregation Dispatcher**:
   - GroupBy uses a dispatcher pattern for extensible aggregations
   - New aggregations can be added by registering functions in `AGGREGATION_DISPATCHER`

3. **Type System**:
   - Simple type aliases: `Row` (dict) and `Relation` (list of dicts)
   - No external dependencies for core functionality

## Important Implementation Notes

- **No Dependencies**: Core library has zero runtime dependencies
- **Python 3.8+**: Minimum Python version requirement
- **Line Length**: Code formatted to 88 characters (black default)
- **Testing**: Uses pytest with coverage reporting
- **Documentation**: MkDocs with Material theme for documentation site
- **Streaming Detection**: Automatically detects when operations can stream
- **Memory Warnings**: Warns users when operations require loading full dataset

## Related Context

The `jaf` repository (located at ../jaf) provides complementary JSON processing capabilities focused on document reshaping and filtering with lazy evaluation. While `jaf` operates on individual JSON documents with S-expression queries, `jsonl-algebra` focuses on relational algebra operations across JSONL collections. Key distinctions:

- **jsonl-algebra**: Relational operations (joins, groupby, set operations) on collections
- **jaf**: Document transformation and filtering with lazy evaluation pipelines

This distinction is important - jsonl-algebra should remain focused on its core mission of providing SQL-like relational algebra operations for JSONL data.