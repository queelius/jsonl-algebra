# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`jsonl-algebra` (ja) is a lightweight command-line tool and Python library for performing relational algebra operations on JSONL data. It provides SQL-like operations without external dependencies, with automatic streaming for memory-efficient processing of large datasets.

## Key Commands

### Development Setup
```bash
# Create virtual environment and install development dependencies
make setup
source .venv/bin/activate

# Install package in development mode
pip install -e .
```

### Testing
```bash
# Run all tests with verbose output
make test

# Run specific test categories
make test-core      # Core functionality tests
make test-jsonpath  # JSONPath tests
make test-cli       # CLI integration tests

# Run with coverage
make test-coverage

# Run a single test file
pytest tests/test_core.py -v

# Run a specific test
pytest tests/test_core.py::TestSelect -v
```

### Code Quality
```bash
# Run all quality checks (lint + typecheck)
make check

# Individual checks
make lint       # Flake8 linting
make format     # Black formatting
make typecheck  # MyPy type checking
```

### Documentation
```bash
# Build documentation
make docs

# Serve documentation locally (http://127.0.0.1:8000)
make docs-serve

# Deploy to GitHub Pages (requires push to main)
make docs-deploy
```

### Building & Distribution
```bash
# Build distribution packages
make build

# Check distribution validity
make check-dist

# Upload to TestPyPI
make upload-test

# Upload to PyPI (requires confirmation)
make upload
```

## Architecture Overview

### Core Module Structure

- **ja/core.py**: Core relational algebra operations (select, project, join, union, etc.)
- **ja/jsonpath.py**: JSONPath query engine for nested JSON operations
- **ja/streaming.py**: Streaming implementations for memory-efficient processing
- **ja/groupby.py**: Group-by operations with aggregation functions
- **ja/cli.py**: Main CLI entry point and argument parsing
- **ja/commands.py**: CLI command handlers that bridge CLI to core functions
- **ja/__init__.py**: Public API exports

### Key Design Patterns

1. **Streaming by Default**: Operations like select, project, rename automatically stream data line-by-line for O(1) memory usage. Memory-intensive operations (join, sort, groupby) can use `--window-size` for approximate windowed processing.

2. **Type System**: Uses Python type hints throughout:
   - `Row = Dict[str, Any]` - Single JSON record
   - `Relation = Iterable[Row]` - Collection of records (can be list or generator)

3. **Extensible Aggregations**: The groupby module uses a dispatcher pattern for aggregations, making it easy to add custom aggregation functions.

4. **JSONPath Integration**: Seamlessly integrated JSONPath support for querying nested JSON structures via dedicated commands (select-path, project-template, etc.)

## Testing Strategy

- **Unit Tests**: Core functionality tested in isolation (test_core.py, test_jsonpath.py)
- **Integration Tests**: End-to-end CLI testing (test_commands.py, test_integration.py)
- **Streaming Tests**: Verify memory-efficient processing (test_streaming.py)
- **Extended Tests**: Complex groupby scenarios (test_groupby_extended.py)

All new features should include corresponding tests. Use pytest fixtures for test data setup.

## CI/CD Pipeline

GitHub Actions workflow (.github/workflows/ci.yml):
- Tests run on Python 3.8-3.12
- Auto-deploy docs to GitHub Pages on main branch push
- Auto-publish to PyPI on GitHub release

## Common Development Tasks

### Adding a New Relational Operation
1. Implement core logic in `ja/core.py`
2. Add streaming version in `ja/streaming.py` if applicable
3. Create CLI command handler in `ja/commands.py`
4. Register command in `ja/cli.py`
5. Add tests in appropriate test file
6. Update documentation

### Adding a New Aggregation Function
1. Define aggregation helper in `ja/groupby.py`
2. Register in `AGGREGATION_DISPATCHER` dictionary
3. Add tests in `test_groupby_extended.py`

### Debugging
```bash
# Interactive Python shell with ja imported
make shell

# Debug session with test data loaded
make debug

# Create sample test data
make example-data
```

## Important Notes

- The project maintains zero external dependencies for the core library
- All operations should handle both list and generator inputs (Iterable[Row])
- Memory efficiency is a primary concern - prefer streaming/generators where possible
- The CLI uses Python expressions for filters (eval-based, be mindful of security implications)
- JSONPath implementation follows standard JSONPath syntax with extensions for practical use