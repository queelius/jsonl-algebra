# Contributing to jsonl-algebra

Thank you for your interest in contributing to **jsonl-algebra**! This document will guide you through the contribution process.

## Ways to Contribute

There are many ways to contribute to jsonl-algebra:

- **Report bugs** - Help us identify and fix issues
- **Suggest features** - Share ideas for improvements
- **Improve documentation** - Fix typos, clarify explanations, add examples
- **Write tutorials** - Share how you use jsonl-algebra
- **Submit code** - Fix bugs or implement features
- **Create integrations** - Build tools that extend jsonl-algebra
- **Answer questions** - Help other users in discussions

All contributions are valuable and appreciated!

## Getting Started

### 1. Set Up Development Environment

Fork and clone the repository:

```bash
# Fork on GitHub first, then clone your fork
git clone https://github.com/YOUR-USERNAME/jsonl-algebra.git
cd jsonl-algebra

# Add upstream remote
git remote add upstream https://github.com/queelius/jsonl-algebra.git
```

Create a virtual environment and install dependencies:

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

Verify installation:

```bash
# Run tests
pytest

# Check code style
black --check .
flake8 .
```

### 2. Create a Branch

Create a feature branch for your work:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

Use descriptive branch names:

- `feature/add-regex-support`
- `fix/groupby-null-handling`
- `docs/improve-quickstart`

## Development Workflow

### Making Changes

1. **Make your changes** - Edit the relevant files
2. **Write tests** - Add tests for new features or bug fixes
3. **Run tests** - Ensure all tests pass
4. **Check style** - Format code with black and check with flake8
5. **Update docs** - Document new features or behavior changes
6. **Commit changes** - Write clear commit messages

### Running Tests

Run the full test suite:

```bash
# All tests
pytest

# Specific test file
pytest tests/test_core.py

# Specific test function
pytest tests/test_core.py::test_select_basic

# With coverage
pytest --cov=ja tests/

# Generate coverage report
pytest --cov=ja --cov-report=html tests/
# Open htmlcov/index.html in browser
```

### Code Style

jsonl-algebra follows Python best practices:

**Format code with black:**

```bash
black .
```

**Check style with flake8:**

```bash
flake8 .
```

**Type hints:**

We use type hints where helpful:

```python
from typing import List, Dict, Iterator

def select(data: List[Dict], expr: str) -> List[Dict]:
    """Filter rows based on expression."""
    ...
```

**Docstrings:**

Use Google-style docstrings:

```python
def my_function(param1: str, param2: int) -> bool:
    """Short description of function.

    Longer description if needed. Explain what the function does,
    any important behavior, and edge cases.

    Args:
        param1: Description of first parameter
        param2: Description of second parameter

    Returns:
        Description of return value

    Raises:
        ValueError: When invalid input is provided

    Example:
        >>> my_function("test", 42)
        True
    """
    ...
```

### Commit Messages

Write clear, descriptive commit messages:

**Good:**
```
Add support for regex in select expressions

- Implement regex matching with =~ operator
- Add tests for regex patterns
- Update documentation with examples
```

**Bad:**
```
fixed stuff
```

**Format:**

```
Short summary (50 chars or less)

More detailed explanation if needed. Wrap at 72 characters.
Explain what changed and why.

- Bullet points for multiple changes
- Each change on its own line

Fixes #123
```

## Testing Guidelines

### Writing Tests

Tests are located in the `tests/` directory, organized by module:

```
tests/
├── test_core.py         # Core operations
├── test_cli.py          # CLI commands
├── test_expr.py         # Expression evaluation
├── test_groupby.py      # Grouping operations
└── ...
```

**Test structure:**

```python
import pytest
from ja.core import select

def test_select_basic():
    """Test basic select operation."""
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]

    result = list(select(data, "age > 27"))

    assert len(result) == 1
    assert result[0]["name"] == "Alice"

def test_select_with_nulls():
    """Test select handles null values."""
    data = [
        {"name": "Alice", "score": 90},
        {"name": "Bob", "score": None},
    ]

    result = list(select(data, "score != null"))

    assert len(result) == 1
    assert result[0]["name"] == "Alice"

@pytest.mark.parametrize("expr,expected_count", [
    ("age > 25", 2),
    ("age >= 30", 1),
    ("age < 30", 1),
])
def test_select_comparisons(expr, expected_count):
    """Test various comparison operators."""
    data = [
        {"age": 25},
        {"age": 30},
        {"age": 35},
    ]

    result = list(select(data, expr))
    assert len(result) == expected_count
```

### Test Coverage

Aim for high test coverage:

```bash
# Generate coverage report
pytest --cov=ja --cov-report=term-missing tests/

# View in browser
pytest --cov=ja --cov-report=html tests/
open htmlcov/index.html
```

**Coverage goals:**

- Core operations: 90%+ coverage
- CLI commands: 80%+ coverage
- Edge cases: Test error conditions
- Integration tests: Test component interaction

## Documentation

### Updating Documentation

Documentation is in the `docs/` directory using MkDocs:

```
docs/
├── index.md                    # Homepage
├── getting-started/
│   ├── installation.md
│   ├── quickstart.md
│   └── concepts.md
├── cli/
│   ├── overview.md
│   └── commands.md
└── ...
```

**Building docs locally:**

```bash
# Install MkDocs
pip install mkdocs mkdocs-material mkdocstrings[python]

# Serve locally
mkdocs serve

# Open http://127.0.0.1:8000 in browser

# Build static site
mkdocs build
```

### Documentation Style

**Be clear and concise:**

- Use simple language
- Provide examples
- Explain the "why", not just the "what"

**Use admonitions for important info:**

```markdown
!!! tip "Pro Tip"
    Filter data early in pipelines to improve performance.

!!! warning "Important"
    This operation loads all data into memory.

!!! info "Note"
    Null values are handled specially in comparisons.
```

**Include runnable examples:**

```markdown
Filter users over 30:

 ```bash
ja select 'age > 30' users.jsonl
 ```

Output:
 ```json
{"id": 1, "name": "Alice", "age": 35}
 ```
```

## Pull Request Process

### Before Submitting

1. **Sync with upstream:**
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run tests:**
   ```bash
   pytest
   ```

3. **Check code style:**
   ```bash
   black --check .
   flake8 .
   ```

4. **Update docs if needed**

5. **Squash commits if needed:**
   ```bash
   git rebase -i HEAD~3  # Squash last 3 commits
   ```

### Submitting a Pull Request

1. **Push your branch:**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Open a Pull Request on GitHub**

3. **Fill out the PR template:**
   - Description of changes
   - Related issues
   - Testing done
   - Screenshots (if UI changes)

4. **Wait for review:**
   - Address reviewer comments
   - Make requested changes
   - Push updates (they'll appear in the PR)

### PR Template

```markdown
## Description

Brief description of what this PR does.

## Related Issues

Fixes #123
Closes #456

## Changes Made

- Added feature X
- Fixed bug Y
- Updated documentation for Z

## Testing

- [ ] Added tests for new functionality
- [ ] All tests pass
- [ ] Manually tested with sample data

## Documentation

- [ ] Updated relevant documentation
- [ ] Added docstrings for new functions
- [ ] Updated CHANGELOG.md

## Screenshots (if applicable)

Before: [screenshot]
After: [screenshot]
```

## Code Review Process

### What to Expect

- Reviews usually happen within a few days
- Maintainers may request changes
- Discussion helps improve the code
- Multiple review rounds are normal

### Being a Good Reviewer

If you're reviewing PRs:

- Be constructive and kind
- Explain the "why" behind suggestions
- Acknowledge good work
- Test the changes if possible
- Approve when ready

## Project Structure

Understanding the codebase:

```
jsonl-algebra/
├── ja/                      # Main package
│   ├── __init__.py
│   ├── core.py             # Core operations (select, project, etc.)
│   ├── cli.py              # CLI entry point
│   ├── commands.py         # CLI command handlers
│   ├── expr.py             # Expression evaluator
│   ├── group.py            # Grouping operations
│   ├── compose.py          # Composability/pipelines
│   ├── schema.py           # Schema inference/validation
│   ├── repl.py             # Interactive REPL
│   ├── shell.py            # ja-shell filesystem navigator
│   └── vfs.py              # Virtual filesystem for ja-shell
├── integrations/           # Integrations
│   ├── mcp_server.py       # MCP server
│   ├── log_analyzer.py     # Log analyzer
│   └── ...
├── tests/                  # Test suite
├── docs/                   # Documentation
├── examples/               # Example data and scripts
└── scripts/                # Utility scripts
```

### Key Modules

**ja/core.py** - Core relational operations:
- `select()` - Filter rows
- `project()` - Choose fields
- `join()` - Combine datasets
- `union()`, `intersection()`, `difference()`
- `distinct()`
- `sort_by()`

**ja/group.py** - Grouping and aggregation:
- `groupby_with_metadata()` - Add grouping metadata
- `groupby_agg()` - Group and aggregate
- Aggregation functions (sum, avg, count, etc.)

**ja/cli.py** - Command-line interface:
- Argument parsing
- Command routing
- Error handling

**ja/expr.py** - Expression evaluation:
- Safe expression parser
- Comparison operators
- Nested field access

## Feature Development

### Adding a New Operation

Example: Adding a `reverse` operation

1. **Implement in core.py:**

```python
def reverse(data: List[Dict]) -> List[Dict]:
    """Reverse the order of rows.

    Args:
        data: List of dictionaries to reverse

    Returns:
        List in reversed order

    Example:
        >>> data = [{"id": 1}, {"id": 2}]
        >>> list(reverse(data))
        [{"id": 2}, {"id": 1}]
    """
    return list(reversed(data))
```

2. **Add CLI command in commands.py:**

```python
def handle_reverse(args, data_stream):
    """Handle reverse command."""
    data = list(data_stream)
    reversed_data = reverse(data)
    for row in reversed_data:
        print(json.dumps(row))
```

3. **Add argument parser in cli.py:**

```python
# In build_parser()
reverse_parser = subparsers.add_parser(
    'reverse',
    help='Reverse row order'
)
reverse_parser.add_argument(
    'file',
    nargs='?',
    help='Input JSONL file (default: stdin)'
)
```

4. **Wire up in cli.py:**

```python
# In main()
elif args.command == 'reverse':
    handle_reverse(args, data_stream)
```

5. **Write tests in tests/test_core.py:**

```python
def test_reverse_basic():
    """Test basic reverse operation."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    result = list(reverse(data))

    assert len(result) == 3
    assert result[0]["id"] == 3
    assert result[1]["id"] == 2
    assert result[2]["id"] == 1

def test_reverse_empty():
    """Test reverse with empty data."""
    data = []
    result = list(reverse(data))
    assert result == []
```

6. **Document in docs/cli/commands.md:**

```markdown
### reverse

Reverse the order of rows.

**Usage:**

 ```bash
ja reverse [file]
 ```

**Examples:**

 ```bash
# Reverse users.jsonl
ja reverse users.jsonl

# Reverse from stdin
cat users.jsonl | ja reverse
 ```
```

7. **Add to CHANGELOG.md**

## Integration Development

Creating a new integration:

1. Create file in `integrations/`
2. Follow existing patterns
3. Add comprehensive docstrings
4. Create tests
5. Write README/documentation
6. Update `integrations/README.md`

See [Integrations Overview](integrations/overview.md) for details.

## Release Process

(For maintainers)

1. **Update version in pyproject.toml**
2. **Update CHANGELOG.md**
3. **Run full test suite:**
   ```bash
   pytest
   ```
4. **Build documentation:**
   ```bash
   mkdocs build
   ```
5. **Create release commit:**
   ```bash
   git commit -am "Release v1.2.0"
   git tag v1.2.0
   ```
6. **Build package:**
   ```bash
   python -m build
   ```
7. **Upload to PyPI:**
   ```bash
   twine upload dist/*
   ```
8. **Push to GitHub:**
   ```bash
   git push origin main --tags
   ```

## Community Guidelines

### Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Assume good intentions
- Give constructive feedback
- Focus on the issue, not the person

### Communication

- **GitHub Issues** - Bug reports and feature requests
- **Pull Requests** - Code contributions
- **Discussions** - General questions and ideas

### Recognition

Contributors are recognized in:

- CONTRIBUTORS.md file
- Release notes
- GitHub contributors page

## Getting Help

### Questions About Contributing

- Check this guide
- Read existing PRs for examples
- Ask in GitHub Discussions
- Open an issue if stuck

### Need Ideas?

Look for issues labeled:

- `good first issue` - Good for newcomers
- `help wanted` - We'd love contributions
- `documentation` - Improve docs
- `enhancement` - New features

## Thank You!

Every contribution makes jsonl-algebra better. Whether you're fixing a typo, reporting a bug, or implementing a major feature, your help is appreciated!

## Additional Resources

- [Testing Strategy](testing.md) - Detailed testing guide
- [Development Setup](development.md) - Advanced setup
- [Architecture Overview](concepts/relational-algebra.md) - Design principles
- [API Reference](reference.md) - Code documentation

Happy contributing! 🚀
