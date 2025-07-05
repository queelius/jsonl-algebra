# Development Guide

This document provides information for developers working on the jsonl-algebra project.

## Quick Start

1. **Setup development environment:**
   ```bash
   make setup
   source .venv/bin/activate
   ```

2. **Run tests:**
   ```bash
   make test
   ```

3. **Build documentation:**
   ```bash
   make docs
   ```

## Development Workflow

### Common Tasks

- `make help` - Show all available commands
- `make test` - Run all tests
- `make test-coverage` - Run tests with coverage report
- `make lint` - Check code style
- `make format` - Format code with black
- `make docs` - Build documentation
- `make docs-serve` - Serve docs locally at http://127.0.0.1:8000

### Code Quality

The project uses several tools for maintaining code quality:

- **Black** for code formatting
- **Flake8** for linting
- **MyPy** for type checking
- **Pytest** for testing

Run all quality checks with:
```bash
make check
```

### Testing

Test categories:
- `make test-core` - Core functionality tests
- `make test-jsonpath` - JSONPath extension tests  
- `make test-cli` - CLI integration tests
- `make test-coverage` - Full test suite with coverage

### Documentation

Documentation is built with MkDocs and deployed to GitHub Pages:

- `make docs` - Build documentation
- `make docs-serve` - Serve locally for development
- `make docs-deploy` - Deploy to GitHub Pages (requires push to main)

### Building and Distribution

- `make build` - Build distribution packages
- `make check-dist` - Validate distribution packages
- `make upload-test` - Upload to TestPyPI
- `make upload` - Upload to PyPI (requires confirmation)

### Project Structure

```
jsonl-algebra/
├── ja/                     # Main package
│   ├── __init__.py        # Public API exports
│   ├── core.py            # Core functionality + JSONPath integration
│   ├── jsonpath.py        # JSONPath engine
│   ├── cli.py             # CLI interface
│   ├── commands.py        # CLI command handlers
│   └── groupby.py         # Group-by operations
├── tests/                 # Test suite
│   ├── test_core.py       # Core functionality tests
│   ├── test_jsonpath.py   # JSONPath tests
│   ├── test_commands.py   # CLI tests
│   └── ...
├── docs/                  # Documentation source
├── .github/workflows/     # GitHub Actions CI/CD
├── Makefile              # Development automation
├── pyproject.toml        # Project configuration
├── mkdocs.yml           # Documentation configuration
└── requirements-dev.txt  # Development dependencies
```

### Environment Variables

For PyPI uploads, set these secrets in GitHub repository settings:
- `PYPI_API_TOKEN` - PyPI API token for package uploads

### GitHub Actions

The project includes automated CI/CD:

- **Tests** - Run on all Python versions (3.8-3.12)
- **Documentation** - Auto-deploy to GitHub Pages on main branch
- **PyPI Release** - Auto-upload on GitHub releases

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and add tests
4. Run `make check` to ensure quality
5. Submit a pull request

### Debugging

- `make shell` - Python shell with package imported
- `make debug` - Debug session with test data loaded
- `make example-data` - Create sample test data

For more detailed information, see the main README.md and documentation.
