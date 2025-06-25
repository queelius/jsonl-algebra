# Makefile for jsonl-algebra project

.PHONY: help test coverage clean build docs docs-serve docs-deploy install-dev lint format check-format pypi-test pypi-prod version-patch version-minor version-major

# Default target
help:
	@echo "Available targets:"
	@echo "  test          - Run unit tests"
	@echo "  coverage      - Run tests with coverage report"
	@echo "  lint          - Run linting checks"
	@echo "  format        - Auto-format code with black"
	@echo "  check-format  - Check code formatting without changing files"
	@echo "  clean         - Clean build artifacts and cache files"
	@echo "  build         - Build the package"
	@echo "  docs          - Build documentation with mkdocs"
	@echo "  docs-serve    - Serve documentation locally"
	@echo "  docs-deploy   - Deploy documentation to GitHub Pages"
	@echo "  install-dev   - Install package in development mode with dev dependencies"
	@echo "  version-patch - Bump patch version (0.9.0 -> 0.9.1)"
	@echo "  version-minor - Bump minor version (0.9.0 -> 0.10.0)"
	@echo "  version-major - Bump major version (0.9.0 -> 1.0.0)"
	@echo "  pypi-test     - Upload to PyPI test repository"
	@echo "  pypi-prod     - Upload to PyPI production repository"

# Development setup
install-dev:
	pip install -e ".[dev]"

# Testing
test:
	python -m pytest tests/ -v

coverage:
	python -m pytest tests/ --cov=ja --cov-report=term-missing --cov-report=html
	@echo "Coverage report generated in htmlcov/"

# Code quality
lint:
	python -m flake8 ja/ tests/
	python -m mypy ja/ --ignore-missing-imports

format:
	python -m black ja/ tests/
	python -m isort ja/ tests/

check-format:
	python -m black --check ja/ tests/
	python -m isort --check-only ja/ tests/

# Cleaning
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf site/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

# Building
build: clean
	python -m build

# Documentation
docs:
	mkdocs build

docs-serve:
	mkdocs serve

docs-deploy:
	mkdocs gh-deploy

# Version management using bump2version
version-patch:
	bump2version patch
	@echo "Version bumped to patch level"

version-minor:
	bump2version minor
	@echo "Version bumped to minor level"

version-major:
	bump2version major
	@echo "Version bumped to major level"

# PyPI deployment
pypi-test: build
	python -m twine upload --repository testpypi dist/*
	@echo "Uploaded to PyPI test repository"

pypi-prod: build
	python -m twine upload dist/*
	@echo "Uploaded to PyPI production repository"

# Quality gate - run before committing
check: test lint check-format
	@echo "All checks passed!"
