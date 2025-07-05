# Makefile for jsonl-algebra project
# Provides commands for development, testing, documentation, and deployment

.PHONY: help install install-dev test test-coverage test-verbose clean build docs docs-serve docs-deploy lint format check dist upload upload-test deps deps-update venv setup all

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip3
VENV_DIR := .venv
PACKAGE_NAME := jsonl-algebra
DOCS_DIR := docs
BUILD_DIR := dist
HTMLCOV_DIR := htmlcov

# Colors for output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

help: ## Show this help message
	@echo "$(BLUE)JSONL Algebra - Development Makefile$(RESET)"
	@echo ""
	@echo "$(GREEN)Available targets:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Quick start:$(RESET)"
	@echo "  make setup     # Setup development environment"
	@echo "  make test      # Run tests"
	@echo "  make docs      # Build documentation"

# === Environment Setup ===

venv: ## Create virtual environment
	@echo "$(BLUE)Creating virtual environment...$(RESET)"
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "$(GREEN)Virtual environment created in $(VENV_DIR)$(RESET)"
	@echo "$(YELLOW)Activate with: source $(VENV_DIR)/bin/activate$(RESET)"

install: ## Install package in current environment
	@echo "$(BLUE)Installing package...$(RESET)"
	$(PIP) install -e .

install-dev: ## Install package with development dependencies
	@echo "$(BLUE)Installing development dependencies...$(RESET)"
	$(PIP) install -e .[dev]

setup: venv install-dev ## Complete development environment setup
	@echo "$(GREEN)Development environment setup complete!$(RESET)"
	@echo "$(YELLOW)Don't forget to activate: source $(VENV_DIR)/bin/activate$(RESET)"

deps: ## Install/update dependencies from pyproject.toml
	@echo "$(BLUE)Installing dependencies...$(RESET)"
	$(PIP) install -e .

deps-update: ## Update all dependencies to latest versions
	@echo "$(BLUE)Updating dependencies...$(RESET)"
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install --upgrade pytest pytest-cov mkdocs mkdocs-material mkdocstrings[python] black flake8 mypy twine build

# === Testing ===

test: ## Run all tests
	@echo "$(BLUE)Running tests...$(RESET)"
	pytest tests/ -v

test-coverage: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(RESET)"
	pytest tests/ --cov=ja --cov-report=html --cov-report=term --cov-report=xml
	@echo "$(GREEN)Coverage report generated in $(HTMLCOV_DIR)/$(RESET)"

test-verbose: ## Run tests with verbose output
	@echo "$(BLUE)Running tests with verbose output...$(RESET)"
	pytest tests/ -v -s

test-jsonpath: ## Run only JSONPath tests
	@echo "$(BLUE)Running JSONPath tests...$(RESET)"
	pytest tests/test_jsonpath.py -v

test-core: ## Run only core functionality tests  
	@echo "$(BLUE)Running core tests...$(RESET)"
	pytest tests/test_core.py -v

test-cli: ## Run CLI integration tests
	@echo "$(BLUE)Running CLI tests...$(RESET)"
	pytest tests/test_commands.py -v

# === Code Quality ===

lint: ## Run linting checks
	@echo "$(BLUE)Running linting checks...$(RESET)"
	flake8 ja/ tests/ --max-line-length=88 --extend-ignore=E203,W503
	@echo "$(GREEN)Linting complete$(RESET)"

format: ## Format code with black
	@echo "$(BLUE)Formatting code...$(RESET)"
	black ja/ tests/ --line-length=88
	@echo "$(GREEN)Code formatted$(RESET)"

typecheck: ## Run type checking with mypy
	@echo "$(BLUE)Running type checks...$(RESET)"
	mypy ja/ --ignore-missing-imports
	@echo "$(GREEN)Type checking complete$(RESET)"

check: lint typecheck ## Run all code quality checks
	@echo "$(GREEN)All quality checks passed$(RESET)"

# === Documentation ===

docs: ## Build documentation
	@echo "$(BLUE)Building documentation...$(RESET)"
	mkdocs build
	@echo "$(GREEN)Documentation built in site/$(RESET)"

docs-serve: ## Serve documentation locally for development
	@echo "$(BLUE)Serving documentation at http://127.0.0.1:8000$(RESET)"
	mkdocs serve

docs-deploy: ## Deploy documentation to GitHub Pages
	@echo "$(BLUE)Deploying documentation to GitHub Pages...$(RESET)"
	mkdocs gh-deploy --force
	@echo "$(GREEN)Documentation deployed to GitHub Pages$(RESET)"

docs-clean: ## Clean documentation build artifacts
	@echo "$(BLUE)Cleaning documentation artifacts...$(RESET)"
	rm -rf site/
	@echo "$(GREEN)Documentation artifacts cleaned$(RESET)"

# === Building and Distribution ===

clean: ## Clean build artifacts and cache files
	@echo "$(BLUE)Cleaning build artifacts...$(RESET)"
	rm -rf $(BUILD_DIR)/
	rm -rf $(HTMLCOV_DIR)/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf site/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "$(GREEN)Clean complete$(RESET)"

build: clean ## Build distribution packages
	@echo "$(BLUE)Building distribution packages...$(RESET)"
	$(PYTHON) -m build
	@echo "$(GREEN)Built packages in $(BUILD_DIR)/$(RESET)"

dist: build ## Create distribution (alias for build)

check-dist: build ## Check distribution packages
	@echo "$(BLUE)Checking distribution packages...$(RESET)"
	twine check $(BUILD_DIR)/*
	@echo "$(GREEN)Distribution packages are valid$(RESET)"

# === PyPI Upload ===

upload-test: build check-dist ## Upload to TestPyPI
	@echo "$(BLUE)Uploading to TestPyPI...$(RESET)"
	@echo "$(YELLOW)You will be prompted for TestPyPI credentials$(RESET)"
	twine upload --repository testpypi $(BUILD_DIR)/*
	@echo "$(GREEN)Uploaded to TestPyPI$(RESET)"

upload: build check-dist ## Upload to PyPI
	@echo "$(RED)WARNING: This will upload to the real PyPI!$(RESET)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "$(BLUE)Uploading to PyPI...$(RESET)"; \
		twine upload $(BUILD_DIR)/*; \
		echo "$(GREEN)Uploaded to PyPI$(RESET)"; \
	else \
		echo "$(YELLOW)Upload cancelled$(RESET)"; \
	fi

# === Development Helpers ===

version: ## Show current version
	@echo "$(BLUE)Current version:$(RESET)"
	@grep "version" pyproject.toml | head -1

demo: ## Run a quick demo of JSONPath functionality
	@echo "$(BLUE)Running JSONPath demo...$(RESET)"
	@echo '{"user": {"name": "Alice", "age": 30}, "orders": [{"item": "Book", "price": 15.99}], "tags": ["python", "data"]}' | ja select-path '$.user.age > 25'
	@echo "$(GREEN)Demo complete$(RESET)"

example-data: ## Create example test data
	@echo "$(BLUE)Creating example test data...$(RESET)"
	@echo '{"user": {"name": "Alice", "age": 30}, "orders": [{"item": "Book", "price": 15.99}, {"item": "Pen", "price": 2.5}], "tags": ["python", "data"]}' > test_data.jsonl
	@echo '{"user": {"name": "Bob", "age": 25}, "orders": [{"item": "Notebook", "price": 8.99}], "tags": ["javascript", "web"]}' >> test_data.jsonl
	@echo '{"user": {"name": "Charlie", "age": 35}, "orders": [], "tags": ["python", "web"]}' >> test_data.jsonl
	@echo "$(GREEN)Example data created in test_data.jsonl$(RESET)"

# === All-in-one targets ===

all: clean install-dev test docs ## Run complete development workflow
	@echo "$(GREEN)Complete development workflow finished$(RESET)"

ci: lint typecheck test-coverage ## Run CI pipeline (for GitHub Actions)
	@echo "$(GREEN)CI pipeline complete$(RESET)"

release: check test-coverage build upload ## Complete release workflow
	@echo "$(GREEN)Release workflow complete$(RESET)"

# === Information ===

info: ## Show project information
	@echo "$(BLUE)Project Information:$(RESET)"
	@echo "  Name: $(PACKAGE_NAME)"
	@echo "  Version: $$(grep 'version' pyproject.toml | head -1 | cut -d'=' -f2 | tr -d ' \"')"
	@echo "  Python: $$($(PYTHON) --version)"
	@echo "  Pip: $$($(PIP) --version)"
	@echo "  Virtual env: $(VENV_DIR)"
	@echo "  Docs: $(DOCS_DIR)"
	@echo "  Tests: tests/"
	@echo "  Build: $(BUILD_DIR)"

# === GitHub Workflow Support ===

gh-setup: ## Setup for GitHub Actions (install dependencies only)
	$(PIP) install -e .[dev]

gh-test: ## GitHub Actions test target
	pytest tests/ --cov=ja --cov-report=xml --cov-report=term

gh-docs: ## GitHub Actions docs build target
	mkdocs build

# === Interactive helpers ===

shell: ## Open Python shell with package imported
	@echo "$(BLUE)Opening Python shell with jsonl-algebra imported as 'ja'$(RESET)"
	$(PYTHON) -c "import ja; print('jsonl-algebra imported as ja'); import code; code.interact(local=dict(ja=ja))"

debug: ## Run debug session with test data
	@echo "$(BLUE)Debug session with test data...$(RESET)"
	$(PYTHON) -c "import ja; import json; data=[json.loads(line) for line in open('test_data.jsonl')]; print('Loaded', len(data), 'records'); import code; code.interact(local=dict(ja=ja, data=data))"
