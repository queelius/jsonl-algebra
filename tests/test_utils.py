"""
Test utilities and fixtures for jsonl-algebra

This module provides test utilities including:
- Deterministic dataset generation for consistent tests
- Helper functions for creating test data
- Common test fixtures
"""

import json
import tempfile
import os
import subprocess
import random
import shlex
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path


def setup_randomness(seed: int = 42, deterministic: bool = True):
    """Set up deterministic randomness for reproducible tests."""
    random.seed(seed)


def write_jsonl(data: List[Dict[str, Any]], filepath: str):
    """Write data to a JSONL file."""
    with open(filepath, 'w') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')


def generate_companies(n: int) -> List[Dict[str, Any]]:
    """Generate company data with nested structure for testing."""
    industries = ["tech", "finance", "healthcare", "retail", "manufacturing"]
    cities = ["New York", "San Francisco", "Chicago", "Austin", "Seattle"]
    states = ["NY", "CA", "IL", "TX", "WA"]
    sizes = ["small", "medium", "large", "enterprise"]

    companies = []
    for i in range(n):
        companies.append({
            "id": i + 1,
            "name": f"Company_{i + 1}",
            "industry": industries[i % len(industries)],
            "headquarters": {
                "city": cities[i % len(cities)],
                "state": states[i % len(states)],
                "country": "USA"
            },
            "size": sizes[i % len(sizes)],
            "founded": 1990 + (i * 3) % 30,
            "employees": (i + 1) * 100,
            "revenue": (i + 1) * 1000000
        })
    return companies


def generate_people(n: int, max_projects: int = 3, company_names: List[str] = None) -> List[Dict[str, Any]]:
    """Generate person data with nested structure for testing.

    People in the same household share the same last name and location.
    """
    if company_names is None:
        company_names = [f"Company_{i}" for i in range(1, 6)]

    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller", "Wilson"]
    titles = ["Engineer", "Manager", "Director", "Analyst", "Developer", "Designer", "Consultant"]
    cities = ["New York", "San Francisco", "Chicago", "Austin", "Seattle", "Boston", "Denver"]
    states = ["NY", "CA", "IL", "TX", "WA", "MA", "CO"]

    people = []
    household_id = 0
    household_last_name = last_names[0]
    household_city = cities[0]
    household_state = states[0]

    for i in range(n):
        # Create household groups (3 people per household share last name and location)
        if i % 3 == 0:
            household_id = (i // 3) + 1
            household_last_name = last_names[household_id % len(last_names)]
            household_city = cities[household_id % len(cities)]
            household_state = states[household_id % len(states)]

        num_projects = random.randint(1, max_projects)
        age = 22 + random.randint(0, 40)
        salary = 50000 + (i * 1000) + random.randint(0, 10000)

        people.append({
            "id": i + 1,
            "household_id": household_id,
            "person": {
                "name": {
                    "first": first_names[i % len(first_names)],
                    "last": household_last_name
                },
                "age": age,
                "job": {
                    "title": titles[i % len(titles)],
                    "company_name": company_names[i % len(company_names)],
                    "salary": salary
                },
                "location": {
                    "city": household_city,
                    "state": household_state
                }
            },
            "projects": [f"Project_{j + 1}" for j in range(num_projects)]
        })
    return people


class TestDataGenerator:
    """Utility class for generating test datasets."""

    def __init__(self, seed: int = 42):
        """Initialize with a specific seed for deterministic data."""
        self.seed = seed
        self.setup_randomness()

    def setup_randomness(self):
        """Set up deterministic randomness."""
        setup_randomness(self.seed, deterministic=True)

    def create_minimal_dataset(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Create a minimal dataset for basic testing (5 companies, 20 people)."""
        companies = generate_companies(5)
        company_names = [c["name"] for c in companies]
        people = generate_people(20, 3, company_names)
        return companies, people

    def create_small_dataset(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Create a small dataset for moderate testing (10 companies, 50 people)."""
        companies = generate_companies(10)
        company_names = [c["name"] for c in companies]
        people = generate_people(50, 4, company_names)
        return companies, people

    def create_large_dataset(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Create a larger dataset for performance testing (50 companies, 500 people)."""
        companies = generate_companies(50)
        company_names = [c["name"] for c in companies]
        people = generate_people(500, 5, company_names)
        return companies, people


class TempDataFiles:
    """Context manager for creating temporary JSONL test files."""

    def __init__(self, companies: List[Dict[str, Any]], people: List[Dict[str, Any]]):
        self.companies = companies
        self.people = people
        self.temp_dir = None
        self.companies_file = None
        self.people_file = None

    def __enter__(self):
        self.temp_dir = tempfile.mkdtemp()
        self.companies_file = os.path.join(self.temp_dir, "companies.jsonl")
        self.people_file = os.path.join(self.temp_dir, "people.jsonl")

        write_jsonl(self.companies, self.companies_file)
        write_jsonl(self.people, self.people_file)

        return self.companies_file, self.people_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up temp files
        import shutil
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)


def create_simple_orders() -> List[Dict[str, Any]]:
    """Create simple order data for basic testing."""
    return [
        {"id": 1, "customer": "Alice", "amount": 100.50, "status": "shipped"},
        {"id": 2, "customer": "Bob", "amount": 75.25, "status": "pending"},
        {"id": 3, "customer": "Alice", "amount": 204.22, "status": "shipped"},
        {"id": 4, "customer": "Charlie", "amount": 50.00, "status": "shipped"},
        {"id": 5, "customer": "Bob", "amount": 125.75, "status": "cancelled"},
    ]


def create_nested_users() -> List[Dict[str, Any]]:
    """Create nested user data for testing nested operations."""
    return [
        {
            "id": 1,
            "profile": {"name": "Alice Johnson", "age": 28},
            "settings": {"theme": "dark", "notifications": True},
            "location": {"city": "San Francisco", "state": "CA"}
        },
        {
            "id": 2,
            "profile": {"name": "Bob Smith", "age": 34},
            "settings": {"theme": "light", "notifications": False},
            "location": {"city": "New York", "state": "NY"}
        },
        {
            "id": 3,
            "profile": {"name": "Charlie Brown", "age": 22},
            "settings": {"theme": "dark", "notifications": True},
            "location": {"city": "Austin", "state": "TX"}
        }
    ]


def create_test_files_from_data(data: List[Dict[str, Any]], filename: str = "test.jsonl") -> str:
    """Create a temporary file from test data."""
    temp_dir = tempfile.mkdtemp()
    filepath = os.path.join(temp_dir, filename)
    write_jsonl(data, filepath)
    return filepath


def assert_jsonl_equal(actual: List[Dict[str, Any]], expected: List[Dict[str, Any]],
                      ignore_keys: Optional[List[str]] = None):
    """Assert that two lists of JSON objects are equal, optionally ignoring certain keys."""
    ignore_keys = ignore_keys or []

    def clean_dict(d):
        return {k: v for k, v in d.items() if k not in ignore_keys}

    actual_clean = [clean_dict(row) for row in actual]
    expected_clean = [clean_dict(row) for row in expected]

    assert len(actual_clean) == len(expected_clean), f"Length mismatch: {len(actual_clean)} vs {len(expected_clean)}"

    for i, (actual_row, expected_row) in enumerate(zip(actual_clean, expected_clean)):
        assert actual_row == expected_row, f"Row {i} mismatch:\nActual: {actual_row}\nExpected: {expected_row}"


def run_ja_command(command: str, input_file: Optional[str] = None,
                  stdin_data: Optional[str] = None) -> Tuple[str, str, int]:
    """
    Run a ja command and return stdout, stderr, and return code.

    Args:
        command: The ja command to run (without 'ja' prefix)
        input_file: Optional input file to pass to the command
        stdin_data: Optional data to pass via stdin

    Returns:
        Tuple of (stdout, stderr, return_code)
    """
    # Use shlex.split to properly handle quoted arguments
    cmd_parts = ["python", "-m", "ja.cli"] + shlex.split(command)
    if input_file:
        cmd_parts.append(input_file)

    result = subprocess.run(
        cmd_parts,
        input=stdin_data,
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.dirname(__file__))  # Run from project root
    )

    return result.stdout, result.stderr, result.returncode


def parse_jsonl_output(output: str) -> List[Dict[str, Any]]:
    """Parse JSONL output from ja commands."""
    lines = output.strip().split('\n')
    return [json.loads(line) for line in lines if line.strip()]


# Test fixtures that can be imported by test modules
FIXTURE_ORDERS = create_simple_orders()
FIXTURE_NESTED_USERS = create_nested_users()

# Create a singleton test data generator for consistent test data
TEST_DATA_GENERATOR = TestDataGenerator(seed=42)
