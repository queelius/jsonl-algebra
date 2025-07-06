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
import sys
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path

# Import our dataset generator functions
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from scripts.generate_dataset import (
    generate_companies, 
    generate_people, 
    write_jsonl,
    setup_randomness
)


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
    cmd_parts = ["python", "-m", "ja"] + command.split()
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
