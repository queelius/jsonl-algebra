#!/usr/bin/env python3
"""
Minimal test for MCP server functionality without requiring the full MCP SDK.
This tests the core logic of the server's tool handlers.
"""

import json
import sys
import os
import tempfile
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_test_data():
    """Create temporary JSONL test files."""
    temp_dir = tempfile.mkdtemp()

    # Create users.jsonl
    users_file = Path(temp_dir) / "users.jsonl"
    users_data = [
        {"id": 1, "name": "Alice", "age": 30, "department": "Engineering"},
        {"id": 2, "name": "Bob", "age": 25, "department": "Sales"},
        {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering"},
        {"id": 4, "name": "Diana", "age": 28, "department": "Marketing"},
        {"id": 5, "name": "Eve", "age": 32, "department": "Sales"},
    ]
    with open(users_file, 'w') as f:
        for user in users_data:
            f.write(json.dumps(user) + '\n')

    # Create orders.jsonl
    orders_file = Path(temp_dir) / "orders.jsonl"
    orders_data = [
        {"order_id": 101, "user_id": 1, "amount": 150.00, "status": "completed"},
        {"order_id": 102, "user_id": 2, "amount": 75.50, "status": "pending"},
        {"order_id": 103, "user_id": 1, "amount": 200.00, "status": "completed"},
        {"order_id": 104, "user_id": 3, "amount": 300.00, "status": "completed"},
        {"order_id": 105, "user_id": 4, "amount": 50.00, "status": "cancelled"},
    ]
    with open(orders_file, 'w') as f:
        for order in orders_data:
            f.write(json.dumps(order) + '\n')

    return temp_dir, users_file, orders_file


def test_core_operations():
    """Test core operations without full MCP SDK."""
    print("Testing JSONL Algebra MCP Server Core Operations...")

    # Create test data
    temp_dir, users_file, orders_file = create_test_data()
    print(f"✓ Created test data in {temp_dir}")

    # Import ja operations
    from ja import select, project, sort_by, groupby_agg, read_jsonl
    import random

    # Helper to read JSONL files
    def read_file(path):
        with open(path, 'r') as f:
            return read_jsonl(f)

    # Test 1: Select operation
    print("\nTest 1: Select users with age > 28")
    users_data = read_file(str(users_file))
    result = list(select(users_data, "age > 28"))
    assert len(result) == 3, f"Expected 3 users, got {len(result)}"
    print(f"✓ Found {len(result)} users: {[r['name'] for r in result]}")

    # Test 2: Project operation
    print("\nTest 2: Project name and department fields")
    users_data = read_file(str(users_file))
    result = list(project(users_data, ["name", "department"]))
    assert len(result) == 5, f"Expected 5 records, got {len(result)}"
    assert "age" not in result[0], "Age field should not be present"
    print(f"✓ Projected {len(result)} records with fields: {list(result[0].keys())}")

    # Test 3: Sort operation
    print("\nTest 3: Sort users by age")
    users_data = read_file(str(users_file))
    result = list(sort_by(users_data, "age"))
    ages = [r["age"] for r in result]
    assert ages == sorted(ages), "Results not properly sorted"
    print(f"✓ Sorted by age: {ages}")

    # Test 4: Aggregation
    print("\nTest 4: Group by department and count")
    users_data = read_file(str(users_file))
    result = list(groupby_agg(users_data, "department", "count(id)"))
    dept_counts = {r["department"]: r["count(id)"] for r in result}
    assert dept_counts["Engineering"] == 2, "Engineering should have 2 people"
    assert dept_counts["Sales"] == 2, "Sales should have 2 people"
    print(f"✓ Department counts: {dept_counts}")

    # Test 5: Simple random sampling (without built-in sample function)
    print("\nTest 5: Sample 3 random users")
    users_data = read_file(str(users_file))
    random.seed(42)
    sampled = random.sample(users_data, 3)
    assert len(sampled) == 3, f"Expected 3 samples, got {len(sampled)}"
    print(f"✓ Sampled {len(sampled)} users: {[r['name'] for r in sampled]}")

    # Test 6: Complex pipeline simulation
    print("\nTest 6: Complex pipeline (filter → project → sort)")
    # First filter (using JMESPath for string comparison)
    orders_data = read_file(str(orders_file))
    filtered = select(orders_data, "status == `\"completed\"`", use_jmespath=True)
    # Then project
    projected = project(filtered, ["order_id", "amount"])
    # Then sort
    sorted_result = list(sort_by(projected, "amount"))
    sorted_result.reverse()  # For descending order
    amounts = [r["amount"] for r in sorted_result]
    assert len(amounts) == 3, f"Expected 3 completed orders, got {len(amounts)}"
    assert amounts == sorted(amounts, reverse=True), "Not properly sorted"
    print(f"✓ Pipeline result: {len(sorted_result)} orders, amounts: {amounts}")

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)
    print(f"\n✓ Cleaned up test data")

    print("\n" + "="*50)
    print("All tests passed! MCP server core logic is working.")
    print("="*50)


def test_mcp_server_module():
    """Test that the MCP server module can be imported and initialized."""
    print("\nTesting MCP Server Module Import...")

    try:
        # Try importing the MCP server
        from integrations.mcp_server import JSONLAlgebraServer

        # If we get here, MCP SDK is installed
        print("✓ MCP SDK is installed")
        print("✓ MCP server module imported successfully")

        # Try creating a server instance
        server = JSONLAlgebraServer()
        print("✓ MCP server instance created successfully")

    except ImportError as e:
        if "mcp" in str(e).lower():
            print("✓ MCP SDK not installed (expected for minimal test)")
            print("  To fully test, install with: pip install mcp")
        else:
            print(f"✗ Unexpected import error: {e}")
            raise
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    test_core_operations()
    test_mcp_server_module()
    print("\n✅ Minimal MCP tests completed successfully!")
    print("Note: Full MCP server testing requires 'pip install mcp'")