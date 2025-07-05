#!/usr/bin/env python3
"""
Test script to demonstrate streaming warnings and functionality.
"""

import subprocess
import sys
import tempfile
import json

def create_test_data():
    """Create some test JSONL data."""
    test_data = [
        {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 24, "city": "London"},
        {"id": 3, "name": "Charlie", "age": 30, "city": "Paris"},
        {"id": 4, "name": "Diana", "age": 28, "city": "Tokyo"},
    ]
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for row in test_data:
            json.dump(row, f)
            f.write('\n')
        return f.name

def run_ja_command(command, input_file=None):
    """Run a ja command and capture output and warnings."""
    if input_file:
        cmd = ['python', '-m', 'ja'] + command.split() + [input_file]
    else:
        cmd = ['python', '-m', 'ja'] + command.split()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='/home/spinoza/github/workspace/jsonl-algebra')
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        return "", str(e), 1

def test_streaming_functionality():
    """Test various streaming scenarios."""
    print("=== Testing Streaming Functionality ===\n")
    
    test_file = create_test_data()
    print(f"Created test data file: {test_file}")
    
    # Test 1: Normal streaming operation
    print("\n1. Testing normal streaming operation (select --stream):")
    stdout, stderr, code = run_ja_command("select 'age >= 25' --stream", test_file)
    print(f"Exit code: {code}")
    print(f"Output lines: {len(stdout.strip().split() if stdout.strip() else [])}")
    if stderr:
        print(f"Warnings: {stderr}")
    
    # Test 2: Streaming on non-streamable operation
    print("\n2. Testing streaming on non-streamable operation (sort --stream):")
    stdout, stderr, code = run_ja_command("sort age --stream", test_file)
    print(f"Exit code: {code}")
    print(f"Output lines: {len(stdout.strip().split() if stdout.strip() else [])}")
    if stderr:
        print(f"Warnings: {stderr}")
    
    # Test 3: Memory-intensive operation (should warn)
    print("\n3. Testing memory-intensive operation (should warn about memory usage):")
    # Create a second file for join
    test_file2 = create_test_data()
    stdout, stderr, code = run_ja_command(f"join {test_file} {test_file2} --on id=id")
    print(f"Exit code: {code}")
    print(f"Output lines: {len(stdout.strip().split() if stdout.strip() else [])}")
    if stderr:
        print(f"Warnings: {stderr}")
    
    # Test 4: Chain streaming operations
    print("\n4. Testing chained streaming operations:")
    stdout, stderr, code = run_ja_command("select 'age >= 25' --stream", test_file)
    if stdout:
        # Pipe to second command
        cmd2 = ['python', '-m', 'ja', 'project', 'name,age', '--stream']
        result2 = subprocess.run(cmd2, input=stdout, capture_output=True, text=True, cwd='/home/spinoza/github/workspace/jsonl-algebra')
        print(f"Chained result lines: {len(result2.stdout.strip().split() if result2.stdout.strip() else [])}")
        if result2.stderr:
            print(f"Warnings: {result2.stderr}")
    
    # Cleanup
    import os
    os.unlink(test_file)
    if 'test_file2' in locals():
        os.unlink(test_file2)
    
    print("\n=== Streaming Tests Complete ===")

if __name__ == "__main__":
    test_streaming_functionality()
