#!/usr/bin/env python3
"""
Comprehensive streaming demonstration for jsonl-algebra.

This script demonstrates all streaming capabilities, performance benefits,
and warning systems implemented in jsonl-algebra.
"""

import tempfile
import json
import os
import time
import subprocess
import sys


def create_large_test_data(filename, num_records=10000):
    """Create a large JSONL test file."""
    print(f"Creating test file with {num_records} records...")
    with open(filename, 'w') as f:
        for i in range(num_records):
            record = {
                "id": i,
                "name": f"User{i}",
                "age": 20 + (i % 50),
                "category": "A" if i % 2 == 0 else "B",
                "score": round(i * 0.1, 2),
                "timestamp": f"2024-01-{(i % 30) + 1:02d}",
                "department": ["engineering", "sales", "marketing", "hr"][i % 4],
                "active": i % 3 != 0
            }
            json.dump(record, f)
            f.write('\n')
    print(f"Created {filename} with {num_records} records")


def run_ja_command(command, input_file=None, measure_time=False):
    """Run a ja command and capture output, timing, and warnings."""
    if input_file:
        cmd = ['ja'] + command.split() + [input_file]
    else:
        cmd = ['ja'] + command.split()
    
    start_time = time.time() if measure_time else None
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time() if measure_time else None
        
        execution_time = (end_time - start_time) if measure_time else None
        output_lines = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
        
        return {
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode,
            'output_lines': output_lines,
            'execution_time': execution_time
        }
    except Exception as e:
        return {
            'stdout': '',
            'stderr': str(e),
            'returncode': 1,
            'output_lines': 0,
            'execution_time': None
        }


def demo_streaming_operations():
    """Demonstrate basic streaming operations."""
    print("\n" + "="*60)
    print("STREAMING OPERATIONS DEMONSTRATION")
    print("="*60)
    
    # Create small test file for basic demos
    small_test = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    test_data = [
        {"id": 1, "name": "Alice", "age": 30, "department": "engineering", "active": True},
        {"id": 2, "name": "Bob", "age": 25, "department": "sales", "active": False},
        {"id": 3, "name": "Charlie", "age": 35, "department": "engineering", "active": True},
        {"id": 4, "name": "Diana", "age": 28, "department": "marketing", "active": True},
        {"id": 5, "name": "Eve", "age": 32, "department": "hr", "active": False},
    ]
    
    for record in test_data:
        json.dump(record, small_test)
        small_test.write('\n')
    small_test.close()
    
    print(f"\nUsing test data: {len(test_data)} records")
    
    # Demo 1: Basic streaming select
    print("\n1. Streaming SELECT (filter active users):")
    result = run_ja_command("select 'active == True' --stream", small_test.name)
    print(f"   Command: ja select 'active == True' --stream")
    print(f"   Result: {result['output_lines']} records")
    if result['stderr']:
        print(f"   Warnings: {result['stderr'].strip()}")
    
    # Demo 2: Streaming project
    print("\n2. Streaming PROJECT (select specific columns):")
    result = run_ja_command("project name,department --stream", small_test.name)
    print(f"   Command: ja project name,department --stream")
    print(f"   Result: {result['output_lines']} records")
    
    # Demo 3: Streaming rename
    print("\n3. Streaming RENAME (rename columns):")
    result = run_ja_command("rename age=years,department=dept --stream", small_test.name)
    print(f"   Command: ja rename age=years,department=dept --stream")
    print(f"   Result: {result['output_lines']} records")
    
    # Demo 4: Chained streaming operations
    print("\n4. CHAINED STREAMING operations:")
    print("   Pipeline: select -> project -> rename")
    
    # First operation
    cmd1 = ['ja', 'select', 'age >= 28', '--stream', small_test.name]
    p1 = subprocess.run(cmd1, capture_output=True, text=True)
    
    # Second operation (piped)
    cmd2 = ['ja', 'project', 'name,age,department', '--stream']
    p2 = subprocess.run(cmd2, input=p1.stdout, capture_output=True, text=True)
    
    # Third operation (piped)
    cmd3 = ['ja', 'rename', 'age=years', '--stream']
    p3 = subprocess.run(cmd3, input=p2.stdout, capture_output=True, text=True)
    
    final_lines = len(p3.stdout.strip().split('\n')) if p3.stdout.strip() else 0
    print(f"   Final result: {final_lines} records")
    
    # Cleanup
    os.unlink(small_test.name)


def demo_warning_system():
    """Demonstrate the warning system for non-streamable operations."""
    print("\n" + "="*60)
    print("WARNING SYSTEM DEMONSTRATION")
    print("="*60)
    
    # Create test file
    test_file = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    for i in range(5):
        json.dump({"id": i, "value": i * 10, "category": "A" if i % 2 == 0 else "B"}, test_file)
        test_file.write('\n')
    test_file.close()
    
    # Test streaming warning for non-streamable operations
    print("\n1. Requesting streaming for NON-STREAMABLE operations:")
    
    non_streamable_ops = [
        ("sort value --stream", "Sort operation"),
        ("groupby category --agg count --stream", "GroupBy operation"),
    ]
    
    for command, description in non_streamable_ops:
        print(f"\n   Testing: {description}")
        print(f"   Command: ja {command}")
        result = run_ja_command(command, test_file.name)
        print(f"   Exit code: {result['returncode']}")
        print(f"   Output lines: {result['output_lines']}")
        if result['stderr']:
            warnings = result['stderr'].strip().split('\n')
            for warning in warnings:
                if warning.strip():
                    print(f"   ⚠️  {warning.strip()}")
    
    # Test memory warnings for large operations
    print("\n2. Memory-intensive operations (automatic warnings):")
    
    memory_ops = [
        ("sort value", "Sort operation (memory-intensive)"),
        ("groupby category --agg count", "GroupBy operation (memory-intensive)"),
    ]
    
    for command, description in memory_ops:
        print(f"\n   Testing: {description}")
        print(f"   Command: ja {command}")
        result = run_ja_command(command, test_file.name)
        if result['stderr']:
            warnings = result['stderr'].strip().split('\n')
            for warning in warnings:
                if warning.strip():
                    print(f"   ⚠️  {warning.strip()}")
    
    # Cleanup
    os.unlink(test_file.name)


def demo_performance_comparison():
    """Demonstrate performance benefits of streaming."""
    print("\n" + "="*60)
    print("PERFORMANCE COMPARISON DEMONSTRATION")
    print("="*60)
    
    # Create larger test file
    large_file = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    create_large_test_data(large_file.name, 50000)
    
    print(f"\nPerformance test with 50,000 records")
    print(f"File size: {os.path.getsize(large_file.name) / 1024 / 1024:.1f} MB")
    
    # Test streaming vs non-streaming performance
    operations = [
        ("select 'age >= 30'", "Filter records (age >= 30)"),
        ("project id,name,age", "Project 3 columns"),
        ("rename age=years", "Rename column"),
    ]
    
    for command, description in operations:
        print(f"\n{description}:")
        
        # Non-streaming
        result_normal = run_ja_command(command, large_file.name, measure_time=True)
        print(f"   Normal mode:    {result_normal['execution_time']:.3f}s, {result_normal['output_lines']} records")
        
        # Streaming
        result_stream = run_ja_command(command + " --stream", large_file.name, measure_time=True)
        print(f"   Streaming mode: {result_stream['execution_time']:.3f}s, {result_stream['output_lines']} records")
        
        # Performance comparison
        if result_normal['execution_time'] and result_stream['execution_time']:
            speedup = result_normal['execution_time'] / result_stream['execution_time']
            if speedup > 1:
                print(f"   ⚡ Streaming is {speedup:.1f}x faster")
            elif speedup < 1:
                print(f"   📊 Normal is {1/speedup:.1f}x faster")
            else:
                print(f"   📊 Similar performance")
    
    # Cleanup
    os.unlink(large_file.name)


def demo_jsonpath_streaming():
    """Demonstrate JSONPath operations with streaming."""
    print("\n" + "="*60)
    print("JSONPATH STREAMING DEMONSTRATION")
    print("="*60)
    
    # Create test file with nested data
    jsonpath_file = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    nested_data = [
        {"id": 1, "user": {"name": "Alice", "profile": {"age": 30, "city": "NYC"}}, "orders": [{"amount": 100}, {"amount": 50}]},
        {"id": 2, "user": {"name": "Bob", "profile": {"age": 25, "city": "London"}}, "orders": [{"amount": 200}]},
        {"id": 3, "user": {"name": "Charlie", "profile": {"age": 35, "city": "Paris"}}, "orders": [{"amount": 75}, {"amount": 125}]},
    ]
    
    for record in nested_data:
        json.dump(record, jsonpath_file)
        jsonpath_file.write('\n')
    jsonpath_file.close()
    
    print(f"\nUsing nested JSON data: {len(nested_data)} records")
    
    # JSONPath streaming operations
    jsonpath_ops = [
        ("select-path '$.user.profile.age' --stream", "Select by nested age field"),
        ("project-template '{\"name\": \"$.user.name\", \"age\": \"$.user.profile.age\"}' --stream", "Project nested fields"),
    ]
    
    for command, description in jsonpath_ops:
        print(f"\n{description}:")
        print(f"   Command: ja {command}")
        result = run_ja_command(command, jsonpath_file.name)
        print(f"   Result: {result['output_lines']} records")
        if result['stderr']:
            print(f"   Warnings: {result['stderr'].strip()}")
    
    # Cleanup
    os.unlink(jsonpath_file.name)


def main():
    """Main demonstration function."""
    print("JSONL-ALGEBRA STREAMING COMPREHENSIVE DEMONSTRATION")
    print("="*60)
    print("This demo showcases the complete streaming functionality")
    print("including performance benefits and warning systems.")
    
    try:
        # Check if ja command is available
        subprocess.run(['ja', '--help'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("\n❌ Error: 'ja' command not found.")
        print("Please make sure jsonl-algebra is installed and 'ja' is in your PATH.")
        return 1
    
    try:
        demo_streaming_operations()
        demo_warning_system()
        demo_performance_comparison()
        demo_jsonpath_streaming()
        
        print("\n" + "="*60)
        print("DEMONSTRATION COMPLETE")
        print("="*60)
        print("\n✅ All streaming features demonstrated successfully!")
        print("\nKey takeaways:")
        print("• Streaming mode enables memory-efficient processing of large files")
        print("• Chain streaming operations for complex data pipelines")
        print("• Automatic warnings help identify non-streamable operations")
        print("• JSONPath operations support streaming for nested data")
        print("• Performance benefits depend on data size and operation complexity")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Demo interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
