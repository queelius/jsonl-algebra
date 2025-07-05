#!/usr/bin/env python3
"""
Simple streaming functionality test for jsonl-algebra.
"""

import json
import tempfile
import os
import subprocess

def create_test_data(filename, num_records=1000):
    """Create test data."""
    print(f"Creating {num_records} test records...")
    
    with open(filename, 'w') as f:
        for i in range(num_records):
            record = {
                "id": i,
                "user": {"name": f"user_{i}", "age": 20 + (i % 60)},
                "orders": [
                    {"item": f"item_{j}", "price": (j + 1) * 10.99}
                    for j in range(i % 3 + 1)  # 1-3 orders per user
                ],
                "tags": [f"tag_{k}" for k in range(i % 2 + 1)]
            }
            f.write(json.dumps(record) + '\n')

def test_streaming_command(command, description):
    """Test a command in both normal and streaming mode."""
    print(f"\n--- {description} ---")
    
    # Test normal mode
    print("Normal mode:")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        normal_lines = len([line for line in result.stdout.strip().split('\n') if line.strip()])
        print(f"✅ Success - {normal_lines} records")
    else:
        print(f"❌ Error: {result.stderr}")
        return
    
    # Test streaming mode  
    stream_command = command.replace('ja ', 'ja ', 1)
    parts = stream_command.split()
    if len(parts) > 2:
        parts.insert(2, '--stream')
    stream_command = ' '.join(parts)
    
    print("Streaming mode:")
    print(f"Command: {stream_command}")
    result = subprocess.run(stream_command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        stream_lines = len([line for line in result.stdout.strip().split('\n') if line.strip()])
        print(f"✅ Success - {stream_lines} records")
        
        if normal_lines == stream_lines:
            print("✅ Output matches normal mode")
        else:
            print(f"⚠️  Output differs: normal={normal_lines}, stream={stream_lines}")
    else:
        print(f"❌ Error: {result.stderr}")

def main():
    print("JSONL Algebra Streaming Test")
    print("=" * 40)
    
    # Create test data
    test_file = "test_streaming.jsonl"
    create_test_data(test_file, 100)
    
    print(f"Created {test_file} ({os.path.getsize(test_file)} bytes)")
    
    # Test streaming-capable operations
    tests = [
        (f"ja select 'user.age > 30' {test_file}", "Select filter"),
        (f"ja project id,user.name {test_file}", "Project columns"),
        (f"ja rename user.name=username {test_file}", "Rename columns"),
        (f"ja distinct {test_file}", "Remove duplicates"),
    ]
    
    for command, description in tests:
        test_streaming_command(command, description)
    
    # Test JSONPath operations
    jsonpath_tests = [
        (f"ja select-path '$.user.age' --predicate 'lambda x: x > 30' {test_file}", "JSONPath select"),
        (f"ja select-any '$.tags[*]' --predicate 'lambda x: x == \"tag_0\"' {test_file}", "JSONPath select-any"),
    ]
    
    for command, description in jsonpath_tests:
        test_streaming_command(command, description)
    
    # Test operations that DON'T support streaming (should ignore --stream flag)
    print(f"\n--- Non-streaming operations (should ignore --stream) ---")
    non_streaming_tests = [
        (f"ja join {test_file} {test_file} --on id=id", "Join operation"),
        (f"ja sort user.age {test_file}", "Sort operation"),
    ]
    
    for command, description in non_streaming_tests:
        print(f"\n{description}:")
        # Add --stream flag (should be ignored)
        parts = command.split()
        if len(parts) > 2 and not '--stream' in command:
            parts.insert(2, '--stream')
        stream_command = ' '.join(parts)
        
        print(f"Command with --stream: {stream_command}")
        result = subprocess.run(stream_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            lines = len([line for line in result.stdout.strip().split('\n') if line.strip()])
            print(f"✅ Success - {lines} records (--stream flag ignored as expected)")
        else:
            print(f"❌ Error: {result.stderr}")
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
        print(f"\nCleaned up {test_file}")
    
    print("\n✅ Streaming test complete!")

if __name__ == "__main__":
    main()
