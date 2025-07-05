#!/usr/bin/env python3
"""
Streaming functionality demonstration for jsonl-algebra.

This script demonstrates the memory efficiency of streaming mode vs regular mode.
"""

import sys
import time
import psutil
import os
from pathlib import Path
import json
import tempfile
import subprocess

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # Convert to MB

def create_test_data(filename, num_records=100000):
    """Create a large test dataset."""
    print(f"Creating test dataset with {num_records:,} records...")
    
    with open(filename, 'w') as f:
        for i in range(num_records):
            record = {
                "id": i,
                "user": {"name": f"user_{i}", "age": 20 + (i % 60)},
                "orders": [
                    {"item": f"item_{j}", "price": (j + 1) * 10.99, "quantity": j + 1}
                    for j in range(i % 5 + 1)  # 1-5 orders per user
                ],
                "tags": [f"tag_{k}" for k in range(i % 3 + 1)],  # 1-3 tags per user
                "metadata": {
                    "created": f"2024-01-{(i % 30) + 1:02d}",
                    "score": (i * 7) % 100,
                    "active": i % 2 == 0
                }
            }
            f.write(json.dumps(record) + '\n')
    
    print(f"Created {filename} ({os.path.getsize(filename) / 1024 / 1024:.1f} MB)")

def benchmark_operation(command, description, stream_mode=False):
    """Benchmark a ja operation and measure memory usage."""
    print(f"\n--- {description} ---")
    
    # Add --stream flag if requested
    if stream_mode:
        # Insert --stream before any file arguments or at the end
        cmd_parts = command.split()
        if len(cmd_parts) > 2:
            # Insert --stream after the operation name
            cmd_parts.insert(2, '--stream')
        command = ' '.join(cmd_parts)
        print(f"Command (streaming): {command}")
    else:
        print(f"Command (memory): {command}")
    
    # Measure memory before
    mem_before = get_memory_usage()
    
    # Run command and measure time
    start_time = time.time()
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            timeout=30  # 30 second timeout
        )
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return None
            
        end_time = time.time()
        
        # Count output lines
        output_lines = len([line for line in result.stdout.strip().split('\n') if line.strip()])
        
        # Measure memory after
        mem_after = get_memory_usage()
        
        duration = end_time - start_time
        mem_peak = mem_after  # Approximation
        
        print(f"Duration: {duration:.2f}s")
        print(f"Memory before: {mem_before:.1f} MB")
        print(f"Memory peak: {mem_peak:.1f} MB")
        print(f"Memory increase: {mem_peak - mem_before:.1f} MB")
        print(f"Output records: {output_lines:,}")
        
        return {
            'duration': duration,
            'memory_before': mem_before,
            'memory_peak': mem_peak,
            'memory_increase': mem_peak - mem_before,
            'output_records': output_lines
        }
        
    except subprocess.TimeoutExpired:
        print("Command timed out!")
        return None

def main():
    print("JSONL Algebra Streaming Mode Demonstration")
    print("=" * 50)
    
    # Create test data
    test_file = "streaming_test_data.jsonl"
    
    if not os.path.exists(test_file):
        create_test_data(test_file, num_records=10000)  # Start with 10K records
    
    print(f"\nFile size: {os.path.getsize(test_file) / 1024 / 1024:.1f} MB")
    
    # Test operations that support streaming
    template_json = json.dumps({"user_id": "$.id", "name": "$.user.name"})
    test_cases = [
        ("Select operation", f"python -m ja select 'user.age > 30' {test_file}"),
        ("Project operation", f"python -m ja project id,user.name {test_file}"),
        ("JSONPath select", f"python -m ja select-path '$.user.age' --predicate 'lambda x: x > 30' {test_file}"),
        ("Template projection", f"python -m ja project-template '{template_json}' {test_file}"),
    ]
    
    results = {}
    
    for test_name, command in test_cases:
        print(f"\n{'='*60}")
        print(f"Testing: {test_name}")
        print(f"{'='*60}")
        
        # Test memory mode
        mem_result = benchmark_operation(command, f"{test_name} (Memory Mode)", stream_mode=False)
        
        # Test streaming mode  
        stream_result = benchmark_operation(command, f"{test_name} (Streaming Mode)", stream_mode=True)
        
        if mem_result and stream_result:
            results[test_name] = {
                'memory_mode': mem_result,
                'streaming_mode': stream_result
            }
            
            # Compare results
            print(f"\n📊 Comparison:")
            print(f"Speed: Streaming is {mem_result['duration'] / stream_result['duration']:.1f}x the speed of memory mode")
            print(f"Memory: Streaming uses {stream_result['memory_increase'] / max(mem_result['memory_increase'], 0.1):.1f}x the memory of memory mode")
    
    # Summary
    if results:
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        
        total_mem_time = sum(r['memory_mode']['duration'] for r in results.values())
        total_stream_time = sum(r['streaming_mode']['duration'] for r in results.values())
        avg_mem_increase_mem = sum(r['memory_mode']['memory_increase'] for r in results.values()) / len(results)
        avg_mem_increase_stream = sum(r['streaming_mode']['memory_increase'] for r in results.values()) / len(results)
        
        print(f"Overall speed improvement: {total_mem_time / total_stream_time:.1f}x")
        print(f"Average memory usage (Memory mode): {avg_mem_increase_mem:.1f} MB")
        print(f"Average memory usage (Streaming mode): {avg_mem_increase_stream:.1f} MB")
        print(f"Memory efficiency: {avg_mem_increase_mem / max(avg_mem_increase_stream, 0.1):.1f}x less memory with streaming")
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
        print(f"\nCleaned up {test_file}")

if __name__ == "__main__":
    main()
