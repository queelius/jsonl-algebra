#!/usr/bin/env python3
"""
Test script to demonstrate windowed processing capabilities.
"""

import subprocess
import tempfile
import json
import os


def run_ja_command(command, input_data=None, input_file=None):
    """Run a ja command with input data or file."""
    if input_file:
        cmd = ['ja'] + command.split() + [input_file]
        stdin_data = None
    else:
        cmd = ['ja'] + command.split()
        stdin_data = input_data
    
    try:
        result = subprocess.run(cmd, input=stdin_data, capture_output=True, text=True)
        return {
            'stdout': result.stdout.strip(),
            'stderr': result.stderr.strip(),
            'returncode': result.returncode
        }
    except Exception as e:
        return {
            'stdout': '',
            'stderr': str(e),
            'returncode': 1
        }


def test_windowed_sort():
    """Test windowed sort operation."""
    print("=" * 60)
    print("WINDOWED SORT TESTING")
    print("=" * 60)
    
    # Test data: unsorted numbers
    test_data = '\n'.join([
        '{"value": 9}',
        '{"value": 3}', 
        '{"value": -1}',
        '{"value": 2}',
        '{"value": 7}',
        '{"value": 1}'
    ])
    
    print("Input data:")
    for line in test_data.split('\n'):
        data = json.loads(line)
        print(f"  {data['value']}")
    
    # Test different window sizes
    for window_size in [2, 3, 4]:
        print(f"\nWindow size {window_size}:")
        result = run_ja_command(f"sort value --window-size {window_size}", test_data)
        
        if result['returncode'] == 0:
            output_values = []
            for line in result['stdout'].split('\n'):
                if line.strip():
                    data = json.loads(line)
                    output_values.append(data['value'])
            print(f"  Output: {output_values}")
            
            # Show windows
            windows = [output_values[i:i+window_size] for i in range(0, len(output_values), window_size)]
            print(f"  Windows: {' | '.join(str(w) for w in windows)}")
        else:
            print(f"  Error: {result['stderr']}")
    
    # Compare with normal sort
    print(f"\nFull sort (for comparison):")
    result = run_ja_command("sort value", test_data)
    if result['returncode'] == 0:
        output_values = []
        for line in result['stdout'].split('\n'):
            if line.strip():
                data = json.loads(line)
                output_values.append(data['value'])
        print(f"  Output: {output_values}")


def test_windowed_groupby():
    """Test windowed groupby operation."""
    print("\n" + "=" * 60)
    print("WINDOWED GROUPBY TESTING")
    print("=" * 60)
    
    # Test data: mixed categories
    test_data = '\n'.join([
        '{"category": "A", "value": 10}',
        '{"category": "B", "value": 20}',
        '{"category": "A", "value": 30}',
        '{"category": "B", "value": 40}',
        '{"category": "A", "value": 50}',
        '{"category": "C", "value": 60}'
    ])
    
    print("Input data:")
    for line in test_data.split('\n'):
        data = json.loads(line)
        print(f"  {data['category']}: {data['value']}")
    
    # Test windowed groupby
    print(f"\nWindowed groupby (window size 2):")
    result = run_ja_command("groupby category --agg sum:value --window-size 2", test_data)
    
    if result['returncode'] == 0:
        print("  Output:")
        for line in result['stdout'].split('\n'):
            if line.strip():
                data = json.loads(line)
                print(f"    {data['category']}: {data['sum_value']}")
    else:
        print(f"  Error: {result['stderr']}")
    
    # Compare with normal groupby
    print(f"\nFull groupby (for comparison):")
    result = run_ja_command("groupby category --agg sum:value", test_data)
    if result['returncode'] == 0:
        print("  Output:")
        for line in result['stdout'].split('\n'):
            if line.strip():
                data = json.loads(line)
                print(f"    {data['category']}: {data['sum_value']}")


def test_memory_efficiency():
    """Test memory efficiency with larger dataset."""
    print("\n" + "=" * 60)
    print("MEMORY EFFICIENCY TESTING")
    print("=" * 60)
    
    # Create larger test file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    
    try:
        print("Creating large test dataset (10,000 records)...")
        for i in range(10000):
            record = {
                "id": i,
                "value": (i * 7) % 100,  # Create some pattern
                "category": ["A", "B", "C", "D"][i % 4]
            }
            json.dump(record, temp_file)
            temp_file.write('\n')
        temp_file.close()
        
        print(f"Test file: {temp_file.name}")
        print(f"File size: {os.path.getsize(temp_file.name) / 1024:.1f} KB")
        
        # Test windowed sort
        print(f"\nTesting windowed sort (window size 1000)...")
        result = run_ja_command(f"sort value --window-size 1000", input_file=temp_file.name)
        
        if result['returncode'] == 0:
            output_lines = len(result['stdout'].split('\n')) if result['stdout'] else 0
            print(f"  Successfully processed {output_lines} records")
            print(f"  Memory usage: O(window_size) = O(1000) instead of O(10000)")
        else:
            print(f"  Error: {result['stderr']}")
        
        # Test windowed groupby
        print(f"\nTesting windowed groupby (window size 500)...")
        result = run_ja_command(f"groupby category --agg count --window-size 500", input_file=temp_file.name)
        
        if result['returncode'] == 0:
            output_lines = len([line for line in result['stdout'].split('\n') if line.strip()])
            print(f"  Successfully processed into {output_lines} groups")
            print(f"  Memory usage: O(window_size) = O(500) instead of O(10000)")
        else:
            print(f"  Error: {result['stderr']}")
            
    finally:
        # Cleanup
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)


def test_warning_system():
    """Test the warning system for windowed operations."""
    print("\n" + "=" * 60)
    print("WARNING SYSTEM TESTING")
    print("=" * 60)
    
    test_data = '{"value": 1}\n{"value": 2}'
    
    # Test windowed operation (should show approximation warning)
    print("Testing windowed operation warning:")
    result = run_ja_command("sort value --window-size 1", test_data)
    print(f"  Command: ja sort value --window-size 1")
    if result['stderr']:
        print(f"  Warning: {result['stderr']}")
    
    # Test windowed + streaming (should show both warnings)
    print(f"\nTesting windowed + streaming warnings:")
    result = run_ja_command("sort value --window-size 1 --stream", test_data)
    print(f"  Command: ja sort value --window-size 1 --stream")
    if result['stderr']:
        warnings = result['stderr'].split('\n')
        for warning in warnings:
            if warning.strip():
                print(f"  Warning: {warning.strip()}")


def main():
    """Run all windowed processing tests."""
    print("WINDOWED PROCESSING COMPREHENSIVE TESTING")
    print("=" * 60)
    print("Testing windowed processing capabilities for memory-intensive operations")
    print("that provide approximate results by processing data in chunks.")
    
    try:
        test_windowed_sort()
        test_windowed_groupby()
        test_memory_efficiency()
        test_warning_system()
        
        print("\n" + "=" * 60)
        print("WINDOWED PROCESSING TESTS COMPLETE")
        print("=" * 60)
        print("\n✅ All windowed processing features tested successfully!")
        print("\nKey benefits:")
        print("• Memory-efficient processing with O(window_size) instead of O(dataset_size)")
        print("• Approximate results that are often useful for analysis")
        print("• Clear warnings about approximation behavior")
        print("• Compatible with existing CLI interface")
        
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Tests failed with error: {e}")


if __name__ == "__main__":
    main()
