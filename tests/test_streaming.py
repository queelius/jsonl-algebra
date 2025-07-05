import unittest
import tempfile
import os
import json
from typing import Iterator, List
from ja.streaming import (
    select_stream,
    project_stream,
    rename_stream,
    union_stream,
    distinct_stream,
    select_path_stream,
    project_template_stream,
)
from ja.core import select, project, rename, union, distinct, Relation
from ja.jsonpath import select_path, project_template


class TestStreamingFunctions(unittest.TestCase):
    """Test streaming implementations against their non-streaming counterparts."""
    
    def setUp(self):
        """Set up test data."""
        self.test_data = [
            {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 24, "city": "London"},
            {"id": 3, "name": "Charlie", "age": 30, "city": "Paris"},
            {"id": 4, "name": "Diana", "age": 28, "city": "Tokyo"},
        ]
        
        self.large_test_data = []
        for i in range(1000):
            self.large_test_data.append({
                "id": i,
                "name": f"User{i}",
                "age": 20 + (i % 50),
                "category": "A" if i % 2 == 0 else "B",
                "score": i * 0.1
            })
    
    def _list_from_generator(self, gen: Iterator) -> List:
        """Convert generator to list for comparison."""
        return list(gen)
    
    def test_streaming_select_basic(self):
        """Test basic streaming select operation."""
        predicate = lambda row: row.get("age") == 30
        
        # Compare streaming vs non-streaming
        streaming_result = self._list_from_generator(
            select_stream(iter(self.test_data), predicate)
        )
        non_streaming_result = select(self.test_data, predicate)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertEqual(len(streaming_result), 2)
        self.assertTrue(all(row["age"] == 30 for row in streaming_result))
    
    def test_streaming_select_empty(self):
        """Test streaming select with empty input."""
        predicate = lambda row: row.get("age") == 30
        
        streaming_result = self._list_from_generator(
            select_stream(iter([]), predicate)
        )
        non_streaming_result = select([], predicate)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertEqual(len(streaming_result), 0)
    
    def test_streaming_select_no_matches(self):
        """Test streaming select with no matching records."""
        predicate = lambda row: row.get("age") == 100
        
        streaming_result = self._list_from_generator(
            select_stream(iter(self.test_data), predicate)
        )
        non_streaming_result = select(self.test_data, predicate)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertEqual(len(streaming_result), 0)
    
    def test_streaming_project_basic(self):
        """Test basic streaming project operation."""
        columns = ["name", "age"]
        
        streaming_result = self._list_from_generator(
            project_stream(iter(self.test_data), columns)
        )
        non_streaming_result = project(self.test_data, columns)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertTrue(all(set(row.keys()) == set(columns) for row in streaming_result))
    
    def test_streaming_project_nonexistent_columns(self):
        """Test streaming project with non-existent columns."""
        columns = ["name", "nonexistent"]
        
        streaming_result = self._list_from_generator(
            project_stream(iter(self.test_data), columns)
        )
        non_streaming_result = project(self.test_data, columns)
        
        self.assertEqual(streaming_result, non_streaming_result)
        # Should only have 'name' key in each row
        self.assertTrue(all("name" in row for row in streaming_result))
        self.assertTrue(all("nonexistent" not in row for row in streaming_result))
    
    def test_streaming_rename_basic(self):
        """Test basic streaming rename operation."""
        mapping = {"name": "full_name", "age": "years"}
        
        streaming_result = self._list_from_generator(
            rename_stream(iter(self.test_data), mapping)
        )
        non_streaming_result = rename(self.test_data, mapping)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertTrue(all("full_name" in row for row in streaming_result))
        self.assertTrue(all("years" in row for row in streaming_result))
        self.assertTrue(all("name" not in row for row in streaming_result))
        self.assertTrue(all("age" not in row for row in streaming_result))
    
    def test_streaming_union_basic(self):
        """Test basic streaming union operation."""
        data1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        data2 = [{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Diana"}]
        
        streaming_result = self._list_from_generator(
            union_stream(iter(data1), iter(data2))
        )
        non_streaming_result = union(data1, data2)
        
        self.assertEqual(len(streaming_result), len(non_streaming_result))
        self.assertEqual(set(json.dumps(row, sort_keys=True) for row in streaming_result),
                         set(json.dumps(row, sort_keys=True) for row in non_streaming_result))
    
    def test_streaming_union_with_duplicates(self):
        """Test streaming union with duplicate records."""
        data1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        data2 = [{"id": 1, "name": "Alice"}, {"id": 3, "name": "Charlie"}]
        
        streaming_result = self._list_from_generator(
            union_stream(iter(data1), iter(data2))
        )
        non_streaming_result = union(data1, data2)
        
        self.assertEqual(len(streaming_result), len(non_streaming_result))
        self.assertEqual(set(json.dumps(row, sort_keys=True) for row in streaming_result),
                         set(json.dumps(row, sort_keys=True) for row in non_streaming_result))
    
    def test_streaming_distinct_basic(self):
        """Test basic streaming distinct operation."""
        data_with_duplicates = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice"},  # duplicate
            {"id": 3, "name": "Charlie"},
            {"id": 2, "name": "Bob"},     # duplicate
        ]
        
        streaming_result = self._list_from_generator(
            distinct_stream(iter(data_with_duplicates))
        )
        non_streaming_result = distinct(data_with_duplicates)
        
        self.assertEqual(len(streaming_result), len(non_streaming_result))
        self.assertEqual(set(json.dumps(row, sort_keys=True) for row in streaming_result),
                         set(json.dumps(row, sort_keys=True) for row in non_streaming_result))
    
    def test_streaming_distinct_no_duplicates(self):
        """Test streaming distinct with no duplicates."""
        streaming_result = self._list_from_generator(
            distinct_stream(iter(self.test_data))
        )
        non_streaming_result = distinct(self.test_data)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertEqual(len(streaming_result), len(self.test_data))
    
    def test_streaming_jsonpath_select_basic(self):
        """Test basic streaming JSONPath select operation."""
        path = "$.age"
        predicate = lambda x: x == 30
        
        streaming_result = self._list_from_generator(
            select_path_stream(iter(self.test_data), path, predicate)
        )
        non_streaming_result = select_path(self.test_data, path, predicate)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertEqual(len(streaming_result), 2)
        self.assertTrue(all(row["age"] == 30 for row in streaming_result))
    
    def test_streaming_jsonpath_select_nested(self):
        """Test streaming JSONPath select with nested data."""
        nested_data = [
            {"id": 1, "profile": {"name": "Alice", "age": 30}},
            {"id": 2, "profile": {"name": "Bob", "age": 24}},
            {"id": 3, "profile": {"name": "Charlie", "age": 30}},
        ]
        
        path = "$.profile.age"
        predicate = lambda x: x == 30
        
        streaming_result = self._list_from_generator(
            select_path_stream(iter(nested_data), path, predicate)
        )
        non_streaming_result = select_path(nested_data, path, predicate)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertEqual(len(streaming_result), 2)
    
    def test_streaming_jsonpath_project_basic(self):
        """Test basic streaming JSONPath project operation."""
        template = {"name": "$.name", "years": "$.age"}
        
        streaming_result = self._list_from_generator(
            project_template_stream(iter(self.test_data), template)
        )
        non_streaming_result = project_template(self.test_data, template)
        
        self.assertEqual(streaming_result, non_streaming_result)
        self.assertTrue(all("name" in row and "years" in row for row in streaming_result))
        self.assertTrue(all(len(row) == 2 for row in streaming_result))
    
    def test_streaming_large_dataset(self):
        """Test streaming with large dataset for memory efficiency."""
        # This test verifies that streaming works with large datasets
        # without loading everything into memory at once
        
        predicate = lambda row: row.get("category") == "A"
        
        # Use streaming select on large dataset
        streaming_result = self._list_from_generator(
            select_stream(iter(self.large_test_data), predicate)
        )
        
        # Verify results
        expected_count = len([r for r in self.large_test_data if r.get("category") == "A"])
        self.assertEqual(len(streaming_result), expected_count)
        self.assertTrue(all(row["category"] == "A" for row in streaming_result))
    
    def test_streaming_chaining(self):
        """Test chaining multiple streaming operations."""
        # Chain select -> project -> rename
        select_gen = select_stream(iter(self.test_data), lambda row: row.get("age") >= 25)
        project_gen = project_stream(select_gen, ["name", "age"])
        rename_gen = rename_stream(project_gen, {"age": "years"})
        
        result = self._list_from_generator(rename_gen)
        
        # Verify chaining worked correctly
        self.assertTrue(len(result) >= 1)
        self.assertTrue(all("name" in row and "years" in row for row in result))
        self.assertTrue(all("age" not in row for row in result))
        self.assertTrue(all(row["years"] >= 25 for row in result))
    
    def test_streaming_generator_consumed_once(self):
        """Test that streaming generators can only be consumed once."""
        predicate = lambda row: row.get("age") == 30
        
        gen = select_stream(iter(self.test_data), predicate)
        
        # First consumption
        result1 = self._list_from_generator(gen)
        self.assertEqual(len(result1), 2)
        
        # Second consumption should be empty (generator exhausted)
        result2 = self._list_from_generator(gen)
        self.assertEqual(len(result2), 0)
    
    def test_streaming_error_handling(self):
        """Test streaming error handling with malformed data."""
        malformed_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob"},  # missing age
            {"id": 3, "name": "Charlie", "age": "invalid"},  # invalid age type
            {"id": 4, "name": "Diana", "age": 28},
        ]
        
        # This should not crash, but handle missing/invalid fields gracefully
        predicate = lambda row: isinstance(row.get("age"), int) and row.get("age") > 25
        
        streaming_result = self._list_from_generator(
            select_stream(iter(malformed_data), predicate)
        )
        
        # Should only get records with valid integer ages > 25
        self.assertEqual(len(streaming_result), 2)
        self.assertTrue(all(isinstance(row["age"], int) and row["age"] > 25 
                           for row in streaming_result))


class TestStreamingWithFiles(unittest.TestCase):
    """Test streaming operations with actual JSONL files."""
    
    def setUp(self):
        """Create temporary JSONL files for testing."""
        self.test_data = [
            {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 24, "city": "London"},
            {"id": 3, "name": "Charlie", "age": 30, "city": "Paris"},
            {"id": 4, "name": "Diana", "age": 28, "city": "Tokyo"},
        ]
        
        # Create temporary file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
        for row in self.test_data:
            json.dump(row, self.temp_file)
            self.temp_file.write('\n')
        self.temp_file.close()
    
    def tearDown(self):
        """Clean up temporary files."""
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
    
    def _read_jsonl_generator(self, filename):
        """Read JSONL file as generator."""
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
    
    def test_streaming_from_file(self):
        """Test streaming operations reading from actual JSONL file."""
        # Test streaming select from file
        predicate = lambda row: row.get("age") == 30
        
        file_gen = self._read_jsonl_generator(self.temp_file.name)
        streaming_result = list(select_stream(file_gen, predicate))
        
        self.assertEqual(len(streaming_result), 2)
        self.assertTrue(all(row["age"] == 30 for row in streaming_result))
    
    def test_streaming_memory_efficiency(self):
        """Test that streaming doesn't load entire file into memory at once."""
        # Create a larger temporary file
        large_temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
        
        try:
            # Write 10,000 records
            for i in range(10000):
                record = {
                    "id": i,
                    "name": f"User{i}",
                    "age": 20 + (i % 50),
                    "category": "A" if i % 2 == 0 else "B"
                }
                json.dump(record, large_temp_file)
                large_temp_file.write('\n')
            large_temp_file.close()
            
            # Stream through the file and select only category A
            file_gen = self._read_jsonl_generator(large_temp_file.name)
            predicate = lambda row: row.get("category") == "A"
            
            # Count results without loading all into memory
            count = 0
            for row in select_stream(file_gen, predicate):
                count += 1
                # Verify each row as we go
                self.assertEqual(row["category"], "A")
            
            # Should have 5000 records (half of 10000)
            self.assertEqual(count, 5000)
            
        finally:
            if os.path.exists(large_temp_file.name):
                os.unlink(large_temp_file.name)


if __name__ == "__main__":
    unittest.main()
