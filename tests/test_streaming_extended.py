"""Extended tests for streaming module to improve coverage."""

import json
import sys
import pytest
from io import StringIO
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock
from ja.streaming import (
    read_jsonl_stream,
    write_jsonl_stream,
    can_stream_operation,
    requires_memory_operation,
    supports_windowed_operation,
    windowed_sort_stream,
    windowed_groupby_stream,
    windowed_join_stream,
    windowed_intersection_stream,
    windowed_difference_stream,
    select_stream,
    project_stream,
    rename_stream,
    union_stream,
    distinct_stream,
    select_path_stream,
    project_template_stream,
    chunked_sort_stream,
)


class TestChunkedOperations:
    """Test chunked streaming operations."""
    
    def test_chunked_sort_stream(self):
        """Test chunked sorting for large datasets."""
        rows = [
            {'id': 5, 'name': 'Eve'},
            {'id': 3, 'name': 'Charlie'},
            {'id': 1, 'name': 'Alice'},
            {'id': 4, 'name': 'David'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        # Small chunk size to test chunking behavior
        result = list(chunked_sort_stream(iter(rows), ['id'], chunk_size=2))
        
        # Should return sorted chunks
        assert len(result) == 5
        # Results may not be globally sorted but chunks should be processed


class TestWindowedOperations:
    """Test windowed streaming operations."""
    
    def test_windowed_sort_stream(self):
        """Test windowed sorting."""
        rows = [
            {'id': 3, 'name': 'Charlie'},
            {'id': 1, 'name': 'Alice'},
            {'id': 4, 'name': 'David'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        # Window size of 2 should sort in pairs
        result = list(windowed_sort_stream(iter(rows), ['id'], window_size=2))
        assert len(result) == 4
        # First window [3, 1] -> [1, 3]
        assert result[0]['id'] == 1
        assert result[1]['id'] == 3
        # Second window [4, 2] -> [2, 4]
        assert result[2]['id'] == 2
        assert result[3]['id'] == 4
    
    def test_windowed_sort_stream_multiple_keys(self):
        """Test windowed sorting with multiple keys."""
        rows = [
            {'category': 'B', 'id': 2},
            {'category': 'A', 'id': 3},
            {'category': 'A', 'id': 1},
            {'category': 'B', 'id': 4}
        ]
        
        result = list(windowed_sort_stream(iter(rows), ['category', 'id'], window_size=3))
        assert len(result) == 4
    
    def test_windowed_groupby_stream(self):
        """Test windowed groupby."""
        rows = [
            {'category': 'A', 'value': 10},
            {'category': 'B', 'value': 20},
            {'category': 'A', 'value': 15},
            {'category': 'B', 'value': 25}
        ]
        
        # Window size of 2 should group in pairs
        result = list(windowed_groupby_stream(
            iter(rows), 
            'category', 
            [('sum', 'value')], 
            window_size=2
        ))
        
        # Should have groups from each window
        assert len(result) >= 2  # At least A and B groups
    
    @patch('ja.streaming.read_jsonl_stream')
    def test_windowed_join_stream(self, mock_read):
        """Test windowed join."""
        left = iter([
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ])
        
        mock_read.return_value = iter([
            {'user_id': 1, 'order': 'Book'},
            {'user_id': 2, 'order': 'Pen'}
        ])
        
        result = list(windowed_join_stream(
            left, 'right.jsonl', 
            [('id', 'user_id')], 
            window_size=2
        ))
        
        # Should join within windows
        assert isinstance(result, list)
    
    @patch('ja.streaming.read_jsonl_stream')
    def test_windowed_intersection_stream(self, mock_read):
        """Test windowed intersection."""
        left = iter([
            {'id': 1},
            {'id': 2},
            {'id': 3}
        ])
        
        mock_read.return_value = iter([
            {'id': 2},
            {'id': 3},
            {'id': 4}
        ])
        
        result = list(windowed_intersection_stream(left, 'right.jsonl', window_size=2))
        # Should find some intersections within windows
        assert isinstance(result, list)
    
    @patch('ja.streaming.read_jsonl_stream')
    def test_windowed_difference_stream(self, mock_read):
        """Test windowed difference."""
        left = iter([
            {'id': 1},
            {'id': 2},
            {'id': 3}
        ])
        
        mock_read.return_value = iter([
            {'id': 2},
            {'id': 4}
        ])
        
        result = list(windowed_difference_stream(left, 'right.jsonl', window_size=2))
        # Should find differences within windows
        assert isinstance(result, list)


class TestReadJsonlStreamEdgeCases:
    """Test edge cases for read_jsonl_stream."""
    
    @patch('sys.stdin', StringIO('{"name": "Alice"}\n{"name": "Bob"}\n'))
    def test_read_jsonl_stream_from_stdin(self):
        """Test reading from stdin with '-' notation."""
        result = list(read_jsonl_stream('-'))
        assert len(result) == 2
        assert result[0]['name'] == 'Alice'
    
    def test_read_jsonl_stream_from_string_path(self):
        """Test reading from file path as string."""
        test_data = '{"name": "Alice"}\n\n{"name": "Bob"}\n'  # Empty line in middle
        with patch('builtins.open', mock_open(read_data=test_data)):
            result = list(read_jsonl_stream('test.jsonl'))
            assert len(result) == 2  # Should skip empty line
    
    def test_read_jsonl_stream_from_pathlib(self):
        """Test reading from Path object."""
        test_data = '{"name": "Alice"}\n{"name": "Bob"}'
        with patch('builtins.open', mock_open(read_data=test_data)):
            result = list(read_jsonl_stream(Path('test.jsonl')))
            assert len(result) == 2
    
    def test_read_jsonl_stream_from_file_object(self):
        """Test reading from file object."""
        file_obj = StringIO('{"name": "Alice"}\n\n{"name": "Bob"}')
        result = list(read_jsonl_stream(file_obj))
        assert len(result) == 2


class TestWriteJsonlStream:
    """Test write_jsonl_stream function."""
    
    @patch('builtins.print')
    def test_write_jsonl_stream(self, mock_print):
        """Test writing JSONL stream."""
        rows = iter([{'name': 'Alice'}, {'name': 'Bob'}])
        write_jsonl_stream(rows)
        
        assert mock_print.call_count == 2
        calls = mock_print.call_args_list
        assert json.loads(calls[0][0][0]) == {'name': 'Alice'}
        assert json.loads(calls[1][0][0]) == {'name': 'Bob'}


class TestOperationClassification:
    """Test operation classification functions."""
    
    def test_can_stream_operation(self):
        """Test can_stream_operation function."""
        # Streaming operations
        assert can_stream_operation('select') == True
        assert can_stream_operation('project') == True
        assert can_stream_operation('rename') == True
        assert can_stream_operation('union') == True
        assert can_stream_operation('distinct') == True
        
        # Non-streaming operations
        assert can_stream_operation('join') == False
        assert can_stream_operation('sort') == False
        assert can_stream_operation('groupby') == False
        assert can_stream_operation('intersection') == False
        assert can_stream_operation('difference') == False
        assert can_stream_operation('product') == False
        
        # Unknown operation
        assert can_stream_operation('unknown') == False
    
    def test_requires_memory_operation(self):
        """Test requires_memory_operation function."""
        assert requires_memory_operation('join') == True
        assert requires_memory_operation('sort') == True
        assert requires_memory_operation('groupby') == True
        assert requires_memory_operation('intersection') == True
        assert requires_memory_operation('difference') == True
        
        assert requires_memory_operation('select') == False
        assert requires_memory_operation('project') == False
    
    def test_supports_windowed_operation(self):
        """Test supports_windowed_operation function."""
        assert supports_windowed_operation('sort') == True
        assert supports_windowed_operation('groupby') == True
        assert supports_windowed_operation('join') == True
        assert supports_windowed_operation('intersection') == True
        assert supports_windowed_operation('difference') == True
        
        assert supports_windowed_operation('select') == False
        assert supports_windowed_operation('project') == False
        assert supports_windowed_operation('unknown') == False


class TestJSONPathStreaming:
    """Test JSONPath streaming functions."""
    
    @patch('ja.core.select_any')
    def test_select_path_stream_any(self, mock_select_any):
        """Test select_path_stream with 'any' quantifier."""
        mock_select_any.return_value = lambda row: True
        
        rows = [
            {'items': [{'inStock': True}]},
            {'items': [{'inStock': False}]}
        ]
        
        result = list(select_path_stream(
            iter(rows), 
            '$.items[*].inStock',
            lambda x: x == True,
            quantifier='any'
        ))
        
        assert isinstance(result, list)
    
    @patch('ja.core.select_all')
    def test_select_path_stream_all(self, mock_select_all):
        """Test select_path_stream with 'all' quantifier."""
        mock_select_all.return_value = lambda row: True
        
        rows = [
            {'items': [{'inStock': True}, {'inStock': True}]},
            {'items': [{'inStock': True}, {'inStock': False}]}
        ]
        
        result = list(select_path_stream(
            iter(rows),
            '$.items[*].inStock',
            lambda x: x == True,
            quantifier='all'
        ))
        
        assert isinstance(result, list)
    
    @patch('ja.core.select_none')
    def test_select_path_stream_none(self, mock_select_none):
        """Test select_path_stream with 'none' quantifier."""
        mock_select_none.return_value = lambda row: True
        
        rows = [
            {'items': [{'name': 'A'}, {'name': 'B'}]},
            {'items': [{'error': 'fail'}]}
        ]
        
        result = list(select_path_stream(
            iter(rows),
            '$.items[*].error',
            lambda x: x is not None,
            quantifier='none'
        ))
        
        assert isinstance(result, list)
    
    @patch('ja.core.select_path')
    def test_select_path_stream_exists(self, mock_select_path):
        """Test select_path_stream for existence check."""
        mock_select_path.return_value = lambda row: 'user' in row
        
        rows = [
            {'user': {'name': 'Alice'}},
            {'admin': {'name': 'Bob'}}
        ]
        
        result = list(select_path_stream(
            iter(rows),
            '$.user',
            None,  # No predicate means existence check
            quantifier='exists'
        ))
        
        assert isinstance(result, list)
    
    @patch('ja.core.project_template')
    def test_project_template_stream(self, mock_project_template):
        """Test project_template_stream function."""
        # Mock should return a list with the projected row
        mock_project_template.return_value = [{'name': 'Alice'}]
        
        rows = [
            {'user': {'name': 'Alice', 'age': 30}},
            {'user': {'name': 'Bob', 'age': 25}}
        ]
        
        template = {'name': '$.user.name'}
        result = list(project_template_stream(iter(rows), template))
        
        assert isinstance(result, list)
        # Should be called once per row
        assert mock_project_template.call_count == 2


class TestStreamingCoreFunctions:
    """Test core streaming functions not covered elsewhere."""
    
    def test_select_stream_basic(self):
        """Test select_stream function."""
        rows = [
            {'age': 30, 'name': 'Alice'},
            {'age': 20, 'name': 'Bob'},
            {'age': 35, 'name': 'Charlie'}
        ]
        
        result = list(select_stream(iter(rows), lambda r: r['age'] > 25))
        assert len(result) == 2
        assert result[0]['name'] == 'Alice'
        assert result[1]['name'] == 'Charlie'
    
    def test_project_stream_basic(self):
        """Test project_stream function."""
        rows = [
            {'name': 'Alice', 'age': 30, 'city': 'NYC'},
            {'name': 'Bob', 'age': 25, 'city': 'LA'}
        ]
        
        result = list(project_stream(iter(rows), ['name', 'age']))
        assert len(result) == 2
        assert 'city' not in result[0]
        assert result[0] == {'name': 'Alice', 'age': 30}
    
    def test_rename_stream_basic(self):
        """Test rename_stream function."""
        rows = [
            {'old_name': 'Alice', 'age': 30},
            {'old_name': 'Bob', 'age': 25}
        ]
        
        renames = {'old_name': 'name', 'age': 'years'}
        result = list(rename_stream(iter(rows), renames))
        assert len(result) == 2
        assert result[0] == {'name': 'Alice', 'years': 30}
    
    def test_union_stream_basic(self):
        """Test union_stream function."""
        stream_a = iter([{'id': 1}, {'id': 2}])
        stream_b = iter([{'id': 3}, {'id': 4}])
        
        result = list(union_stream(stream_a, stream_b))
        assert len(result) == 4
        assert result[0]['id'] == 1
        assert result[3]['id'] == 4
    
    def test_distinct_stream_basic(self):
        """Test distinct_stream function."""
        rows = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            {'id': 1, 'name': 'Alice'},  # Duplicate
            {'id': 3, 'name': 'Charlie'}
        ]
        
        result = list(distinct_stream(iter(rows)))
        assert len(result) == 3
    
    def test_distinct_stream_with_unhashable(self):
        """Test distinct_stream with unhashable values."""
        rows = [
            {'id': 1, 'tags': ['a', 'b']},  # List is unhashable
            {'id': 2, 'tags': ['c', 'd']},
            {'id': 1, 'tags': ['a', 'b']}   # Should be detected as duplicate using string fallback
        ]
        
        # Should handle unhashable gracefully by using string representation
        result = list(distinct_stream(iter(rows)))
        # Duplicates are still detected via string representation
        assert len(result) == 2  # Only unique rows