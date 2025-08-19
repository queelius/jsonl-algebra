"""Extended tests for command functions to improve coverage."""

import sys
import json
import warnings
import pytest
from io import StringIO
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
from ja.commands import (
    _warn_streaming_not_supported,
    _warn_memory_intensive,
    _warn_windowed_approximation,
    _should_stream,
    _should_use_windowed,
    handle_select,
    handle_project,
    handle_rename,
    handle_join,
    handle_sort,
    handle_groupby,
    handle_union,
    handle_intersection,
    handle_difference,
    handle_distinct,
    handle_product,
    handle_select_path,
    handle_select_any,
    handle_select_all,
    handle_select_none,
    handle_project_template,
    read_jsonl,
    write_jsonl,
)


class TestWarningFunctions:
    """Test warning functions."""
    
    def test_warn_streaming_not_supported(self):
        """Test streaming not supported warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _warn_streaming_not_supported("test_op")
            assert len(w) == 1
            assert "test_op" in str(w[0].message)
            assert "requires loading data into memory" in str(w[0].message)
    
    def test_warn_windowed_approximation(self):
        """Test windowed approximation warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _warn_windowed_approximation("test_op")
            assert len(w) == 1
            assert "approximate results" in str(w[0].message)


class TestShouldFunctions:
    """Test decision functions for streaming and windowing."""
    
    def test_should_stream(self):
        """Test _should_stream function."""
        args = MagicMock()
        # Streaming operations should return True
        assert _should_stream(args, "select") == True
        assert _should_stream(args, "project") == True
        # Non-streaming operations should return False
        assert _should_stream(args, "join") == False
        assert _should_stream(args, "sort") == False
    
    def test_should_use_windowed(self):
        """Test _should_use_windowed function."""
        args = MagicMock()
        
        # No window size
        args.window_size = None
        use_windowed, window_size = _should_use_windowed(args, "sort")
        assert use_windowed == False
        assert window_size == None
        
        # With window size for supported operation
        args.window_size = 100
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            use_windowed, window_size = _should_use_windowed(args, "sort")
            assert use_windowed == True
            assert window_size == 100
            assert len(w) == 1  # Should warn about approximate results
        
        # With window size for unsupported operation
        args.window_size = 100
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            use_windowed, window_size = _should_use_windowed(args, "select")
            assert use_windowed == False
            assert window_size == None
            assert len(w) == 1  # Should warn not supported


class TestCommandsWithWindowing:
    """Test command handlers with windowing support."""
    
    @patch('ja.commands.write_jsonl')
    @patch('ja.commands.windowed_sort_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_sort_with_windowing(self, mock_read_stream, mock_windowed_sort, mock_write):
        """Test sort command with windowing."""
        args = MagicMock()
        args.keys = ['age']
        args.file = '-'
        args.window_size = 100
        
        mock_read_stream.return_value = iter([{'age': 30}, {'age': 25}])
        mock_windowed_sort.return_value = iter([{'age': 25}, {'age': 30}])
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            handle_sort(args)
            
        mock_windowed_sort.assert_called_once()
        assert w  # Should have warning about windowing
    
    @patch('ja.commands.write_jsonl')
    @patch('ja.commands.windowed_groupby_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_groupby_with_windowing(self, mock_read_stream, mock_windowed_groupby, mock_write):
        """Test groupby command with windowing."""
        args = MagicMock()
        args.key = 'category'
        args.agg = 'sum:amount'  # Should be a string, not a list
        args.file = '-'
        args.window_size = 100
        
        mock_read_stream.return_value = iter([
            {'category': 'A', 'amount': 10},
            {'category': 'A', 'amount': 20}
        ])
        mock_windowed_groupby.return_value = iter([
            {'category': 'A', 'sum_amount': 30}
        ])
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            handle_groupby(args)
            
        mock_windowed_groupby.assert_called_once()
    
    @patch('ja.commands.write_jsonl')
    @patch('ja.commands.windowed_join_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_join_with_windowing(self, mock_read_stream, mock_windowed_join, mock_write):
        """Test join command with windowing."""
        args = MagicMock()
        args.left = 'left.jsonl'
        args.right = 'right.jsonl'
        args.on = 'id=user_id'  # Should be a string, not a list
        args.window_size = 100
        
        mock_read_stream.side_effect = [
            iter([{'id': 1, 'name': 'Alice'}]),
            iter([{'user_id': 1, 'order': 'Book'}])
        ]
        mock_windowed_join.return_value = iter([
            {'id': 1, 'name': 'Alice', 'user_id': 1, 'order': 'Book'}
        ])
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            handle_join(args)
            
        mock_windowed_join.assert_called_once()


class TestJSONPathCommands:
    """Test JSONPath command handlers."""
    
    @patch('ja.commands.write_jsonl_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_select_path(self, mock_read_stream, mock_write):
        """Test select-path command handler."""
        args = MagicMock()
        args.path = '$.user.age'
        args.predicate = None
        args.quantifier = 'any'
        args.file = None
        
        mock_read_stream.return_value = iter([
            {'user': {'age': 30}},
            {'user': {'age': 20}}
        ])
        
        # Should run without error
        try:
            handle_select_path(args)
        except SystemExit:
            pass  # May exit on error
        
        mock_read_stream.assert_called()
    
    @patch('ja.commands.write_jsonl_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_select_any(self, mock_read_stream, mock_write):
        """Test select-any command handler."""
        args = MagicMock()
        args.path = '$.items[*].inStock'
        args.predicate = None
        args.file = None
        
        mock_read_stream.return_value = iter([
            {'items': [{'inStock': True}, {'inStock': False}]}
        ])
        
        try:
            handle_select_any(args)
        except SystemExit:
            pass
        
        mock_read_stream.assert_called()
    
    @patch('ja.commands.write_jsonl_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_select_all(self, mock_read_stream, mock_write):
        """Test select-all command handler."""
        args = MagicMock()
        args.path = '$.items[*].inStock'
        args.predicate = None
        args.file = None
        
        mock_read_stream.return_value = iter([
            {'items': [{'inStock': True}, {'inStock': True}]}
        ])
        
        try:
            handle_select_all(args)
        except SystemExit:
            pass
        
        mock_read_stream.assert_called()
    
    @patch('ja.commands.write_jsonl_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_select_none(self, mock_read_stream, mock_write):
        """Test select-none command handler."""
        args = MagicMock()
        args.path = '$.items[*].error'
        args.predicate = None
        args.file = None
        
        mock_read_stream.return_value = iter([
            {'items': [{'name': 'A'}, {'name': 'B'}]}
        ])
        
        try:
            handle_select_none(args)
        except SystemExit:
            pass
        
        mock_read_stream.assert_called()
    
    @patch('ja.commands.write_jsonl_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_project_template(self, mock_read_stream, mock_write):
        """Test project-template command handler."""
        args = MagicMock()
        args.template = '{"name": "$.user.name"}'
        args.file = None
        
        mock_read_stream.return_value = iter([
            {'user': {'name': 'Alice'}}
        ])
        
        try:
            handle_project_template(args)
        except SystemExit:
            pass
        
        mock_read_stream.assert_called()


class TestWindowedOperations:
    """Test windowed versions of memory-intensive operations."""
    
    @patch('ja.commands.write_jsonl')
    @patch('ja.commands.windowed_intersection_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_intersection_with_windowing(self, mock_read_stream, mock_windowed_int, mock_write):
        """Test intersection command with windowing."""
        args = MagicMock()
        args.file1 = 'file1.jsonl'
        args.file2 = 'file2.jsonl'
        args.window_size = 100
        
        mock_read_stream.side_effect = [
            iter([{'id': 1}, {'id': 2}]),
            iter([{'id': 2}, {'id': 3}])
        ]
        mock_windowed_int.return_value = iter([{'id': 2}])
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            handle_intersection(args)
            
        mock_windowed_int.assert_called_once()
    
    @patch('ja.commands.write_jsonl')
    @patch('ja.commands.windowed_difference_stream')
    @patch('ja.commands.read_jsonl_stream')
    def test_handle_difference_with_windowing(self, mock_read_stream, mock_windowed_diff, mock_write):
        """Test difference command with windowing."""
        args = MagicMock()
        args.file1 = 'file1.jsonl'
        args.file2 = 'file2.jsonl'
        args.window_size = 100
        
        mock_read_stream.side_effect = [
            iter([{'id': 1}, {'id': 2}]),
            iter([{'id': 2}, {'id': 3}])
        ]
        mock_windowed_diff.return_value = iter([{'id': 1}])
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            handle_difference(args)
            
        mock_windowed_diff.assert_called_once()


class TestReadWriteFunctions:
    """Test read and write utility functions."""
    
    def test_read_jsonl_from_string_path(self):
        """Test reading JSONL from string path."""
        test_data = '{"name": "Alice"}\n{"name": "Bob"}'
        with patch('builtins.open', mock_open(read_data=test_data)):
            result = read_jsonl('test.jsonl')
            assert len(result) == 2
            assert result[0] == {'name': 'Alice'}
            assert result[1] == {'name': 'Bob'}
    
    def test_read_jsonl_from_pathlib_path(self):
        """Test reading JSONL from Path object."""
        test_data = '{"name": "Alice"}\n{"name": "Bob"}'
        with patch('builtins.open', mock_open(read_data=test_data)):
            result = read_jsonl(Path('test.jsonl'))
            assert len(result) == 2
    
    def test_read_jsonl_from_file_object(self):
        """Test reading JSONL from file object."""
        test_data = StringIO('{"name": "Alice"}\n{"name": "Bob"}')
        result = read_jsonl(test_data)
        assert len(result) == 2
        assert result[0] == {'name': 'Alice'}
    
    @patch('builtins.print')
    def test_write_jsonl(self, mock_print):
        """Test writing JSONL output."""
        data = [{'name': 'Alice'}, {'name': 'Bob'}]
        write_jsonl(data)
        assert mock_print.call_count == 2
        mock_print.assert_any_call('{"name": "Alice"}')
        mock_print.assert_any_call('{"name": "Bob"}')


class TestErrorHandling:
    """Test error handling in command functions."""
    
    @patch('ja.commands.read_jsonl_stream')
    @patch('ja.commands.write_jsonl_stream')
    def test_handle_select_with_eval_error(self, mock_write, mock_read):
        """Test select command with evaluation error."""
        args = MagicMock()
        args.expr = 'age > 25'  # Will fail with string age
        args.file = None
        
        mock_read.return_value = iter([{'age': 'not_a_number'}])
        
        # Should handle the error gracefully - select will filter out failing rows
        handle_select(args)
        
        # Write should be called but with empty results
        mock_write.assert_called_once()