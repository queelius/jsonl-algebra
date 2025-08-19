"""Tests for the CLI module."""

import sys
import json
import pytest
from unittest.mock import patch, MagicMock
from io import StringIO
from ja.cli import main, add_window_argument


class TestCLI:
    """Test the main CLI entry point and argument parsing."""

    def test_add_window_argument(self):
        """Test add_window_argument helper function."""
        import argparse
        parser = argparse.ArgumentParser()
        add_window_argument(parser)
        args = parser.parse_args(['--window-size', '100'])
        assert args.window_size == 100
        
        args = parser.parse_args([])
        assert args.window_size is None

    @patch('ja.cli.handle_select')
    def test_select_command(self, mock_handle):
        """Test select command parsing."""
        test_args = ['ja', 'select', 'age > 25', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.expr == 'age > 25'
            assert args.file == 'test.jsonl'

    @patch('ja.cli.handle_project')
    def test_project_command(self, mock_handle):
        """Test project command parsing."""
        test_args = ['ja', 'project', 'name,age', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.columns == 'name,age'
            assert args.file == 'test.jsonl'

    @patch('ja.cli.handle_join')
    def test_join_command(self, mock_handle):
        """Test join command parsing."""
        test_args = ['ja', 'join', 'left.jsonl', 'right.jsonl', '--on', 'id=user_id']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.left == 'left.jsonl'
            assert args.right == 'right.jsonl'
            assert args.on == 'id=user_id'  # --on is a single string, not a list

    @patch('ja.cli.handle_rename')
    def test_rename_command(self, mock_handle):
        """Test rename command parsing."""
        test_args = ['ja', 'rename', 'old=new', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.mapping == 'old=new'  # Argument is 'mapping' not 'renames'
            assert args.file == 'test.jsonl'

    @patch('ja.cli.handle_union')
    def test_union_command(self, mock_handle):
        """Test union command parsing."""
        test_args = ['ja', 'union', 'file1.jsonl', 'file2.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.left == 'file1.jsonl'  # Arguments are 'left' and 'right'
            assert args.right == 'file2.jsonl'

    @patch('ja.cli.handle_difference')
    def test_difference_command(self, mock_handle):
        """Test difference command parsing."""
        test_args = ['ja', 'difference', 'file1.jsonl', 'file2.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.left == 'file1.jsonl'  # Arguments are 'left' and 'right'
            assert args.right == 'file2.jsonl'

    @patch('ja.cli.handle_distinct')
    def test_distinct_command(self, mock_handle):
        """Test distinct command parsing."""
        test_args = ['ja', 'distinct', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.file == 'test.jsonl'

    @patch('ja.cli.handle_intersection')
    def test_intersection_command(self, mock_handle):
        """Test intersection command parsing."""
        test_args = ['ja', 'intersection', 'file1.jsonl', 'file2.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.left == 'file1.jsonl'  # Arguments are 'left' and 'right'
            assert args.right == 'file2.jsonl'

    @patch('ja.cli.handle_sort')
    def test_sort_command(self, mock_handle):
        """Test sort command parsing."""
        test_args = ['ja', 'sort', 'age', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.columns == 'age'  # Argument is 'columns' not 'keys'
            assert args.file == 'test.jsonl'

    @patch('ja.cli.handle_sort')
    def test_sort_command_multiple_keys(self, mock_handle):
        """Test sort command with multiple keys."""
        test_args = ['ja', 'sort', 'age,name', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.columns == 'age,name'  # Columns as single string

    @patch('ja.cli.handle_product')
    def test_product_command(self, mock_handle):
        """Test product command parsing."""
        test_args = ['ja', 'product', 'file1.jsonl', 'file2.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.left == 'file1.jsonl'  # Arguments are 'left' and 'right'
            assert args.right == 'file2.jsonl'

    @patch('ja.cli.handle_groupby')
    def test_groupby_command(self, mock_handle):
        """Test groupby command parsing."""
        test_args = ['ja', 'groupby', 'category', 'test.jsonl', '--agg', 'sum:amount']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.key == 'category'
            assert args.agg == 'sum:amount'  # agg is a string, not a list
            assert args.file == 'test.jsonl'

    @patch('ja.cli.handle_groupby')
    def test_groupby_with_window_size(self, mock_handle):
        """Test groupby command with window size."""
        test_args = ['ja', 'groupby', 'category', 'test.jsonl', '--agg', 'sum:amount', 
                     '--window-size', '100']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.window_size == 100

    # JSONPath commands
    @patch('ja.cli.handle_select_path')
    def test_select_path_command(self, mock_handle):
        """Test select-path command parsing."""
        test_args = ['ja', 'select-path', '$.user.age', 'test.jsonl', '--predicate', 'lambda x: x > 25']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.path == '$.user.age'  # Argument is 'path'
            assert args.file == 'test.jsonl'
            assert args.predicate == 'lambda x: x > 25'

    @patch('ja.cli.handle_select_any')
    def test_select_any_command(self, mock_handle):
        """Test select-any command parsing."""
        test_args = ['ja', 'select-any', '$.items[*].inStock', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()

    @patch('ja.cli.handle_select_all')
    def test_select_all_command(self, mock_handle):
        """Test select-all command parsing."""
        test_args = ['ja', 'select-all', '$.items[*].inStock', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()

    @patch('ja.cli.handle_select_none')
    def test_select_none_command(self, mock_handle):
        """Test select-none command parsing."""
        test_args = ['ja', 'select-none', '$.items[*].error', 'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()

    @patch('ja.cli.handle_project_template')
    def test_project_template_command(self, mock_handle):
        """Test project-template command parsing."""
        test_args = ['ja', 'project-template', 
                     '{"name": "$.user.name", "total": "sum($.items[*].price)"}', 
                     'test.jsonl']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()

    def test_no_arguments_shows_help(self):
        """Test that running without arguments shows help."""
        test_args = ['ja']
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 2  # argparse exits with 2 for missing required args

    @patch('ja.cli.handle_select')
    def test_stdin_notation(self, mock_handle):
        """Test that '-' is properly passed for stdin."""
        test_args = ['ja', 'select', 'age > 25', '-']
        with patch.object(sys, 'argv', test_args):
            main()
            mock_handle.assert_called_once()
            args = mock_handle.call_args[0][0]
            assert args.file == '-'