import unittest
import tempfile
import os
import sys
import json
from io import StringIO
from pathlib import Path
from unittest.mock import patch, MagicMock

from ja.commands import (
    read_jsonl,
    write_jsonl,
    handle_select,
    handle_project,
    handle_join,
    handle_product,
    handle_rename,
    handle_union,
    handle_intersection,
    handle_difference,
    handle_distinct,
    handle_sort,
    handle_groupby,
)


class TestCommands(unittest.TestCase):

    def setUp(self):
        """Create temporary test data files."""
        self.temp_dir = tempfile.mkdtemp()

        # Create test data
        self.test_data = [
            {"id": 1, "name": "Alice", "age": 30, "category": "A", "value": 100},
            {"id": 2, "name": "Bob", "age": 25, "category": "B", "value": 200},
            {"id": 3, "name": "Charlie", "age": 30, "category": "A", "value": 150},
        ]

        self.test_file1 = os.path.join(self.temp_dir, "data1.jsonl")
        self.test_file2 = os.path.join(self.temp_dir, "data2.jsonl")

        # Write test data to files
        with open(self.test_file1, "w") as f:
            for row in self.test_data:
                f.write(json.dumps(row) + "\n")

        # Second test file for joins/unions
        test_data2 = [
            {"user_id": 1, "order": "Book"},
            {"user_id": 2, "order": "Pen"},
            {"user_id": 3, "order": "Paper"},
        ]
        with open(self.test_file2, "w") as f:
            for row in test_data2:
                f.write(json.dumps(row) + "\n")

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_read_jsonl_from_file(self):
        """Test reading JSONL from file path."""
        result = read_jsonl(self.test_file1)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["name"], "Alice")

    def test_read_jsonl_from_pathlib(self):
        """Test reading JSONL from pathlib.Path."""
        result = read_jsonl(Path(self.test_file1))
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["name"], "Alice")

    def test_read_jsonl_from_file_object(self):
        """Test reading JSONL from file object."""
        with open(self.test_file1, "r") as f:
            result = read_jsonl(f)
            self.assertEqual(len(result), 3)
            self.assertEqual(result[0]["name"], "Alice")

    @patch("sys.stdout", new_callable=StringIO)
    def test_write_jsonl(self, mock_stdout):
        """Test writing JSONL data."""
        test_data = [{"a": 1}, {"b": 2}]
        write_jsonl(test_data)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {"a": 1})
        self.assertEqual(json.loads(lines[1]), {"b": 2})

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_select_with_file(self, mock_stdout):
        """Test handle_select with file input."""
        args = MagicMock()
        args.expr = "age > 25"
        args.file = self.test_file1

        handle_select(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)  # Alice and Charlie

        result_data = [json.loads(line) for line in lines]
        names = [row["name"] for row in result_data]
        self.assertIn("Alice", names)
        self.assertIn("Charlie", names)

    @patch("sys.stdin", StringIO('{"age": 35, "name": "Test"}\n'))
    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_select_with_stdin(self, mock_stdout):
        """Test handle_select with stdin input."""
        args = MagicMock()
        args.expr = "age > 30"
        args.file = None

        handle_select(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)

        result = json.loads(lines[0])
        self.assertEqual(result["name"], "Test")

    @patch("sys.stderr", new_callable=StringIO)
    def test_handle_select_invalid_expression(self, mock_stderr):
        """Test handle_select with invalid expression."""
        args = MagicMock()
        args.expr = "invalid python syntax !!!"
        args.file = self.test_file1

        # The function should catch the exception and print to stderr, then exit
        with patch("sys.exit") as mock_exit:
            handle_select(args)
            mock_exit.assert_called_once_with(1)

        error_output = mock_stderr.getvalue()
        self.assertIn("Invalid expression", error_output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_project_with_file(self, mock_stdout):
        """Test handle_project with file input."""
        args = MagicMock()
        args.columns = "name,age"
        args.file = self.test_file1

        handle_project(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 3)

        result = json.loads(lines[0])
        self.assertEqual(set(result.keys()), {"name", "age"})
        self.assertEqual(result["name"], "Alice")

    @patch("sys.stdin", StringIO('{"a": 1, "b": 2, "c": 3}\n'))
    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_project_with_stdin(self, mock_stdout):
        """Test handle_project with stdin input."""
        args = MagicMock()
        args.columns = "a,c"
        args.file = None

        handle_project(args)
        output = mock_stdout.getvalue()
        result = json.loads(output.strip())
        self.assertEqual(result, {"a": 1, "c": 3})

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_join(self, mock_stdout):
        """Test handle_join functionality."""
        args = MagicMock()
        args.left = self.test_file1
        args.right = self.test_file2
        args.on = "id=user_id"

        handle_join(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 3)

        result = json.loads(lines[0])
        self.assertIn("name", result)
        self.assertIn("order", result)

    @patch("sys.stdin", StringIO('{"id": 1, "name": "Test"}\n'))
    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_join_with_stdin_left(self, mock_stdout):
        """Test handle_join with stdin as left input."""
        args = MagicMock()
        args.left = "-"
        args.right = self.test_file2
        args.on = "id=user_id"

        handle_join(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_product(self, mock_stdout):
        """Test handle_product functionality."""
        args = MagicMock()
        args.left = self.test_file1
        args.right = self.test_file2

        handle_product(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        # Should be 3 * 3 = 9 combinations
        self.assertEqual(len(lines), 9)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_rename_with_file(self, mock_stdout):
        """Test handle_rename with file input."""
        args = MagicMock()
        args.mapping = "name=full_name,age=years"
        args.file = self.test_file1

        handle_rename(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        result = json.loads(lines[0])

        self.assertIn("full_name", result)
        self.assertIn("years", result)
        self.assertNotIn("name", result)
        self.assertNotIn("age", result)

    @patch("sys.stdin", StringIO('{"old_name": "value"}\n'))
    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_rename_with_stdin(self, mock_stdout):
        """Test handle_rename with stdin input."""
        args = MagicMock()
        args.mapping = "old_name=new_name"
        args.file = None

        handle_rename(args)
        output = mock_stdout.getvalue()
        result = json.loads(output.strip())
        self.assertEqual(result, {"new_name": "value"})

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_union(self, mock_stdout):
        """Test handle_union functionality."""
        args = MagicMock()
        args.left = self.test_file1
        args.right = self.test_file2

        handle_union(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        # Should be 3 + 3 = 6 rows
        self.assertEqual(len(lines), 6)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_intersection(self, mock_stdout):
        """Test handle_intersection functionality."""
        # Create two files with some common data
        common_data = [{"id": 1, "name": "Alice"}]
        temp_file3 = os.path.join(self.temp_dir, "common.jsonl")
        with open(temp_file3, "w") as f:
            for row in common_data:
                f.write(json.dumps(row) + "\n")

        args = MagicMock()
        args.left = self.test_file1
        args.right = temp_file3

        handle_intersection(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_difference(self, mock_stdout):
        """Test handle_difference functionality."""
        # Create a file with subset of data
        subset_data = [
            {"id": 1, "name": "Alice", "age": 30, "category": "A", "value": 100}
        ]
        temp_file3 = os.path.join(self.temp_dir, "subset.jsonl")
        with open(temp_file3, "w") as f:
            for row in subset_data:
                f.write(json.dumps(row) + "\n")

        args = MagicMock()
        args.left = self.test_file1
        args.right = temp_file3

        handle_difference(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)  # Bob and Charlie should remain

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_distinct_with_file(self, mock_stdout):
        """Test handle_distinct with file input."""
        # Create file with duplicates
        duplicate_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice"},  # duplicate
        ]
        temp_file3 = os.path.join(self.temp_dir, "duplicates.jsonl")
        with open(temp_file3, "w") as f:
            for row in duplicate_data:
                f.write(json.dumps(row) + "\n")

        args = MagicMock()
        args.file = temp_file3

        handle_distinct(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)  # Should have only unique rows

    @patch("sys.stdin", StringIO('{"a": 1}\n{"b": 2}\n{"a": 1}\n'))
    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_distinct_with_stdin(self, mock_stdout):
        """Test handle_distinct with stdin input."""
        args = MagicMock()
        args.file = None

        handle_distinct(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_sort_with_file(self, mock_stdout):
        """Test handle_sort with file input."""
        args = MagicMock()
        args.columns = "age,name"
        args.file = self.test_file1

        handle_sort(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        results = [json.loads(line) for line in lines]

        # Should be sorted by age, then name
        self.assertEqual(results[0]["name"], "Bob")  # age 25
        self.assertEqual(results[1]["name"], "Alice")  # age 30, name Alice
        self.assertEqual(results[2]["name"], "Charlie")  # age 30, name Charlie

    @patch("sys.stdin", StringIO('{"value": 3}\n{"value": 1}\n{"value": 2}\n'))
    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_sort_with_stdin(self, mock_stdout):
        """Test handle_sort with stdin input."""
        args = MagicMock()
        args.columns = "value"
        args.file = None

        handle_sort(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        results = [json.loads(line) for line in lines]

        values = [row["value"] for row in results]
        self.assertEqual(values, [1, 2, 3])

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_groupby_with_file(self, mock_stdout):
        """Test handle_groupby with file input."""
        args = MagicMock()
        args.key = "category"
        args.agg = "count,sum:value"
        args.file = self.test_file1

        handle_groupby(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)  # Two categories: A and B

        results = [json.loads(line) for line in lines]
        results_by_cat = {row["category"]: row for row in results}

        self.assertIn("A", results_by_cat)
        self.assertIn("B", results_by_cat)
        self.assertEqual(results_by_cat["A"]["count"], 2)
        self.assertEqual(results_by_cat["B"]["count"], 1)

    @patch(
        "sys.stdin",
        StringIO(
            '{"group": "X", "val": 10}\n{"group": "Y", "val": 20}\n{"group": "X", "val": 15}\n'
        ),
    )
    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_groupby_with_stdin(self, mock_stdout):
        """Test handle_groupby with stdin input."""
        args = MagicMock()
        args.key = "group"
        args.agg = "count,avg:val"
        args.file = None

        handle_groupby(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 2)

        results = [json.loads(line) for line in lines]
        results_by_group = {row["group"]: row for row in results}

        self.assertEqual(results_by_group["X"]["count"], 2)
        self.assertEqual(results_by_group["X"]["avg_val"], 12.5)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_groupby_no_colon_in_agg(self, mock_stdout):
        """Test handle_groupby with aggregation function without column specification."""
        args = MagicMock()
        args.key = "category"
        args.agg = "count"  # No colon, should use empty string as field
        args.file = self.test_file1

        handle_groupby(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        results = [json.loads(line) for line in lines]

        # Should have count column
        for result in results:
            self.assertIn("count", result)

    @patch("sys.stdout", new_callable=StringIO)
    def test_handle_groupby_multiple_aggs(self, mock_stdout):
        """Test handle_groupby with multiple aggregations."""
        args = MagicMock()
        args.key = "category"
        args.agg = "count,sum:value,avg:age,min:value,max:age"
        args.file = self.test_file1

        handle_groupby(args)
        output = mock_stdout.getvalue()
        lines = output.strip().split("\n")
        results = [json.loads(line) for line in lines]

        # Check that all aggregation columns are present
        for result in results:
            self.assertIn("count", result)
            self.assertIn("sum_value", result)
            self.assertIn("avg_age", result)
            self.assertIn("min_value", result)
            self.assertIn("max_age", result)


if __name__ == "__main__":
    unittest.main()
