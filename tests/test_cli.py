import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


class TestCli(unittest.TestCase):
    def setUp(self):
        self.project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..")
        )

    def test_help(self):
        result = subprocess.run(
            [sys.executable, "-m", "ja.cli", "--help"],
            capture_output=True,
            text=True,
            cwd=self.project_root,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("usage: ja", result.stdout)

    def test_select_command(self):
        input_data = '{"a": 1}\n{"a": 2}'
        expression = "a == `1`"
        result = subprocess.run(
            [sys.executable, "-m", "ja.cli", "select", expression],
            input=input_data,
            capture_output=True,
            text=True,
            cwd=self.project_root,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), '{"a": 1}')

    def test_validate_command(self):
        schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
            "required": ["name", "age"],
        }

        valid_data = '{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}'
        invalid_data = '{"name": "Alice", "age": "thirty"}\n{"name": "Bob"}'

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        ) as schema_file:
            json.dump(schema, schema_file)
            schema_filename = schema_file.name

        # Test valid data
        result_valid = subprocess.run(
            [sys.executable, "-m", "ja.cli", "schema", "validate", schema_filename],
            input=valid_data,
            capture_output=True,
            text=True,
            cwd=self.project_root,
        )
        self.assertEqual(result_valid.returncode, 0)
        self.assertEqual(result_valid.stdout.strip(), valid_data.strip())

        # Test invalid data
        result_invalid = subprocess.run(
            [sys.executable, "-m", "ja.cli", "schema", "validate", schema_filename],
            input=invalid_data,
            capture_output=True,
            text=True,
            cwd=self.project_root,
        )
        self.assertEqual(
            result_invalid.returncode, 1
        )  # The tool should exit with 1 on validation failure
        self.assertIn("Validation error on line 1", result_invalid.stderr)
        self.assertIn("Validation error on line 2", result_invalid.stderr)

        os.remove(schema_filename)
