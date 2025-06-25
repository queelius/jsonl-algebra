import unittest
from unittest.mock import patch
import subprocess
import sys
import os

class TestCli(unittest.TestCase):
    def setUp(self):
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    def test_help(self):
        result = subprocess.run(
            [sys.executable, '-m', 'ja.cli', '--help'],
            capture_output=True, text=True, cwd=self.project_root
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn('usage: ja', result.stdout)

    def test_select_command(self):
        input_data = '{"a": 1}\n{"a": 2}'
        expression = 'a == `1`'
        result = subprocess.run(
            [sys.executable, '-m', 'ja.cli', 'select', expression],
            input=input_data,
            capture_output=True,
            text=True,
            cwd=self.project_root
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), '{"a": 1}')
