import unittest
from unittest.mock import patch, mock_open
from ja.importer import csv_to_jsonl_lines
import json

class TestImporter(unittest.TestCase):

    def test_csv_to_jsonl_with_header(self):
        csv_data = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles"
        mock_stream = csv_data.splitlines()
        result = list(csv_to_jsonl_lines(mock_stream, has_header=True))
        self.assertEqual(len(result), 2)
        self.assertEqual(json.loads(result[0]), {"name": "Alice", "age": "30", "city": "New York"})
        self.assertEqual(json.loads(result[1]), {"name": "Bob", "age": "25", "city": "Los Angeles"})

    def test_csv_to_jsonl_no_header(self):
        csv_data = "Alice,30,New York\nBob,25,Los Angeles"
        mock_stream = csv_data.splitlines()
        result = list(csv_to_jsonl_lines(mock_stream, has_header=False))
        self.assertEqual(len(result), 2)
        self.assertEqual(json.loads(result[0]), {"col_0": "Alice", "col_1": "30", "col_2": "New York"})
        self.assertEqual(json.loads(result[1]), {"col_0": "Bob", "col_1": "25", "col_2": "Los Angeles"})

    def test_csv_to_jsonl_empty_input(self):
        csv_data = ""
        mock_stream = csv_data.splitlines()
        result = list(csv_to_jsonl_lines(mock_stream, has_header=True))
        self.assertEqual(len(result), 0)

    def test_csv_to_jsonl_empty_input_no_header(self):
        csv_data = ""
        mock_stream = csv_data.splitlines()
        result = list(csv_to_jsonl_lines(mock_stream, has_header=False))
        self.assertEqual(len(result), 0)

    def test_csv_to_jsonl_with_type_inference(self):
        csv_data = "id,score,is_active,name,notes\n1,95.5,true,Alice,\n2,80,false,Bob,null"
        mock_stream = csv_data.splitlines()
        result = list(csv_to_jsonl_lines(mock_stream, has_header=True, infer_types=True))
        self.assertEqual(len(result), 2)
        self.assertEqual(json.loads(result[0]), {"id": 1, "score": 95.5, "is_active": True, "name": "Alice", "notes": None})
        self.assertEqual(json.loads(result[1]), {"id": 2, "score": 80, "is_active": False, "name": "Bob", "notes": None})

if __name__ == '__main__':
    unittest.main()
