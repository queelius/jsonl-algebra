import unittest
import os
from unittest.mock import patch, mock_open
from ja.importer import csv_to_jsonl_lines, dir_to_jsonl_lines
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

    def test_dir_to_jsonl_lines(self):
        dir_path = 'fake_dir'
        files = ['a.json', 'b.jsonl', 'c.txt']
        file_contents = {
            os.path.join(dir_path, 'a.json'): '{"id": 1, "data": "json"}',
            os.path.join(dir_path, 'b.jsonl'): '{"id": 2, "data": "jsonl1"}\n{"id": 3, "data": "jsonl2"}',
        }

        def mock_open_side_effect(path, mode='r'):
            if path in file_contents:
                return mock_open(read_data=file_contents[path])()
            return mock_open()()

        with patch('os.listdir', return_value=files):
            with patch('builtins.open', side_effect=mock_open_side_effect):
                lines = list(dir_to_jsonl_lines(dir_path))
        
        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[0], '{"id": 1, "data": "json"}')
        self.assertEqual(lines[1], '{"id": 2, "data": "jsonl1"}')
        self.assertEqual(lines[2], '{"id": 3, "data": "jsonl2"}')

if __name__ == '__main__':
    unittest.main()
