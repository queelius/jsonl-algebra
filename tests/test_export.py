import unittest
import json
import os
import shutil
from pathlib import Path
from ja.export import (
    jsonl_to_json_array_string,
    json_array_to_jsonl_lines,
    jsonl_to_dir,
    dir_to_jsonl,
)

class TestExport(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path('test_export_temp_dir')
        self.test_dir.mkdir(exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_jsonl_to_json_array_string(self):
        jsonl_input = ['{"a": 1}\n', '{"b": 2}\n']
        result = jsonl_to_json_array_string(jsonl_input)
        self.assertEqual(json.loads(result), [{ "a": 1 }, { "b": 2 }])

    def test_json_array_to_jsonl_lines(self):
        json_array_input = '[{"a": 1}, {"b": 2}]'
        lines = list(json_array_to_jsonl_lines(json_array_input.splitlines()))
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {"a": 1})
        self.assertEqual(json.loads(lines[1]), {"b": 2})

    def test_jsonl_to_dir_and_back(self):
        jsonl_input = ['{"a": 1}\n', '{"b": 2}\n']
        output_dir = self.test_dir / 'output'
        jsonl_to_dir(jsonl_input, str(output_dir))

        lines = list(dir_to_jsonl(str(output_dir)))
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {"a": 1})
        self.assertEqual(json.loads(lines[1]), {"b": 2})

    def test_jsonl_to_json_array_string_with_invalid_json(self):
        jsonl_input = ['{"a": 1}\n', 'not a json line\n', '{"b": 2}\n']
        result = jsonl_to_json_array_string(jsonl_input)
        self.assertEqual(json.loads(result), [{ "a": 1 }, { "b": 2 }])

    def test_json_array_to_jsonl_lines_with_invalid_json(self):
        json_array_input = '[{"a": 1}, not-a-json, {"b": 2}]'
        with self.assertRaises(ValueError):
            list(json_array_to_jsonl_lines(json_array_input.splitlines()))

    def test_json_array_to_jsonl_lines_with_non_array(self):
        json_array_input = '{"a": 1}'
        with self.assertRaises(ValueError):
            list(json_array_to_jsonl_lines(json_array_input.splitlines()))

    def test_dir_to_jsonl_with_add_filename(self):
        (self.test_dir / "file1.json").write_text('{"a": 1}')
        (self.test_dir / "file2.json").write_text('{"b": 2}')
        lines = list(dir_to_jsonl(str(self.test_dir), add_filename_key='fname'))
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0]), {"a": 1, "fname": "file1.json"})
        self.assertEqual(json.loads(lines[1]), {"b": 2, "fname": "file2.json"})

    def test_dir_to_jsonl_recursive(self):
        sub_dir = self.test_dir / 'sub'
        sub_dir.mkdir()
        (self.test_dir / "file1.json").write_text('{"a": 1}')
        (sub_dir / "file2.json").write_text('{"b": 2}')
        lines = list(dir_to_jsonl(str(self.test_dir), recursive=True))
        self.assertEqual(len(lines), 2)
        # Order is not guaranteed, so check for presence
        results = [json.loads(l) for l in lines]
        self.assertIn({"a": 1}, results)
        self.assertIn({"b": 2}, results)

    def test_dir_to_jsonl_with_invalid_file(self):
        (self.test_dir / "good.json").write_text('{"a": 1}')
        (self.test_dir / "bad.json").write_text('not json')
        lines = list(dir_to_jsonl(str(self.test_dir)))
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0]), {"a": 1})
