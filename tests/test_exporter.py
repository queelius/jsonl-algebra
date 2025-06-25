import unittest
import io
import json
from ja.exporter import jsonl_to_csv_stream

class TestExporter(unittest.TestCase):

    def test_simple_flattening_and_header_discovery(self):
        jsonl_data = [
            {'id': 1, 'user': {'name': 'Alice', 'location': {'city': 'New York'}}, 'status': 'active'},
            {'id': 2, 'user': {'name': 'Bob'}, 'tags': ['dev', 'test']}
        ]
        jsonl_stream = io.StringIO("\n".join(json.dumps(d) for d in jsonl_data))
        output_stream = io.StringIO()

        jsonl_to_csv_stream(jsonl_stream, output_stream)
        
        result = output_stream.getvalue().strip().split('\n')
        self.assertEqual(len(result), 3) # Header + 2 rows
        # Note: The order of headers depends on discovery order, so we check the set of headers.
        self.assertEqual(set(result[0].strip().split(',')), {'id', 'user.name', 'user.location.city', 'status', 'tags'})
        
        # Check one of the rows for content
        # This is tricky without knowing the exact header order, so let's parse it back
        import csv
        reader = csv.DictReader(io.StringIO(output_stream.getvalue()))
        rows = list(reader)
        self.assertEqual(rows[0]['id'], '1')
        self.assertEqual(rows[0]['user.name'], 'Alice')
        self.assertEqual(rows[0]['user.location.city'], 'New York')
        self.assertEqual(rows[0]['status'], 'active')
        self.assertEqual(rows[1]['tags'], '["dev", "test"]') # Check array serialization

    def test_custom_separator(self):
        jsonl_data = [{'a': {'b': 1}}]
        jsonl_stream = io.StringIO(json.dumps(jsonl_data[0]))
        output_stream = io.StringIO()

        jsonl_to_csv_stream(jsonl_stream, output_stream, flatten_sep='_')
        result = output_stream.getvalue().strip()
        self.assertIn('a_b', result)
        self.assertIn('1', result)

    def test_no_flatten(self):
        jsonl_data = [{'id': 1, 'user': {'name': 'Alice'}}]
        jsonl_stream = io.StringIO(json.dumps(jsonl_data[0]))
        output_stream = io.StringIO()

        jsonl_to_csv_stream(jsonl_stream, output_stream, flatten=False)
        result = output_stream.getvalue().strip().split('\n')
        self.assertEqual(set(result[0].strip().split(',')), {'id', 'user'})
        
        import csv
        reader = csv.DictReader(io.StringIO(output_stream.getvalue()))
        rows = list(reader)
        self.assertEqual(rows[0]['user'], json.dumps({'name': 'Alice'}))

    def test_empty_input(self):
        jsonl_stream = io.StringIO("")
        output_stream = io.StringIO()
        jsonl_to_csv_stream(jsonl_stream, output_stream)
        self.assertEqual(output_stream.getvalue(), "")

if __name__ == '__main__':
    unittest.main()
