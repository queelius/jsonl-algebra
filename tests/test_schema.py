import unittest
from ja.schema import infer_schema


class TestSchema(unittest.TestCase):

    def test_infer_schema_simple(self):
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25, "city": "New York"}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'name': {'type': 'string'},
                'age': {'type': 'integer'},
                'city': {'type': 'string'}
            },
            'required': ['age', 'name']
        })

    def test_infer_schema_nested(self):
        data = [
            {"user": {"name": "Alice", "id": 1}, "posts": [{"title": "Post 1", "likes": 10}]},
            {"user": {"name": "Bob", "id": 2}, "posts": [{"title": "Post 2", "likes": 20, "tags": ["a", "b"]}]}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'user': {
                    'type': 'object',
                    'properties': {
                        'name': {'type': 'string'},
                        'id': {'type': 'integer'}
                    },
                    'required': ['id', 'name']
                },
                'posts': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'title': {'type': 'string'},
                            'likes': {'type': 'integer'},
                            'tags': {
                                'type': 'array',
                                'items': {'type': 'string'}
                            }
                        },
                        'required': ['likes', 'title']
                    }
                }
            },
            'required': ['posts', 'user']
        })

    def test_infer_schema_type_union(self):
        data = [
            {"value": 10},
            {"value": "hello"},
            {"value": 15.5}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'value': {'type': ['number', 'string']}
            },
            'required': ['value']
        })

    def test_infer_schema_empty_list(self):
        data = []
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {}
        })

    def test_infer_schema_list_with_empty_dict(self):
        data = [{}]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {}
        })

    def test_infer_schema_with_nulls(self):
        data = [
            {"a": 1, "b": None},
            {"a": None, "b": "hello"}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'a': {'type': ['integer', 'null']},
                'b': {'type': ['null', 'string']}
            },
            'required': ['a', 'b']
        })

    def test_infer_schema_array_of_mixed_types(self):
        data = [{"mixed_array": [1, "a", True, 1.2, None]}]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'mixed_array': {
                    'type': 'array',
                    'items': {'type': ['boolean', 'null', 'number', 'string']}
                }
            },
            'required': ['mixed_array']
        })

    def test_infer_schema_complex_union(self):
        data = [
            {"data": {"a": 1}},
            {"data": [1, 2]},
            {"data": "string"}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'data': {'type': ['array', 'object', 'string']}
            },
            'required': ['data']
        })

if __name__ == '__main__':
    unittest.main()
