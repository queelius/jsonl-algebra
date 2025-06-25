import unittest
from ja.schema import infer_schema, _get_type_name, _schema_to_string_for_union, _merge_schemas

class TestSchemaHelpers(unittest.TestCase):

    def test_get_type_name(self):
        self.assertEqual(_get_type_name("hello"), "string")
        self.assertEqual(_get_type_name(123), "integer")
        self.assertEqual(_get_type_name(123.45), "float")
        self.assertEqual(_get_type_name(True), "boolean")
        self.assertEqual(_get_type_name(None), "null")
        self.assertEqual(_get_type_name([]), "array")
        self.assertEqual(_get_type_name({}), "object")

    def test_schema_to_string_for_union(self):
        self.assertEqual(_schema_to_string_for_union("string"), "string")
        self.assertEqual(_schema_to_string_for_union({"type": "object"}), "object")
        self.assertEqual(_schema_to_string_for_union({"type": "array", "items": "string"}), "array[string]")
        self.assertEqual(_schema_to_string_for_union({"type": "array", "items": {"type": "integer"}}), "array[complex_schema_dict]")

    def test_merge_schemas(self):
        self.assertEqual(_merge_schemas("string", "integer"), "integer | string")
        self.assertEqual(_merge_schemas("string", "string"), "string")
        self.assertEqual(_merge_schemas(None, "string"), "string")
        self.assertEqual(_merge_schemas({"type": "object", "properties": {"a": "string"}}, {"type": "object", "properties": {"b": "integer"}}), {'type': 'object', 'properties': {'a': 'string', 'b': 'integer'}})
        self.assertEqual(_merge_schemas({"type": "array", "items": "string"}, {"type": "array", "items": "integer"}), {'type': 'array', 'items': 'integer | string'})
        self.assertEqual(_merge_schemas({"type": "object"}, "string"), "object | string")

class TestSchema(unittest.TestCase):

    def test_infer_schema_simple(self):
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25, "city": "New York"}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            'type': 'object',
            'properties': {
                'name': 'string',
                'age': 'integer',
                'city': 'string'
            }
        })

    def test_infer_schema_nested(self):
        data = [
            {"user": {"name": "Alice", "id": 1}, "posts": [{"title": "Post 1", "likes": 10}]},
            {"user": {"name": "Bob", "id": 2}, "posts": [{"title": "Post 2", "likes": 20, "tags": ["a", "b"]}]}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            'type': 'object',
            'properties': {
                'user': {
                    'type': 'object',
                    'properties': {
                        'name': 'string',
                        'id': 'integer'
                    }
                },
                'posts': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'title': 'string',
                            'likes': 'integer',
                            'tags': {
                                'type': 'array',
                                'items': 'string'
                            }
                        }
                    }
                }
            }
        })

    def test_infer_schema_type_union(self):
        data = [
            {"value": 10},
            {"value": "hello"},
            {"value": 15.5}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            'type': 'object',
            'properties': {
                'value': 'float | integer | string'
            }
        })

    def test_infer_schema_empty_list(self):
        data = []
        schema = infer_schema(data)
        self.assertEqual(schema, {'type': 'object', 'properties': {}})

    def test_infer_schema_list_with_empty_dict(self):
        data = [{}]
        schema = infer_schema(data)
        self.assertEqual(schema, {'type': 'object', 'properties': {}})

    def test_infer_schema_with_nulls(self):
        data = [
            {"a": 1, "b": None},
            {"a": None, "b": "hello"}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            'type': 'object',
            'properties': {
                'a': 'integer | null',
                'b': 'null | string'
            }
        })

    def test_infer_schema_array_of_mixed_types(self):
        data = [{"mixed_array": [1, "a", True, 1.2, None]}]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            'type': 'object',
            'properties': {
                'mixed_array': {
                    'type': 'array',
                    'items': 'boolean | float | integer | null | string'
                }
            }
        })

    def test_infer_schema_complex_union(self):
        data = [
            {"data": {"a": 1}},
            {"data": [1, 2]},
            {"data": "string"}
        ]
        schema = infer_schema(data)
        self.assertEqual(schema, {
            'type': 'object',
            'properties': {
                'data': 'array[integer] | object | string'
            }
        })

if __name__ == '__main__':
    unittest.main()
