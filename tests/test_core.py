import unittest
from ja.core import select, project, Row, Relation

class TestCoreFunctions(unittest.TestCase):

    def test_select(self):
        data: Relation = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 24},
            {"id": 3, "name": "Charlie", "age": 30},
        ]
        
        # Test selecting by age
        selected_by_age = select(data, lambda row: row.get("age") == 30)
        self.assertEqual(len(selected_by_age), 2)
        self.assertEqual(selected_by_age[0]["name"], "Alice")
        self.assertEqual(selected_by_age[1]["name"], "Charlie")

        # Test selecting by name
        selected_by_name = select(data, lambda row: row.get("name") == "Bob")
        self.assertEqual(len(selected_by_name), 1)
        self.assertEqual(selected_by_name[0]["id"], 2)

        # Test selecting with no matches
        selected_no_match = select(data, lambda row: row.get("age") == 100)
        self.assertEqual(len(selected_no_match), 0)

        # Test selecting with an empty relation
        selected_empty = select([], lambda row: row.get("age") == 30)
        self.assertEqual(len(selected_empty), 0)

    def test_project(self):
        data: Relation = [
            {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 24, "city": "London"},
        ]

        # Test projecting specific columns
        projected_name_age = project(data, ["name", "age"])
        self.assertEqual(len(projected_name_age), 2)
        self.assertEqual(projected_name_age[0], {"name": "Alice", "age": 30})
        self.assertEqual(projected_name_age[1], {"name": "Bob", "age": 24})

        # Test projecting a single column
        projected_id = project(data, ["id"])
        self.assertEqual(len(projected_id), 2)
        self.assertEqual(projected_id[0], {"id": 1})
        self.assertEqual(projected_id[1], {"id": 2})

        # Test projecting non-existent columns (should result in empty dicts for those columns)
        projected_non_existent = project(data, ["country"])
        self.assertEqual(len(projected_non_existent), 2)
        self.assertEqual(projected_non_existent[0], {})
        self.assertEqual(projected_non_existent[1], {})
        
        # Test projecting a mix of existing and non-existent columns
        projected_mixed = project(data, ["name", "country"])
        self.assertEqual(len(projected_mixed), 2)
        self.assertEqual(projected_mixed[0], {"name": "Alice"})
        self.assertEqual(projected_mixed[1], {"name": "Bob"})

        # Test projecting with an empty list of columns
        projected_empty_cols = project(data, [])
        self.assertEqual(len(projected_empty_cols), 2)
        self.assertEqual(projected_empty_cols[0], {})
        self.assertEqual(projected_empty_cols[1], {})

        # Test projecting from an empty relation
        projected_empty_relation = project([], ["name", "age"])
        self.assertEqual(len(projected_empty_relation), 0)

if __name__ == '__main__':
    unittest.main()