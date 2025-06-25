import unittest
from ja.core import (
    select, project, join, rename, union, difference,
    distinct, intersection, sort_by, product,
    Row, Relation, _row_to_hashable_key
)

from ja.groupby import groupby_agg

class TestCoreFunctions(unittest.TestCase):

    def test_select(self):
        data: Relation = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 24},
            {"id": 3, "name": "Charlie", "age": 30},
        ]
        
        # Test selecting by age
        selected_by_age = select(data, "age == `30`")
        self.assertEqual(len(selected_by_age), 2)
        self.assertEqual(selected_by_age[0], {"id": 1, "name": "Alice", "age": 30})
        self.assertEqual(selected_by_age[1], {"id": 3, "name": "Charlie", "age": 30})

        # Test selecting by name
        selected_by_name = select(data, "name == 'Bob'")
        self.assertEqual(len(selected_by_name), 1)
        self.assertEqual(selected_by_name[0], {"id": 2, "name": "Bob", "age": 24})

        # Test selecting with no matches
        selected_no_match = select(data, "age == `100`")
        self.assertEqual(len(selected_no_match), 0)

        # Test selecting with an empty relation
        selected_empty = select([], "age == `30`")
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

    def test_row_to_hashable_key(self):
        row1 = {"a": 1, "b": "hello"}
        row2 = {"b": "hello", "a": 1}
        row3 = {"a": 1, "b": "world"}
        self.assertEqual(_row_to_hashable_key(row1), _row_to_hashable_key(row2))
        self.assertNotEqual(_row_to_hashable_key(row1), _row_to_hashable_key(row3))
        with self.assertRaises(TypeError):
            _row_to_hashable_key({"a": 1, "b": [1, 2]}) # list is unhashable

    def test_join_basic(self):
        left: Relation = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        right: Relation = [
            {"user_id": 1, "order": "Book"},
            {"user_id": 2, "order": "Pen"},
            {"user_id": 1, "order": "Paper"},
        ]
        
        joined_data = join(left, right, [("id", "user_id")])
        self.assertEqual(len(joined_data), 3)
        expected_results = [
            {"id": 1, "name": "Alice", "order": "Book"},
            {"id": 1, "name": "Alice", "order": "Paper"},
            {"id": 2, "name": "Bob", "order": "Pen"},
        ]
        for res in expected_results:
            self.assertIn(res, joined_data)


    def test_join_no_matches(self):
        left: Relation = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        right: Relation = [{"user_id": 4, "order": "Eraser"}]
        no_match_join = join(left, right, [("id", "user_id")])
        self.assertEqual(len(no_match_join), 0)

    def test_join_empty_left_relation(self):
        right: Relation = [
            {"user_id": 1, "order": "Book"},
        ]
        empty_left_join = join([], right, [("id", "user_id")])
        self.assertEqual(len(empty_left_join), 0)

    def test_join_empty_right_relation(self):
        left: Relation = [
            {"id": 1, "name": "Alice"},
        ]
        empty_right_join = join(left, [], [("id", "user_id")])
        self.assertEqual(len(empty_right_join), 0)
        
    def test_join_column_collision(self):
        # Test join with column name collision (right column not part of 'on' should be excluded)
        left_collision: Relation = [{"id": 1, "name": "Alice", "detail": "L_detail"}]
        right_collision: Relation = [{"user_id": 1, "name": "Alicia", "detail": "R_detail"}] # 'name' collides
        joined_collision = join(left_collision, right_collision, [("id", "user_id")])
        self.assertEqual(len(joined_collision), 1)
        self.assertEqual(joined_collision[0], {"id": 1, "name": "Alice", "detail": "L_detail"})


    def test_rename(self):
        data: Relation = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 24},
        ]
        renamed_data = rename(data, {"id": "user_id", "age": "years"})
        self.assertEqual(len(renamed_data), 2)
        self.assertEqual(renamed_data[0], {"user_id": 1, "name": "Alice", "years": 30})
        self.assertEqual(renamed_data[1], {"user_id": 2, "name": "Bob", "years": 24})

        # Test renaming non-existent column (should be a no-op for that column)
        renamed_non_existent = rename(data, {"city": "location"})
        self.assertEqual(renamed_non_existent, data)

        # Test renaming with empty mapping
        renamed_empty_map = rename(data, {})
        self.assertEqual(renamed_empty_map, data)
        
        # Test renaming on empty relation
        renamed_empty_relation = rename([], {"id": "user_id"})
        self.assertEqual(len(renamed_empty_relation), 0)

    def test_union(self):
        r1: Relation = [{"id": 1}, {"id": 2}]
        r2: Relation = [{"id": 2}, {"id": 3}]
        unioned = union(r1, r2)
        self.assertEqual(len(unioned), 4)
        self.assertEqual(unioned, [{"id": 1}, {"id": 2}, {"id": 2}, {"id": 3}])

        # Test union with empty relations
        self.assertEqual(union(r1, []), r1)
        self.assertEqual(union([], r2), r2)
        self.assertEqual(union([], []), [])

    def test_difference(self):
        r1: Relation = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}]
        r2: Relation = [{"id": 2, "name": "B"}, {"id": 4, "name": "D"}]
        diff = difference(r1, r2)
        self.assertEqual(len(diff), 2)
        self.assertIn({"id": 1, "name": "A"}, diff)
        self.assertIn({"id": 3, "name": "C"}, diff)

        # Test difference with no common elements
        r3: Relation = [{"id": 5, "name": "E"}]
        self.assertEqual(difference(r1, r3), r1)

        # Test difference where r2 is a superset
        self.assertEqual(difference(r2, r1), [{"id": 4, "name": "D"}])
        
        # Test difference with empty relations
        self.assertEqual(difference(r1, []), r1)
        self.assertEqual(difference([], r1), [])
        self.assertEqual(difference([], []), [])

    def test_distinct(self):
        data: Relation = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice"}, # duplicate
            {"id": 3, "name": "Charlie"},
        ]
        distinct_data = distinct(data)
        self.assertEqual(len(distinct_data), 3)
        self.assertIn({"id": 1, "name": "Alice"}, distinct_data)
        self.assertIn({"id": 2, "name": "Bob"}, distinct_data)
        self.assertIn({"id": 3, "name": "Charlie"}, distinct_data)
        
        # Test distinct on already distinct data
        already_distinct: Relation = [{"id": 1}, {"id": 2}]
        self.assertEqual(len(distinct(already_distinct)), 2)

        # Test distinct on empty relation
        self.assertEqual(distinct([]), [])
        
        # Test distinct preserves order of first appearance
        ordered_data: Relation = [
            {"a":1}, {"b":2}, {"a":1}, {"c":3}, {"b":2}
        ]
        expected_ordered_distinct: Relation = [{"a":1}, {"b":2}, {"c":3}]
        self.assertEqual(distinct(ordered_data), expected_ordered_distinct)


    def test_intersection(self):
        r1: Relation = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}]
        r2: Relation = [{"id": 2, "name": "B"}, {"id": 4, "name": "D"}, {"id": 3, "name": "C"}]
        intersected = intersection(r1, r2)
        self.assertEqual(len(intersected), 2)
        self.assertIn({"id": 2, "name": "B"}, intersected)
        self.assertIn({"id": 3, "name": "C"}, intersected)

        # Test intersection with no common elements
        r3: Relation = [{"id": 5, "name": "E"}]
        self.assertEqual(intersection(r1, r3), [])

        # Test intersection with empty relations
        self.assertEqual(intersection(r1, []), [])
        self.assertEqual(intersection([], r1), [])
        self.assertEqual(intersection([], []), [])

    def test_sort_by(self):
        data: Relation = [
            {"name": "Charlie", "age": 30},
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Alice", "age": 20},
        ]
        
        # Sort by name
        sorted_by_name = sort_by(data, ["name"])
        self.assertEqual([r["name"] for r in sorted_by_name], ["Alice", "Alice", "Bob", "Charlie"])
        
        # Sort by age, then name
        sorted_by_age_name = sort_by(data, ["age", "name"])
        expected_age_name: Relation = [
            {"name": "Alice", "age": 20},
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 30},
        ]
        self.assertEqual(sorted_by_age_name, expected_age_name)

    def test_sort_by_empty(self):
        # Sort empty relation
        self.assertEqual(sort_by([], ["name"]), [])
 
    def test_sort_by_nonexistent_column(self):
        # Sort by non-existent key (should treat as None, typically sorting first)
        # The exact behavior of None depends on Python's sort, usually None < any value
        data_with_none: Relation = [
            {"id": 1, "val": 10},
            {"id": 2}, # val is None
            {"id": 3, "val": 5}
        ]
        sorted_by_val = sort_by(data_with_none, ["val"])
        # Assuming None values come first
        self.assertEqual([r.get("id") for r in sorted_by_val], [2, 3, 1])

    def test_product(self):
        r1: Relation = [{"id": 1, "val": "A"}, {"id": 2, "val": "B"}]
        r2: Relation = [{"count": 10}, {"count": 20}]
        
        prod = product(r1, r2)
        self.assertEqual(len(prod), 4)
        expected_prod_results = [
            {"id": 1, "val": "A", "count": 10},
            {"id": 1, "val": "A", "count": 20},
            {"id": 2, "val": "B", "count": 10},
            {"id": 2, "val": "B", "count": 20},
        ]
        for res in expected_prod_results:
            self.assertIn(res, prod)

        # Test product with empty relation
        self.assertEqual(product(r1, []), [])
        self.assertEqual(product([], r2), [])
        self.assertEqual(product([], []), [])

        # Test product with key collision
        r_collide1: Relation = [{"id": 1, "name": "X"}]
        r_collide2: Relation = [{"id": 10, "name": "Y"}] # 'name' will collide
        prod_collide = product(r_collide1, r_collide2)
        self.assertEqual(len(prod_collide), 1)
        self.assertEqual(prod_collide[0], {"id": 1, "name": "X", "b_id": 10, "b_name": "Y"})


    def test_groupby_agg_basic(self):
        data: Relation = [
            {"category": "A", "amount": 10, "value": 100},
            {"category": "B", "amount": 20, "value": 200},
            {"category": "A", "amount": 15, "value": 150},
            {"category": "B", "amount": 25, "value": 250},
            {"category": "A", "amount": 10}, # missing 'value'
            {"category": "C", "amount": 30, "value": 300},
        ]
        aggs = [("count", ""), ("sum", "amount"), ("avg", "value"), ("min", "amount"), ("max", "value")]
        grouped = groupby_agg(data, "category", aggs)
        grouped_dict = {g["category"]: g for g in grouped}

        self.assertEqual(len(grouped), 3)
        
        # Category A
        self.assertIn("A", grouped_dict)
        cat_a = grouped_dict["A"]
        self.assertEqual(cat_a["count"], 3)
        self.assertEqual(cat_a["sum_amount"], 10 + 15 + 10)
        self.assertEqual(cat_a["avg_value"], (100 + 150) / 2) # Only 2 'value' entries for A
        self.assertEqual(cat_a["min_amount"], 10)
        self.assertEqual(cat_a["max_value"], 150)

        # Category B
        self.assertIn("B", grouped_dict)
        cat_b = grouped_dict["B"]
        self.assertEqual(cat_b["count"], 2)
        self.assertEqual(cat_b["sum_amount"], 20 + 25)
        self.assertEqual(cat_b["avg_value"], (200 + 250) / 2)
        self.assertEqual(cat_b["min_amount"], 20)
        self.assertEqual(cat_b["max_value"], 250)

        # Category C
        self.assertIn("C", grouped_dict)
        cat_c = grouped_dict["C"]
        self.assertEqual(cat_c["count"], 1)
        self.assertEqual(cat_c["sum_amount"], 30)
        self.assertEqual(cat_c["avg_value"], 300 / 1)
        self.assertEqual(cat_c["min_amount"], 30)
        self.assertEqual(cat_c["max_value"], 300)

    def test_groupby_agg_empty_relation(self):
        self.assertEqual(groupby_agg([], "category", [("count", "")]), [])

    def test_groupby_agg_unsupported_function(self):
        data: Relation = [{"category": "A", "amount": 10}]
        with self.assertRaises(ValueError):
            groupby_agg(data, "category", [("unknown_func", "amount")])
            
    def test_groupby_agg_non_numeric_aggregation(self):
        # Test aggregation on non-numeric field for sum/avg/min/max
        # Current implementation converts to float, so non-convertible would raise ValueError
        data_non_numeric: Relation = [{"key": "X", "val": "abc"}]
        with self.assertRaises(ValueError): # or TypeError depending on float conversion
             groupby_agg(data_non_numeric, "key", [("sum", "val")])
        with self.assertRaises(ValueError):
             groupby_agg(data_non_numeric, "key", [("avg", "val")])
        with self.assertRaises(ValueError):
             groupby_agg(data_non_numeric, "key", [("min", "val")])
        with self.assertRaises(ValueError):
             groupby_agg(data_non_numeric, "key", [("max", "val")])


    def test_groupby_agg_avg_no_valid_values(self):
        data_no_valid_avg: Relation = [{"key": "X", "val": None}, {"key": "X"}] # no numeric 'val'
        grouped_no_avg = groupby_agg(data_no_valid_avg, "key", [("avg", "val")])
        self.assertEqual(grouped_no_avg[0]["avg_val"], None)

    def test_groupby_agg_min_max_no_valid_values(self):
        data_no_valid_vals: Relation = [{"key": "X", "val": None}, {"key": "X"}] # no numeric 'val'
        grouped_no_min = groupby_agg(data_no_valid_vals, "key", [("min", "val")])
        self.assertEqual(grouped_no_min[0]["min_val"], None)
        grouped_no_max = groupby_agg(data_no_valid_vals, "key", [("max", "val")])
        self.assertEqual(grouped_no_max[0]["max_val"], None)

    def test_sort_by_descending(self):
        data: Relation = [
            {"name": "Charlie", "age": 30},
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
            {"name": "Alice", "age": 20},
        ]
        
        # Sort by name descending
        sorted_by_name_desc = sort_by(data, ["name"], reverse=True)
        self.assertEqual([r["name"] for r in sorted_by_name_desc], ["Charlie", "Bob", "Alice", "Alice"])
        
        # Sort by age descending, then name ascending
        sorted_by_age_desc_name_asc = sort_by(data, ["age", "name"], reverse=True)
        expected_age_desc_name_asc: Relation = [
            {"name": "Charlie", "age": 30},
            {"name": "Bob", "age": 30},
            {"name": "Alice", "age": 25},
            {"name": "Alice", "age": 20},
        ]
        # This is tricky. The primary key (age) is reversed, but the secondary (name) is not.
        # Python's sort is stable, so for equal ages, the original order of names is preserved.
        # To sort by age descending and name ascending, we would need a more complex key.
        # The current implementation sorts all keys in the same direction (all asc or all desc).
        # Let's test the current behavior.
        self.assertEqual(sorted_by_age_desc_name_asc, expected_age_desc_name_asc)


if __name__ == '__main__':
    unittest.main()