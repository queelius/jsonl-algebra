import unittest
import tempfile
import json
import os
from ja import read_jsonl
from ja.commands import write_jsonl
from ja.core import *
from ja.groupby import groupby_agg


class TestUtilityFunctions(unittest.TestCase):
    """Test utility functions and edge cases not covered in other tests."""

    def setUp(self):
        """Set up test data."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_read_write_jsonl_integration(self):
        """Test reading and writing JSONL files."""
        test_data = [
            {"id": 1, "name": "Alice", "data": {"nested": "value"}},
            {"id": 2, "name": "Bob", "tags": ["tag1", "tag2"]},
            {"id": 3, "name": "Charlie", "empty_field": None},
        ]

        # Write data
        test_file = os.path.join(self.temp_dir, "test.jsonl")
        with open(test_file, "w") as f:
            for row in test_data:
                f.write(json.dumps(row) + "\n")

        # Read data back
        loaded_data = read_jsonl(test_file)
        self.assertEqual(loaded_data, test_data)

    def test_join_multiple_keys(self):
        """Test join with multiple key pairs."""
        left = [
            {"a": 1, "b": "x", "value": "left1"},
            {"a": 1, "b": "y", "value": "left2"},
            {"a": 2, "b": "x", "value": "left3"},
        ]

        right = [
            {"c": 1, "d": "x", "info": "right1"},
            {"c": 1, "d": "y", "info": "right2"},
            {"c": 2, "d": "z", "info": "right3"},  # This won't match
        ]

        # Join on multiple conditions: a=c AND b=d
        result = join(left, right, [("a", "c"), ("b", "d")])

        self.assertEqual(len(result), 2)

        # Check that matching rows are joined correctly
        result_values = [row["value"] for row in result]
        self.assertIn("left1", result_values)
        self.assertIn("left2", result_values)

        result_infos = [row["info"] for row in result]
        self.assertIn("right1", result_infos)
        self.assertIn("right2", result_infos)

    def test_sort_by_multiple_keys_with_mixed_types(self):
        """Test sorting with mixed data types."""
        data = [
            {"priority": 1, "name": "Charlie", "value": None},
            {"priority": 2, "name": "Alice", "value": 10},
            {"priority": 1, "name": "Bob", "value": 5},
            {"priority": None, "name": "David", "value": 20},
        ]

        # Sort by priority (None should come first), then by name
        sorted_data = sort_by(data, ["priority", "name"])

        # None should sort first
        self.assertIsNone(sorted_data[0]["priority"])
        self.assertEqual(sorted_data[0]["name"], "David")

        # Then priority 1 entries sorted by name
        self.assertEqual(sorted_data[1]["priority"], 1)
        self.assertEqual(sorted_data[1]["name"], "Bob")
        self.assertEqual(sorted_data[2]["priority"], 1)
        self.assertEqual(sorted_data[2]["name"], "Charlie")

        # Finally priority 2
        self.assertEqual(sorted_data[3]["priority"], 2)
        self.assertEqual(sorted_data[3]["name"], "Alice")

    def test_product_with_colliding_keys_complex(self):
        """Test product with multiple colliding keys."""
        left = [
            {"id": 1, "name": "Alice", "type": "user"},
            {"id": 2, "name": "Bob", "type": "user"},
        ]

        right = [
            {"id": 101, "name": "Project A", "type": "project"},
            {"id": 102, "name": "Project B", "type": "project"},
        ]

        result = product(left, right)

        # Should have 2 × 2 = 4 rows
        self.assertEqual(len(result), 4)

        # Check that all rows have both original and prefixed columns
        for row in result:
            # Original left columns should be preserved
            self.assertIn("id", row)
            self.assertIn("name", row)
            self.assertIn("type", row)

            # Right columns should be prefixed
            self.assertIn("b_id", row)
            self.assertIn("b_name", row)
            self.assertIn("b_type", row)

            # Check that left values are preserved
            self.assertIn(row["type"], ["user"])
            self.assertIn(row["b_type"], ["project"])

    def test_select_with_complex_expressions(self):
        """Test select with complex Python expressions."""
        data = [
            {"score": 85, "age": 25, "tags": ["python", "data"]},
            {"score": 92, "age": 30, "tags": ["java", "web"]},
            {"score": 78, "age": 22, "tags": ["python", "ml"]},
            {"score": 95, "age": 28, "tags": ["python", "web", "data"]},
        ]

        # Complex expression: high score AND young age AND has python tag
        result = select(
            data,
            lambda row: row.get("score", 0) > 80
            and row.get("age", 0) < 30
            and "python" in row.get("tags", []),
        )

        self.assertEqual(len(result), 2)  # Should match first and fourth rows

        for row in result:
            self.assertGreater(row["score"], 80)
            self.assertLess(row["age"], 30)
            self.assertIn("python", row["tags"])

    def test_groupby_with_complex_expressions_via_select(self):
        """Test combining select and groupby for complex analytics."""
        sales_data = [
            {"date": "2023-01", "product": "A", "amount": 100, "region": "North"},
            {"date": "2023-01", "product": "B", "amount": 150, "region": "North"},
            {"date": "2023-01", "product": "A", "amount": 120, "region": "South"},
            {"date": "2023-02", "product": "A", "amount": 200, "region": "North"},
            {"date": "2023-02", "product": "B", "amount": 180, "region": "South"},
            {"date": "2023-02", "product": "A", "amount": 90, "region": "South"},
        ]

        # First, select only high-value sales (> 100)
        high_value_sales = select(sales_data, lambda row: row.get("amount", 0) > 100)

        # Then group by product and analyze
        product_analysis = groupby_agg(
            high_value_sales,
            "product",
            [("count", ""), ("sum", "amount"), ("avg", "amount"), ("list", "region")],
        )

        product_dict = {p["product"]: p for p in product_analysis}

        # Product A should have 2 high-value sales (120 + 200)
        self.assertEqual(product_dict["A"]["count"], 2)
        self.assertEqual(product_dict["A"]["sum_amount"], 120 + 200)

        # Product B should have 2 high-value sales
        self.assertEqual(product_dict["B"]["count"], 2)
        self.assertEqual(product_dict["B"]["sum_amount"], 150 + 180)

    def test_chaining_operations(self):
        """Test chaining multiple operations together."""
        raw_data = [
            {"user_id": 1, "action": "login", "timestamp": "2023-01-01", "duration": 5},
            {
                "user_id": 1,
                "action": "view",
                "timestamp": "2023-01-01",
                "duration": 120,
            },
            {"user_id": 2, "action": "login", "timestamp": "2023-01-01", "duration": 3},
            {
                "user_id": 1,
                "action": "logout",
                "timestamp": "2023-01-01",
                "duration": 2,
            },
            {"user_id": 2, "action": "view", "timestamp": "2023-01-01", "duration": 90},
            {"user_id": 3, "action": "login", "timestamp": "2023-01-01", "duration": 4},
        ]

        # Chain: select long sessions → project relevant fields → group by user → sort by engagement

        # Step 1: Select sessions longer than 5 seconds
        long_sessions = select(raw_data, lambda row: row.get("duration", 0) > 5)

        # Step 2: Project only user_id and duration
        user_durations = project(long_sessions, ["user_id", "duration"])

        # Step 3: Group by user and sum durations
        user_engagement = groupby_agg(
            user_durations,
            "user_id",
            [("count", ""), ("sum", "duration"), ("avg", "duration")],
        )

        # Step 4: Sort by total engagement time
        sorted_engagement = sort_by(user_engagement, ["sum_duration"])

        # Verify the pipeline
        self.assertEqual(
            len(sorted_engagement), 2
        )  # Only users 1 and 2 have long sessions

        # User 2 should be first (90 seconds), then user 1 (120 seconds)
        self.assertEqual(sorted_engagement[0]["user_id"], 2)
        self.assertEqual(sorted_engagement[0]["sum_duration"], 90.0)
        self.assertEqual(sorted_engagement[1]["user_id"], 1)
        self.assertEqual(sorted_engagement[1]["sum_duration"], 120.0)

    def test_large_data_scalability_simulation(self):
        """Test with larger datasets to simulate real-world usage."""
        # Generate a larger dataset
        import random

        large_data = []
        categories = ["A", "B", "C", "D", "E"]
        regions = ["North", "South", "East", "West"]

        for i in range(1000):
            large_data.append(
                {
                    "id": i,
                    "category": random.choice(categories),
                    "region": random.choice(regions),
                    "value": random.randint(1, 100),
                    "score": random.uniform(0, 1),
                }
            )

        # Test distinct operation on large dataset
        distinct_result = distinct(large_data)
        self.assertEqual(
            len(distinct_result), 1000
        )  # All should be unique due to unique ids

        # Test groupby on large dataset
        category_analysis = groupby_agg(
            large_data, "category", [("count", ""), ("avg", "value"), ("sum", "value")]
        )

        # Should have 5 categories
        self.assertEqual(len(category_analysis), 5)

        # Verify counts add up to total
        total_count = sum(group["count"] for group in category_analysis)
        self.assertEqual(total_count, 1000)

    def test_edge_case_empty_strings_and_special_values(self):
        """Test handling of empty strings, zeros, and other edge case values."""
        edge_case_data = [
            {"key": "", "value": 0, "flag": False},
            {"key": "normal", "value": 100, "flag": True},
            {"key": "", "value": 50, "flag": False},
            {"key": "normal", "value": 0, "flag": True},
        ]

        # Test groupby with empty string keys
        grouped = groupby_agg(
            edge_case_data, "key", [("count", ""), ("sum", "value"), ("list", "flag")]
        )

        grouped_dict = {g["key"]: g for g in grouped}

        # Empty string group
        self.assertIn("", grouped_dict)
        self.assertEqual(grouped_dict[""]["count"], 2)
        self.assertEqual(grouped_dict[""]["sum_value"], 50.0)  # 0 + 50

        # Normal group
        self.assertEqual(grouped_dict["normal"]["count"], 2)
        self.assertEqual(grouped_dict["normal"]["sum_value"], 100.0)  # 100 + 0

        # Test selecting with boolean values
        true_flags = select(edge_case_data, lambda row: row.get("flag") is True)
        self.assertEqual(len(true_flags), 2)

        false_flags = select(edge_case_data, lambda row: row.get("flag") is False)
        self.assertEqual(len(false_flags), 2)


if __name__ == "__main__":
    unittest.main()
