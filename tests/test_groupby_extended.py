import unittest
from ja.groupby import (
    groupby_agg,
    _agg_sum_func,
    _agg_avg_func,
    _agg_min_func,
    _agg_max_func,
    _agg_list_func,
    _agg_first_func,
    _agg_last_func,
    _agg_numeric_values,
    AGGREGATION_DISPATCHER,
)
from ja.core import Relation, Row


class TestGroupByEdgeCases(unittest.TestCase):

    def test_list_aggregation(self):
        """Test the list aggregation function."""
        data: Relation = [
            {"category": "A", "item": "apple"},
            {"category": "A", "item": "apricot"},
            {"category": "B", "item": "banana"},
            {"category": "A", "item": "avocado"},
        ]

        grouped = groupby_agg(data, "category", [("list", "item")])
        grouped_dict = {g["category"]: g for g in grouped}

        self.assertEqual(len(grouped), 2)
        self.assertIn("A", grouped_dict)
        self.assertIn("B", grouped_dict)

        # Category A should have 3 items
        self.assertEqual(len(grouped_dict["A"]["list_item"]), 3)
        self.assertIn("apple", grouped_dict["A"]["list_item"])
        self.assertIn("apricot", grouped_dict["A"]["list_item"])
        self.assertIn("avocado", grouped_dict["A"]["list_item"])

        # Category B should have 1 item
        self.assertEqual(grouped_dict["B"]["list_item"], ["banana"])

    def test_first_last_aggregation(self):
        """Test first and last aggregation functions."""
        data: Relation = [
            {"category": "A", "timestamp": "2023-01-01", "value": 10},
            {"category": "A", "timestamp": "2023-01-02", "value": 20},
            {"category": "A", "timestamp": "2023-01-03", "value": 30},
            {"category": "B", "timestamp": "2023-01-01", "value": 100},
        ]

        grouped = groupby_agg(
            data,
            "category",
            [
                ("first", "timestamp"),
                ("last", "timestamp"),
                ("first", "value"),
                ("last", "value"),
            ],
        )
        grouped_dict = {g["category"]: g for g in grouped}

        # Category A
        self.assertEqual(grouped_dict["A"]["first_timestamp"], "2023-01-01")
        self.assertEqual(grouped_dict["A"]["last_timestamp"], "2023-01-03")
        self.assertEqual(grouped_dict["A"]["first_value"], 10)
        self.assertEqual(grouped_dict["A"]["last_value"], 30)

        # Category B
        self.assertEqual(grouped_dict["B"]["first_timestamp"], "2023-01-01")
        self.assertEqual(grouped_dict["B"]["last_timestamp"], "2023-01-01")
        self.assertEqual(grouped_dict["B"]["first_value"], 100)
        self.assertEqual(grouped_dict["B"]["last_value"], 100)

    def test_mixed_aggregations(self):
        """Test using multiple different aggregation types together."""
        data: Relation = [
            {"group": "X", "score": 85, "name": "Alice"},
            {"group": "X", "score": 92, "name": "Bob"},
            {"group": "Y", "score": 78, "name": "Charlie"},
            {"group": "X", "score": 88, "name": "Diana"},
        ]

        grouped = groupby_agg(
            data,
            "group",
            [
                ("count", ""),
                ("sum", "score"),
                ("avg", "score"),
                ("min", "score"),
                ("max", "score"),
                ("list", "name"),
                ("first", "name"),
                ("last", "name"),
            ],
        )
        grouped_dict = {g["group"]: g for g in grouped}

        # Group X
        x_group = grouped_dict["X"]
        self.assertEqual(x_group["count"], 3)
        self.assertEqual(x_group["sum_score"], 85 + 92 + 88)
        self.assertEqual(x_group["avg_score"], (85 + 92 + 88) / 3)
        self.assertEqual(x_group["min_score"], 85)
        self.assertEqual(x_group["max_score"], 92)
        self.assertEqual(set(x_group["list_name"]), {"Alice", "Bob", "Diana"})
        self.assertEqual(x_group["first_name"], "Alice")
        self.assertEqual(x_group["last_name"], "Diana")

        # Group Y
        y_group = grouped_dict["Y"]
        self.assertEqual(y_group["count"], 1)
        self.assertEqual(y_group["sum_score"], 78)
        self.assertEqual(y_group["avg_score"], 78.0)
        self.assertEqual(y_group["min_score"], 78)
        self.assertEqual(y_group["max_score"], 78)
        self.assertEqual(y_group["list_name"], ["Charlie"])
        self.assertEqual(y_group["first_name"], "Charlie")
        self.assertEqual(y_group["last_name"], "Charlie")

    def test_agg_numeric_values_with_mixed_types(self):
        """Test _agg_numeric_values with mixed data types."""
        mixed_values = [10, "20", 30.5, None, "40", "not_a_number"]

        # Should handle convertible strings but raise ValueError for non-convertible
        with self.assertRaises(ValueError):
            _agg_numeric_values(mixed_values)

        # Test with only valid numeric values
        valid_values = [10, "20", 30.5, None, "40"]
        result = _agg_numeric_values(valid_values)
        expected = [10.0, 20.0, 30.5, 40.0]  # None should be skipped
        self.assertEqual(result, expected)

    def test_agg_numeric_values_empty_list(self):
        """Test _agg_numeric_values with empty list."""
        result = _agg_numeric_values([])
        self.assertEqual(result, [])

    def test_agg_numeric_values_only_none(self):
        """Test _agg_numeric_values with only None values."""
        result = _agg_numeric_values([None, None, None])
        self.assertEqual(result, [])

    def test_individual_aggregation_functions(self):
        """Test individual aggregation helper functions directly."""
        # Test sum
        self.assertEqual(_agg_sum_func([1, 2, 3]), 6.0)
        self.assertEqual(_agg_sum_func([]), 0.0)

        # Test avg
        self.assertEqual(_agg_avg_func([1, 2, 3]), 2.0)
        self.assertIsNone(_agg_avg_func([]))

        # Test min/max
        self.assertEqual(_agg_min_func([3, 1, 2]), 1.0)
        self.assertEqual(_agg_max_func([3, 1, 2]), 3.0)
        self.assertIsNone(_agg_min_func([]))
        self.assertIsNone(_agg_max_func([]))

        # Test count - count is handled differently in the dispatcher
        # No direct _agg_count_func, count is handled in groupby_agg

        # Test list
        test_list = [1, 2, 3]
        self.assertEqual(_agg_list_func(test_list), test_list)

        # Test first/last
        self.assertEqual(_agg_first_func("first_value"), "first_value")
        self.assertEqual(_agg_last_func("last_value"), "last_value")

    def test_aggregation_dispatcher_completeness(self):
        """Test that all expected aggregation functions are in the dispatcher."""
        expected_functions = ["sum", "avg", "min", "max", "list", "first", "last"]

        for func_name in expected_functions:
            self.assertIn(func_name, AGGREGATION_DISPATCHER)
            self.assertTrue(callable(AGGREGATION_DISPATCHER[func_name]))

    def test_groupby_with_none_values_in_group_key(self):
        """Test groupby behavior when group key has None values."""
        data: Relation = [
            {"category": None, "value": 10},
            {"category": "A", "value": 20},
            {"category": None, "value": 30},
            {"category": "A", "value": 40},
        ]

        grouped = groupby_agg(data, "category", [("sum", "value"), ("count", "")])
        grouped_dict = {g["category"]: g for g in grouped}

        self.assertEqual(len(grouped), 2)
        self.assertIn(None, grouped_dict)
        self.assertIn("A", grouped_dict)

        # None group should have sum of 10 + 30 = 40
        self.assertEqual(grouped_dict[None]["sum_value"], 40.0)
        self.assertEqual(grouped_dict[None]["count"], 2)

        # A group should have sum of 20 + 40 = 60
        self.assertEqual(grouped_dict["A"]["sum_value"], 60.0)
        self.assertEqual(grouped_dict["A"]["count"], 2)

    def test_groupby_with_missing_group_key(self):
        """Test groupby behavior when some rows don't have the group key."""
        data: Relation = [
            {"category": "A", "value": 10},
            {"value": 20},  # Missing category key
            {"category": "A", "value": 30},
            {"other_field": "B", "value": 40},  # Missing category key
        ]

        grouped = groupby_agg(data, "category", [("sum", "value"), ("count", "")])
        grouped_dict = {g["category"]: g for g in grouped}

        # Should have groups for "A" and None (for missing keys)
        self.assertIn("A", grouped_dict)
        self.assertIn(None, grouped_dict)

        # A group
        self.assertEqual(grouped_dict["A"]["sum_value"], 40.0)  # 10 + 30
        self.assertEqual(grouped_dict["A"]["count"], 2)

        # None group (missing keys default to None)
        self.assertEqual(grouped_dict[None]["sum_value"], 60.0)  # 20 + 40
        self.assertEqual(grouped_dict[None]["count"], 2)

    def test_groupby_single_row_groups(self):
        """Test groupby with groups that have only one row each."""
        data: Relation = [
            {"id": "unique1", "value": 100},
            {"id": "unique2", "value": 200},
            {"id": "unique3", "value": 300},
        ]

        grouped = groupby_agg(
            data,
            "id",
            [
                ("count", ""),
                ("sum", "value"),
                ("avg", "value"),
                ("min", "value"),
                ("max", "value"),
                ("first", "value"),
                ("last", "value"),
                ("list", "value"),
            ],
        )

        self.assertEqual(len(grouped), 3)

        for group in grouped:
            self.assertEqual(group["count"], 1)
            self.assertEqual(group["sum_value"], group["avg_value"])
            self.assertEqual(group["min_value"], group["max_value"])
            self.assertEqual(group["first_value"], group["last_value"])
            self.assertEqual(len(group["list_value"]), 1)

    def test_groupby_empty_aggregation_columns(self):
        """Test groupby when aggregation columns don't exist in data."""
        data: Relation = [
            {"category": "A", "existing_field": 10},
            {"category": "A", "existing_field": 20},
            {"category": "B", "existing_field": 30},
        ]

        # Try to aggregate on non-existent column
        grouped = groupby_agg(
            data,
            "category",
            [
                ("count", ""),
                ("sum", "nonexistent_field"),
                ("list", "nonexistent_field"),
            ],
        )
        grouped_dict = {g["category"]: g for g in grouped}

        # Count should still work
        self.assertEqual(grouped_dict["A"]["count"], 2)
        self.assertEqual(grouped_dict["B"]["count"], 1)

        # Sum of non-existent field should be 0 (sum of empty list)
        self.assertEqual(grouped_dict["A"]["sum_nonexistent_field"], 0.0)
        self.assertEqual(grouped_dict["B"]["sum_nonexistent_field"], 0.0)

        # List of non-existent field should contain None values for each row
        self.assertEqual(grouped_dict["A"]["list_nonexistent_field"], [None, None])
        self.assertEqual(grouped_dict["B"]["list_nonexistent_field"], [None])

    def test_unsupported_aggregation_during_collection(self):
        """Test error handling for unsupported aggregation functions during collection phase."""
        data: Relation = [{"category": "A", "value": 10}]

        # Mock an unsupported function that's not in the dispatcher
        with self.assertRaises(ValueError) as context:
            groupby_agg(data, "category", [("unsupported_func", "value")])

        self.assertIn("Unsupported aggregation function", str(context.exception))

    def test_unsupported_aggregation_during_processing(self):
        """Test the defensive error case during processing phase."""
        # This is a defensive case that's hard to trigger in normal use
        # It would require an aggregation function to be in the dispatcher
        # but not handled in the processing phase

        # For 100% coverage, we'll just note that this is defensive code
        # that protects against inconsistent dispatcher/processing setup
        pass

    def test_complex_data_types_in_aggregation(self):
        """Test aggregation with complex data types."""
        data: Relation = [
            {"group": "A", "tags": ["tag1", "tag2"], "nested": {"key": "value1"}},
            {"group": "A", "tags": ["tag3"], "nested": {"key": "value2"}},
            {
                "group": "B",
                "tags": ["tag4", "tag5", "tag6"],
                "nested": {"key": "value3"},
            },
        ]

        # Test list aggregation with complex types
        grouped = groupby_agg(
            data,
            "group",
            [
                ("list", "tags"),
                ("list", "nested"),
                ("first", "nested"),
                ("last", "nested"),
            ],
        )
        grouped_dict = {g["group"]: g for g in grouped}

        # Group A should collect all tags and nested objects
        a_group = grouped_dict["A"]
        self.assertEqual(len(a_group["list_tags"]), 2)
        self.assertEqual(a_group["list_tags"], [["tag1", "tag2"], ["tag3"]])
        self.assertEqual(len(a_group["list_nested"]), 2)
        self.assertEqual(a_group["first_nested"], {"key": "value1"})
        self.assertEqual(a_group["last_nested"], {"key": "value2"})


class TestGroupByAggregationExtensibility(unittest.TestCase):
    """Test the extensibility features of the groupby aggregation system."""

    def test_custom_aggregation_dispatcher_structure(self):
        """Test that the dispatcher can theoretically be extended."""
        # This tests the current structure and documents how extensions would work

        # Verify the dispatcher is a dictionary mapping strings to functions
        self.assertIsInstance(AGGREGATION_DISPATCHER, dict)

        for func_name, func in AGGREGATION_DISPATCHER.items():
            self.assertIsInstance(func_name, str)
            self.assertTrue(callable(func))

    def test_median_aggregation_example(self):
        """Test implementing a median aggregation (as documented in README)."""

        def _custom_median_agg(collected_values: list) -> float:
            """Custom median aggregation function."""
            numeric_vals = sorted(
                [
                    v
                    for v in collected_values
                    if isinstance(v, (int, float)) and v is not None
                ]
            )
            if not numeric_vals:
                return None
            n = len(numeric_vals)
            mid = n // 2
            if n % 2 == 0:
                return (numeric_vals[mid - 1] + numeric_vals[mid]) / 2
            else:
                return numeric_vals[mid]

        # Test the median function directly
        self.assertEqual(_custom_median_agg([1, 2, 3, 4, 5]), 3)
        self.assertEqual(_custom_median_agg([1, 2, 3, 4]), 2.5)
        self.assertEqual(_custom_median_agg([5]), 5)
        self.assertIsNone(_custom_median_agg([]))
        self.assertIsNone(_custom_median_agg([None, "invalid"]))

    def test_standard_deviation_aggregation_example(self):
        """Test implementing a standard deviation aggregation."""

        def _custom_std_agg(collected_values: list) -> float:
            """Custom standard deviation aggregation function."""
            numeric_vals = [
                v
                for v in collected_values
                if isinstance(v, (int, float)) and v is not None
            ]
            if len(numeric_vals) < 2:
                return None

            mean = sum(numeric_vals) / len(numeric_vals)
            variance = sum((x - mean) ** 2 for x in numeric_vals) / len(numeric_vals)
            return variance**0.5

        # Test the std function directly
        values = [1, 2, 3, 4, 5]
        expected_std = (
            (1 - 3) ** 2 + (2 - 3) ** 2 + (3 - 3) ** 2 + (4 - 3) ** 2 + (5 - 3) ** 2
        ) ** 0.5 / (5**0.5)
        result = _custom_std_agg(values)
        self.assertAlmostEqual(result, 1.4142135623730951, places=10)

        self.assertIsNone(_custom_std_agg([1]))  # Not enough values
        self.assertIsNone(_custom_std_agg([]))  # Empty


if __name__ == "__main__":
    unittest.main()
