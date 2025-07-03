import unittest
from ja.core import (
    groupby_with_metadata,
    groupby_chained,
    aggregate_grouped_data,
    Relation
)


class TestChainedGroupBy(unittest.TestCase):
    def setUp(self):
        """Set up test data for various grouping scenarios."""
        self.sales_data: Relation = [
            {"region": "North", "product": "Widget", "amount": 100, "date": "2024-01"},
            {"region": "North", "product": "Gadget", "amount": 150, "date": "2024-01"},
            {"region": "North", "product": "Widget", "amount": 200, "date": "2024-02"},
            {"region": "South", "product": "Widget", "amount": 250, "date": "2024-01"},
            {"region": "South", "product": "Gadget", "amount": 300, "date": "2024-02"},
        ]
        
        self.nested_data: Relation = [
            {"user": {"id": 1, "region": "US"}, "product": {"id": "A", "category": "Electronics"}, "amount": 50},
            {"user": {"id": 1, "region": "US"}, "product": {"id": "B", "category": "Books"}, "amount": 20},
            {"user": {"id": 2, "region": "EU"}, "product": {"id": "A", "category": "Electronics"}, "amount": 60},
            {"user": {"id": 2, "region": "EU"}, "product": {"id": "A", "category": "Electronics"}, "amount": 40},
        ]

    def test_single_groupby_metadata(self):
        """Test that groupby_with_metadata adds correct metadata fields."""
        grouped = groupby_with_metadata(self.sales_data, "region")
        
        # Check all rows have metadata
        for row in grouped:
            self.assertIn("_group", row)
            self.assertIn("_group_field", row)
            self.assertIn("_group_size", row)
            self.assertIn("_group_index", row)
        
        # Check North region
        north_rows = [r for r in grouped if r["_group"] == "North"]
        self.assertEqual(len(north_rows), 3)
        for row in north_rows:
            self.assertEqual(row["_group_field"], "region")
            self.assertEqual(row["_group_size"], 3)
        
        # Check South region
        south_rows = [r for r in grouped if r["_group"] == "South"]
        self.assertEqual(len(south_rows), 2)
        for row in south_rows:
            self.assertEqual(row["_group_field"], "region")
            self.assertEqual(row["_group_size"], 2)

    def test_chained_groupby(self):
        """Test chaining two groupby operations."""
        # First group by region
        grouped1 = groupby_with_metadata(self.sales_data, "region")
        
        # Then group by product
        grouped2 = groupby_chained(grouped1, "product")
        
        # Check compound groups
        north_widget = [r for r in grouped2 if r["_group"] == "North.Widget"]
        self.assertEqual(len(north_widget), 2)
        for row in north_widget:
            self.assertEqual(row["_parent_group"], "North")
            self.assertEqual(row["_group_trail"], ["region", "product"])
            self.assertEqual(row["_group_size"], 2)
        
        # Check single-item groups
        south_gadget = [r for r in grouped2 if r["_group"] == "South.Gadget"]
        self.assertEqual(len(south_gadget), 1)
        self.assertEqual(south_gadget[0]["_group_size"], 1)

    def test_three_level_groupby(self):
        """Test chaining three groupby operations."""
        grouped1 = groupby_with_metadata(self.sales_data, "region")
        grouped2 = groupby_chained(grouped1, "product")
        grouped3 = groupby_chained(grouped2, "date")
        
        # Check a specific three-level group
        north_widget_jan = [r for r in grouped3 if r["_group"] == "North.Widget.2024-01"]
        self.assertEqual(len(north_widget_jan), 1)
        self.assertEqual(north_widget_jan[0]["_group_trail"], ["region", "product", "date"])
        self.assertEqual(north_widget_jan[0]["amount"], 100)

    def test_aggregate_grouped_data(self):
        """Test aggregating data with group metadata."""
        # Group and then aggregate
        grouped = groupby_with_metadata(self.sales_data, "region")
        aggregated = aggregate_grouped_data(grouped, "total=sum(amount),count,avg=avg(amount)")
        
        # Check results
        self.assertEqual(len(aggregated), 2)
        
        north = next(r for r in aggregated if r["region"] == "North")
        self.assertEqual(north["total"], 450)
        self.assertEqual(north["count"], 3)
        self.assertEqual(north["avg"], 150)
        
        south = next(r for r in aggregated if r["region"] == "South")
        self.assertEqual(south["total"], 550)
        self.assertEqual(south["count"], 2)
        self.assertEqual(south["avg"], 275)

    def test_aggregate_chained_groups(self):
        """Test aggregating after multiple groupby operations."""
        grouped1 = groupby_with_metadata(self.sales_data, "region")
        grouped2 = groupby_chained(grouped1, "product")
        aggregated = aggregate_grouped_data(grouped2, "total=sum(amount),count")
        
        # Check we have the right number of groups
        self.assertEqual(len(aggregated), 4)  # North.Widget, North.Gadget, South.Widget, South.Gadget
        
        # Check specific aggregations
        north_widget = next(r for r in aggregated if r["product"] == "North.Widget")
        self.assertEqual(north_widget["total"], 300)
        self.assertEqual(north_widget["count"], 2)

    def test_groupby_with_nested_fields(self):
        """Test groupby operations on nested fields."""
        # Group by nested field
        grouped = groupby_with_metadata(self.nested_data, "user.region")
        
        us_rows = [r for r in grouped if r["_group"] == "US"]
        self.assertEqual(len(us_rows), 2)
        
        eu_rows = [r for r in grouped if r["_group"] == "EU"]
        self.assertEqual(len(eu_rows), 2)

    def test_chained_groupby_nested_fields(self):
        """Test chaining groupby with nested fields."""
        grouped1 = groupby_with_metadata(self.nested_data, "user.region")
        grouped2 = groupby_chained(grouped1, "product.category")
        
        # Check compound group with nested values
        us_electronics = [r for r in grouped2 if r["_group"] == "US.Electronics"]
        self.assertEqual(len(us_electronics), 1)
        self.assertEqual(us_electronics[0]["amount"], 50)

    def test_empty_groups(self):
        """Test behavior with empty data."""
        empty_data: Relation = []
        grouped = groupby_with_metadata(empty_data, "any_field")
        self.assertEqual(len(grouped), 0)
        
        aggregated = aggregate_grouped_data(grouped, "count")
        self.assertEqual(len(aggregated), 0)

    def test_null_group_values(self):
        """Test grouping when some rows have null values for the group key."""
        data_with_nulls: Relation = [
            {"category": "A", "value": 10},
            {"category": None, "value": 20},
            {"category": "A", "value": 30},
            {"category": None, "value": 40},
        ]
        
        grouped = groupby_with_metadata(data_with_nulls, "category")
        
        # Check that None is a valid group
        none_group = [r for r in grouped if r["_group"] is None]
        self.assertEqual(len(none_group), 2)
        self.assertEqual(sum(r["value"] for r in none_group), 60)

    def test_metadata_preservation(self):
        """Test that original data is preserved through grouping."""
        grouped = groupby_with_metadata(self.sales_data, "region")
        
        # Check that all original fields are still present
        for row in grouped:
            self.assertIn("region", row)
            self.assertIn("product", row)
            self.assertIn("amount", row)
            self.assertIn("date", row)

    def test_group_index_ordering(self):
        """Test that _group_index maintains correct ordering within groups."""
        grouped = groupby_with_metadata(self.sales_data, "region")
        
        # Check North region indices
        north_rows = sorted(
            [r for r in grouped if r["_group"] == "North"],
            key=lambda r: r["_group_index"]
        )
        for i, row in enumerate(north_rows):
            self.assertEqual(row["_group_index"], i)


if __name__ == "__main__":
    unittest.main()