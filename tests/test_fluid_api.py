"""
Tests for the fluid API interface.

This module tests the RelationQuery class and fluid API methods
to ensure chainable operations work correctly.
"""

import unittest
import tempfile
import json
import os
from typing import List, Dict, Any

# Import the fluid API
from ja import query, from_jsonl, from_records, Q, RelationQuery
from ja.relation import GroupedRelation


class TestFluidAPI(unittest.TestCase):
    """Test cases for the fluid API."""
    
    def setUp(self):
        """Set up test data."""
        self.users = [
            {"id": 1, "name": "Alice", "age": 30, "city": "NYC", "active": True},
            {"id": 2, "name": "Bob", "age": 25, "city": "LA", "active": True},
            {"id": 3, "name": "Charlie", "age": 35, "city": "NYC", "active": False},
            {"id": 4, "name": "Diana", "age": 28, "city": "Chicago", "active": True},
            {"id": 5, "name": "Eve", "age": 32, "city": "LA", "active": False},
        ]
        
        self.orders = [
            {"user_id": 1, "product": "Book", "price": 20, "quantity": 2},
            {"user_id": 1, "product": "Pen", "price": 5, "quantity": 3},
            {"user_id": 2, "product": "Notebook", "price": 15, "quantity": 1},
            {"user_id": 3, "product": "Book", "price": 20, "quantity": 1},
            {"user_id": 2, "product": "Pencil", "price": 2, "quantity": 10},
        ]
        
        # Create temporary JSONL files for testing
        self.temp_dir = tempfile.mkdtemp()
        self.users_file = os.path.join(self.temp_dir, "users.jsonl")
        self.orders_file = os.path.join(self.temp_dir, "orders.jsonl")
        
        with open(self.users_file, 'w') as f:
            for user in self.users:
                f.write(json.dumps(user) + '\n')
        
        with open(self.orders_file, 'w') as f:
            for order in self.orders:
                f.write(json.dumps(order) + '\n')
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_basic_chaining(self):
        """Test basic method chaining."""
        result = (query(self.users)
            .select(lambda r: r["age"] > 25)
            .project(["name", "age"])
            .sort("age")
            .collect())
        
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]["name"], "Diana")  # Age 28
        self.assertEqual(result[-1]["name"], "Charlie")  # Age 35
    
    def test_where_alias(self):
        """Test that where() is an alias for select()."""
        result1 = query(self.users).select(lambda r: r["active"]).collect()
        result2 = query(self.users).where(lambda r: r["active"]).collect()
        
        self.assertEqual(result1, result2)
    
    def test_groupby_aggregation(self):
        """Test groupby with aggregations."""
        result = (query(self.users)
            .groupby("city")
            .agg("count", avg_age="avg:age")
            .sort("count", desc=True)
            .collect())
        
        # NYC and LA have 2 users each, Chicago has 1
        # Find the NYC result specifically
        nyc_result = next(r for r in result if r["city"] == "NYC")
        chicago_result = next(r for r in result if r["city"] == "Chicago")
        
        self.assertEqual(nyc_result["count"], 2)
        self.assertEqual(nyc_result["avg_age"], 32.5)  # (30 + 35) / 2
        self.assertEqual(chicago_result["count"], 1)
    
    def test_join_operation(self):
        """Test join with another relation."""
        result = (query(self.users)
            .select(lambda r: r["active"])
            .join(self.orders, on=[("id", "user_id")])
            .project(["name", "product", "price"])
            .collect())
        
        # Should have orders for active users (Alice and Bob)
        names = [r["name"] for r in result]
        self.assertIn("Alice", names)
        self.assertIn("Bob", names)
        self.assertNotIn("Charlie", names)  # Not active
    
    def test_limit_and_skip(self):
        """Test limit and skip operations."""
        result1 = query(self.users).sort("age").limit(2).collect()
        self.assertEqual(len(result1), 2)
        self.assertEqual(result1[0]["name"], "Bob")  # Youngest
        
        result2 = query(self.users).sort("age").skip(2).limit(2).collect()
        self.assertEqual(len(result2), 2)
        self.assertEqual(result2[0]["name"], "Alice")  # 3rd youngest
    
    def test_map_transformation(self):
        """Test map operation for transformations."""
        result = (query(self.users)
            .map(lambda r: {**r, "age_group": "adult" if r["age"] >= 30 else "young"})
            .project(["name", "age_group"])
            .collect())
        
        alice = next(r for r in result if r["name"] == "Alice")
        bob = next(r for r in result if r["name"] == "Bob")
        
        self.assertEqual(alice["age_group"], "adult")
        self.assertEqual(bob["age_group"], "young")
    
    def test_distinct_operation(self):
        """Test distinct removes duplicates."""
        data_with_dupes = self.users[:2] + self.users[:2]  # Duplicate first 2
        result = query(data_with_dupes).distinct().collect()
        
        self.assertEqual(len(result), 2)
    
    def test_union_operation(self):
        """Test union combines relations."""
        young = query(self.users).select(lambda r: r["age"] < 30).collect()
        old = query(self.users).select(lambda r: r["age"] >= 30).collect()
        
        result = query(young).union(old).collect()
        self.assertEqual(len(result), len(self.users))
    
    def test_execution_methods(self):
        """Test different execution methods."""
        q = query(self.users).select(lambda r: r["active"])
        
        # Test collect
        collected = q.collect()
        self.assertIsInstance(collected, list)
        
        # Test first
        first = q.first()
        self.assertIsInstance(first, dict)
        self.assertTrue(first["active"])
        
        # Test count
        count = q.count()
        self.assertEqual(count, 3)  # 3 active users
        
        # Test stream
        stream = q.stream()
        self.assertTrue(hasattr(stream, '__iter__'))
    
    def test_from_jsonl(self):
        """Test loading from JSONL file."""
        result = (from_jsonl(self.users_file)
            .select(lambda r: r["age"] > 30)
            .project(["name"])
            .collect())
        
        names = [r["name"] for r in result]
        self.assertIn("Charlie", names)
        self.assertIn("Eve", names)
        self.assertNotIn("Bob", names)
    
    def test_explain_method(self):
        """Test query explanation."""
        explanation = (query(self.users)
            .select(lambda r: r["active"])
            .project(["name", "age"])
            .sort("age")
            .explain())
        
        self.assertIn("select", explanation)
        self.assertIn("project", explanation)
        self.assertIn("sort", explanation)
    
    def test_Q_alias(self):
        """Test Q is an alias for query."""
        result1 = query(self.users).select(lambda r: r["active"]).collect()
        result2 = Q(self.users).select(lambda r: r["active"]).collect()
        
        self.assertEqual(result1, result2)
    
    def test_complex_pipeline(self):
        """Test a complex multi-step pipeline."""
        result = (query(self.users)
            .select(lambda r: r["active"])
            .join(self.orders, on=[("id", "user_id")])
            .map(lambda r: {**r, "total": r["price"] * r["quantity"]})
            .groupby("name")
            .agg(
                order_count="count",
                total_spent="sum:total",
                products="list:product"
            )
            .sort("total_spent", desc=True)
            .limit(5)
            .collect())
        
        # Verify the pipeline worked
        self.assertGreater(len(result), 0)
        # Results should be sorted by total_spent descending
        if len(result) > 1:
            self.assertGreaterEqual(
                result[0]["total_spent"], 
                result[1]["total_spent"]
            )
    
    def test_new_aggregation_functions(self):
        """Test newly added aggregation functions."""
        data = [
            {"category": "A", "value": 10, "name": "item1"},
            {"category": "A", "value": 20, "name": "item2"},
            {"category": "A", "value": 15, "name": "item3"},
            {"category": "B", "value": 30, "name": "item4"},
            {"category": "B", "value": 30, "name": "item5"},
        ]
        
        result = (query(data)
            .groupby("category")
            .agg(
                count="count",
                median="median:value",
                mode="mode:value",
                std="std:value",
                unique="unique:name",
                concat="concat:name"
            )
            .collect())
        
        # Find category A results
        cat_a = next(r for r in result if r["category"] == "A")
        
        self.assertEqual(cat_a["count"], 3)
        self.assertEqual(cat_a["median"], 15.0)  # Middle value
        self.assertAlmostEqual(cat_a["std"], 4.08, places=1)  # Std dev
        self.assertEqual(len(cat_a["unique"]), 3)  # 3 unique names
        self.assertIn("item1", cat_a["concat"])
    
    def test_chained_groupby_operations(self):
        """Test multiple groupby operations in sequence."""
        # First group by city, then regroup results
        result = (query(self.users)
            .groupby("city")
            .agg("count", avg_age="avg:age")
            .map(lambda r: {**r, "size": "large" if r["count"] > 1 else "small"})
            .groupby("size")
            .count()
            .collect())
        
        # Should have both large and small groups
        sizes = [r["size"] for r in result]
        self.assertIn("large", sizes)
        self.assertIn("small", sizes)
    
    def test_rename_operation(self):
        """Test renaming columns."""
        result = (query(self.users)
            .select(lambda r: r["age"] < 30)
            .rename({"name": "full_name", "age": "years"})
            .project(["full_name", "years"])
            .collect())
        
        # Check renamed columns exist
        self.assertIn("full_name", result[0])
        self.assertIn("years", result[0])
        self.assertNotIn("name", result[0])
        self.assertNotIn("age", result[0])
    
    def test_iteration_interface(self):
        """Test that RelationQuery is iterable."""
        q = query(self.users).select(lambda r: r["active"])
        
        count = 0
        for row in q:
            self.assertTrue(row["active"])
            count += 1
        
        self.assertEqual(count, 3)  # 3 active users


class TestGroupedRelation(unittest.TestCase):
    """Test cases for GroupedRelation class."""
    
    def setUp(self):
        """Set up test data."""
        self.data = [
            {"category": "A", "value": 10, "score": 85},
            {"category": "A", "value": 20, "score": 90},
            {"category": "B", "value": 15, "score": 75},
            {"category": "B", "value": 25, "score": 80},
        ]
    
    def test_grouped_count(self):
        """Test count on grouped relation."""
        result = query(self.data).groupby("category").count().collect()
        
        self.assertEqual(len(result), 2)
        for row in result:
            self.assertEqual(row["count"], 2)
    
    def test_grouped_sum(self):
        """Test sum on grouped relation."""
        result = query(self.data).groupby("category").sum("value").collect()
        
        cat_a = next(r for r in result if r["category"] == "A")
        self.assertEqual(cat_a["sum_value"], 30.0)
    
    def test_grouped_avg(self):
        """Test avg on grouped relation."""
        result = query(self.data).groupby("category").avg("score").collect()
        
        cat_a = next(r for r in result if r["category"] == "A")
        self.assertEqual(cat_a["avg_score"], 87.5)
    
    def test_grouped_min_max(self):
        """Test min/max on grouped relation."""
        grouped = query(self.data).groupby("category")
        
        min_result = grouped.min("value").collect()
        max_result = grouped.max("value").collect()
        
        cat_b_min = next(r for r in min_result if r["category"] == "B")
        cat_b_max = next(r for r in max_result if r["category"] == "B")
        
        self.assertEqual(cat_b_min["min_value"], 15.0)
        self.assertEqual(cat_b_max["max_value"], 25.0)


if __name__ == "__main__":
    unittest.main()