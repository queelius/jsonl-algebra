"""
Enhanced core function tests using both deterministic and generated data.

This module combines traditional unit tests with tests using the dataset generator
to ensure ja operations work correctly on both simple and complex data.
"""

import unittest
from test_utils import (
    TestDataGenerator, 
    FIXTURE_ORDERS, 
    FIXTURE_NESTED_USERS,
    assert_jsonl_equal
)

from ja.core import (
    Relation,
    select,
    project,
    join,
    union,
    intersection,
    difference,
    distinct,
    sort_by,
)


class TestCoreWithFixtures(unittest.TestCase):
    """Test core functions with deterministic fixtures."""
    
    def test_select_simple_data(self):
        """Test select with simple order data."""
        # Filter shipped orders
        shipped = select(FIXTURE_ORDERS, "status == 'shipped'")
        
        expected = [
            {"id": 1, "customer": "Alice", "amount": 100.50, "status": "shipped"},
            {"id": 3, "customer": "Alice", "amount": 204.22, "status": "shipped"},
            {"id": 4, "customer": "Charlie", "amount": 50.00, "status": "shipped"},
        ]
        
        assert_jsonl_equal(shipped, expected)
    
    def test_select_nested_data(self):
        """Test select with nested user data."""
        # Filter users with dark theme
        dark_theme_users = select(FIXTURE_NESTED_USERS, "settings.theme == 'dark'")
        
        expected = [
            {
                "id": 1,
                "profile": {"name": "Alice Johnson", "age": 28},
                "settings": {"theme": "dark", "notifications": True},
                "location": {"city": "San Francisco", "state": "CA"}
            },
            {
                "id": 3,
                "profile": {"name": "Charlie Brown", "age": 22},
                "settings": {"theme": "dark", "notifications": True},
                "location": {"city": "Austin", "state": "TX"}
            }
        ]
        
        assert_jsonl_equal(dark_theme_users, expected)
    
    def test_project_simple_data(self):
        """Test project with simple order data."""
        # Project customer and amount
        projected = project(FIXTURE_ORDERS, ["customer", "amount"])
        
        expected = [
            {"customer": "Alice", "amount": 100.50},
            {"customer": "Bob", "amount": 75.25},
            {"customer": "Alice", "amount": 204.22},
            {"customer": "Charlie", "amount": 50.00},
            {"customer": "Bob", "amount": 125.75},
        ]
        
        assert_jsonl_equal(projected, expected)
    
    def test_project_nested_data(self):
        """Test project with nested user data."""
        # Project just profile information
        projected = project(FIXTURE_NESTED_USERS, ["id", "profile.name", "profile.age"])
        
        expected = [
            {"id": 1, "profile": {"name": "Alice Johnson", "age": 28}},
            {"id": 2, "profile": {"name": "Bob Smith", "age": 34}},
            {"id": 3, "profile": {"name": "Charlie Brown", "age": 22}},
        ]
        
        assert_jsonl_equal(projected, expected)
    
    def test_sort_by_simple(self):
        """Test sorting with simple data."""
        # Sort by amount descending
        sorted_orders = sort_by(FIXTURE_ORDERS, "amount", reverse=True)
        
        # Should be sorted from highest to lowest amount
        amounts = [order["amount"] for order in sorted_orders]
        self.assertEqual(amounts, sorted(amounts, reverse=True))
    
    def test_sort_by_nested(self):
        """Test sorting with nested data."""
        # Sort by age
        sorted_users = sort_by(FIXTURE_NESTED_USERS, "profile.age")
        
        # Should be sorted from youngest to oldest
        ages = [user["profile"]["age"] for user in sorted_users]
        self.assertEqual(ages, [22, 28, 34])
    
    def test_distinct_simple(self):
        """Test distinct operation."""
        # Create data with duplicates
        data_with_dupes = FIXTURE_ORDERS + [FIXTURE_ORDERS[0]]  # Duplicate first order
        
        distinct_orders = distinct(data_with_dupes)
        
        # Should be same as original (no duplicates)
        assert_jsonl_equal(distinct_orders, FIXTURE_ORDERS)
    
    def test_union_operations(self):
        """Test union operation."""
        alice_orders = select(FIXTURE_ORDERS, "customer == 'Alice'")
        bob_orders = select(FIXTURE_ORDERS, "customer == 'Bob'")
        
        combined = union(alice_orders, bob_orders)
        
        # Should have all Alice and Bob orders
        expected_customers = {"Alice", "Bob"}
        actual_customers = set(order["customer"] for order in combined)
        self.assertEqual(actual_customers, expected_customers)
    
    def test_intersection_operations(self):
        """Test intersection operation."""
        shipped_orders = select(FIXTURE_ORDERS, "status == 'shipped'")
        alice_orders = select(FIXTURE_ORDERS, "customer == 'Alice'")
        
        alice_shipped = intersection(shipped_orders, alice_orders)
        
        # Should have only Alice's shipped orders
        for order in alice_shipped:
            self.assertEqual(order["customer"], "Alice")
            self.assertEqual(order["status"], "shipped")
    
    def test_difference_operations(self):
        """Test difference operation."""
        all_orders = FIXTURE_ORDERS
        alice_orders = select(FIXTURE_ORDERS, "customer == 'Alice'")
        
        non_alice_orders = difference(all_orders, alice_orders)
        
        # Should have no Alice orders
        for order in non_alice_orders:
            self.assertNotEqual(order["customer"], "Alice")


class TestCoreWithGeneratedData(unittest.TestCase):
    """Test core functions with generated datasets."""
    
    def setUp(self):
        """Set up generated test data."""
        self.generator = TestDataGenerator(seed=777)
        self.companies, self.people = self.generator.create_small_dataset()
    
    def test_select_performance_with_large_data(self):
        """Test select operation on larger generated dataset."""
        # Create larger dataset for performance testing
        large_companies, large_people = self.generator.create_large_dataset()
        
        # Filter high earners
        high_earners = select(large_people, "person.job.salary >= 120000")
        
        # Verify all results meet criteria
        for person in high_earners:
            self.assertGreaterEqual(person["person"]["job"]["salary"], 120000)
        
        # Should have reasonable number of high earners (not empty, not too many)
        self.assertGreater(len(high_earners), 0)
        self.assertLess(len(high_earners), len(large_people))
    
    def test_project_nested_paths(self):
        """Test projection of deeply nested paths."""
        # Project nested job information
        job_info = project(self.people, [
            "id", 
            "person.name.first", 
            "person.name.last",
            "person.job.title", 
            "person.job.company_name",
            "person.job.salary"
        ])
        
        # Verify structure
        for person in job_info:
            self.assertIn("id", person)
            self.assertIn("person", person)
            
            p = person["person"]
            self.assertIn("name", p)
            self.assertIn("job", p)
            
            # Should have name fields
            name = p["name"]
            self.assertIn("first", name)
            self.assertIn("last", name)
            
            # Should have job fields
            job = p["job"]
            self.assertIn("title", job)
            self.assertIn("company_name", job)
            self.assertIn("salary", job)
            
            # Should NOT have other fields
            self.assertNotIn("age", p)
            self.assertNotIn("location", p)
    
    def test_join_complex_data(self):
        """Test join operation with complex nested data."""
        # Join people with companies
        joined = join(
            self.people,
            self.companies,
            left_key="person.job.company_name",
            right_key="name"
        )
        
        # Should maintain all people (inner join should work)
        self.assertEqual(len(joined), len(self.people))
        
        # Each joined record should have both person and company data
        for row in joined:
            # From people
            self.assertIn("person", row)
            self.assertIn("household_id", row)
            
            # From companies  
            self.assertIn("industry", row)
            self.assertIn("headquarters", row)
            self.assertIn("size", row)
            self.assertIn("founded", row)
            
            # Join condition should be satisfied
            self.assertEqual(
                row["person"]["job"]["company_name"],
                row["name"]
            )
    
    def test_sort_by_nested_fields(self):
        """Test sorting by nested fields."""
        # Sort by salary descending
        sorted_by_salary = sort_by(self.people, "person.job.salary", reverse=True)
        
        # Verify sorting
        salaries = [p["person"]["job"]["salary"] for p in sorted_by_salary]
        self.assertEqual(salaries, sorted(salaries, reverse=True))
        
        # Sort by nested location
        sorted_by_location = sort_by(self.people, "person.location.state")
        
        # Verify sorting
        states = [p["person"]["location"]["state"] for p in sorted_by_location]
        self.assertEqual(states, sorted(states))
    
    def test_data_quality_after_operations(self):
        """Test that data quality is maintained after operations."""
        # Apply several operations in sequence
        result = self.people
        
        # Filter to adults
        result = select(result, "person.age >= 18")
        
        # Project to essential fields
        result = project(result, [
            "id", 
            "person.name", 
            "person.age", 
            "person.job.company_name",
            "person.location.state"
        ])
        
        # Sort by age
        result = sort_by(result, "person.age")
        
        # Verify data integrity
        self.assertGreater(len(result), 0)
        
        for person in result:
            # Should have all projected fields
            self.assertIn("id", person)
            self.assertIn("person", person)
            
            p = person["person"]
            self.assertIn("name", p)
            self.assertIn("age", p)
            self.assertIn("job", p)
            self.assertIn("location", p)
            
            # Age constraint should be satisfied
            self.assertGreaterEqual(p["age"], 18)
            
            # Should not have unprojected fields
            self.assertNotIn("interests", p)
            self.assertNotIn("email", p)
    
    def test_edge_cases_with_generated_data(self):
        """Test edge cases using generated data."""
        # Empty selection
        empty_result = select(self.people, "person.age > 200")
        self.assertEqual(len(empty_result), 0)
        
        # Select all
        all_result = select(self.people, "person.age >= 0")
        self.assertEqual(len(all_result), len(self.people))
        
        # Project single field
        single_field = project(self.people, ["id"])
        for person in single_field:
            self.assertEqual(len(person), 1)
            self.assertIn("id", person)
        
        # Project non-existent field (should handle gracefully)
        try:
            empty_project = project(self.people, ["nonexistent.field"])
            # Should not crash, may return empty or partial results
        except Exception as e:
            # If it throws, should be a meaningful error
            self.assertIsInstance(e, (KeyError, AttributeError, ValueError))


if __name__ == "__main__":
    unittest.main()
