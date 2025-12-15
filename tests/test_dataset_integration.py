"""
Integration tests using generated datasets

These tests use the dataset generator to create realistic test data
and verify that ja operations work correctly on complex, nested data.
"""

import unittest
import tempfile
import os
from typing import List, Dict, Any

from .test_utils import (
    TestDataGenerator,
    TempDataFiles,
    assert_jsonl_equal,
    run_ja_command,
    parse_jsonl_output
)

from ja.core import Relation, select, project, join
from ja.group import groupby_with_metadata
from ja.agg import aggregate_grouped_data


class TestDatasetIntegration(unittest.TestCase):
    """Test ja operations using generated datasets."""
    
    def setUp(self):
        """Set up test data."""
        self.generator = TestDataGenerator(seed=123)  # Use different seed from default
        self.companies, self.people = self.generator.create_small_dataset()
    
    def test_basic_data_structure(self):
        """Test that generated data has expected structure."""
        # Test companies structure
        self.assertEqual(len(self.companies), 10)
        for company in self.companies:
            self.assertIn("id", company)
            self.assertIn("name", company)
            self.assertIn("industry", company)
            self.assertIn("headquarters", company)
            self.assertIn("size", company)
            self.assertIn("founded", company)
            
            # Test nested headquarters structure
            hq = company["headquarters"]
            self.assertIn("city", hq)
            self.assertIn("state", hq)
            self.assertIn("country", hq)
            self.assertEqual(hq["country"], "USA")
        
        # Test people structure
        self.assertEqual(len(self.people), 50)
        for person in self.people:
            self.assertIn("id", person)
            self.assertIn("household_id", person)
            self.assertIn("person", person)
            
            # Test nested person structure
            p = person["person"]
            self.assertIn("name", p)
            self.assertIn("age", p)
            self.assertIn("job", p)
            self.assertIn("location", p)
            
            # Test name structure
            name = p["name"]
            self.assertIn("first", name)
            self.assertIn("last", name)
            
            # Test job structure
            job = p["job"]
            self.assertIn("title", job)
            self.assertIn("company_name", job)
            self.assertIn("salary", job)
    
    def test_relational_integrity(self):
        """Test that people's companies exist in the companies dataset."""
        company_names = set(c["name"] for c in self.companies)
        
        for person in self.people:
            company_name = person["person"]["job"]["company_name"]
            self.assertIn(company_name, company_names, 
                         f"Person's company '{company_name}' not found in companies dataset")
    
    def test_household_relationships(self):
        """Test that household relationships are consistent."""
        households = {}
        
        # Group people by household
        for person in self.people:
            hh_id = person["household_id"]
            if hh_id not in households:
                households[hh_id] = []
            households[hh_id].append(person)
        
        # Check household consistency
        for hh_id, members in households.items():
            if len(members) > 1:
                # All members should have same last name
                last_names = set(m["person"]["name"]["last"] for m in members)
                self.assertEqual(len(last_names), 1, 
                               f"Household {hh_id} has mixed last names: {last_names}")
                
                # All members should have same location
                locations = set(
                    f"{m['person']['location']['city']}, {m['person']['location']['state']}" 
                    for m in members
                )
                self.assertEqual(len(locations), 1,
                               f"Household {hh_id} has mixed locations: {locations}")
    
    def test_select_operations(self):
        """Test select operations on nested data."""
        # Filter people over 30
        older_people = select(self.people, "person.age > 30")
        
        # Verify all results are actually over 30
        for person in older_people:
            self.assertGreater(person["person"]["age"], 30)
        
        # Filter by location
        ca_people = select(self.people, "person.location.state == 'CA'")
        
        # Verify all results are from CA
        for person in ca_people:
            self.assertEqual(person["person"]["location"]["state"], "CA")
        
        # Filter by salary range
        high_earners = select(self.people, "person.job.salary >= 100000")
        
        # Verify all results have high salary
        for person in high_earners:
            self.assertGreaterEqual(person["person"]["job"]["salary"], 100000)
    
    def test_projection_operations(self):
        """Test projection operations on nested data."""
        # Project just names and ages
        name_age = project(self.people, ["person.name", "person.age"])
        
        for person in name_age:
            self.assertIn("person", person)
            p = person["person"]
            self.assertIn("name", p)
            self.assertIn("age", p)
            # Should not have other fields like job, location
            self.assertNotIn("job", p)
            self.assertNotIn("location", p)
        
        # Project nested paths
        locations = project(self.people, ["person.location.city", "person.location.state"])
        
        for person in locations:
            loc = person["person"]["location"]
            self.assertIn("city", loc)
            self.assertIn("state", loc)
            # Should not have country
            self.assertNotIn("country", loc)
    
    def test_join_operations(self):
        """Test joining people with companies."""
        # Join people with their companies
        joined = join(
            self.people,
            self.companies,
            on=[("person.job.company_name", "name")]
        )

        # Should have same number of people (inner join)
        self.assertEqual(len(joined), len(self.people))

        # Each result should have both person and company data
        for row in joined:
            self.assertIn("person", row)  # From people
            self.assertIn("industry", row)  # From companies
            self.assertIn("headquarters", row)  # From companies
            # Note: 'name' from companies is excluded as it's the join key
            # The value is available via person.job.company_name
    
    def test_groupby_operations(self):
        """Test groupby operations."""
        # Group by company
        grouped = groupby_with_metadata(self.people, "person.job.company_name")

        # Check metadata was added (uses _groups list format)
        for person in grouped:
            self.assertIn("_groups", person)
            self.assertIn("_group_size", person)
            self.assertEqual(len(person["_groups"]), 1)
            self.assertEqual(person["_groups"][0]["field"], "person.job.company_name")

        # Verify grouping is correct
        company_groups = {}
        for person in grouped:
            company = person["_groups"][0]["value"]
            if company not in company_groups:
                company_groups[company] = []
            company_groups[company].append(person)

        # Check each group has consistent company name
        for company_name, group_members in company_groups.items():
            for member in group_members:
                self.assertEqual(
                    member["person"]["job"]["company_name"],
                    company_name
                )
    
    def test_aggregation_operations(self):
        """Test aggregation operations."""
        # Group people by location state
        grouped = groupby_with_metadata(self.people, "person.location.state")

        # Aggregate average salary by state (string format with named outputs)
        agg_result = aggregate_grouped_data(
            grouped,
            "avg_salary=avg(person.job.salary),count,max_age=max(person.age)"
        )

        # Should have one row per state
        states = set(p["person"]["location"]["state"] for p in self.people)
        self.assertEqual(len(agg_result), len(states))

        # Each result should have aggregated fields
        for row in agg_result:
            self.assertIn("avg_salary", row)
            self.assertIn("count", row)
            self.assertIn("max_age", row)
            self.assertIsInstance(row["avg_salary"], (int, float))
            self.assertIsInstance(row["count"], int)
            self.assertIsInstance(row["max_age"], (int, float))


class TestCLIIntegration(unittest.TestCase):
    """Test CLI operations using generated datasets."""
    
    def setUp(self):
        """Set up test data files."""
        self.generator = TestDataGenerator(seed=456)
        self.companies, self.people = self.generator.create_minimal_dataset()
    
    def test_cli_basic_operations(self):
        """Test basic CLI operations with generated data."""
        with TempDataFiles(self.companies, self.people) as (companies_file, people_file):
            # Test select operation - file must come right after the expression
            stdout, stderr, returncode = run_ja_command(
                f"select 'person.age > 25' {people_file}"
            )
            self.assertEqual(returncode, 0, f"select failed: {stderr}")

            # Parse and verify results
            results = parse_jsonl_output(stdout)
            for person in results:
                self.assertGreater(person["person"]["age"], 25)

            # Test project operation
            stdout, stderr, returncode = run_ja_command(
                f"project id,person.name.first {people_file}"
            )
            self.assertEqual(returncode, 0, f"project failed: {stderr}")
    
    def test_cli_join_operations(self):
        """Test CLI join operations."""
        with TempDataFiles(self.companies, self.people) as (companies_file, people_file):
            # Test join operation: ja join left right --on 'left_col=right_col'
            stdout, stderr, returncode = run_ja_command(
                f"join {people_file} {companies_file} --on person.job.company_name=name"
            )
            self.assertEqual(returncode, 0, f"join failed: {stderr}")

            # Parse results
            results = parse_jsonl_output(stdout)

            # Should have joined data
            for row in results:
                self.assertIn("person", row)  # From people
                self.assertIn("industry", row)  # From companies
    
    def test_cli_aggregation(self):
        """Test CLI aggregation operations."""
        with TempDataFiles(self.companies, self.people) as (companies_file, people_file):
            # Test groupby with --agg flag (file must come before options)
            stdout, stderr, returncode = run_ja_command(
                f"groupby person.location.state {people_file} --agg count"
            )
            self.assertEqual(returncode, 0, f"groupby --agg failed: {stderr}")

            # Parse results
            results = parse_jsonl_output(stdout)

            # Should have aggregated data
            for row in results:
                self.assertIn("count", row)
                self.assertIsInstance(row["count"], int)
                self.assertGreater(row["count"], 0)


class TestDataGeneratorConsistency(unittest.TestCase):
    """Test that the data generator produces consistent results."""
    
    def test_deterministic_generation(self):
        """Test that same seed produces same data."""
        gen1 = TestDataGenerator(seed=999)
        companies1, people1 = gen1.create_minimal_dataset()
        
        gen2 = TestDataGenerator(seed=999)
        companies2, people2 = gen2.create_minimal_dataset()
        
        # Should be identical
        self.assertEqual(companies1, companies2)
        self.assertEqual(people1, people2)
    
    def test_different_seeds_produce_different_data(self):
        """Test that different seeds produce different people data."""
        gen1 = TestDataGenerator(seed=111)
        companies1, people1 = gen1.create_minimal_dataset()

        gen2 = TestDataGenerator(seed=222)
        companies2, people2 = gen2.create_minimal_dataset()

        # Companies are deterministic by design (based on index, not random)
        # But people should differ due to random age, salary, projects
        self.assertNotEqual(people1, people2)
    
    def test_scaling_consistency(self):
        """Test that different dataset sizes maintain data quality."""
        generator = TestDataGenerator(seed=333)
        
        # Test different sizes
        companies_min, people_min = generator.create_minimal_dataset()
        self.assertEqual(len(companies_min), 5)
        self.assertEqual(len(people_min), 20)
        
        generator.setup_randomness()  # Reset to same seed
        companies_small, people_small = generator.create_small_dataset()
        self.assertEqual(len(companies_small), 10)
        self.assertEqual(len(people_small), 50)
        
        # Verify data quality is maintained
        all_datasets = [
            (companies_min, people_min),
            (companies_small, people_small)
        ]
        
        for companies, people in all_datasets:
            # Check company names are unique
            company_names = [c["name"] for c in companies]
            self.assertEqual(len(company_names), len(set(company_names)))
            
            # Check person IDs are unique
            person_ids = [p["id"] for p in people]
            self.assertEqual(len(person_ids), len(set(person_ids)))
            
            # Check referential integrity
            company_name_set = set(company_names)
            for person in people:
                self.assertIn(person["person"]["job"]["company_name"], company_name_set)


if __name__ == "__main__":
    unittest.main()
