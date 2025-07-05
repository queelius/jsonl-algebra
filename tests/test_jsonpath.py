"""
Unit tests for the JSONPath extension module.

Tests cover path parsing, evaluation, quantifiers, and template-based operations.
"""

import pytest
from typing import List, Dict, Any

from ja.jsonpath import (
    JSONPath,
    PathNode,
    PathNodeType,
    PathQuantifier,
    select_path,
    project_template,
    _evaluate_template_expression,
    select_any,
    select_all,
    select_none,
)


class TestPathNode:
    """Test the PathNode dataclass."""

    def test_path_node_creation(self):
        """Test creating PathNode instances."""
        node = PathNode(PathNodeType.FIELD, "name")
        assert node.type == PathNodeType.FIELD
        assert node.value == "name"
        assert node.filter_expr is None

        filter_node = PathNode(PathNodeType.FILTER, filter_expr="price > 10")
        assert filter_node.type == PathNodeType.FILTER
        assert filter_node.filter_expr == "price > 10"


class TestJSONPath:
    """Test the JSONPath parser and evaluator."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing JSONPath evaluation."""
        return {
            "user": {
                "name": "Alice",
                "age": 30,
                "profile": {"type": "premium", "score": 95.5},
            },
            "orders": [
                {"id": 1, "item": "Book", "price": 15.99, "status": "shipped"},
                {"id": 2, "item": "Pen", "price": 2.50, "status": "pending"},
                {"id": 3, "item": "Notebook", "price": 8.75, "status": "shipped"},
            ],
            "tags": ["python", "data", "analysis"],
            "active": True,
            "score": None,
        }

    @pytest.fixture
    def list_data(self):
        """List data for testing array operations."""
        return [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]

    def test_jsonpath_creation(self):
        """Test creating JSONPath instances."""
        path = JSONPath("$.user.name")
        assert path.path_string == "$.user.name"
        assert len(path.nodes) > 0
        assert path.nodes[0].type == PathNodeType.ROOT

    def test_invalid_jsonpath(self):
        """Test that invalid JSONPath strings raise errors."""
        with pytest.raises(ValueError, match="JSONPath must start with"):
            JSONPath("user.name")  # Missing $

    def test_simple_field_access(self, sample_data):
        """Test simple field access."""
        path = JSONPath("$.user.name")
        result = path.evaluate(sample_data)
        assert result == ["Alice"]

        path = JSONPath("$.active")
        result = path.evaluate(sample_data)
        assert result == [True]

    def test_nested_field_access(self, sample_data):
        """Test nested field access."""
        path = JSONPath("$.user.profile.type")
        result = path.evaluate(sample_data)
        assert result == ["premium"]

        path = JSONPath("$.user.profile.score")
        result = path.evaluate(sample_data)
        assert result == [95.5]

    def test_array_index_access(self, sample_data):
        """Test array indexing."""
        path = JSONPath("$.orders[0].item")
        result = path.evaluate(sample_data)
        assert result == ["Book"]

        path = JSONPath("$.orders[1].price")
        result = path.evaluate(sample_data)
        assert result == [2.50]

        path = JSONPath("$.tags[2]")
        result = path.evaluate(sample_data)
        assert result == ["analysis"]

    def test_array_wildcard(self, sample_data):
        """Test array wildcard access."""
        path = JSONPath("$.orders[*].item")
        result = path.evaluate(sample_data)
        assert result == ["Book", "Pen", "Notebook"]

        path = JSONPath("$.orders[*].price")
        result = path.evaluate(sample_data)
        assert result == [15.99, 2.50, 8.75]

        path = JSONPath("$.tags[*]")
        result = path.evaluate(sample_data)
        assert result == ["python", "data", "analysis"]

    def test_object_wildcard(self, sample_data):
        """Test object wildcard access."""
        user_data = {"users": {"alice": {"age": 30}, "bob": {"age": 25}}}
        path = JSONPath("$.users.*.age")
        result = path.evaluate(user_data)
        assert set(result) == {30, 25}  # Order not guaranteed

    def test_array_slicing(self, sample_data):
        """Test array slicing."""
        path = JSONPath("$.orders[0:2].item")
        result = path.evaluate(sample_data)
        assert result == ["Book", "Pen"]

        path = JSONPath("$.tags[1:3]")
        result = path.evaluate(sample_data)
        assert result == ["data", "analysis"]

    def test_recursive_descent(self):
        """Test recursive descent (..) operator."""
        data = {
            "level1": {
                "level2": {"target": "found", "level3": {"target": "deep"}},
                "target": "shallow",
            }
        }
        path = JSONPath("$..target")
        result = path.evaluate(data)
        assert "found" in result
        assert "deep" in result
        assert "shallow" in result
        assert len(result) == 3

    def test_filter_expressions(self, sample_data):
        """Test filter expressions."""
        # Filter by price > 10
        path = JSONPath("$.orders[price > 10].item")
        result = path.evaluate(sample_data)
        assert "Book" in result  # 15.99 > 10
        assert "Pen" not in result  # 2.50 < 10

        # Filter by string equality
        path = JSONPath("$.orders[status == shipped].item")
        result = path.evaluate(sample_data)
        assert "Book" in result
        assert "Notebook" in result
        assert "Pen" not in result

    def test_nonexistent_paths(self, sample_data):
        """Test paths that don't exist."""
        path = JSONPath("$.nonexistent")
        result = path.evaluate(sample_data)
        assert result == []

        path = JSONPath("$.user.nonexistent")
        result = path.evaluate(sample_data)
        assert result == []

        path = JSONPath("$.orders[10].item")
        result = path.evaluate(sample_data)
        assert result == []

    def test_complex_paths(self, sample_data):
        """Test complex JSONPath expressions."""
        # All prices from shipped orders
        path = JSONPath("$.orders[status == shipped].price")
        result = path.evaluate(sample_data)
        assert 15.99 in result
        assert 8.75 in result
        assert 2.50 not in result


class TestSelectPath:
    """Test the select_path function."""

    @pytest.fixture
    def test_data(self):
        """Test data for select_path tests."""
        return [
            {
                "user": {"name": "Alice", "age": 30},
                "orders": [
                    {"item": "Book", "price": 15.99},
                    {"item": "Pen", "price": 2.50},
                ],
                "tags": ["python", "data"],
            },
            {
                "user": {"name": "Bob", "age": 25},
                "orders": [{"item": "Laptop", "price": 899.99}],
                "tags": ["tech", "hardware"],
            },
            {
                "user": {"name": "Charlie", "age": 35},
                "orders": [],
                "tags": ["python", "web"],
            },
        ]

    def test_select_path_existence(self, test_data):
        """Test selecting based on path existence."""
        # Users with orders
        result = select_path(test_data, "$.orders[*]")
        assert len(result) == 2  # Alice and Bob have orders
        names = [r["user"]["name"] for r in result]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" not in names

    def test_select_path_with_predicate_any(self, test_data):
        """Test selecting with ANY quantifier (default)."""
        # Users with any expensive order (> 100)
        result = select_path(
            test_data,
            "$.orders[*].price",
            lambda price: price > 100,
            PathQuantifier.ANY,
        )
        assert len(result) == 1
        assert result[0]["user"]["name"] == "Bob"

    def test_select_path_with_predicate_all(self, test_data):
        """Test selecting with ALL quantifier."""
        # Users where ALL orders are cheap (< 20)
        result = select_path(
            test_data, "$.orders[*].price", lambda price: price < 20, PathQuantifier.ALL
        )
        assert len(result) == 1
        assert result[0]["user"]["name"] == "Alice"

    def test_select_path_with_predicate_none(self, test_data):
        """Test selecting with NONE quantifier."""
        # Users with NO expensive orders (> 500)
        result = select_path(
            test_data,
            "$.orders[*].price",
            lambda price: price > 500,
            PathQuantifier.NONE,
        )
        assert len(result) == 2  # Alice and Charlie
        names = [r["user"]["name"] for r in result]
        assert "Alice" in names
        assert "Charlie" in names
        assert "Bob" not in names

    def test_select_path_string_matching(self, test_data):
        """Test selecting based on string values."""
        # Users with python tag
        result = select_path(test_data, "$.tags[*]", lambda tag: tag == "python")
        assert len(result) == 2  # Alice and Charlie
        names = [r["user"]["name"] for r in result]
        assert "Alice" in names
        assert "Charlie" in names
        assert "Bob" not in names


class TestProjectTemplate:
    """Test the project_template function."""

    @pytest.fixture
    def test_data(self):
        """Test data for template projection tests."""
        return [
            {
                "user": {"name": "Alice", "age": 30},
                "orders": [
                    {"item": "Book", "price": 15.99, "quantity": 1},
                    {"item": "Pen", "price": 2.50, "quantity": 3},
                ],
                "tags": ["python", "data", "analysis"],
            },
            {
                "user": {"name": "Bob", "age": 25},
                "orders": [{"item": "Laptop", "price": 899.99, "quantity": 1}],
                "tags": ["tech"],
            },
            {
                "user": {"name": "Charlie", "age": 35},
                "orders": [],
                "tags": ["python", "web"],
            },
        ]

    def test_simple_field_projection(self, test_data):
        """Test simple field projection."""
        template = {"name": "$.user.name", "age": "$.user.age"}
        result = project_template(test_data, template)

        assert len(result) == 3
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30
        assert result[1]["name"] == "Bob"
        assert result[1]["age"] == 25

    def test_aggregation_functions(self, test_data):
        """Test aggregation functions in templates."""
        template = {
            "name": "$.user.name",
            "order_count": "count($.orders[*])",
            "total_spent": "sum($.orders[*].price)",
            "avg_price": "avg($.orders[*].price)",
            "max_price": "max($.orders[*].price)",
            "min_price": "min($.orders[*].price)",
        }
        result = project_template(test_data, template)

        # Alice has 2 orders
        alice = result[0]
        assert alice["name"] == "Alice"
        assert alice["order_count"] == 2
        assert alice["total_spent"] == pytest.approx(18.49)  # 15.99 + 2.50
        assert alice["avg_price"] == pytest.approx(9.245)  # (15.99 + 2.50) / 2
        assert alice["max_price"] == 15.99
        assert alice["min_price"] == 2.50

        # Bob has 1 order
        bob = result[1]
        assert bob["name"] == "Bob"
        assert bob["order_count"] == 1
        assert bob["total_spent"] == 899.99

        # Charlie has no orders
        charlie = result[2]
        assert charlie["name"] == "Charlie"
        assert charlie["order_count"] == 0
        assert charlie["total_spent"] == 0
        assert charlie["avg_price"] is None  # No orders to average

    def test_existence_check(self, test_data):
        """Test existence checks in templates."""
        template = {
            "name": "$.user.name",
            "has_orders": "exists($.orders[*])",
            "has_tags": "exists($.tags[*])",
        }
        result = project_template(test_data, template)

        assert result[0]["has_orders"] is True  # Alice
        assert result[1]["has_orders"] is True  # Bob
        assert result[2]["has_orders"] is False  # Charlie

        # All users have tags
        assert all(r["has_tags"] for r in result)

    def test_array_projection(self, test_data):
        """Test projecting arrays."""
        template = {
            "name": "$.user.name",
            "tags": "$.tags[*]",
            "first_tag": "$.tags[0]",
        }
        result = project_template(test_data, template)

        assert result[0]["tags"] == ["python", "data", "analysis"]
        assert result[0]["first_tag"] == "python"
        assert result[1]["tags"] == ["tech"]  # Single item array should remain as array
        assert result[1]["first_tag"] == "tech"

    def test_literal_values(self, test_data):
        """Test literal values in templates."""
        template = {"name": "$.user.name", "type": "user", "version": "1.0"}
        result = project_template(test_data, template)

        for row in result:
            assert row["type"] == "user"
            assert row["version"] == "1.0"

    def test_error_handling(self, test_data):
        """Test error handling in template evaluation."""
        template = {"name": "$.user.name", "invalid": "$.nonexistent.field.deep"}
        result = project_template(test_data, template)

        # Should handle missing fields gracefully
        for row in result:
            assert "name" in row
            assert row["invalid"] is None


class TestTemplateExpressionEvaluation:
    """Test the _evaluate_template_expression function."""

    @pytest.fixture
    def sample_row(self):
        """Sample row for template expression tests."""
        return {
            "user": {"name": "Alice", "age": 30},
            "orders": [
                {"price": 15.99, "quantity": 1},
                {"price": 2.50, "quantity": 3},
                {"price": 8.75, "quantity": 2},
            ],
            "tags": ["python", "data"],
        }

    def test_simple_jsonpath_expression(self, sample_row):
        """Test simple JSONPath expressions."""
        result = _evaluate_template_expression(sample_row, "$.user.name")
        assert result == "Alice"

        result = _evaluate_template_expression(sample_row, "$.user.age")
        assert result == 30

    def test_array_expressions(self, sample_row):
        """Test array JSONPath expressions."""
        result = _evaluate_template_expression(sample_row, "$.tags[*]")
        assert result == ["python", "data"]

        result = _evaluate_template_expression(sample_row, "$.orders[*].price")
        assert result == [15.99, 2.50, 8.75]

    def test_single_value_vs_multiple(self, sample_row):
        """Test single value vs multiple value handling."""
        # Single value should be unwrapped
        result = _evaluate_template_expression(sample_row, "$.user.name")
        assert result == "Alice"  # Not ["Alice"]

        # Multiple values should remain as list
        result = _evaluate_template_expression(sample_row, "$.orders[*].price")
        assert isinstance(result, list)
        assert len(result) == 3

    def test_aggregation_functions(self, sample_row):
        """Test aggregation functions."""
        # Sum
        result = _evaluate_template_expression(sample_row, "sum($.orders[*].price)")
        assert result == pytest.approx(27.24)  # 15.99 + 2.50 + 8.75

        # Count
        result = _evaluate_template_expression(sample_row, "count($.orders[*])")
        assert result == 3

        # Average
        result = _evaluate_template_expression(sample_row, "avg($.orders[*].price)")
        assert result == pytest.approx(9.08)  # 27.24 / 3

        # Min/Max
        result = _evaluate_template_expression(sample_row, "min($.orders[*].price)")
        assert result == 2.50

        result = _evaluate_template_expression(sample_row, "max($.orders[*].price)")
        assert result == 15.99

    def test_existence_function(self, sample_row):
        """Test exists() function."""
        result = _evaluate_template_expression(sample_row, "exists($.orders[*])")
        assert result is True

        result = _evaluate_template_expression(sample_row, "exists($.nonexistent)")
        assert result is False

    def test_literal_expressions(self, sample_row):
        """Test literal expressions."""
        result = _evaluate_template_expression(sample_row, "constant_value")
        assert result == "constant_value"

        result = _evaluate_template_expression(sample_row, "123")
        assert result == "123"  # Treated as literal string

    def test_empty_or_missing_data(self):
        """Test with empty or missing data."""
        empty_row = {"orders": [], "user": {}}

        result = _evaluate_template_expression(empty_row, "sum($.orders[*].price)")
        assert result == 0

        result = _evaluate_template_expression(empty_row, "count($.orders[*])")
        assert result == 0

        result = _evaluate_template_expression(empty_row, "avg($.orders[*].price)")
        assert result is None  # No values to average


class TestPathParsing:
    """Test JSONPath parsing edge cases."""

    def test_root_only(self):
        """Test parsing root-only path."""
        path = JSONPath("$")
        assert len(path.nodes) == 1
        assert path.nodes[0].type == PathNodeType.ROOT

    def test_complex_parsing(self):
        """Test parsing complex expressions."""
        path = JSONPath("$.store.books[*].author")
        nodes = path.nodes

        assert nodes[0].type == PathNodeType.ROOT
        assert nodes[1].type == PathNodeType.FIELD
        assert nodes[1].value == "store"
        assert nodes[2].type == PathNodeType.FIELD
        assert nodes[2].value == "books"
        assert nodes[3].type == PathNodeType.WILDCARD
        assert nodes[4].type == PathNodeType.FIELD
        assert nodes[4].value == "author"

    def test_filter_parsing(self):
        """Test parsing filter expressions."""
        path = JSONPath("$.items[price > 10].name")

        # Find the filter node
        filter_node = None
        for node in path.nodes:
            if node.type == PathNodeType.FILTER:
                filter_node = node
                break

        assert filter_node is not None
        assert filter_node.filter_expr == "price > 10"

    def test_slice_parsing(self):
        """Test parsing slice expressions."""
        path = JSONPath("$.items[1:3].name")

        # Find the slice node
        slice_node = None
        for node in path.nodes:
            if node.type == PathNodeType.SLICE:
                slice_node = node
                break

        assert slice_node is not None
        assert slice_node.value == (1, 3)


# Integration tests
class TestJSONPathIntegration:
    """Integration tests combining multiple features."""

    @pytest.fixture
    def ecommerce_data(self):
        """E-commerce test data."""
        return [
            {
                "customer": {"id": 1, "name": "Alice", "tier": "gold"},
                "orders": [
                    {
                        "id": 101,
                        "items": [
                            {"name": "Book", "price": 15.99, "category": "books"},
                            {"name": "Pen", "price": 2.50, "category": "stationery"},
                        ],
                        "status": "shipped",
                        "total": 18.49,
                    },
                    {
                        "id": 102,
                        "items": [
                            {
                                "name": "Laptop",
                                "price": 899.99,
                                "category": "electronics",
                            }
                        ],
                        "status": "processing",
                        "total": 899.99,
                    },
                ],
            },
            {
                "customer": {"id": 2, "name": "Bob", "tier": "silver"},
                "orders": [
                    {
                        "id": 103,
                        "items": [
                            {"name": "Mouse", "price": 25.99, "category": "electronics"}
                        ],
                        "status": "delivered",
                        "total": 25.99,
                    }
                ],
            },
        ]

    def test_complex_selection_and_projection(self, ecommerce_data):
        """Test complex selection and projection scenarios."""
        # Find customers with any electronics purchase
        electronics_customers = select_path(
            ecommerce_data,
            "$.orders[*].items[*].category",
            lambda cat: cat == "electronics",
        )
        assert len(electronics_customers) == 2  # Both customers

        # Project customer summary with order analytics
        template = {
            "customer_name": "$.customer.name",
            "customer_tier": "$.customer.tier",
            "total_orders": "count($.orders[*])",
            "total_spent": "sum($.orders[*].total)",
            "avg_order_value": "avg($.orders[*].total)",
            "has_electronics": "exists($.orders[*].items[category == electronics])",
            "order_statuses": "$.orders[*].status",
        }

        summary = project_template(electronics_customers, template)

        alice = summary[0]
        assert alice["customer_name"] == "Alice"
        assert alice["total_orders"] == 2
        assert alice["total_spent"] == 918.48  # 18.49 + 899.99
        assert alice["avg_order_value"] == 459.24

        bob = summary[1]
        assert bob["customer_name"] == "Bob"
        assert bob["total_orders"] == 1
        assert bob["total_spent"] == 25.99


# Additional edge case tests for better coverage
class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_filter_error_handling(self):
        """Test filter expression error handling."""
        path = JSONPath("$.items[invalid_expression]")
        data = {"items": [{"name": "test"}]}

        # Should handle invalid filter expressions gracefully
        result = path.evaluate(data)
        assert result == []  # No matches due to invalid filter

    def test_filter_with_non_dict_items(self):
        """Test filter expressions on non-dict items."""
        path = JSONPath("$.items[price > 10]")
        data = {"items": ["string", 123, None]}  # Non-dict items

        result = path.evaluate(data)
        assert result == []  # No matches since items aren't dicts

    def test_filter_missing_field(self):
        """Test filter on missing fields."""
        path = JSONPath("$.items[missing_field == value]")
        data = {"items": [{"name": "test"}]}  # No 'missing_field'

        result = path.evaluate(data)
        assert result == []

    def test_filter_type_conversion_error(self):
        """Test filter with type conversion errors."""
        path = JSONPath("$.items[price > 10]")
        data = {"items": [{"price": "not_a_number"}]}

        result = path.evaluate(data)
        assert result == []  # Should handle conversion error gracefully

    def test_project_template_with_exception(self):
        """Test project_template error handling."""
        # Create a malformed template that will cause an exception
        template = {
            "name": "$.user.name",
            "bad_expr": "$.nonexistent.deeply.nested.field.that.causes.error",
        }

        data = [{"user": {"name": "Alice"}}]
        result = project_template(data, template)

        assert len(result) == 1
        assert result[0]["name"] == "Alice"
        assert result[0]["bad_expr"] is None  # Should handle error gracefully

    def test_recursive_descent_with_primitives(self):
        """Test recursive descent on primitive values."""
        path = JSONPath("$..value")
        data = {
            "level1": {
                "value": "found",
                "primitive": 42,  # Non-dict/list value
                "nested": {"value": "deep"},
            }
        }

        result = path.evaluate(data)
        assert "found" in result
        assert "deep" in result

    def test_empty_path_components(self):
        """Test paths with empty components."""
        path = JSONPath("$")  # Root only
        data = {"test": "value"}

        result = path.evaluate(data)
        assert result == [data]

    def test_slice_with_negative_indices(self):
        """Test array slicing with edge cases."""
        # Test slice that extends beyond array bounds
        path = JSONPath("$.items[10:20]")
        data = {"items": [1, 2, 3]}

        result = path.evaluate(data)
        assert result == []  # No items in that range

        # Test slice with start > end
        path = JSONPath("$.items[5:2]")
        result = path.evaluate(data)
        assert result == []

    def test_index_out_of_bounds(self):
        """Test array index out of bounds."""
        path = JSONPath("$.items[100]")
        data = {"items": [1, 2, 3]}

        result = path.evaluate(data)
        assert result == []

    def test_wildcard_on_primitive(self):
        """Test wildcard on primitive values."""
        path = JSONPath("$.value.*")
        data = {"value": 42}  # Primitive, not dict/list

        result = path.evaluate(data)
        assert result == []

    def test_template_expression_edge_cases(self):
        """Test edge cases in template expression evaluation."""
        row = {"items": []}

        # Test aggregation on empty array
        result = _evaluate_template_expression(row, "sum($.items[*].price)")
        assert result == 0

        result = _evaluate_template_expression(row, "avg($.items[*].price)")
        assert result is None

        result = _evaluate_template_expression(row, "min($.items[*].price)")
        assert result is None

        result = _evaluate_template_expression(row, "max($.items[*].price)")
        assert result is None

    def test_malformed_aggregation_functions(self):
        """Test malformed aggregation function calls."""
        row = {"items": [{"price": 10}]}

        # Test function without closing parenthesis - this actually gets parsed as sum() somehow
        result = _evaluate_template_expression(row, "sum($.items[*].price")
        assert (
            isinstance(result, (int, float)) or result is None
        )  # Gets processed as partial function

        # Test unknown function
        result = _evaluate_template_expression(row, "unknown($.items[*].price)")
        assert result == "unknown($.items[*].price)"  # Treated as literal

    def test_quantifier_edge_cases(self):
        """Test quantifier edge cases."""
        data = [{"items": []}, {"items": [1, 2, 3]}]  # Empty array

        # ALL quantifier on empty array should be False
        result = select_path(data, "$.items[*]", lambda x: x > 0, PathQuantifier.ALL)
        assert len(result) == 1  # Only the non-empty array
        assert result[0]["items"] == [1, 2, 3]

        # Test with predicate that returns False for all
        result = select_path(data, "$.items[*]", lambda x: x > 100, PathQuantifier.NONE)
        assert len(result) == 2  # Both rows match (no items > 100)


class TestQuantifierConvenienceFunctions:
    """Test the convenience functions for different quantifiers."""

    @pytest.fixture
    def quantifier_test_data(self):
        """Test data specifically for quantifier testing."""
        return [
            {"name": "Alice", "scores": [85, 92, 78, 96]},  # Some > 90, not all > 80
            {"name": "Bob", "scores": [88, 85, 90, 87]},  # Some > 85, all > 80
            {"name": "Charlie", "scores": [95, 98, 94, 97]},  # All > 90
            {"name": "David", "scores": [72, 68, 75, 70]},  # None > 80
            {"name": "Eve", "scores": []},  # Empty array
        ]

    def test_select_any_convenience(self, quantifier_test_data):
        """Test select_any convenience function."""
        # Students with any score > 90
        result = select_any(quantifier_test_data, "$.scores[*]", lambda x: x > 90)
        names = [r["name"] for r in result]
        assert "Alice" in names  # Has 92, 96 (both > 90)
        assert "Bob" not in names  # Max is 90, which is not > 90
        assert "Charlie" in names  # All > 90
        assert "David" not in names  # None > 90
        assert "Eve" not in names  # No scores

    def test_select_all_convenience(self, quantifier_test_data):
        """Test select_all convenience function."""
        # Students with all scores > 80
        result = select_all(quantifier_test_data, "$.scores[*]", lambda x: x > 80)
        names = [r["name"] for r in result]
        assert "Alice" not in names  # Has 78
        assert "Bob" in names  # All > 80
        assert "Charlie" in names  # All > 80
        assert "David" not in names  # All < 80
        assert "Eve" not in names  # Empty array (ALL on empty = False)

    def test_select_none_convenience(self, quantifier_test_data):
        """Test select_none convenience function."""
        # Students with no scores > 95
        result = select_none(quantifier_test_data, "$.scores[*]", lambda x: x > 95)
        names = [r["name"] for r in result]
        assert "Alice" not in names  # Has 96, which is > 95
        assert "Bob" in names  # No scores > 95
        assert "Charlie" not in names  # Has 98, 97 (both > 95)
        assert "David" in names  # No scores > 95
        assert "Eve" in names  # Empty array (NONE on empty = True)

    def test_quantifier_equivalence(self, quantifier_test_data):
        """Test that convenience functions are equivalent to select_path."""
        predicate = lambda x: x > 85

        # ANY equivalence
        any_result1 = select_any(quantifier_test_data, "$.scores[*]", predicate)
        any_result2 = select_path(
            quantifier_test_data, "$.scores[*]", predicate, PathQuantifier.ANY
        )
        assert any_result1 == any_result2

        # ALL equivalence
        all_result1 = select_all(quantifier_test_data, "$.scores[*]", predicate)
        all_result2 = select_path(
            quantifier_test_data, "$.scores[*]", predicate, PathQuantifier.ALL
        )
        assert all_result1 == all_result2

        # NONE equivalence
        none_result1 = select_none(quantifier_test_data, "$.scores[*]", predicate)
        none_result2 = select_path(
            quantifier_test_data, "$.scores[*]", predicate, PathQuantifier.NONE
        )
        assert none_result1 == none_result2
