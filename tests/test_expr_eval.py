"""Unit tests for the expression evaluation engine."""

import pytest

from ja.expr import ExprEval


class TestExprEval:
    """Test suite for the ExprEval class."""

    @pytest.fixture
    def parser(self):
        """Provides an instance of ExprEval for each test."""
        return ExprEval()

    @pytest.fixture
    def sample_data(self):
        """Provides a sample data object for context-based tests."""
        return {
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "active": True,
            "score": None,
            "user": {
                "id": 123,
                "profile": {"type": "premium", "credits": 50},
                "preferences": {"theme": "dark"},
            },
            "orders": [{"id": 1, "amount": 100.50}, {"id": 2, "amount": 75.25}],
            "salary": 80000,
            "bonus": 5000,
        }

    # Tests for parse_value
    @pytest.mark.parametrize(
        "input_str, expected",
        [
            ("42", 42),
            ("3.14", 3.14),
            ("-10", -10),
            ("0", 0),
            ("true", True),
            ("True", True),
            ("false", False),
            ("False", False),
            ("null", None),
            ("None", None),
            ("active", "active"),
            ("'quoted string'", "quoted string"),
            ('"double quoted"', "double quoted"),
            ("user@example.com", "user@example.com"),
            ("", ""),
        ],
    )
    def test_parse_value(self, parser, input_str, expected):
        """Test parsing of various literal value strings."""
        assert parser.parse_value(input_str) == expected

    # Tests for get_field_value
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("name", "Alice"),
            ("age", 30),
            ("active", True),
            ("score", None),
            ("user.id", 123),
            ("user.profile.type", "premium"),
            ("orders[0].id", 1),
            ("orders[1].amount", 75.25),
            ("missing_field", None),
            ("user.missing", None),
            ("orders[10].id", None),
        ],
    )
    def test_get_field_value(self, parser, sample_data, path, expected):
        """Test getting values from simple, nested, and array fields."""
        assert parser.get_field_value(sample_data, path) == expected

    # Tests for set_field_value
    def test_set_field_value_simple(self, parser):
        """Test setting a value on a simple top-level field."""
        data = {"a": 1}
        parser.set_field_value(data, "b", 2)
        assert data == {"a": 1, "b": 2}

    def test_set_field_value_nested(self, parser):
        """Test setting a value on a nested field, creating dicts as needed."""
        data = {"a": {"b": 1}}
        parser.set_field_value(data, "a.c.d", 100)
        assert data == {"a": {"b": 1, "c": {"d": 100}}}

    # Tests for evaluate (boolean/comparison expressions)
    @pytest.mark.parametrize(
        "expression, expected",
        [
            # Numeric comparisons
            ("age == 30", True),
            ("age != 25", True),
            ("age > 25", True),
            ("age >= 30", True),
            ("age < 35", True),
            ("age <= 30", True),
            ("age == 31", False),
            # String comparisons (unquoted)
            ("name == Alice", True),
            ("user.profile.type == premium", True),
            ("email == alice@example.com", True),
            ("name == Bob", False),
            # Boolean comparisons
            ("active == true", True),
            ("active != false", True),
            # Null comparisons
            ("score == null", True),
            ("score != null", False),
            ("name != null", True),
            # Nested field comparisons
            ("user.id == 123", True),
            ("user.profile.credits > 40", True),
            ("orders[0].amount > 100", True),
            # Field vs Field comparisons
            ("salary > bonus", True),
            ("age == bonus", False),
            # Existence/Truthiness checks
            ("name", True),
            ("active", True),
            ("score", False),  # None is falsy
            ("missing_field", False),
        ],
    )
    def test_evaluate(self, parser, sample_data, expression, expected):
        """Test evaluation of various comparison and truthiness expressions."""
        assert parser.evaluate(expression, sample_data) is expected

    # Tests for evaluate_arithmetic
    @pytest.mark.parametrize(
        "expression, expected",
        [
            # Field and literal
            ("salary * 1.1", 88000.0),
            ("age + 5", 35.0),
            ("credits - 10", None),  # 'credits' is not a top-level field
            ("user.profile.credits - 10", 40.0),
            ("salary / 2", 40000.0),
            # Field and field
            ("salary + bonus", 85000.0),
            ("salary - bonus", 75000.0),
            ("age * bonus", 150000.0),
            # Literal and literal
            ("10 * 5", 50.0),
            # Non-arithmetic expression
            ("name", None),
            # Missing field
            ("missing * 2", None),
        ],
    )
    def test_evaluate_arithmetic(self, parser, sample_data, expression, expected):
        """Test evaluation of arithmetic expressions."""
        result = parser.evaluate_arithmetic(expression, sample_data)
        if expected is None:
            assert result is None
        else:
            assert result == pytest.approx(expected)
