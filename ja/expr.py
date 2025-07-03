"""Expression parser for ja commands.

This module provides a lightweight expression parser that allows intuitive
syntax without quotes for most common cases.
"""

import operator
import re
from typing import Any, Dict, List, Optional, Tuple, Union


class ExprEval:
    """Parse and evaluate expressions for filtering, comparison, and arithmetic."""

    def __init__(self):
        # Operators in precedence order (longest first to handle >= before >)
        self.operators = [
            ("==", operator.eq),
            ("!=", operator.ne),
            (">=", operator.ge),
            ("<=", operator.le),
            (">", operator.gt),
            ("<", operator.lt),
        ]

    def parse_value(self, value_str: str) -> Any:
        """Parse a value string into appropriate Python type.

        Examples:
            "123" -> 123
            "12.5" -> 12.5
            "true" -> True
            "false" -> False
            "null" -> None
            "active" -> "active" (string)
        """
        value_str = value_str.strip()

        # Empty string
        if not value_str:
            return ""

        # Boolean literals (case-insensitive)
        if value_str.lower() == "true":
            return True
        if value_str.lower() == "false":
            return False

        # Null literal
        if value_str.lower() in ("null", "none"):
            return None

        # Numbers
        try:
            if "." in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass

        # Quoted strings (remove quotes)
        if (value_str.startswith('"') and value_str.endswith('"')) or (
            value_str.startswith("'") and value_str.endswith("'")
        ):
            return value_str[1:-1]

        # Unquoted strings (the nice default!)
        return value_str

    def get_field_value(self, obj: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested object using dot notation.

        Examples:
            get_field_value({"user": {"name": "Alice"}}, "user.name") -> "Alice"
            get_field_value({"items": [{"id": 1}]}, "items[0].id") -> 1
        """
        if not field_path:
            return obj

        current = obj

        # Handle array indexing and dots
        parts = re.split(r"\.|\[|\]", field_path)
        parts = [p for p in parts if p]  # Remove empty strings

        for part in parts:
            if current is None:
                return None

            # Try as dict key
            if isinstance(current, dict):
                current = current.get(part)
            # Try as array index
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx] if 0 <= idx < len(current) else None
                except (ValueError, IndexError):
                    return None
            else:
                return None

        return current

    def set_field_value(self, obj: Dict[str, Any], field_path: str, value: Any) -> None:
        """Set value in nested object using dot notation."""
        if not field_path:
            return

        parts = field_path.split(".")
        current = obj

        # Navigate to the parent of the target field
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        # Set the value
        current[parts[-1]] = value

    def evaluate_comparison(self, left: Any, op_str: str, right: Any) -> bool:
        """Evaluate a comparison operation."""
        op_func = None
        for op, func in self.operators:
            if op == op_str:
                op_func = func
                break

        if op_func is None:
            raise ValueError(f"Unknown operator: {op_str}")

        # Special handling for null comparisons
        if left is None or right is None:
            if op_str == "==":
                return left == right
            elif op_str == "!=":
                return left != right
            else:
                return False

        # Type coercion for comparison
        try:
            return op_func(left, right)
        except (TypeError, ValueError):
            # If comparison fails, try string comparison
            try:
                return op_func(str(left), str(right))
            except:
                return False

    def evaluate(self, expr: str, context: Dict[str, Any]) -> bool:
        """Parse and evaluate an expression.

        Examples:
            "status == active"
            "age > 30"
            "user.type == premium"
        """
        expr = expr.strip()

        # Empty expression is false
        if not expr:
            return False

        # Check for operators
        for op_str, op_func in self.operators:
            if op_str in expr:
                # Split on the FIRST occurrence of the operator
                parts = expr.split(op_str, 1)
                if len(parts) == 2:
                    left_expr = parts[0].strip()
                    right_expr = parts[1].strip()

                    # Left side is always a field path
                    left_val = self.get_field_value(context, left_expr)

                    # Right side: check if it's a field or a literal
                    # A token is a field if it exists as a key in the context
                    # and is not a boolean/null keyword.
                    if right_expr in context and right_expr.lower() not in ['true', 'false', 'null', 'none']:
                        # It's a field reference
                        right_val = self.get_field_value(context, right_expr)
                    else:
                        # It's a literal value
                        right_val = self.parse_value(right_expr)

                    return self.evaluate_comparison(left_val, op_str, right_val)

        # No operator found - treat as existence/truthiness check
        value = self.get_field_value(context, expr)
        return bool(value)

    def evaluate_arithmetic(
        self, expr: str, context: Dict[str, Any]
    ) -> Optional[float]:
        """Evaluate simple arithmetic expressions.

        Examples:
            "amount * 1.1"
            "score + bonus"
        """
        # Simple arithmetic support
        for op, func in [
            ("*", operator.mul),
            ("+", operator.add),
            ("-", operator.sub),
            ("/", operator.truediv),
        ]:
            if op in expr:
                parts = expr.split(op, 1)
                if len(parts) == 2:
                    left_str = parts[0].strip()
                    right_str = parts[1].strip()

                    # Get left value (field or literal)
                    left_val = self.get_field_value(context, left_str)
                    if left_val is None:
                        left_val = self.parse_value(left_str)

                    # Get right value (field or literal)
                    right_val = self.get_field_value(context, right_str)
                    if right_val is None:
                        right_val = self.parse_value(right_str)

                    try:
                        return func(float(left_val), float(right_val))
                    except (TypeError, ValueError):
                        return None

        # No operator - try as field or literal
        val = self.get_field_value(context, expr)
        if val is None:
            val = self.parse_value(expr)

        try:
            return float(val)
        except (TypeError, ValueError):
            return None
