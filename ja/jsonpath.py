"""
Prototype JSONPath extension for jsonl-algebra

This module explores adding JSONPath-like capabilities to the existing
relational algebra operations.
"""

import re
from typing import Any, List, Dict, Union, Callable, Optional
from dataclasses import dataclass
from enum import Enum


class PathNodeType(Enum):
    ROOT = "root"
    FIELD = "field"
    WILDCARD = "wildcard"
    RECURSIVE = "recursive"
    INDEX = "index"
    SLICE = "slice"
    FILTER = "filter"


@dataclass
class PathNode:
    type: PathNodeType
    value: Any = None
    filter_expr: Optional[str] = None


class JSONPath:
    """
    A simplified JSONPath implementation for jsonl-algebra.

    Supports:
    - $.field - Direct field access
    - $.field.subfield - Nested field access
    - $[*] or $.* - Wildcard (any child)
    - $.field[*] - Array wildcard
    - $.field[0] - Array index
    - $.field[0:3] - Array slicing
    - $..field - Recursive descent
    - $.field[condition] - Filtered access
    """

    def __init__(self, path_string: str):
        self.path_string = path_string
        self.nodes = self._parse(path_string)

    def _parse(self, path: str) -> List[PathNode]:
        """Parse a JSONPath string into a list of PathNode objects."""
        if not path.startswith("$"):
            raise ValueError("JSONPath must start with '$'")

        # Remove the leading $
        path = path[1:]
        nodes = [PathNode(PathNodeType.ROOT)]

        # Simple regex-based parser (would need more sophistication for production)
        tokens = re.findall(r"\.\.|\[[^\]]*\]|\.?[^.\[]+", path)

        for token in tokens:
            if token == "..":
                nodes.append(PathNode(PathNodeType.RECURSIVE))
            elif token.startswith("[") and token.endswith("]"):
                inner = token[1:-1]
                if inner == "*":
                    nodes.append(PathNode(PathNodeType.WILDCARD))
                elif ":" in inner:
                    # Slice notation [start:end]
                    start, end = inner.split(":", 1)
                    nodes.append(
                        PathNode(
                            PathNodeType.SLICE,
                            (int(start) if start else 0, int(end) if end else None),
                        )
                    )
                elif inner.isdigit():
                    nodes.append(PathNode(PathNodeType.INDEX, int(inner)))
                else:
                    # Filter expression
                    nodes.append(PathNode(PathNodeType.FILTER, filter_expr=inner))
            elif token.startswith("."):
                field_name = token[1:]
                if field_name == "*":
                    nodes.append(PathNode(PathNodeType.WILDCARD))
                else:
                    nodes.append(PathNode(PathNodeType.FIELD, field_name))
            else:
                # Direct field name
                nodes.append(PathNode(PathNodeType.FIELD, token))

        return nodes

    def evaluate(self, data: Any) -> List[Any]:
        """
        Evaluate the JSONPath against data and return all matching values.

        Returns a list of values that match the path.
        """
        return self._evaluate_nodes(self.nodes, [data])

    def _evaluate_nodes(self, nodes: List[PathNode], contexts: List[Any]) -> List[Any]:
        """Recursively evaluate path nodes against current contexts."""
        if not nodes:
            return contexts

        node = nodes[0]
        remaining = nodes[1:]
        results = []

        for context in contexts:
            if node.type == PathNodeType.ROOT:
                results.extend(self._evaluate_nodes(remaining, [context]))

            elif node.type == PathNodeType.FIELD:
                if isinstance(context, dict) and node.value in context:
                    results.extend(
                        self._evaluate_nodes(remaining, [context[node.value]])
                    )

            elif node.type == PathNodeType.WILDCARD:
                if isinstance(context, dict):
                    results.extend(
                        self._evaluate_nodes(remaining, list(context.values()))
                    )
                elif isinstance(context, list):
                    results.extend(self._evaluate_nodes(remaining, context))

            elif node.type == PathNodeType.RECURSIVE:
                # Recursive descent - find all descendants
                descendants = self._get_all_descendants(context)
                results.extend(self._evaluate_nodes(remaining, descendants))

            elif node.type == PathNodeType.INDEX:
                if isinstance(context, list) and 0 <= node.value < len(context):
                    results.extend(
                        self._evaluate_nodes(remaining, [context[node.value]])
                    )

            elif node.type == PathNodeType.SLICE:
                if isinstance(context, list):
                    start, end = node.value
                    sliced = context[start:end]
                    results.extend(self._evaluate_nodes(remaining, sliced))

            elif node.type == PathNodeType.FILTER:
                # Simple filter evaluation (would need proper expression parser)
                if isinstance(context, list):
                    filtered = [
                        item
                        for item in context
                        if self._evaluate_filter(item, node.filter_expr)
                    ]
                    results.extend(self._evaluate_nodes(remaining, filtered))

        return results

    def _get_all_descendants(self, obj: Any) -> List[Any]:
        """Get all descendant values recursively."""
        descendants = [obj]

        if isinstance(obj, dict):
            for value in obj.values():
                descendants.extend(self._get_all_descendants(value))
        elif isinstance(obj, list):
            for item in obj:
                descendants.extend(self._get_all_descendants(item))

        return descendants

    def _evaluate_filter(self, item: Any, filter_expr: str) -> bool:
        """Evaluate a simple filter expression (placeholder implementation)."""
        # This would need a proper expression parser
        # For now, just handle simple cases like "price > 10"
        try:
            if ">" in filter_expr:
                field, value = filter_expr.split(">", 1)
                field = field.strip()
                value = float(value.strip())
                return (
                    isinstance(item, dict)
                    and field in item
                    and float(item[field]) > value
                )
            elif "==" in filter_expr:
                field, value = filter_expr.split("==", 1)
                field = field.strip()
                value = value.strip().strip("\"'")
                return (
                    isinstance(item, dict)
                    and field in item
                    and str(item[field]) == value
                )
        except:
            pass
        return False


class PathQuantifier(Enum):
    ANY = "any"
    ALL = "all"
    NONE = "none"


def select_path(
    relation: List[Dict],
    path_expr: str,
    predicate: Callable[[Any], bool] = None,
    quantifier: PathQuantifier = PathQuantifier.ANY,
) -> List[Dict]:
    """
    Select rows where a JSONPath expression matches according to the quantifier.

    Args:
        relation: List of dictionaries to filter
        path_expr: JSONPath expression
        predicate: Optional predicate function to apply to path values
        quantifier: ANY (default), ALL, or NONE

    Returns:
        Filtered list of dictionaries
    """
    path = JSONPath(path_expr)
    result = []

    for row in relation:
        values = path.evaluate(row)

        if predicate is None:
            # Just check if path exists (has values)
            matches = len(values) > 0
        else:
            # Apply predicate to values
            predicate_results = [predicate(v) for v in values]

            if quantifier == PathQuantifier.ANY:
                matches = any(predicate_results)
            elif quantifier == PathQuantifier.ALL:
                matches = all(predicate_results) if predicate_results else False
            elif quantifier == PathQuantifier.NONE:
                matches = not any(predicate_results)
            else:
                matches = False

        if matches:
            result.append(row)

    return result


def select_any(
    relation: List[Dict], path_expr: str, predicate: Callable[[Any], bool] = None
) -> List[Dict]:
    """
    Select rows where ANY element in the path matches the predicate.

    This is equivalent to select_path() with PathQuantifier.ANY (the default).
    """
    return select_path(relation, path_expr, predicate, PathQuantifier.ANY)


def select_all(
    relation: List[Dict], path_expr: str, predicate: Callable[[Any], bool] = None
) -> List[Dict]:
    """
    Select rows where ALL elements in the path match the predicate.

    Args:
        relation: List of dictionaries to filter
        path_expr: JSONPath expression
        predicate: Predicate function to apply to path values

    Returns:
        Rows where all path values satisfy the predicate
    """
    return select_path(relation, path_expr, predicate, PathQuantifier.ALL)


def select_none(
    relation: List[Dict], path_expr: str, predicate: Callable[[Any], bool] = None
) -> List[Dict]:
    """
    Select rows where NO elements in the path match the predicate.

    Args:
        relation: List of dictionaries to filter
        path_expr: JSONPath expression
        predicate: Predicate function to apply to path values

    Returns:
        Rows where no path values satisfy the predicate
    """
    return select_path(relation, path_expr, predicate, PathQuantifier.NONE)


def project_template(relation: List[Dict], template: Dict[str, str]) -> List[Dict]:
    """
    Project using template expressions that can include JSONPath and aggregations.

    Args:
        relation: List of dictionaries to project
        template: Dictionary mapping output field names to template expressions

    Returns:
        List of dictionaries with projected fields
    """
    result = []

    for row in relation:
        projected_row = {}

        for output_field, template_expr in template.items():
            try:
                value = _evaluate_template_expression(row, template_expr)
                projected_row[output_field] = value
            except Exception as e:
                # Handle errors gracefully
                projected_row[output_field] = None

        result.append(projected_row)

    return result


def _evaluate_template_expression(row: Dict, expr: str) -> Any:
    """
    Evaluate a template expression against a row.

    Supports:
    - JSONPath expressions: $.field.subfield
    - Aggregation functions: sum($.array[*]), count($.array[*])
    - Simple functions: exists($.field)
    """
    expr = expr.strip()

    # Handle aggregation functions
    if expr.startswith(("sum(", "avg(", "min(", "max(", "count(")):
        func_name = expr.split("(")[0]
        path_expr = expr[len(func_name) + 1 : -1]  # Extract path from function

        path = JSONPath(path_expr)
        values = path.evaluate(row)

        if func_name == "sum":
            return sum(v for v in values if isinstance(v, (int, float)))
        elif func_name == "avg":
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        elif func_name == "min":
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            return min(numeric_values) if numeric_values else None
        elif func_name == "max":
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            return max(numeric_values) if numeric_values else None
        elif func_name == "count":
            return len(values)

    # Handle existence check
    elif expr.startswith("exists("):
        path_expr = expr[7:-1]  # Remove 'exists(' and ')'
        path = JSONPath(path_expr)
        values = path.evaluate(row)
        return len(values) > 0

    # Handle direct JSONPath
    elif expr.startswith("$"):
        path = JSONPath(expr)
        values = path.evaluate(row)

        # For array wildcard expressions, always return as list
        if "[*]" in expr or ".*" in expr:
            return values

        # Return single value if only one, otherwise return list
        if len(values) == 1:
            return values[0]
        elif len(values) == 0:
            return None
        else:
            return values

    # Fallback: treat as literal
    else:
        return expr
