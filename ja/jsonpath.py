"""
JSONPath operations for nested and hierarchical data in JSONL.

This module extends relational algebra with JSONPath capabilities, enabling
queries and transformations on nested JSON structures. It bridges the gap
between flat relational operations and document-oriented data.

Theoretical Foundation:
    JSONPath extends XPath concepts to JSON, providing a query language for
    hierarchical data structures. In the context of relational algebra:
    
    1. **Path-based Selection** (σ_path):
       Extends selection predicates to nested attributes
       σ_{path,p}(R) = {r ∈ R | p(path(r)) = true}
       
    2. **Path-based Projection** (π_path):
       Projects nested attributes into flat structure
       π_{template}(R) = {template(r) | r ∈ R}
       
    3. **Quantified Predicates**:
       - ∃ (exists/any): At least one path value matches
       - ∀ (all): Every path value matches
       - ¬∃ (none): No path value matches

JSONPath Syntax Supported:
    - `$` : Root object
    - `.field` : Direct field access
    - `..field` : Recursive descent
    - `[*]` : Array wildcard
    - `[n]` : Array index
    - `[n:m]` : Array slice
    - `[?expr]` : Filter expression

Integration with Relational Algebra:
    JSONPath operations are composable with standard relational operations,
    allowing queries like:
    1. Select rows where nested array contains value > threshold
    2. Project computed fields from nested structures
    3. Join on nested attributes
    4. Aggregate values from nested arrays

Example:
    >>> # Find orders with expensive items
    >>> data = [
    ...     {"id": 1, "items": [{"name": "A", "price": 10}, {"name": "B", "price": 150}]},
    ...     {"id": 2, "items": [{"name": "C", "price": 20}]}
    ... ]
    >>> expensive = select_any(data, "$.items[*].price", lambda x: x > 100)
    >>> # Returns order with id=1
"""

import re
from typing import Any, List, Dict, Union, Callable, Optional
from dataclasses import dataclass
from enum import Enum


class PathNodeType(Enum):
    """
    Types of nodes in a JSONPath expression tree.
    
    Each node type represents a different navigation or selection
    operation in the path traversal.
    """
    ROOT = "root"           # $ - Document root
    FIELD = "field"         # .fieldname - Object field access
    WILDCARD = "wildcard"   # * - Any child (object values or array elements)
    RECURSIVE = "recursive" # .. - Recursive descent
    INDEX = "index"         # [n] - Array index access
    SLICE = "slice"         # [n:m] - Array slice
    FILTER = "filter"       # [?expr] - Conditional filter


@dataclass
class PathNode:
    """
    A single node in the JSONPath expression tree.
    
    Attributes:
        type: The type of path operation
        value: Associated value (field name, index, slice bounds, etc.)
        filter_expr: Filter expression string for FILTER type nodes
    """
    type: PathNodeType
    value: Any = None
    filter_expr: Optional[str] = None


class JSONPath:
    """
    JSONPath expression parser and evaluator.
    
    Implements a subset of JSONPath specification optimized for JSONL
    processing. Provides lazy evaluation semantics compatible with
    streaming operations.
    
    Attributes:
        path_string: Original JSONPath expression string
        nodes: Parsed expression tree as list of PathNode objects
        
    Supported Syntax:
        - `$.field` : Direct field access
        - `$.field.subfield` : Nested field access  
        - `$[*]` or `$.*` : Wildcard (all children)
        - `$.field[*]` : Array wildcard (all elements)
        - `$.field[0]` : Array index access
        - `$.field[0:3]` : Array slice [start:end]
        - `$..field` : Recursive descent (all descendants)
        - `$.field[?expr]` : Filter expression
        
    Evaluation Semantics:
        - Returns list of all matching values
        - Missing paths return empty list (not error)
        - Wildcards expand to multiple values
        - Recursive descent searches entire subtree
        
    Example:
        >>> path = JSONPath("$.users[*].email")
        >>> data = {"users": [{"email": "a@b.com"}, {"email": "c@d.com"}]}
        >>> path.evaluate(data)
        ['a@b.com', 'c@d.com']
    """

    def __init__(self, path_string: str):
        self.path_string = path_string
        self.nodes = self._parse(path_string)

    def _parse(self, path: str) -> List[PathNode]:
        """
        Parse a JSONPath string into an expression tree.
        
        Converts string representation to structured PathNode list
        using regex-based tokenization.
        
        Args:
            path: JSONPath expression string starting with '$'
            
        Returns:
            List of PathNode objects representing the expression
            
        Raises:
            ValueError: If path doesn't start with '$'
            
        Note:
            Current implementation uses simple regex parsing.
            Production system would use proper lexer/parser.
        """
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
        Evaluate JSONPath expression against data.
        
        Traverses the data structure according to the path expression,
        collecting all matching values.
        
        Args:
            data: JSON-compatible data structure (dict, list, or primitive)
            
        Returns:
            List of all values matching the path (empty if no matches)
            
        Time Complexity:
            O(n) for simple paths where n = data size
            O(n*m) for recursive descent where m = tree depth
            
        Example:
            >>> path = JSONPath("$..price")
            >>> data = {"items": [{"price": 10}, {"price": 20}]}
            >>> path.evaluate(data)
            [10, 20]
        """
        return self._evaluate_nodes(self.nodes, [data])

    def _evaluate_nodes(self, nodes: List[PathNode], contexts: List[Any]) -> List[Any]:
        """
        Recursively evaluate path nodes against current contexts.
        
        Core evaluation engine that processes path nodes left-to-right,
        maintaining a list of current context values.
        
        Args:
            nodes: Remaining path nodes to evaluate
            contexts: Current values to evaluate against
            
        Returns:
            List of values after applying all path nodes
            
        Algorithm:
            For each node, apply its operation to all current contexts,
            collecting results for the next node.
        """
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
        """
        Get all descendant values for recursive descent.
        
        Performs depth-first traversal of the data structure,
        collecting all nested values.
        
        Args:
            obj: Root object to traverse
            
        Returns:
            List containing obj and all its descendants
            
        Note:
            Includes the root object itself in results.
            Handles circular references by value (not by reference).
        """
        descendants = [obj]

        if isinstance(obj, dict):
            for value in obj.values():
                descendants.extend(self._get_all_descendants(value))
        elif isinstance(obj, list):
            for item in obj:
                descendants.extend(self._get_all_descendants(item))

        return descendants

    def _evaluate_filter(self, item: Any, filter_expr: str) -> bool:
        """
        Evaluate a filter expression against an item.
        
        Simple expression evaluator for filter predicates.
        Currently supports basic comparisons.
        
        Args:
            item: Data item to test
            filter_expr: Filter expression string
            
        Returns:
            True if item matches filter, False otherwise
            
        Supported Operators:
            - `>` : Greater than (numeric)
            - `==` : Equality (string comparison)
            
        Note:
            This is a simplified implementation.
            Full JSONPath would support complex expressions.
        """
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
    """
    Quantifiers for path-based predicates.
    
    Determines how multiple path values are combined
    when evaluating predicates.
    
    Mathematical Semantics:
        - ANY: ∃x ∈ path(row) : predicate(x) (existential)
        - ALL: ∀x ∈ path(row) : predicate(x) (universal)
        - NONE: ¬∃x ∈ path(row) : predicate(x) (negated existential)
    """
    ANY = "any"    # At least one value matches
    ALL = "all"    # All values must match
    NONE = "none"  # No values may match


def select_path(
    relation: List[Dict],
    path_expr: str,
    predicate: Callable[[Any], bool] = None,
    quantifier: PathQuantifier = PathQuantifier.ANY,
) -> List[Dict]:
    """
    Select rows using JSONPath expressions with quantified predicates.
    
    Extends relational selection (σ) to handle nested JSON structures
    by evaluating JSONPath expressions with optional predicates and
    quantifiers.
    
    Args:
        relation: Input relation (list of row dictionaries)
        path_expr: JSONPath expression to evaluate
        predicate: Optional function to test path values
        quantifier: How to combine multiple path matches (ANY/ALL/NONE)
        
    Returns:
        Filtered relation containing only matching rows
        
    Mathematical Notation:
        σ_{path,p,q}(R) where:
        - path: JSONPath expression
        - p: predicate function
        - q: quantifier (ANY/ALL/NONE)
        
    Semantics:
        - If predicate is None: Check if path exists (has any values)
        - With predicate:
            - ANY: Row included if ∃ value matching predicate
            - ALL: Row included if ∀ values match predicate
            - NONE: Row included if ¬∃ value matching predicate
            
    Example:
        >>> data = [
        ...     {"id": 1, "scores": [85, 90, 78]},
        ...     {"id": 2, "scores": [92, 95, 88]}
        ... ]
        >>> # Find students with all scores >= 80
        >>> honor_roll = select_path(
        ...     data,
        ...     "$.scores[*]",
        ...     lambda x: x >= 80,
        ...     PathQuantifier.ALL
        ... )
        >>> # Returns only id=2
        
    Note:
        Empty path results with ALL quantifier return False.
        This maintains logical consistency: ∀x ∈ ∅ : p(x) is vacuously true,
        but we use False for practical filtering semantics.
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
    Select rows where ANY path value matches the predicate (existential).
    
    Convenience function for existential quantification over path values.
    Most common use case for path-based selection.
    
    Args:
        relation: Input relation
        path_expr: JSONPath expression
        predicate: Optional predicate for path values
        
    Returns:
        Rows with at least one matching path value
        
    Mathematical Notation:
        σ_{∃path,p}(R) = {r ∈ R | ∃v ∈ path(r) : p(v)}
        
    Example:
        >>> # Find orders with any item over $100
        >>> expensive_orders = select_any(
        ...     orders,
        ...     "$.items[*].price",
        ...     lambda x: x > 100
        ... )
        
    See Also:
        select_path: Full interface with quantifier parameter
        select_all: Universal quantification
        select_none: Negated existential quantification
    """
    return select_path(relation, path_expr, predicate, PathQuantifier.ANY)


def select_all(
    relation: List[Dict], path_expr: str, predicate: Callable[[Any], bool] = None
) -> List[Dict]:
    """
    Select rows where ALL path values match the predicate (universal).
    
    Implements universal quantification over path values.
    Useful for ensuring all nested values meet a criterion.
    
    Args:
        relation: Input relation
        path_expr: JSONPath expression
        predicate: Predicate to test on all path values
        
    Returns:
        Rows where every path value satisfies predicate
        
    Mathematical Notation:
        σ_{∀path,p}(R) = {r ∈ R | ∀v ∈ path(r) : p(v)}
        
    Semantics:
        - Empty path results return False (no values to test)
        - All values must exist and satisfy predicate
        
    Example:
        >>> # Find products where all reviews are 4+ stars
        >>> quality_products = select_all(
        ...     products,
        ...     "$.reviews[*].rating",
        ...     lambda x: x >= 4
        ... )
        
    Note:
        Careful with empty collections - they return False,
        not vacuous truth as in formal logic.
    """
    return select_path(relation, path_expr, predicate, PathQuantifier.ALL)


def select_none(
    relation: List[Dict], path_expr: str, predicate: Callable[[Any], bool] = None
) -> List[Dict]:
    """
    Select rows where NO path values match the predicate (negated existential).
    
    Implements negated existential quantification.
    Useful for filtering out rows with unwanted nested values.
    
    Args:
        relation: Input relation
        path_expr: JSONPath expression
        predicate: Predicate that must not match any value
        
    Returns:
        Rows where no path value satisfies predicate
        
    Mathematical Notation:
        σ_{¬∃path,p}(R) = {r ∈ R | ¬∃v ∈ path(r) : p(v)}
        Equivalent to: {r ∈ R | ∀v ∈ path(r) : ¬p(v)}
        
    Example:
        >>> # Find orders with no failed payments
        >>> successful_orders = select_none(
        ...     orders,
        ...     "$.payments[*].status",
        ...     lambda x: x == "failed"
        ... )
        
    Note:
        Empty path results return True (no values match).
        Complement of select_any for the same predicate.
    """
    return select_path(relation, path_expr, predicate, PathQuantifier.NONE)


def project_template(relation: List[Dict], template: Dict[str, str]) -> List[Dict]:
    """
    Project and transform rows using JSONPath template expressions.
    
    Extends projection (π) to handle nested data extraction, transformation,
    and aggregation within documents. Each template expression can extract
    values from nested paths or compute aggregates.
    
    Args:
        relation: Input relation
        template: Mapping from output field names to template expressions
        
    Returns:
        Transformed relation with template-defined structure
        
    Mathematical Notation:
        π_{template}(R) where template: field → expression
        
    Template Expression Types:
        1. **JSONPath**: Extract nested values
           - `"$.user.name"` → Single value
           - `"$.items[*].price"` → Array of values
           
        2. **Aggregations**: Compute over arrays
           - `"sum($.items[*].price)"` → Total price
           - `"count($.items[*])"` → Number of items
           - `"avg($.scores[*])"` → Average score
           - `"min($.values[*])"` → Minimum value
           - `"max($.values[*])"` → Maximum value
           
        3. **Existence**: Check if path exists
           - `"exists($.optional_field)"` → Boolean
           
        4. **Literals**: Static values
           - `"constant_value"` → String literal
           
    Example:
        >>> template = {
        ...     "customer_id": "$.customer.id",
        ...     "customer_name": "$.customer.name",
        ...     "order_total": "sum($.items[*].price)",
        ...     "item_count": "count($.items[*])",
        ...     "has_discount": "exists($.discount_code)",
        ...     "order_type": "standard"  # Literal
        ... }
        >>> orders_summary = project_template(orders, template)
        
    Error Handling:
        - Invalid paths return None
        - Failed expressions return None
        - Graceful degradation for malformed data
        
    Note:
        This provides document-level transformations similar to
        MongoDB aggregation pipeline's $project stage.
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
    Evaluate a single template expression against a row.
    
    Internal function that handles different expression types:
    JSONPath, aggregations, existence checks, and literals.
    
    Args:
        row: Data row to evaluate against
        expr: Template expression string
        
    Returns:
        Evaluated result (can be any JSON-compatible type)
        
    Expression Grammar:
        expression ::= aggregation | existence | jsonpath | literal
        aggregation ::= ('sum'|'avg'|'min'|'max'|'count') '(' jsonpath ')'
        existence ::= 'exists(' jsonpath ')'
        jsonpath ::= '$' path_components
        literal ::= any_string
        
    Evaluation Rules:
        1. Aggregations: Apply function to all path values
        2. Existence: Return boolean for path presence
        3. JSONPath alone:
           - Single value: Return the value
           - Multiple values: Return as array
           - No values: Return None
        4. Other strings: Return as literal
        
    Example:
        >>> row = {"prices": [10, 20, 30]}
        >>> _evaluate_template_expression(row, "sum($.prices[*])")
        60
        >>> _evaluate_template_expression(row, "$.prices[1]")
        20
        
    Note:
        Aggregations filter non-numeric values for numeric operations.
        This ensures robustness with mixed-type arrays.
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
