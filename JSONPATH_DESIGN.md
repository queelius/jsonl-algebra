# JSONPath Extensions for JSONL Algebra

## Overview

This document explores extending jsonl-algebra with XPath-like path expressions and wildcards for more sophisticated JSON data manipulation.

## Core Concepts

### 1. Path Expressions

Instead of simple column names, support path expressions:

```python
# Current: simple field access
ja.select(data, lambda row: row["user"]["name"] == "Alice")

# Proposed: path expressions
ja.select(data, "$.user.name == 'Alice'")
ja.select(data, "$.orders[*].amount > 100")  # Any order > 100
ja.select(data, "$.tags[*] == 'python'")     # Any tag is 'python'
```

### 2. Wildcard Support

- `*` - Any immediate child
- `**` - Recursive descent (any descendant)
- `[*]` - Any array element
- `[n]` - Specific array index

### 3. Quantifiers for Predicates

Quantifiers control how predicates are applied to multiple values from path expressions:

- `any(path, predicate)` - True if ANY element matches (**default for filtering**)
- `all(path, predicate)` - True if ALL elements match
- `none(path, predicate)` - True if NO elements match

These are **filtering-only operations** - they determine which rows to include/exclude, not how to transform data.

### 4. Template-Based Transformations

For mapping operations, use templates to handle multiple values:

```python
# Template syntax for transformations
ja.project(data, {
    "user_name": "$.user.name",
    "order_total": "sum($.orders[*].amount)",
    "tag_list": "$.tags[*]",
    "first_order": "$.orders[0].item"
})
```

## Advanced Concepts

### 5. Template Pattern Challenges

Your insight about template patterns is crucial. Consider these scenarios:

```python
# Simple case: one-to-one mapping
"$.user.name" → "Alice"

# Complex case: one-to-many mapping  
"$.orders[*].item" → ["Book", "Pen", "Paper"]

# Template challenge: How do we handle this in projection?
{
    "customer": "$.user.name",           # "Alice" 
    "items": "$.orders[*].item",         # ["Book", "Pen", "Paper"]
    "item_count": "count($.orders[*])",  # 3
    "first_item": "$.orders[0].item"     # "Book"
}
```

### 6. Quantifier Semantics

Different quantifiers have profound implications for **filtering operations**:

```python
# ANY (default): True if at least one element satisfies predicate
ja.select(data, "any($.scores[*] > 80)")  # Rows where any score > 80
ja.select(data, "$.scores[*] > 80")       # Same as above (any is default)

# ALL: True if every element satisfies predicate  
ja.select(data, "all($.scores[*] > 80)")  # Rows where all scores > 80

# NONE: True if no elements satisfy predicate
ja.select(data, "none($.scores[*] > 80)") # Rows where no scores > 80

# EXISTS: True if path exists (regardless of value)
ja.select(data, "exists($.optional_field)") # Rows that have the field

# Note: These are for FILTERING only, not transformation
# For transformation, use template expressions in project()
```

**Key Insight**: Quantifiers determine **which rows to select**, while templates determine **how to transform** the selected data.

### 7. Path Context and Scope

Paths need context awareness:

```python
# Global context: operates on entire row
"$.user.orders[*].amount > 100"

# Local context: operates within a specific scope
"$.departments[*] where any(employees[*].salary > 100000)"

# Cross-reference: relate different parts of the data
"$.user.id in $.orders[*].customer_id"
```

## Deep Dive: Template System Design

### Template Evaluation Modes

1. **Scalar Mode**: Single value extraction
   ```python
   "$.user.name" → "Alice"
   ```

2. **Array Mode**: Multiple value collection
   ```python
   "$.orders[*].item" → ["Book", "Pen"]
   ```

3. **Aggregation Mode**: Computed values
   ```python
   "sum($.orders[*].amount)" → 125.50
   ```

4. **Conditional Mode**: Branching logic
   ```python
   "if($.orders[*], 'has_orders', 'no_orders')" → "has_orders"
   ```

### Template Composition

Templates can be nested and composed:

```python
{
    "customer_info": {
        "name": "$.user.name",
        "email": "$.user.email",
        "tier": "if(sum($.orders[*].amount) > 1000, 'premium', 'standard')"
    },
    "order_summary": {
        "total_orders": "count($.orders[*])",
        "total_value": "sum($.orders[*].amount)",
        "items": "$.orders[*].item",
        "latest_order": "$.orders[-1].date"  # Last element
    },
    "computed_fields": {
        "avg_order_value": "sum($.orders[*].amount) / count($.orders[*])",
        "frequent_items": "mode($.orders[*].item)",  # Most common items
        "order_frequency": "count($.orders[*]) / days_between($.user.created, now())"
    }
}
```

## Implementation Strategy

### Phase 1: Path Expression Parser

Create a JSONPath parser that can:
1. Parse path expressions into AST
2. Evaluate paths against JSON data
3. Handle wildcards and array indexing

### Phase 2: Predicate Extensions

Extend the select operation to support:
1. Path-based predicates
2. Quantifier functions (any/all/none)
3. Complex boolean expressions

### Phase 3: Template System

Implement template-based projections:
1. Template parsing and evaluation
2. Aggregation functions in templates
3. Multiple value handling

## Performance and Optimization Strategies

### 1. Path Compilation and Caching

```python
class CompiledPath:
    def __init__(self, path_expr: str):
        self.expression = path_expr
        self.ast = self._parse(path_expr)
        self.compiled_func = self._compile(self.ast)
        
    def evaluate(self, data):
        return self.compiled_func(data)

# Cache compiled paths
path_cache = {}

def get_compiled_path(expr):
    if expr not in path_cache:
        path_cache[expr] = CompiledPath(expr)
    return path_cache[expr]
```

### 2. Optimization Patterns

```python
# Index-aware operations for arrays
"$.large_array[1000:2000]"  # Slice optimization

# Short-circuit evaluation for predicates
"any($.items[*].price > 1000)"  # Stop on first match

# Lazy evaluation for large datasets
"$.data[*] where complex_condition"  # Stream processing

# Parallel evaluation for independent paths
{
    "field1": "$.path1",
    "field2": "$.path2",  # Can evaluate in parallel
    "field3": "$.path3"
}
```

## Technical Considerations

### 1. Backward Compatibility

All existing functionality must continue to work:
- Simple column access
- Lambda predicates
- Current project syntax

### 2. Performance

Path expressions should be compiled/cached:
- Parse once, evaluate many times
- Optimize common patterns
- Lazy evaluation where possible

### 3. Error Handling

Clear error messages for:
- Invalid path syntax
- Missing paths
- Type mismatches

## Integration with Existing JSONL Algebra

### Backward Compatibility Strategy

```python
# Original API still works
ja.select(data, lambda row: row["age"] > 25)
ja.project(data, ["name", "age"])

# New path-based API
ja.select(data, "$.age > 25")
ja.project(data, {"name": "$.name", "age": "$.age"})

# Mixed usage
ja.select(data, lambda row: path_eval("$.orders[*].amount", row).any() > 100)
```

### Extended Core Functions

```python
def select_path(relation, path_predicate, quantifier="any"):
    """
    Select rows using JSONPath predicates
    
    Args:
        relation: List of JSON objects
        path_predicate: JSONPath expression with predicate
        quantifier: "any", "all", "none", or "exists"
    """
    
def project_template(relation, template):
    """
    Project using template-based transformations
    
    Args:
        relation: List of JSON objects  
        template: Dict with JSONPath expressions as values
    """
    
def groupby_path(relation, group_path, aggregation_template):
    """
    Group by JSONPath and aggregate using templates
    
    Args:
        relation: List of JSON objects
        group_path: JSONPath expression for grouping key
        aggregation_template: Template for aggregated values
    """
```

## Example Use Cases

### 1. E-commerce Data Analysis

```python
# Find orders with any item over $100
expensive_orders = ja.select(orders, "any($.items[*].price > 100)")

# Get customer names and their total order value
customer_summary = ja.project(orders, {
    "customer": "$.customer.name",
    "total_value": "sum($.items[*].price * $.items[*].quantity)",
    "item_count": "count($.items[*])"
})
```

### 2. Log Analysis

```python
# Find logs with any error in nested components
error_logs = ja.select(logs, "any($.components.**.level == 'ERROR')")

# Extract error details
error_summary = ja.project(error_logs, {
    "timestamp": "$.timestamp",
    "service": "$.service",
    "error_messages": "$.components.**.message[level == 'ERROR']"
})
```

### 3. Social Media Data

```python
# Posts with any hashtag related to Python
python_posts = ja.select(posts, "any($.hashtags[*] matches 'python|py|coding')")

# User engagement metrics
engagement = ja.project(posts, {
    "user": "$.author.username",
    "post_id": "$.id",
    "total_engagement": "sum($.likes, $.shares, $.comments.count)",
    "top_hashtags": "$.hashtags[*][0:3]"  # First 3 hashtags
})
```

## Implementation Challenges

### 1. Path Resolution Ambiguity

When a path matches multiple values, how do we handle:
- Aggregation vs. collection
- Type coercion
- Nested transformations

### 2. Template Complexity

Templates need to handle:
- Multiple value injection
- Conditional expressions
- Nested templates
- Function application

### 3. Performance Optimization

- Caching compiled path expressions
- Optimizing wildcard searches
- Batching operations
- Memory efficiency with large datasets

## Proposed API Extensions

### New Functions

```python
# Path-based selection with quantifiers
ja.select_path(data, "$.user.age > 25")                    # any() is default
ja.select_any(data, "$.orders[*].status", "pending")       # explicit any()
ja.select_all(data, "$.scores[*]", lambda x: x > 80)       # all() quantifier
ja.select_none(data, "$.orders[*].status", "cancelled")    # none() quantifier

# Template-based projection (for transformation)
ja.project_template(data, {
    "name": "$.user.name",
    "avg_score": "avg($.scores[*])",
    "has_orders": "exists($.orders[*])"
})

# Path-based aggregation
ja.groupby_path(data, "$.department", {
    "employee_count": "count($)",
    "avg_salary": "avg($.employees[*].salary)",
    "departments": "unique($.employees[*].department)"
})
```

**Design Principle**: 
- **Quantifiers** (`any`/`all`/`none`) are for **filtering** - they determine which rows to keep
- **Templates** are for **transformation** - they determine how to reshape the data

## Quantifier Examples in Practice

Consider this sample data for filtering operations:

```python
students = [
    {"name": "Alice", "scores": [85, 92, 78, 96]},
    {"name": "Bob", "scores": [88, 85, 90, 87]},
    {"name": "Charlie", "scores": [95, 98, 94, 97]},
    {"name": "David", "scores": [72, 68, 75, 70]},
    {"name": "Eve", "scores": []}  # No scores yet
]
```

### Quantifier Behavior

```python
# ANY (default): Students with at least one score > 90
ja.select(students, "any($.scores[*] > 90)")
ja.select(students, "$.scores[*] > 90")  # Same as above
# Result: Alice, Bob, Charlie (David has no scores > 90, Eve has no scores)

# ALL: Students where all scores > 90  
ja.select(students, "all($.scores[*] > 90)")
# Result: Only Charlie (all his scores are > 90)

# NONE: Students with no scores > 90
ja.select(students, "none($.scores[*] > 90)")  
# Result: David, Eve (David has scores but none > 90, Eve has no scores)

# EXISTS: Students who have any scores at all
ja.select(students, "exists($.scores[*])")
# Result: Alice, Bob, Charlie, David (Eve has empty array)
```

### Edge Cases

```python
# Empty arrays with ALL quantifier
ja.select(students, "all($.scores[*] > 50)")
# Eve's empty array evaluates to False for ALL (vacuous truth handled as False)

# Empty arrays with NONE quantifier  
ja.select(students, "none($.scores[*] > 50)")
# Eve's empty array evaluates to True for NONE (no scores violate condition)

# Combining quantifiers with AND logic
ja.select(students, "any($.scores[*] > 90) and all($.scores[*] > 70)")
# Students with at least one score > 90 AND all scores > 70
# Result: Bob, Charlie
```

### Extended Syntax

```python
# Complex path expressions
"$.store.books[*].author"              # All book authors
"$.store.books[price > 10].title"      # Titles of expensive books
"$.store..price"                       # All prices (recursive)
"$.users[age > 18].orders[*].total"    # Order totals for adults
```

This represents a significant evolution from simple relational algebra to a full JSON query and transformation language. The key insight about needing template patterns for mapping operations is crucial - it addresses the fundamental challenge of handling one-to-many path relationships.
