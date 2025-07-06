# The Algebra Behind ja

## What Makes it an Algebra?

An algebra is a mathematical structure with:

1. A set of elements (relations/tables)
2. Operations on those elements
3. Properties that those operations satisfy

In `ja`, our algebra consists of:

- **Elements**: Relations (collections of JSON objects)
- **Operations**: select, project, join, union, etc.
- **Properties**: Closure, associativity, commutativity (where applicable)

## The Relational Model

### Relations as Sets

In `ja`, a JSONL file represents a relation - an unordered set of tuples (JSON objects):

```json
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}
```

This is a relation with schema `(id: number, name: string, age: number)`.

### Operations as Functions

Each operation is a function that transforms relations:

```
σ[age > 30](R) → R'
```

This reads as: "select (σ) where age > 30 from relation R produces relation R'"

## Core Operations

### 1. Selection (σ) - Filter

Selection filters rows based on a predicate:

```bash
ja select 'age > 30' people.jsonl
```

**Algebraic notation**: σ[age > 30](People)

**Properties**:

- Commutative: σ[p1](σ[p2](R)) = σ[p2](σ[p1](R))
- Can be combined: σ[p1](σ[p2](R)) = σ[p1 ∧ p2](R)

### 2. Projection (π) - Transform

Projection selects and computes columns:

```bash
ja project name,income=salary*12 employees.jsonl
```

**Algebraic notation**: π[name, income=salary×12](Employees)

**Properties**:

- Not commutative in general
- Idempotent for simple projections: π[a](π[a,b](R)) = π[a](R)

### 3. Join (⋈) - Combine

Join combines relations based on a condition:

```bash
ja join users.jsonl orders.jsonl --on id=user_id
```

**Algebraic notation**: Users ⋈[id=user_id] Orders

**Properties**:

- Commutative: R ⋈ S = S ⋈ R
- Associative: (R ⋈ S) ⋈ T = R ⋈ (S ⋈ T)

### 4. Union (∪) - Combine All

Union combines all rows from two relations:

```bash
ja union employees.jsonl contractors.jsonl
```

**Properties**:

- Commutative: R ∪ S = S ∪ R
- Associative: (R ∪ S) ∪ T = R ∪ (S ∪ T)
- Identity: R ∪ ∅ = R

## Advanced Concepts

### Grouping and Aggregation

Grouping extends relational algebra with aggregation:

```bash
ja groupby department employees.jsonl | ja agg avg_salary=avg(salary)
```

**Algebraic notation**: γ[department, avg_salary=AVG(salary)](Employees)

### Chained Grouping

Our innovation: grouping that preserves composability:

```bash
cat sales.jsonl \
  | ja groupby region \      # First level grouping
  | ja groupby product \     # Second level grouping  
  | ja agg total=sum(amount) # Final aggregation
```

Each groupby adds metadata rather than aggregating, maintaining the relation structure.

The key insight is using JSON-native metadata to preserve grouping hierarchy:

```json
{
  "order_id": 101,
  "user_id": 1,
  "amount": 250,
  "_groups": [
    {"field": "user_id", "value": 1},
    {"field": "amount", "value": 250}
  ],
  "_group_size": 1,
  "_group_index": 0
}
```

## Theoretical Guarantees

### 1. Closure Property

Every operation produces a valid relation, ensuring composability:

```
Relation → Operation → Relation
```

### 2. Optimization Opportunities

The algebraic properties enable optimizations:

```bash
# These are equivalent (push selection down)
ja join huge.jsonl small.jsonl --on id=id | ja select 'active == true'
ja select 'active == true' huge.jsonl | ja join - small.jsonl --on id=id
```

### 3. Declarative Nature

You specify *what* you want, not *how* to compute it:

```bash
# Declarative
ja groupby user_id orders.jsonl | ja agg total=sum(amount)

# vs Imperative (pseudocode)
for order in orders:
    totals[order.user_id] += order.amount
```

## Why This Matters

1. **Predictability**: Mathematical foundations mean predictable behavior
2. **Composability**: Operations can be combined in any order (where valid)
3. **Optimization**: Algebraic laws enable automatic optimization
4. **Reasoning**: You can reason about transformations algebraically

## Design Insights

### JSON-Native Metadata

Rather than encoding grouping information in compound strings like "1.250", we use structured JSON:

```json
"_groups": [
  {"field": "user_id", "value": 1},
  {"field": "amount", "value": 250}
]
```

This approach:

- Preserves data types (no string parsing needed)
- Is self-documenting
- Enables complex querying of grouping structure
- Maintains the principle that data stays sensible throughout pipelines

### Streaming by Design

Each operation processes data incrementally, enabling:

- Processing of arbitrarily large datasets
- Real-time data processing
- Low memory footprint
- Natural integration with Unix pipes

## Further Reading

- [Composability in Practice](composability.md)
- [Chained Grouping Deep Dive](chained-groups.md)
- [Expression Language](expression-language.md)
