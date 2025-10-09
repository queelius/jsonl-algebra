# JSONL Algebra Improvements & Integration Showcase

## Executive Summary

After analyzing the `jsonl-algebra` codebase, I've identified several areas where we can make the API more elegant, composable, and powerful while maintaining simplicity. The tool already has a solid foundation with good Unix philosophy adherence, but there are opportunities to enhance its composability and showcase its power through strategic integrations.

## 1. API Refinements

### 1.1 Core Issues to Address

#### Debug Statements in Production Code
- **Issue**: `ja/group.py` contains debug print statements (lines 49-50)
- **Fix**: Remove all debug statements and use proper logging

#### Inconsistent API Signatures
- **Issue**: `groupby_agg` expects string but tests pass list
- **Fix**: Support both string and list inputs for flexibility

#### Missing Type Hints
- **Issue**: Inconsistent type annotations across modules
- **Fix**: Add comprehensive type hints using Python 3.8+ features

### 1.2 Composability Improvements

#### Stream Processing First-Class Support
```python
# Current: Operations return lists
result = select(data, "age > 30")

# Proposed: Generator-based operations for true streaming
def select_stream(data: Iterator[Row], expr: str) -> Iterator[Row]:
    """Lazy evaluation for massive datasets"""
    parser = ExprEval()
    for row in data:
        if parser.evaluate(expr, row):
            yield row
```

#### Functional Composition Pattern
```python
# New compose.py module
from typing import Callable, TypeVar
from functools import reduce

T = TypeVar('T')

class Pipeline:
    """Composable pipeline for chaining operations"""

    def __init__(self, *ops: Callable):
        self.ops = ops

    def __call__(self, data):
        return reduce(lambda d, op: op(d), self.ops, data)

    def __or__(self, other):
        """Unix pipe operator: pipeline | other_op"""
        return Pipeline(*self.ops, other)

# Usage:
pipeline = (
    Pipeline()
    | partial(select, expr="age > 30")
    | partial(project, fields=["name", "email"])
    | partial(sort_by, keys="name")
)
result = pipeline(data)
```

#### Builder Pattern for Complex Operations
```python
# New query.py module
class Query:
    """Fluent interface for building complex queries"""

    def __init__(self, data=None):
        self._data = data
        self._ops = []

    def select(self, expr: str):
        self._ops.append(('select', expr))
        return self

    def project(self, *fields):
        self._ops.append(('project', fields))
        return self

    def groupby(self, key: str):
        self._ops.append(('groupby', key))
        return self

    def agg(self, **specs):
        self._ops.append(('agg', specs))
        return self

    def execute(self, data=None):
        """Execute the query pipeline"""
        result = data or self._data
        for op, args in self._ops:
            result = getattr(operations, op)(result, args)
        return result

# Usage:
q = Query().select("age > 30").project("name", "email").groupby("city")
result = q.execute(data)
```

### 1.3 Expression Language Enhancements

#### Support for Lambda Expressions
```python
# Enhanced expr.py
def parse_lambda(expr: str) -> Callable:
    """Parse lambda expressions for more complex operations

    Examples:
        'x => x.age * 2'
        'row => row.price * row.quantity'
    """
    # Implementation using AST parsing for safety
```

#### JSONPath Full Support
```python
# Already supports JMESPath, but could add JSONPath for compatibility
def select_jsonpath(data: Relation, expr: str) -> Relation:
    """JSONPath support for those familiar with it"""
    from jsonpath_ng import parse
    parsed = parse(expr)
    return [row for row in data if parsed.find(row)]
```

## 2. Code Quality Improvements

### 2.1 Remove Debug Code
```python
# ja/group.py - Remove lines 48-50
# Remove: import sys
# Remove: print("hi", file=sys.stderr)
```

### 2.2 Fix Test Compatibility
```python
# ja/group.py - Update groupby_agg to handle both string and list
def groupby_agg(data: Relation, group_key: str,
                agg_spec: Union[str, List[Tuple[str, str]]]) -> Relation:
    """Accept both string and list formats for backward compatibility"""
    parser = ExprEval()

    # Group data
    groups = defaultdict(list)
    for row in data:
        key = parser.get_field_value(row, group_key)
        groups[key].append(row)

    # Handle both string and list inputs
    if isinstance(agg_spec, str):
        agg_specs = parse_agg_specs(agg_spec)
    else:
        # Convert old list format to new format
        agg_specs = []
        for name, field in agg_spec:
            if name == "count":
                agg_specs.append(("count", "count"))
            elif name in ["sum", "avg", "min", "max"]:
                agg_specs.append((f"{name}_{field}", f"{name}({field})"))

    # Apply aggregations
    result = []
    for key, group_rows in groups.items():
        row_result = {group_key: key}
        for spec in agg_specs:
            row_result.update(apply_single_agg(spec, group_rows))
        result.append(row_result)

    return result
```

### 2.3 Add Comprehensive Error Handling
```python
# New errors.py module
class JAError(Exception):
    """Base exception for JA operations"""
    pass

class ExpressionError(JAError):
    """Invalid expression syntax"""
    pass

class FieldNotFoundError(JAError):
    """Field doesn't exist in data"""
    pass

class TypeMismatchError(JAError):
    """Operation type mismatch"""
    pass

# Usage in core.py
def select(data: Relation, expr: str, use_jmespath: bool = False) -> Relation:
    try:
        # ... existing code ...
    except Exception as e:
        raise ExpressionError(f"Invalid expression '{expr}': {e}") from e
```

## 3. Showcase Integrations

### 3.1 Real-time Log Analysis (`integrations/log_analysis/`)

```python
#!/usr/bin/env python3
"""Real-time log analysis with ja - showcase streaming capabilities"""

import sys
import json
from ja import select, project, groupby_agg
from collections import deque
from datetime import datetime, timedelta

class LogAnalyzer:
    """Analyze streaming logs with sliding windows"""

    def __init__(self, window_minutes=5):
        self.window = timedelta(minutes=window_minutes)
        self.buffer = deque()

    def process_line(self, line: str):
        """Process a single log line"""
        try:
            log = json.loads(line)
            log['_timestamp'] = datetime.fromisoformat(log['timestamp'])

            # Add to buffer and remove old entries
            self.buffer.append(log)
            cutoff = datetime.now() - self.window
            while self.buffer and self.buffer[0]['_timestamp'] < cutoff:
                self.buffer.popleft()

            # Analyze current window
            logs = list(self.buffer)

            # Find errors
            errors = select(logs, "level == 'ERROR'")

            # Group by endpoint
            by_endpoint = groupby_agg(logs, "endpoint",
                                     "count=count,avg_response_time=avg(response_time)")

            # Alert on anomalies
            slow_endpoints = select(by_endpoint, "avg_response_time > 1000")

            if slow_endpoints:
                print(f"ALERT: Slow endpoints detected: {json.dumps(slow_endpoints)}")

        except json.JSONDecodeError:
            pass  # Skip malformed lines

    def run(self):
        """Process stdin continuously"""
        for line in sys.stdin:
            self.process_line(line.strip())

if __name__ == "__main__":
    analyzer = LogAnalyzer()
    analyzer.run()
```

### 3.2 Data Pipeline with Apache Airflow (`integrations/airflow/`)

```python
"""Airflow DAG using ja for ETL operations"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import ja

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ja_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline using jsonl-algebra',
    schedule_interval=timedelta(days=1),
)

def extract_transform(**context):
    """Extract and transform data using ja"""
    import json

    # Read from data lake
    with open('/data/raw/events.jsonl') as f:
        data = [json.loads(line) for line in f]

    # Transform with ja
    pipeline = (
        ja.Pipeline()
        | partial(ja.select, expr="event_type != 'heartbeat'")
        | partial(ja.project, fields=["user_id", "event_type", "timestamp", "properties"])
        | partial(ja.rename, mapping={"properties": "metadata"})
    )

    transformed = list(pipeline(data))

    # Save to staging
    with open('/data/staging/events_clean.jsonl', 'w') as f:
        for row in transformed:
            f.write(json.dumps(row) + '\n')

    return len(transformed)

def aggregate_metrics(**context):
    """Aggregate metrics using ja"""
    import json

    with open('/data/staging/events_clean.jsonl') as f:
        data = [json.loads(line) for line in f]

    # Complex aggregation
    daily_metrics = ja.groupby_agg(
        data,
        "date(timestamp)",
        "total_events=count,unique_users=count_distinct(user_id)"
    )

    # Save metrics
    with open('/data/metrics/daily.jsonl', 'w') as f:
        for row in daily_metrics:
            f.write(json.dumps(row) + '\n')

extract_transform_task = PythonOperator(
    task_id='extract_transform',
    python_callable=extract_transform,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_metrics',
    python_callable=aggregate_metrics,
    dag=dag,
)

extract_transform_task >> aggregate_task
```

### 3.3 Interactive Data Explorer (`integrations/explorer/`)

```python
#!/usr/bin/env python3
"""Interactive data explorer using ja with rich terminal UI"""

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Input, Footer, Header
from textual.containers import Container, Horizontal, Vertical
import ja
import json

class DataExplorer(App):
    """Textual app for exploring JSONL data"""

    CSS = """
    Input {
        margin: 1;
    }
    DataTable {
        height: 100%;
    }
    """

    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.original_data = []
        self.current_data = []
        self.load_data()

    def load_data(self):
        """Load JSONL file"""
        with open(self.filename) as f:
            self.original_data = [json.loads(line) for line in f]
            self.current_data = self.original_data[:]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Input(placeholder="Enter ja expression (e.g., select age > 30)", id="query"),
            DataTable(id="results"),
        )
        yield Footer()

    def on_mount(self):
        """Initialize the data table"""
        table = self.query_one("#results", DataTable)
        self.refresh_table()

    def refresh_table(self):
        """Refresh the data table with current data"""
        table = self.query_one("#results", DataTable)
        table.clear()

        if not self.current_data:
            return

        # Add columns
        columns = list(self.current_data[0].keys())
        for col in columns:
            table.add_column(col)

        # Add rows
        for row in self.current_data[:100]:  # Limit display
            table.add_row(*[str(row.get(col, "")) for col in columns])

    async def on_input_submitted(self, event):
        """Process ja query"""
        query = event.value.strip()

        if not query:
            self.current_data = self.original_data[:]
        else:
            try:
                # Parse and execute ja command
                parts = query.split(maxsplit=1)
                if len(parts) == 2:
                    command, args = parts

                    if command == "select":
                        self.current_data = ja.select(self.current_data, args)
                    elif command == "project":
                        fields = args.split(",")
                        self.current_data = ja.project(self.current_data, fields)
                    elif command == "sort":
                        self.current_data = ja.sort_by(self.current_data, args)
                    # ... more commands

            except Exception as e:
                self.notify(f"Error: {e}", severity="error")
                return

        self.refresh_table()
        self.notify(f"Showing {len(self.current_data)} rows")

if __name__ == "__main__":
    import sys
    app = DataExplorer(sys.argv[1] if len(sys.argv) > 1 else "data.jsonl")
    app.run()
```

### 3.4 Machine Learning Pipeline (`integrations/ml_pipeline/`)

```python
#!/usr/bin/env python3
"""ML feature engineering pipeline using ja"""

import ja
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import pandas as pd
import json
import numpy as np

class FeatureEngineer:
    """Feature engineering using ja operations"""

    def __init__(self):
        self.scaler = StandardScaler()

    def prepare_features(self, data_file: str):
        """Prepare ML features using ja"""

        # Load data
        with open(data_file) as f:
            data = [json.loads(line) for line in f]

        # Feature engineering with ja
        features = ja.project(data, [
            "age",
            "income",
            "age_income_ratio=age/income",
            "high_earner=income>100000",
            "age_group=floor(age/10)*10",
        ])

        # Aggregate features by group
        grouped_features = ja.groupby_agg(
            features,
            "age_group",
            "avg_income=avg(income),count=count"
        )

        # Join back to get enriched features
        enriched = ja.join(
            features,
            grouped_features,
            on=[("age_group", "age_group")]
        )

        # Convert to DataFrame for ML
        df = pd.DataFrame(enriched)

        # Numeric features
        numeric_cols = ['age', 'income', 'age_income_ratio', 'avg_income']
        X = df[numeric_cols]

        # Scale features
        X_scaled = self.scaler.fit_transform(X)

        return X_scaled, df

    def create_train_test_split(self, X, y, test_size=0.2):
        """Create train/test split and save as JSONL"""
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )

        # Convert back to JSONL for storage
        train_data = [
            {"features": x.tolist(), "target": float(y)}
            for x, y in zip(X_train, y_train)
        ]

        test_data = [
            {"features": x.tolist(), "target": float(y)}
            for x, y in zip(X_test, y_test)
        ]

        # Save using ja's efficient streaming
        with open('train.jsonl', 'w') as f:
            for row in train_data:
                f.write(json.dumps(row) + '\n')

        with open('test.jsonl', 'w') as f:
            for row in test_data:
                f.write(json.dumps(row) + '\n')

        return len(train_data), len(test_data)

if __name__ == "__main__":
    engineer = FeatureEngineer()
    X, df = engineer.prepare_features('customers.jsonl')
    print(f"Engineered {X.shape[1]} features for {X.shape[0]} samples")
```

### 3.5 GraphQL Integration (`integrations/graphql/`)

```python
#!/usr/bin/env python3
"""GraphQL server with ja-powered queries"""

from flask import Flask
from flask_graphql import GraphQLView
import graphene
import ja
import json

class JAQuery(graphene.ObjectType):
    """GraphQL queries powered by ja"""

    query_jsonl = graphene.Field(
        graphene.JSONString,
        file=graphene.String(required=True),
        operations=graphene.List(graphene.JSONString, required=True)
    )

    def resolve_query_jsonl(self, info, file, operations):
        """Execute ja operations via GraphQL"""

        # Load data
        with open(file) as f:
            data = [json.loads(line) for line in f]

        # Apply operations in sequence
        result = data
        for op in operations:
            op_type = op.get('type')

            if op_type == 'select':
                result = ja.select(result, op['expression'])
            elif op_type == 'project':
                result = ja.project(result, op['fields'])
            elif op_type == 'groupby':
                result = ja.groupby_agg(result, op['key'], op.get('agg', 'count'))
            elif op_type == 'sort':
                result = ja.sort_by(result, op['keys'],
                                  descending=op.get('descending', False))
            # ... more operations

        return list(result)

# Example GraphQL query:
# {
#   queryJsonl(
#     file: "users.jsonl",
#     operations: [
#       {type: "select", expression: "age > 25"},
#       {type: "project", fields: ["name", "email", "age"]},
#       {type: "sort", keys: "age", descending: true}
#     ]
#   )
# }

schema = graphene.Schema(query=JAQuery)

app = Flask(__name__)
app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True)
)

if __name__ == '__main__':
    app.run(debug=True)
```

## 4. Performance Optimizations

### 4.1 Lazy Evaluation
```python
# New lazy.py module
from typing import Iterator, Callable
import itertools

class LazyPipeline:
    """Lazy evaluation for streaming large datasets"""

    def __init__(self, source: Iterator):
        self.source = source
        self.ops = []

    def select(self, expr: str):
        """Add lazy select operation"""
        self.ops.append(('select', expr))
        return self

    def project(self, fields):
        """Add lazy project operation"""
        self.ops.append(('project', fields))
        return self

    def take(self, n: int):
        """Take first n results"""
        self.ops.append(('take', n))
        return self

    def __iter__(self):
        """Execute pipeline lazily"""
        result = self.source

        for op, arg in self.ops:
            if op == 'select':
                parser = ExprEval()
                result = (row for row in result if parser.evaluate(arg, row))
            elif op == 'project':
                result = (project_single(row, arg) for row in result)
            elif op == 'take':
                result = itertools.islice(result, arg)

        return result
```

### 4.2 Parallel Processing
```python
# New parallel.py module
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

def parallel_select(data: List[Row], expr: str, workers: int = None) -> List[Row]:
    """Parallel select for large datasets"""
    workers = workers or mp.cpu_count()

    # Split data into chunks
    chunk_size = len(data) // workers
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

    # Process in parallel
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(select, chunk, expr) for chunk in chunks]
        results = [f.result() for f in futures]

    # Combine results
    return list(itertools.chain.from_iterable(results))
```

## 5. Testing Improvements

### 5.1 Property-Based Testing
```python
# tests/test_properties.py
from hypothesis import given, strategies as st
import ja

@given(st.lists(st.dictionaries(
    keys=st.text(min_size=1, max_size=10),
    values=st.one_of(st.integers(), st.text(), st.floats(allow_nan=False))
)))
def test_select_preserves_structure(data):
    """Select should preserve row structure"""
    result = ja.select(data, "true")  # Select all
    assert result == data

@given(st.lists(st.dictionaries(
    keys=st.text(min_size=1, max_size=10),
    values=st.integers()
)))
def test_distinct_idempotent(data):
    """Distinct should be idempotent"""
    once = ja.distinct(data)
    twice = ja.distinct(once)
    assert once == twice
```

## 6. Documentation Improvements

### 6.1 Interactive Tutorial
```python
#!/usr/bin/env python3
"""Interactive ja tutorial"""

import ja
import json
from typing import List, Dict

class Tutorial:
    """Interactive tutorial for ja"""

    def __init__(self):
        self.sample_data = [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
            {"name": "Carol", "age": 35, "city": "NYC"},
        ]

    def lesson_1_select(self):
        """Lesson 1: Filtering with select"""
        print("=== Lesson 1: Select ===")
        print("Sample data:", json.dumps(self.sample_data, indent=2))
        print("\nLet's filter for people over 25:")

        result = ja.select(self.sample_data, "age > 25")
        print("Result:", json.dumps(result, indent=2))

        print("\nTry it yourself! Enter a select expression:")
        expr = input("> ")
        try:
            result = ja.select(self.sample_data, expr)
            print("Result:", json.dumps(result, indent=2))
        except Exception as e:
            print(f"Error: {e}")

    # More lessons...

if __name__ == "__main__":
    tutorial = Tutorial()
    tutorial.lesson_1_select()
```

## Summary

These improvements focus on:

1. **API Elegance**: Functional composition, fluent interfaces, and lazy evaluation
2. **Unix Philosophy**: True streaming support, composable operations, do one thing well
3. **Pythonic Design**: Generator expressions, context managers, operator overloading
4. **Practical Integrations**: Real-world use cases that showcase ja's power
5. **Performance**: Parallel processing and lazy evaluation for large datasets

The integrations demonstrate ja's versatility in:
- Real-time data processing
- ETL pipelines
- Interactive exploration
- Machine learning workflows
- API backends

Each integration is a complete, working example that users can adapt to their needs.