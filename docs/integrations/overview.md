# Integrations Overview

jsonl-algebra comes with powerful integrations that extend its capabilities beyond the core CLI and library. These integrations demonstrate real-world applications and provide ready-to-use tools for common data processing tasks.

## Available Integrations

### 1. MCP Server - AI Assistant Integration

**Model Context Protocol server for AI assistants and agentic coders**

The MCP server exposes jsonl-algebra operations as structured tools that AI assistants (like Claude, ChatGPT, etc.) can use to manipulate JSONL files through natural language.

```bash
# Setup
pip install mcp
python -m integrations.mcp_server
```

**Features:**

- 9 specialized tools for JSONL manipulation
- Natural language query interface
- Automatic file discovery as resources
- Multiple output formats (JSONL, JSON, table, summary)
- Complex transformation pipelines
- JMESPath expression support

**Use Cases:**

- "Show me all users older than 25 from users.jsonl"
- "Calculate average salary by department"
- "Join orders and customers files on customer_id"
- "Get statistics about the sales data"

[Learn More →](mcp.md)

### 2. Log Analyzer - Real-time Monitoring

**Streaming log analysis with alerts and dashboards**

Analyze log files in real-time with sliding windows, alert systems, and terminal visualization.

```bash
python integrations/log_analyzer.py /var/log/app.log --window 60 --threshold 10
```

**Features:**

- Streaming log processing
- Sliding window analysis
- Error rate monitoring
- Performance anomaly detection
- Terminal dashboard
- Customizable alert system
- Pattern detection

**Use Cases:**

- Monitor application error rates
- Detect performance degradation
- Real-time log filtering
- Alert on specific patterns
- Track request rates

[Learn More →](log-analyzer.md)

### 3. Data Explorer - Interactive REPL

**SQL-like interactive exploration for JSONL files**

Explore JSONL files interactively with SQL-like syntax, tab completion, and data profiling.

```bash
python integrations/data_explorer.py data.jsonl
```

**Features:**

- SQL-like query syntax
- Tab completion for commands and fields
- Data profiling and statistics
- Export to multiple formats
- Command history and recall
- Visual result formatting
- Schema inference

**Example Session:**

```sql
> SELECT name, age WHERE age > 25
> GROUP BY city AGGREGATE count, avg(age)
> PROFILE
> EXPORT results.csv
```

**Use Cases:**

- Ad-hoc data exploration
- Quick data quality checks
- Interactive analysis
- Export filtered datasets
- Schema discovery

[Learn More →](data-explorer.md)

### 4. ML Pipeline - Machine Learning Integration

**Feature engineering with scikit-learn integration**

Use jsonl-algebra for ML data preprocessing and feature engineering.

```python
from integrations.ml_pipeline import JSONLFeatureEngine

engine = JSONLFeatureEngine("training.jsonl")
engine.add_feature("age_squared", lambda r: r["age"] ** 2)
X, y = engine.prepare_features(["age", "age_squared"], "target")
```

**Features:**

- Feature engineering with ja transformations
- scikit-learn pipeline integration
- Automated preprocessing
- Model evaluation utilities
- Cross-validation support
- Feature selection helpers

**Use Cases:**

- Clean training data
- Engineer features from JSONL
- Build ML pipelines
- Preprocess for scikit-learn
- Feature transformation

[Learn More →](ml-pipeline.md)

### 5. Composability Module - Functional Pipelines

**Built into core library - functional programming patterns**

The composability module provides Pipeline classes and functional operators for elegant data transformations.

```python
from ja.compose import Pipeline, Select, Project, Sort

pipeline = (
    Pipeline()
    | Select("age > 25")
    | Project(["name", "email"])
    | Sort("name")
)

results = pipeline.run("data.jsonl")
```

**Features:**

- Pipeline class for composition
- Unix pipe operator support (`|`)
- Lazy evaluation for large datasets
- Functional helpers (`compose`, `pipe`)
- Operation classes (Select, Project, Sort, etc.)
- Chainable transformations

**Use Cases:**

- Build reusable pipelines
- Functional data processing
- Lazy evaluation for big data
- Elegant API usage
- Complex transformations

[Learn More →](../api/composability.md)

## Comparison Matrix

| Integration | Use Case | Language | Interactive | AI-Ready |
|-------------|----------|----------|-------------|----------|
| **MCP Server** | AI assistant integration | Python | No | ✅ Yes |
| **Log Analyzer** | Real-time monitoring | Python | Yes (Dashboard) | No |
| **Data Explorer** | Ad-hoc exploration | Python | Yes (REPL) | No |
| **ML Pipeline** | Machine learning | Python | No | No |
| **Composability** | Library usage | Python | No | No |

## Installation

### All Integrations

Install jsonl-algebra with all optional dependencies:

```bash
pip install jsonl-algebra[integrations]
```

### Individual Integrations

=== "MCP Server"

    ```bash
    pip install mcp
    ./integrations/setup_mcp.sh
    ```

=== "Log Analyzer"

    ```bash
    pip install rich  # For terminal UI
    python integrations/log_analyzer.py
    ```

=== "Data Explorer"

    ```bash
    pip install prompt-toolkit rich
    python integrations/data_explorer.py
    ```

=== "ML Pipeline"

    ```bash
    pip install scikit-learn pandas
    python -c "from integrations.ml_pipeline import JSONLFeatureEngine"
    ```

=== "Composability"

    ```bash
    # Built-in - no extra install needed
    pip install jsonl-algebra
    python -c "from ja.compose import Pipeline"
    ```

## Common Workflows

### Workflow 1: AI-Powered Data Analysis

Use the MCP server with an AI assistant:

```
User: "Analyze the sales data and find top products by revenue"

AI: *Uses MCP server*
    1. jsonl_query: "SELECT * FROM sales.jsonl"
    2. jsonl_aggregate: group_by=['product'], agg={'revenue': 'sum'}
    3. jsonl_sort: by='revenue', reverse=true

Result: Top products with revenue displayed
```

### Workflow 2: Real-time Monitoring → Alert → Analysis

```bash
# 1. Monitor logs in real-time
python integrations/log_analyzer.py /var/log/app.log --alert-threshold 10

# 2. When alert triggers, explore interactively
python integrations/data_explorer.py /var/log/app.log

# 3. Export filtered data for deeper analysis
> SELECT * WHERE level = 'ERROR' AND timestamp > '2025-10-27'
> EXPORT error_logs.jsonl
```

### Workflow 3: ETL with ML Training

```python
from ja.compose import Pipeline, Select, Project
from integrations.ml_pipeline import JSONLFeatureEngine

# 1. ETL pipeline
pipeline = (
    Pipeline()
    | Select("status == 'complete'")
    | Project(["user_id", "score", "timestamp"])
)

cleaned = pipeline.run("raw_data.jsonl")

# 2. Feature engineering
engine = JSONLFeatureEngine(cleaned)
engine.add_feature("score_squared", lambda r: r["score"] ** 2)
engine.add_feature("hour", lambda r: extract_hour(r["timestamp"]))

# 3. Prepare for ML
X, y = engine.prepare_features(["score", "score_squared", "hour"], "target")

# 4. Train model
from sklearn.ensemble import RandomForestClassifier
model = RandomForestClassifier()
model.fit(X, y)
```

## Design Philosophy

All integrations follow these principles:

### 1. Unix Philosophy
Do one thing well, compose easily:

```bash
# Each tool focuses on one aspect
ja select ... | python integrations/log_analyzer.py -
```

### 2. Pythonic Patterns
Explicit, simple, readable:

```python
# Clear, idiomatic Python
pipeline = Pipeline() | Select("x > 0") | Project(["name"])
```

### 3. Streaming First
Handle massive datasets efficiently:

```python
# Processes data line-by-line
for record in pipeline.run_lazy("huge.jsonl"):
    process(record)
```

### 4. Real-World Utility
Solve actual problems elegantly:

```bash
# Real monitoring use case
python integrations/log_analyzer.py production.log --alert-email admin@example.com
```

### 5. Comprehensive Documentation
Examples and guides for everything:

- Detailed README for each integration
- Usage examples
- API documentation
- Tutorial walkthroughs

## Creating Your Own Integration

Want to build a custom integration? Here's the pattern:

```python
# integrations/my_custom_tool.py

from ja.core import read_jsonl, select, project
from typing import Iterator, Dict

class MyCustomTool:
    """
    My custom integration for jsonl-algebra.

    Does something specific and useful.
    """

    def __init__(self, input_file: str):
        self.input_file = input_file

    def process(self) -> Iterator[Dict]:
        """Process data using ja operations."""
        data = read_jsonl(self.input_file)

        # Use core operations
        filtered = select(data, "status == 'active'")
        projected = project(filtered, ["id", "name"])

        return projected

    def run(self):
        """Main entry point."""
        for record in self.process():
            # Do something with record
            print(record)

if __name__ == "__main__":
    import sys
    tool = MyCustomTool(sys.argv[1])
    tool.run()
```

### Integration Checklist

- [ ] Clear, focused purpose
- [ ] Uses ja core operations
- [ ] Handles streaming data
- [ ] Comprehensive docstrings
- [ ] Usage examples
- [ ] Error handling
- [ ] Tests
- [ ] README documentation

## Testing Integrations

Each integration has tests:

```bash
# Run all integration tests
pytest integrations/

# Run specific integration tests
pytest integrations/test_mcp_minimal.py
pytest integrations/test_log_analyzer.py
```

### Test Coverage

```bash
pytest --cov=integrations integrations/
```

## Dependencies

### Core Dependencies

All integrations require:

- Python 3.8+
- jsonl-algebra

### Optional Dependencies

| Integration | Requires |
|-------------|----------|
| MCP Server | `mcp` |
| Log Analyzer | `rich` |
| Data Explorer | `prompt-toolkit`, `rich`, `pandas` (optional) |
| ML Pipeline | `scikit-learn`, `pandas` |

### Installing All

```bash
# Everything in one command
pip install jsonl-algebra[integrations]
```

## Performance Considerations

### Memory Usage

| Integration | Memory Pattern | Best For |
|-------------|----------------|----------|
| MCP Server | Per-request | Small to medium datasets |
| Log Analyzer | Sliding window | Streaming data |
| Data Explorer | Buffered | Interactive exploration |
| ML Pipeline | Dataset-sized | Training data preparation |
| Composability | Configurable (lazy/eager) | Any size |

### Optimization Tips

1. **Use lazy pipelines** for large datasets
2. **Filter early** to reduce data size
3. **Adjust cache sizes** for your workload
4. **Monitor memory** with large aggregations
5. **Stream when possible** instead of buffering

## Troubleshooting

### Common Issues

**Import Error**
```bash
ModuleNotFoundError: No module named 'mcp'
```
**Solution:** `pip install mcp`

**Memory Error with Large Files**
```bash
MemoryError: Unable to allocate array
```
**Solution:** Use lazy evaluation or streaming mode

**MCP Server Not Starting**
```bash
Error: Cannot start MCP server
```
**Solution:** Check MCP SDK installation and configuration

## Community Integrations

Have you built an integration? Share it!

1. Create a PR to add it to `integrations/`
2. Follow the integration checklist
3. Add documentation
4. Include tests

## Next Steps

Explore each integration in detail:

- [MCP Server Guide](mcp.md) - AI assistant integration
- [Log Analyzer Guide](log-analyzer.md) - Real-time monitoring
- [Data Explorer Guide](data-explorer.md) - Interactive REPL
- [ML Pipeline Guide](ml-pipeline.md) - Machine learning
- [Composability API](../api/composability.md) - Functional patterns

Or try a tutorial:

- [Real-time Monitoring Tutorial](../tutorials/monitoring.md)
- [ETL Pipeline Tutorial](../tutorials/etl.md)
- [Data Analysis Tutorial](../tutorials/data-analysis.md)
