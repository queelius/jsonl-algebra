# JSONL Algebra Integrations

This directory contains powerful integrations that showcase the capabilities of jsonl-algebra in real-world scenarios.

## Available Integrations

### 1. MCP Server (`mcp_server.py`)

**Model Context Protocol server for AI assistants and agentic coders**

The MCP server exposes jsonl-algebra operations as structured tools that AI assistants (like Claude, ChatGPT, etc.) can use to manipulate JSONL files through natural language.

**Features:**
- 9 specialized tools for JSONL manipulation
- Natural language query interface
- Automatic file discovery as resources
- Multiple output formats (JSONL, JSON, table, summary)
- Complex transformation pipelines
- JMESPath expression support

**Setup:**
```bash
# Install MCP SDK
pip install mcp

# Run setup script
./integrations/setup_mcp.sh

# Or manually configure your MCP client
python -m integrations.mcp_server
```

**See:** [MCP_README.md](MCP_README.md) for detailed documentation

### 2. Log Analyzer (`log_analyzer.py`)

**Real-time streaming log analysis with alerts and dashboards**

Analyze log files in real-time with sliding windows, alert systems, and terminal visualization.

**Features:**
- Streaming log processing
- Sliding window analysis
- Error rate monitoring
- Performance anomaly detection
- Terminal dashboard
- Alert system

**Usage:**
```bash
python integrations/log_analyzer.py /var/log/app.log --window 60 --threshold 10
```

### 3. Data Explorer (`data_explorer.py`)

**Interactive REPL for JSONL exploration**

Explore JSONL files interactively with SQL-like syntax, tab completion, and data profiling.

**Features:**
- SQL-like query syntax
- Tab completion
- Data profiling and statistics
- Export to multiple formats
- History and command recall
- Visual result formatting

**Usage:**
```bash
python integrations/data_explorer.py data.jsonl
# Then use commands like:
# > SELECT name, age WHERE age > 25
# > PROFILE
# > EXPORT results.csv
```

### 4. ML Pipeline (`ml_pipeline.py`)

**Machine learning feature engineering with scikit-learn integration**

Use jsonl-algebra for ML data preprocessing and feature engineering.

**Features:**
- Feature engineering with ja transformations
- scikit-learn pipeline integration
- Automated preprocessing
- Model evaluation utilities
- Cross-validation support

**Usage:**
```python
from integrations.ml_pipeline import JSONLFeatureEngine

engine = JSONLFeatureEngine("training.jsonl")
engine.add_feature("age_squared", lambda r: r["age"] ** 2)
X, y = engine.prepare_features(["age", "age_squared"], "target")
```

## Composability Module (`ja/compose.py`)

New composability features added to the core library:

**Features:**
- Pipeline class for functional composition
- Unix pipe operator support (`|`)
- Lazy evaluation for large datasets
- Functional helpers (`compose`, `pipe`)
- Operation classes (Select, Project, Sort, etc.)

**Example:**
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

## Testing

Run the comprehensive test suite:

```bash
# Minimal tests (tests core logic without full MCP SDK)
python integrations/test_mcp_minimal.py

# Integration tests (requires all dependencies)
pytest integrations/
```

## Design Philosophy

All integrations follow these principles:

1. **Unix Philosophy**: Do one thing well, compose easily
2. **Pythonic Patterns**: Explicit, simple, readable
3. **Streaming First**: Handle massive datasets efficiently
4. **Real-World Utility**: Solve actual problems elegantly
5. **Documentation**: Comprehensive examples and guides

## Contributing

To add a new integration:

1. Create a new file in `integrations/`
2. Follow the existing patterns for imports and structure
3. Add comprehensive docstrings and examples
4. Create tests in `tests/integrations/`
5. Update this README

## Dependencies

Core integrations require only:
- `jsonl-algebra` (obviously!)
- Python 3.8+

Optional dependencies:
- `mcp`: For MCP server functionality
- `scikit-learn`: For ML pipeline
- `pandas`: For enhanced data exploration
- `rich`: For terminal formatting (log analyzer)

Install all optional dependencies:
```bash
pip install jsonl-algebra[integrations]
```

## License

All integrations are licensed under the same MIT license as jsonl-algebra.