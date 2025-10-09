# JSONL Algebra MCP Server

The Model Context Protocol (MCP) server for jsonl-algebra allows AI assistants and agentic coders to manipulate, query, and transform JSONL files using natural language or structured commands.

## Features

The MCP server exposes the full power of jsonl-algebra through 9 specialized tools:

- **jsonl_query**: Natural language or SQL-like queries
- **jsonl_select**: Filter records with JMESPath expressions
- **jsonl_project**: Select specific fields from records
- **jsonl_aggregate**: Group and aggregate data
- **jsonl_join**: Join multiple JSONL files
- **jsonl_sort**: Sort records by any field
- **jsonl_sample**: Random sampling from datasets
- **jsonl_stats**: Comprehensive file statistics
- **jsonl_transform**: Complex transformation pipelines

## Installation

### Quick Setup

```bash
# Run the setup script
./integrations/setup_mcp.sh
```

### Manual Setup

1. Install the MCP SDK:
```bash
pip install mcp
```

2. Add to your MCP configuration:
```json
{
  "mcpServers": {
    "jsonl-algebra": {
      "command": "python",
      "args": ["-m", "integrations.mcp_server"],
      "env": {}
    }
  }
}
```

## Usage Examples

### For AI Assistants

When connected to the MCP server, AI assistants can use natural language:

```
"Show me all users older than 25 from the users.jsonl file"
"Calculate the average salary by department"
"Join the orders and customers files on customer_id"
"Get statistics about the sales data"
```

### For Developers

The MCP server can be integrated into your development workflow:

```python
# Example: Using with an MCP client
from mcp.client import Client

client = Client("jsonl-algebra")

# Filter records
result = await client.call_tool("jsonl_select", {
    "file_path": "data.jsonl",
    "expression": "status == 'active'"
})

# Aggregate data
stats = await client.call_tool("jsonl_aggregate", {
    "file_path": "sales.jsonl",
    "group_by": ["region"],
    "aggregations": {"revenue": "sum", "orders": "count"}
})
```

## Tool Details

### jsonl_query
Natural language interface for complex queries:
```json
{
  "file_path": "employees.jsonl",
  "query": "select name, salary where department = 'Engineering' order by salary desc limit 10",
  "output_format": "table"
}
```

### jsonl_select
Filter with JMESPath expressions:
```json
{
  "file_path": "products.jsonl",
  "expression": "price > `100` && category == 'Electronics'",
  "limit": 50
}
```

### jsonl_project
Extract specific fields:
```json
{
  "file_path": "users.jsonl",
  "fields": ["id", "email", "created_at"]
}
```

### jsonl_aggregate
Group and compute statistics:
```json
{
  "file_path": "transactions.jsonl",
  "group_by": ["user_id", "month"],
  "aggregations": {
    "amount": "sum",
    "transactions": "count",
    "avg_amount": "avg"
  }
}
```

### jsonl_join
Combine datasets:
```json
{
  "left_file": "orders.jsonl",
  "right_file": "customers.jsonl",
  "left_key": "customer_id",
  "right_key": "id",
  "join_type": "left"
}
```

### jsonl_transform
Apply transformation pipelines:
```json
{
  "file_path": "raw_data.jsonl",
  "pipeline": [
    {"operation": "select", "params": {"expression": "status == 'active'"}},
    {"operation": "project", "params": {"fields": ["id", "name", "score"]}},
    {"operation": "sort", "params": {"by": "score", "reverse": true}},
    {"operation": "head", "params": {"n": 100}}
  ]
}
```

## Output Formats

The MCP server supports multiple output formats:

- **jsonl**: Standard JSONL format (default)
- **json**: Pretty-printed JSON array
- **table**: ASCII table for human readability
- **summary**: Statistical summary

## Integration with Claude

When using Claude Desktop or Claude API with MCP support:

1. Configure the MCP server in your Claude settings
2. Claude can then directly manipulate JSONL files in your conversations
3. Example prompts:
   - "Load the sales data and show me the top performing regions"
   - "Clean the customer dataset by removing duplicates"
   - "Merge these two JSONL files and calculate summary statistics"

## Integration with VS Code

For VS Code users with AI assistants:

1. Install the MCP extension
2. Add jsonl-algebra to your workspace MCP servers
3. Your AI assistant can now manipulate JSONL files in your project

## Advanced Usage

### Custom Pipelines

Create complex data processing workflows:

```python
pipeline = [
    # Filter active users
    {"operation": "select", "params": {"expression": "status == 'active'"}},

    # Add computed fields
    {"operation": "project", "params": {
        "fields": ["id", "name", "age", "joined_date"]
    }},

    # Group by age ranges
    {"operation": "groupby", "params": {
        "by": ["age_range"],
        "agg": ["count(id)", "min(joined_date)", "max(joined_date)"]
    }},

    # Sort by count
    {"operation": "sort", "params": {"by": "count", "reverse": true}}
]
```

### Resource Discovery

The MCP server automatically discovers JSONL files in:
- Current working directory
- ~/data directory
- /tmp directory

These appear as resources that AI assistants can browse and access.

## Testing

Run the test suite:

```bash
# Basic connectivity test
python -m integrations.mcp_server --test

# Full integration test
python -m pytest tests/test_mcp_server.py
```

## Troubleshooting

### Server won't start
- Ensure MCP SDK is installed: `pip install mcp`
- Check Python path includes the jsonl-algebra directory

### Tools not appearing
- Verify the MCP configuration file is properly formatted
- Check server logs for initialization errors

### Performance issues
- For large files, use sampling or limits
- Consider pre-filtering data before complex operations

## Security

The MCP server:
- Only accesses files explicitly specified in requests
- Doesn't modify files unless explicitly requested
- Respects file system permissions
- Sanitizes all inputs to prevent injection attacks

## Contributing

To extend the MCP server:

1. Add new tools in the `_setup_handlers` method
2. Implement handlers following the pattern
3. Update configuration schema
4. Add tests for new functionality

## License

MIT License - same as jsonl-algebra