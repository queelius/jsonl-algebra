#!/usr/bin/env python3
"""
MCP (Model Context Protocol) Server for JSONL Algebra

This server provides tools for AI assistants to manipulate and query JSONL files
using the jsonl-algebra library. It exposes the full power of ja operations through
a structured protocol that's easy for LLMs to understand and use.
"""

import asyncio
import json
import sys
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from contextlib import contextmanager

# Add parent directory to path to import ja
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ja import project, select, sort_by, groupby_agg, join, union, read_jsonl
import random
import itertools

# MCP SDK imports
try:
    from mcp.server import Server
    from mcp.types import (
        Tool,
        TextContent,
        CallToolResult,
        Resource,
        ResourceContents,
        TextResourceContents,
    )
except ImportError as e:
    print(f"MCP SDK not installed or import failed: {e}", file=sys.stderr)
    print("Install with: pip install mcp", file=sys.stderr)
    sys.exit(1)


class JSONLAlgebraServer:
    """MCP server for JSONL manipulation using jsonl-algebra."""

    def __init__(self):
        self.server = Server("jsonl-algebra")
        self.temp_files = {}  # Track temporary files for cleanup
        self._setup_handlers()

    def _read_jsonl_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read a JSONL file and return list of records."""
        with open(file_path, 'r') as f:
            return read_jsonl(f)

    def _setup_handlers(self):
        """Set up the MCP protocol handlers."""

        @self.server.list_tools()
        async def list_tools() -> List[Tool]:
            """List all available JSONL manipulation tools."""
            return [
                Tool(
                    name="jsonl_query",
                    description="Query and transform JSONL data using SQL-like syntax",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {
                                "type": "string",
                                "description": "Path to the JSONL file"
                            },
                            "query": {
                                "type": "string",
                                "description": "Query in natural language or SQL-like syntax"
                            },
                            "output_format": {
                                "type": "string",
                                "enum": ["jsonl", "json", "table", "summary"],
                                "description": "Output format (default: jsonl)"
                            }
                        },
                        "required": ["file_path", "query"]
                    }
                ),
                Tool(
                    name="jsonl_select",
                    description="Filter JSONL records using JMESPath expressions",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "expression": {
                                "type": "string",
                                "description": "JMESPath filter expression"
                            },
                            "limit": {"type": "integer", "description": "Max records to return"}
                        },
                        "required": ["file_path", "expression"]
                    }
                ),
                Tool(
                    name="jsonl_project",
                    description="Select specific fields from JSONL records",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "fields": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of field paths to keep"
                            }
                        },
                        "required": ["file_path", "fields"]
                    }
                ),
                Tool(
                    name="jsonl_aggregate",
                    description="Group and aggregate JSONL data",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "group_by": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Fields to group by"
                            },
                            "aggregations": {
                                "type": "object",
                                "description": "Aggregations as field:operation pairs"
                            }
                        },
                        "required": ["file_path", "aggregations"]
                    }
                ),
                Tool(
                    name="jsonl_join",
                    description="Join two JSONL files",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "left_file": {"type": "string"},
                            "right_file": {"type": "string"},
                            "left_key": {"type": "string"},
                            "right_key": {"type": "string"},
                            "join_type": {
                                "type": "string",
                                "enum": ["inner", "left", "right", "outer"],
                                "default": "inner"
                            }
                        },
                        "required": ["left_file", "right_file", "left_key", "right_key"]
                    }
                ),
                Tool(
                    name="jsonl_sort",
                    description="Sort JSONL records",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "sort_by": {
                                "type": "string",
                                "description": "Field to sort by"
                            },
                            "reverse": {
                                "type": "boolean",
                                "description": "Sort in descending order"
                            }
                        },
                        "required": ["file_path", "sort_by"]
                    }
                ),
                Tool(
                    name="jsonl_sample",
                    description="Take a random sample of JSONL records",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "size": {
                                "type": "integer",
                                "description": "Number of records to sample"
                            },
                            "seed": {
                                "type": "integer",
                                "description": "Random seed for reproducibility"
                            }
                        },
                        "required": ["file_path", "size"]
                    }
                ),
                Tool(
                    name="jsonl_stats",
                    description="Get statistics about a JSONL file",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "detailed": {
                                "type": "boolean",
                                "description": "Include field-level statistics"
                            }
                        },
                        "required": ["file_path"]
                    }
                ),
                Tool(
                    name="jsonl_transform",
                    description="Apply complex transformations using a pipeline",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "pipeline": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "operation": {"type": "string"},
                                        "params": {"type": "object"}
                                    }
                                },
                                "description": "List of operations to apply in sequence"
                            }
                        },
                        "required": ["file_path", "pipeline"]
                    }
                )
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[CallToolResult]:
            """Execute a JSONL manipulation tool."""
            try:
                if name == "jsonl_query":
                    result = await self._handle_query(arguments)
                elif name == "jsonl_select":
                    result = await self._handle_select(arguments)
                elif name == "jsonl_project":
                    result = await self._handle_project(arguments)
                elif name == "jsonl_aggregate":
                    result = await self._handle_aggregate(arguments)
                elif name == "jsonl_join":
                    result = await self._handle_join(arguments)
                elif name == "jsonl_sort":
                    result = await self._handle_sort(arguments)
                elif name == "jsonl_sample":
                    result = await self._handle_sample(arguments)
                elif name == "jsonl_stats":
                    result = await self._handle_stats(arguments)
                elif name == "jsonl_transform":
                    result = await self._handle_transform(arguments)
                else:
                    result = f"Unknown tool: {name}"

                return [TextContent(type="text", text=str(result))]
            except Exception as e:
                return [TextContent(type="text", text=f"Error: {str(e)}")]

        @self.server.list_resources()
        async def list_resources() -> List[Resource]:
            """List available JSONL files as resources."""
            resources = []
            # Look for JSONL files in common locations
            search_paths = [
                Path.cwd(),
                Path.home() / "data",
                Path("/tmp"),
            ]

            for base_path in search_paths:
                if base_path.exists():
                    for jsonl_file in base_path.glob("*.jsonl"):
                        resources.append(Resource(
                            uri=f"file://{jsonl_file.absolute()}",
                            name=jsonl_file.name,
                            description=f"JSONL file: {jsonl_file.name}",
                            mimeType="application/jsonlines"
                        ))

            return resources

        @self.server.read_resource()
        async def read_resource(uri: str) -> Optional[ResourceContents]:
            """Read a JSONL file resource."""
            try:
                if uri.startswith("file://"):
                    file_path = Path(uri[7:])
                    if file_path.exists() and file_path.suffix == ".jsonl":
                        # Read first few lines as preview
                        with open(file_path, 'r') as f:
                            lines = []
                            for i, line in enumerate(f):
                                if i >= 10:  # Preview first 10 records
                                    break
                                lines.append(json.loads(line))

                        preview = json.dumps(lines, indent=2)
                        return [TextResourceContents(
                            uri=uri,
                            mimeType="application/jsonlines",
                            text=f"Preview of {file_path.name} (first 10 records):\n{preview}"
                        )]
            except Exception as e:
                return None
            return None

    async def _handle_query(self, args: Dict[str, Any]) -> str:
        """Handle natural language or SQL-like queries."""
        file_path = args["file_path"]
        query = args["query"].lower()
        output_format = args.get("output_format", "jsonl")

        # Parse the query and convert to ja operations
        operations = self._parse_query(query)

        # Execute the operations
        result = self._execute_operations(file_path, operations)

        # Format the output
        return self._format_output(result, output_format)

    async def _handle_select(self, args: Dict[str, Any]) -> str:
        """Filter records using JMESPath."""
        data = self._read_jsonl_file(args["file_path"])
        data = select(data, args["expression"])
        if "limit" in args:
            data = list(itertools.islice(data, args["limit"]))
        return self._jsonl_to_string(data)

    async def _handle_project(self, args: Dict[str, Any]) -> str:
        """Project specific fields."""
        data = self._read_jsonl_file(args["file_path"])
        data = project(data, args["fields"])
        return self._jsonl_to_string(data)

    async def _handle_aggregate(self, args: Dict[str, Any]) -> str:
        """Group and aggregate data."""
        # Handle group_by which can be a string or array from MCP tool schema
        group_by_arg = args.get("group_by", "")
        if isinstance(group_by_arg, list):
            # If it's a list, take the first element or use empty string
            group_by = group_by_arg[0] if group_by_arg else ""
        else:
            group_by = group_by_arg

        aggregations = args["aggregations"]

        # Convert aggregations dict to the format expected by groupby_agg
        # groupby_agg expects either:
        #   - A single string: "count(id), sum(amount)"
        #   - A list of tuples: [("count", "id"), ("sum", "amount")]
        # We'll use the string format (comma-separated)
        agg_list = [f"{op}({field})" for field, op in aggregations.items()]
        agg_str = ", ".join(agg_list)

        data = self._read_jsonl_file(args["file_path"])
        data = groupby_agg(data, group_by, agg_str)
        return self._jsonl_to_string(data)

    async def _handle_join(self, args: Dict[str, Any]) -> str:
        """Join two JSONL files."""
        left_data = self._read_jsonl_file(args["left_file"])
        right_data = self._read_jsonl_file(args["right_file"])
        # Note: join_type parameter is not used by the current join() implementation
        # The join() function only supports inner join
        data = join(
            left_data,
            right_data,
            on=[(args["left_key"], args["right_key"])]
        )
        return self._jsonl_to_string(data)

    async def _handle_sort(self, args: Dict[str, Any]) -> str:
        """Sort records."""
        data = self._read_jsonl_file(args["file_path"])
        data = sort_by(data, args["sort_by"])
        if args.get("reverse", False):
            data = list(data)
            data.reverse()
        return self._jsonl_to_string(data)

    async def _handle_sample(self, args: Dict[str, Any]) -> str:
        """Sample records randomly."""
        # Read all records
        records = self._read_jsonl_file(args["file_path"])

        # Set random seed if provided
        if "seed" in args:
            random.seed(args["seed"])

        # Sample records
        sample_size = min(args["size"], len(records))
        sampled = random.sample(records, sample_size)

        return self._jsonl_to_string(sampled)

    async def _handle_stats(self, args: Dict[str, Any]) -> str:
        """Get statistics about the JSONL file."""
        file_path = args["file_path"]
        detailed = args.get("detailed", False)

        stats = {
            "file": file_path,
            "record_count": 0,
            "file_size_bytes": Path(file_path).stat().st_size if Path(file_path).exists() else 0
        }

        if detailed and Path(file_path).exists():
            field_stats = {}
            with open(file_path, 'r') as f:
                for line in f:
                    stats["record_count"] += 1
                    record = json.loads(line)
                    for field, value in record.items():
                        if field not in field_stats:
                            field_stats[field] = {
                                "count": 0,
                                "types": set(),
                                "null_count": 0
                            }
                        field_stats[field]["count"] += 1
                        if value is None:
                            field_stats[field]["null_count"] += 1
                        else:
                            field_stats[field]["types"].add(type(value).__name__)

            # Convert sets to lists for JSON serialization
            for field in field_stats:
                field_stats[field]["types"] = list(field_stats[field]["types"])

            stats["fields"] = field_stats
        elif Path(file_path).exists():
            with open(file_path, 'r') as f:
                stats["record_count"] = sum(1 for _ in f)

        return json.dumps(stats, indent=2)

    async def _handle_transform(self, args: Dict[str, Any]) -> str:
        """Apply a pipeline of transformations."""
        file_path = args["file_path"]
        pipeline = args["pipeline"]

        # Start with the input data
        current_data = self._read_jsonl_file(file_path)

        for step in pipeline:
            op = step["operation"]
            params = step.get("params", {})

            if op == "select":
                current_data = select(current_data, params["expression"])
            elif op == "project":
                current_data = project(current_data, params["fields"])
            elif op == "sort":
                current_data = sort_by(current_data, params["by"])
                if params.get("reverse", False):
                    current_data = list(current_data)
                    current_data.reverse()
            elif op == "head":
                current_data = list(itertools.islice(current_data, params["n"]))
            elif op == "tail":
                # Convert to list and take last n elements
                data_list = list(current_data) if not isinstance(current_data, list) else current_data
                current_data = data_list[-params["n"]:]
            elif op == "sample":
                # Convert to list for sampling
                records = list(current_data) if not isinstance(current_data, list) else current_data
                if "seed" in params:
                    random.seed(params["seed"])
                sample_size = min(params["n"], len(records))
                current_data = random.sample(records, sample_size)
            elif op == "groupby":
                current_data = groupby_agg(
                    current_data,
                    params.get("by", []),
                    params.get("agg", [])
                )
            else:
                raise ValueError(f"Unknown operation: {op}")

        return self._jsonl_to_string(current_data)

    def _parse_query(self, query: str) -> List[Dict[str, Any]]:
        """Parse a natural language or SQL-like query into operations."""
        operations = []

        # Simple pattern matching for common queries
        if "select" in query and "where" in query:
            # Extract WHERE clause
            where_idx = query.index("where")
            condition = query[where_idx + 5:].strip()
            operations.append({"op": "select", "expression": condition})

        if "group by" in query:
            # Extract GROUP BY fields
            gb_idx = query.index("group by")
            fields = query[gb_idx + 8:].split(",")
            fields = [f.strip() for f in fields]
            operations.append({"op": "groupby", "fields": fields})

        if "order by" in query or "sort by" in query:
            # Extract ORDER BY field
            ob_idx = query.index("order by") if "order by" in query else query.index("sort by")
            field = query[ob_idx + 8:].strip().split()[0]
            reverse = "desc" in query
            operations.append({"op": "sort", "field": field, "reverse": reverse})

        if "limit" in query:
            # Extract LIMIT value
            limit_idx = query.index("limit")
            limit_str = query[limit_idx + 5:].strip().split()[0]
            try:
                limit = int(limit_str)
                operations.append({"op": "head", "n": limit})
            except ValueError:
                pass

        return operations

    def _execute_operations(self, file_path: str, operations: List[Dict[str, Any]]) -> Any:
        """Execute a series of operations on a JSONL file."""
        current_data = self._read_jsonl_file(file_path)

        for op in operations:
            if op["op"] == "select":
                current_data = select(current_data, op["expression"])
            elif op["op"] == "groupby":
                current_data = groupby_agg(current_data, op["fields"], op.get("agg", []))
            elif op["op"] == "sort":
                current_data = sort_by(current_data, op["field"])
                if op.get("reverse", False):
                    current_data = list(current_data)
                    current_data.reverse()
            elif op["op"] == "head":
                current_data = list(itertools.islice(current_data, op["n"]))

        return current_data

    def _format_output(self, data: Any, format: str) -> str:
        """Format output in the requested format."""
        if format == "json":
            # Convert to JSON array
            records = []
            if isinstance(data, str):
                with open(data, 'r') as f:
                    for line in f:
                        records.append(json.loads(line))
            else:
                for record in data:
                    records.append(record)
            return json.dumps(records, indent=2)
        elif format == "table":
            # Simple ASCII table format
            records = []
            if isinstance(data, str):
                with open(data, 'r') as f:
                    for line in f:
                        records.append(json.loads(line))
            else:
                records = list(data)

            if not records:
                return "No data"

            # Get all unique keys
            keys = set()
            for record in records:
                keys.update(record.keys())
            keys = sorted(keys)

            # Build table
            lines = []
            lines.append(" | ".join(keys))
            lines.append("-" * (len(" | ".join(keys))))

            for record in records[:20]:  # Limit to 20 rows for readability
                values = [str(record.get(k, "")) for k in keys]
                lines.append(" | ".join(values))

            if len(records) > 20:
                lines.append(f"... and {len(records) - 20} more rows")

            return "\n".join(lines)
        elif format == "summary":
            # Statistical summary
            count = 0
            if isinstance(data, str):
                with open(data, 'r') as f:
                    for line in f:
                        count += 1
            else:
                count = sum(1 for _ in data)

            return f"Total records: {count}"
        else:
            # Default JSONL format
            return self._jsonl_to_string(data)

    def _jsonl_to_string(self, data: Any) -> str:
        """Convert data to JSONL string format."""
        lines = []
        if isinstance(data, str):
            # It's a file path
            with open(data, 'r') as f:
                return f.read()
        else:
            # It's an iterable of records
            for record in data:
                lines.append(json.dumps(record))
        return "\n".join(lines)

    async def run(self):
        """Run the MCP server."""
        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )


async def main():
    """Main entry point."""
    server = JSONLAlgebraServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())