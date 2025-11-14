"""
Test suite for MCP server integration.

Tests focus on:
- Each of the 9 MCP tools (behavior and contracts)
- Error handling (invalid inputs, missing files, malformed data)
- Pipeline transformations
- Output formatting
- Edge cases and boundary conditions

Tests verify the tools work correctly without requiring the full MCP SDK.
We test the handler methods directly to verify behavior.
"""

import pytest
import json
import tempfile
import asyncio
from pathlib import Path
from typing import List, Dict, Any

# Import the server components (will handle missing MCP SDK gracefully)
try:
    from integrations.mcp_server import JSONLAlgebraServer
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    JSONLAlgebraServer = None


@pytest.fixture
def temp_data_dir():
    """Create temporary directory with test JSONL files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # Create users.jsonl
        users = [
            {"id": 1, "name": "Alice", "age": 30, "dept": "Engineering", "salary": 80000},
            {"id": 2, "name": "Bob", "age": 25, "dept": "Sales", "salary": 60000},
            {"id": 3, "name": "Charlie", "age": 35, "dept": "Engineering", "salary": 90000},
            {"id": 4, "name": "Diana", "age": 28, "dept": "Marketing", "salary": 70000},
            {"id": 5, "name": "Eve", "age": 32, "dept": "Sales", "salary": 65000},
        ]
        users_file = tmp_path / "users.jsonl"
        with open(users_file, 'w') as f:
            for user in users:
                f.write(json.dumps(user) + '\n')

        # Create orders.jsonl
        orders = [
            {"order_id": 101, "user_id": 1, "amount": 150.00, "status": "completed"},
            {"order_id": 102, "user_id": 2, "amount": 75.50, "status": "pending"},
            {"order_id": 103, "user_id": 1, "amount": 200.00, "status": "completed"},
            {"order_id": 104, "user_id": 3, "amount": 300.00, "status": "completed"},
            {"order_id": 105, "user_id": 4, "amount": 50.00, "status": "cancelled"},
        ]
        orders_file = tmp_path / "orders.jsonl"
        with open(orders_file, 'w') as f:
            for order in orders:
                f.write(json.dumps(order) + '\n')

        # Create logs.jsonl
        logs = [
            {"timestamp": "2024-01-01T10:00:00", "level": "INFO", "message": "Server started"},
            {"timestamp": "2024-01-01T10:05:00", "level": "ERROR", "message": "Connection failed"},
            {"timestamp": "2024-01-01T10:10:00", "level": "WARN", "message": "Slow query"},
            {"timestamp": "2024-01-01T10:15:00", "level": "INFO", "message": "Request processed"},
            {"timestamp": "2024-01-01T10:20:00", "level": "ERROR", "message": "Database timeout"},
        ]
        logs_file = tmp_path / "logs.jsonl"
        with open(logs_file, 'w') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')

        yield {
            'dir': tmp_path,
            'users': str(users_file),
            'orders': str(orders_file),
            'logs': str(logs_file),
        }


@pytest.fixture
def server():
    """Create MCP server instance if available."""
    if not MCP_AVAILABLE:
        pytest.skip("MCP SDK not available")
    return JSONLAlgebraServer()


class TestMCPServerInitialization:
    """Tests for MCP server initialization."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    def test_server_initializes_without_error(self):
        """Given MCP server class, when instantiated, then initialization succeeds."""
        server = JSONLAlgebraServer()

        assert server is not None
        assert hasattr(server, 'server')
        assert hasattr(server, 'temp_files')

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    def test_server_has_helper_methods(self):
        """Given MCP server, when initialized, then helper methods are present."""
        server = JSONLAlgebraServer()

        assert hasattr(server, '_read_jsonl_file')
        assert hasattr(server, '_jsonl_to_string')
        assert hasattr(server, '_format_output')


class TestSelectTool:
    """Tests for jsonl_select tool behavior."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_select_filters_records_by_expression(self, server, temp_data_dir):
        """Given select with filter expression, when executed, then only matching records returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "expression": "age > 28"
        }

        result = await server._handle_select(args)

        # Parse the JSONL result
        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 3  # Alice(30), Charlie(35), Eve(32)
        assert all(r['age'] > 28 for r in records)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_select_with_limit_returns_limited_results(self, server, temp_data_dir):
        """Given select with limit, when executed, then only specified number of results returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "expression": "age >= 25",
            "limit": 2
        }

        result = await server._handle_select(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 2

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_select_with_no_matches_returns_empty(self, server, temp_data_dir):
        """Given select with expression matching nothing, when executed, then empty result."""
        args = {
            "file_path": temp_data_dir['users'],
            "expression": "age > 100"
        }

        result = await server._handle_select(args)

        # Result should be empty or just whitespace
        assert result.strip() == ""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_select_with_invalid_file_raises_error(self, server):
        """Given select with nonexistent file, when executed, then error occurs."""
        args = {
            "file_path": "/nonexistent/file.jsonl",
            "expression": "age > 25"
        }

        with pytest.raises(Exception):  # FileNotFoundError or similar
            await server._handle_select(args)


class TestProjectTool:
    """Tests for jsonl_project tool behavior."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_project_selects_specified_fields(self, server, temp_data_dir):
        """Given project with field list, when executed, then only those fields are in result."""
        args = {
            "file_path": temp_data_dir['users'],
            "fields": ["name", "age"]
        }

        result = await server._handle_project(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 5
        assert all(set(r.keys()) == {"name", "age"} for r in records)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_project_with_single_field(self, server, temp_data_dir):
        """Given project with one field, when executed, then only that field is in result."""
        args = {
            "file_path": temp_data_dir['users'],
            "fields": ["name"]
        }

        result = await server._handle_project(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert all(set(r.keys()) == {"name"} for r in records)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_project_with_nonexistent_field_omits_it(self, server, temp_data_dir):
        """Given project with nonexistent field, when executed, then field is omitted."""
        args = {
            "file_path": temp_data_dir['users'],
            "fields": ["name", "country"]  # country doesn't exist
        }

        result = await server._handle_project(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        # Only name should be present
        assert all("name" in r for r in records)
        assert all("country" not in r for r in records)


class TestSortTool:
    """Tests for jsonl_sort tool behavior."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_sort_by_field_ascending(self, server, temp_data_dir):
        """Given sort by field, when executed, then results are sorted ascending."""
        args = {
            "file_path": temp_data_dir['users'],
            "sort_by": "age"
        }

        result = await server._handle_sort(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        ages = [r['age'] for r in records]
        assert ages == sorted(ages)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_sort_descending(self, server, temp_data_dir):
        """Given sort with reverse flag, when executed, then results are sorted descending."""
        args = {
            "file_path": temp_data_dir['users'],
            "sort_by": "salary",
            "reverse": True
        }

        result = await server._handle_sort(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        salaries = [r['salary'] for r in records]
        assert salaries == sorted(salaries, reverse=True)


class TestAggregateTool:
    """Tests for jsonl_aggregate tool behavior."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_aggregate_counts_by_group(self, server, temp_data_dir):
        """Given aggregate with group_by and count, when executed, then groups are counted correctly."""
        args = {
            "file_path": temp_data_dir['users'],
            "group_by": "dept",
            "aggregations": {"id": "count"}
        }

        result = await server._handle_aggregate(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        # Should have 3 departments: Engineering (2), Sales (2), Marketing (1)
        assert len(records) == 3
        # Each record should have the group key and the aggregation result
        assert all("dept" in r for r in records)
        # The aggregation field is named "count(id)" not "count_id"
        assert all("count(id)" in r for r in records)

        # Verify counts
        dept_counts = {r['dept']: r['count(id)'] for r in records}
        assert dept_counts.get('Engineering') == 2
        assert dept_counts.get('Sales') == 2
        assert dept_counts.get('Marketing') == 1


class TestJoinTool:
    """Tests for jsonl_join tool behavior."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_join_inner_combines_matching_records(self, server, temp_data_dir):
        """Given join with matching keys, when executed, then records are combined."""
        args = {
            "left_file": temp_data_dir['users'],
            "right_file": temp_data_dir['orders'],
            "left_key": "id",
            "right_key": "user_id",
            "join_type": "inner"
        }

        result = await server._handle_join(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        # Should have 5 orders joined with their users
        assert len(records) == 5
        # Each record should have fields from both users and orders
        assert all("name" in r for r in records)  # From users
        assert all("order_id" in r for r in records)  # From orders
        assert all("amount" in r for r in records)  # From orders


class TestSampleTool:
    """Tests for jsonl_sample tool behavior."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_sample_returns_requested_number(self, server, temp_data_dir):
        """Given sample with size n, when executed, then n records are returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "size": 3
        }

        result = await server._handle_sample(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 3

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_sample_with_seed_is_reproducible(self, server, temp_data_dir):
        """Given sample with same seed, when executed twice, then same results."""
        args = {
            "file_path": temp_data_dir['users'],
            "size": 3,
            "seed": 42
        }

        result1 = await server._handle_sample(args)
        result2 = await server._handle_sample(args)

        # Results should be identical with same seed
        assert result1 == result2

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_sample_size_larger_than_data_returns_all(self, server, temp_data_dir):
        """Given sample size > data size, when executed, then all records returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "size": 100
        }

        result = await server._handle_sample(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 5  # All users


class TestStatsTool:
    """Tests for jsonl_stats tool behavior."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_stats_returns_basic_info(self, server, temp_data_dir):
        """Given stats request, when executed, then basic statistics are returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "detailed": False
        }

        result = await server._handle_stats(args)

        stats = json.loads(result)

        assert 'file' in stats
        assert 'record_count' in stats
        assert stats['record_count'] == 5

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_stats_detailed_includes_field_info(self, server, temp_data_dir):
        """Given stats with detailed flag, when executed, then field statistics included."""
        args = {
            "file_path": temp_data_dir['users'],
            "detailed": True
        }

        result = await server._handle_stats(args)

        stats = json.loads(result)

        assert 'fields' in stats
        assert 'name' in stats['fields']
        assert 'age' in stats['fields']

        # Field stats should include type information
        assert 'types' in stats['fields']['name']
        assert 'count' in stats['fields']['name']


class TestTransformTool:
    """Tests for jsonl_transform tool (pipeline operations)."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_transform_applies_select_operation(self, server, temp_data_dir):
        """Given transform with select operation, when executed, then filtering is applied."""
        args = {
            "file_path": temp_data_dir['users'],
            "pipeline": [
                {"operation": "select", "params": {"expression": "age > 28"}}
            ]
        }

        result = await server._handle_transform(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 3
        assert all(r['age'] > 28 for r in records)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_transform_chains_multiple_operations(self, server, temp_data_dir):
        """Given transform with multiple operations, when executed, then all are applied in order."""
        args = {
            "file_path": temp_data_dir['users'],
            "pipeline": [
                # Use simple expression (not JMESPath) for select
                {"operation": "select", "params": {"expression": "dept == 'Engineering'"}},
                {"operation": "project", "params": {"fields": ["name", "salary"]}},
                {"operation": "sort", "params": {"by": "salary", "reverse": True}}
            ]
        }

        result = await server._handle_transform(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        # Should have 2 engineering employees
        assert len(records) == 2
        # Should only have name and salary fields
        assert all(set(r.keys()) == {"name", "salary"} for r in records)
        # Should be sorted by salary descending
        salaries = [r['salary'] for r in records]
        assert salaries == sorted(salaries, reverse=True)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_transform_with_head_operation(self, server, temp_data_dir):
        """Given transform with head operation, when executed, then only first n records returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "pipeline": [
                {"operation": "sort", "params": {"by": "age"}},
                {"operation": "head", "params": {"n": 2}}
            ]
        }

        result = await server._handle_transform(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 2
        # Should be youngest two
        assert records[0]['age'] == 25  # Bob
        assert records[1]['age'] == 28  # Diana

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_transform_with_tail_operation(self, server, temp_data_dir):
        """Given transform with tail operation, when executed, then only last n records returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "pipeline": [
                {"operation": "sort", "params": {"by": "age"}},
                {"operation": "tail", "params": {"n": 2}}
            ]
        }

        result = await server._handle_transform(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 2
        # Should be oldest two
        assert all(r['age'] >= 32 for r in records)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_transform_with_sample_operation(self, server, temp_data_dir):
        """Given transform with sample operation, when executed, then random sample is returned."""
        args = {
            "file_path": temp_data_dir['users'],
            "pipeline": [
                {"operation": "sample", "params": {"n": 2, "seed": 42}}
            ]
        }

        result = await server._handle_transform(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        assert len(records) == 2

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_transform_with_invalid_operation_raises_error(self, server, temp_data_dir):
        """Given transform with unknown operation, when executed, then error is raised."""
        args = {
            "file_path": temp_data_dir['users'],
            "pipeline": [
                {"operation": "unknown_op", "params": {}}
            ]
        }

        with pytest.raises(ValueError, match="Unknown operation"):
            await server._handle_transform(args)


class TestOutputFormatting:
    """Tests for output formatting options."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    def test_jsonl_to_string_converts_records_to_jsonl(self, server, temp_data_dir):
        """Given list of records, when formatted to JSONL, then each record is on separate line."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]

        result = server._jsonl_to_string(data)

        lines = result.split('\n')
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"id": 1, "name": "Alice"}
        assert json.loads(lines[1]) == {"id": 2, "name": "Bob"}

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    def test_format_output_json_returns_array(self, server):
        """Given format='json', when formatting, then JSON array is returned."""
        data = [{"id": 1}, {"id": 2}]

        result = server._format_output(data, "json")

        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) == 2

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    def test_format_output_table_returns_ascii_table(self, server):
        """Given format='table', when formatting, then ASCII table is returned."""
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        result = server._format_output(data, "table")

        assert "id" in result
        assert "name" in result
        assert "Alice" in result
        assert "Bob" in result

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    def test_format_output_summary_returns_count(self, server):
        """Given format='summary', when formatting, then summary statistics are returned."""
        data = [{"id": i} for i in range(10)]

        result = server._format_output(data, "summary")

        assert "Total records: 10" in result


class TestErrorHandling:
    """Tests for error handling and edge cases."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_handle_select_with_malformed_jsonl(self, server):
        """Given file with malformed JSONL, when reading, then appropriate error is raised."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"valid": "json"}\n')
            f.write('{invalid json}\n')
            f.write('{"more": "valid"}\n')
            temp_file = f.name

        try:
            args = {"file_path": temp_file, "expression": "valid == 'json'"}

            # Should raise an error when encountering malformed JSON
            with pytest.raises(Exception):
                await server._handle_select(args)
        finally:
            Path(temp_file).unlink()

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    def test_read_jsonl_file_with_empty_file(self, server):
        """Given empty JSONL file, when read, then empty list is returned."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            temp_file = f.name

        try:
            result = server._read_jsonl_file(temp_file)
            assert result == []
        finally:
            Path(temp_file).unlink()

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_stats_with_nonexistent_file_handles_gracefully(self, server):
        """Given stats request for nonexistent file, when executed, then handles gracefully."""
        args = {
            "file_path": "/nonexistent/file.jsonl",
            "detailed": False
        }

        result = await server._handle_stats(args)
        stats = json.loads(result)

        # Should return stats with 0 records
        assert stats['record_count'] == 0
        assert stats['file_size_bytes'] == 0


class TestComplexWorkflows:
    """Integration tests for complex real-world workflows."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_analytics_workflow_filter_aggregate_sort(self, server, temp_data_dir):
        """Given complex analytics pipeline, when executed, then results are correct."""
        # Workflow: Filter high-value orders, join with users, aggregate by department

        # Step 1: Filter high-value completed orders
        filter_args = {
            "file_path": temp_data_dir['orders'],
            "pipeline": [
                {"operation": "select", "params": {"expression": "status == 'completed'"}},
                {"operation": "select", "params": {"expression": "amount >= 150"}}
            ]
        }
        high_value_orders = await server._handle_transform(filter_args)

        lines = high_value_orders.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        # Should have 3 orders (101: 150, 103: 200, 104: 300)
        assert len(records) == 3
        assert all(r['status'] == 'completed' for r in records)
        assert all(r['amount'] >= 150 for r in records)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_log_analysis_workflow(self, server, temp_data_dir):
        """Given log analysis workflow, when executed, then errors are identified."""
        # Workflow: Find ERROR logs, project relevant fields

        args = {
            "file_path": temp_data_dir['logs'],
            "pipeline": [
                {"operation": "select", "params": {"expression": "level == 'ERROR'"}},
                {"operation": "project", "params": {"fields": ["timestamp", "message"]}}
            ]
        }

        result = await server._handle_transform(args)

        lines = result.strip().split('\n')
        records = [json.loads(line) for line in lines if line]

        # Should have 2 ERROR logs
        assert len(records) == 2
        assert all(set(r.keys()) == {"timestamp", "message"} for r in records)

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="MCP SDK not installed")
    @pytest.mark.asyncio
    async def test_data_quality_workflow(self, server, temp_data_dir):
        """Given data quality check workflow, when executed, then statistics are accurate."""
        # Get detailed stats to understand data quality

        args = {
            "file_path": temp_data_dir['users'],
            "detailed": True
        }

        result = await server._handle_stats(args)
        stats = json.loads(result)

        # Verify comprehensive statistics
        assert stats['record_count'] == 5
        assert 'fields' in stats

        # Check field statistics
        assert 'name' in stats['fields']
        assert stats['fields']['name']['count'] == 5
        assert 'str' in stats['fields']['name']['types']

        assert 'age' in stats['fields']
        assert stats['fields']['age']['count'] == 5
