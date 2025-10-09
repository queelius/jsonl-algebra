#!/usr/bin/env python3
"""Interactive JSONL data explorer with SQL-like query support.

This tool provides an interactive REPL for exploring JSONL data with:
- SQL-like syntax translated to ja operations
- Tab completion for fields and commands
- Query history and result caching
- Data profiling and statistics
- Export to multiple formats

Usage:
    # Interactive exploration
    python data_explorer.py data.jsonl

    # With initial query
    python data_explorer.py data.jsonl --query "SELECT name, age WHERE age > 25"

    # Profile mode
    python data_explorer.py data.jsonl --profile
"""

import sys
import json
import re
import readline
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter, defaultdict
import statistics

from ja import (
    select, project, join, rename, union, difference,
    distinct, intersection, sort_by, product, collect,
    groupby_agg, Pipeline, Select, Project, Sort
)
from ja.commands import read_jsonl
from ja.schema import infer_schema


class SQLTranslator:
    """Translate SQL-like queries to ja operations."""

    def __init__(self):
        self.parser = re.compile(
            r'(?P<select>SELECT\s+(?P<fields>.+?))?'
            r'(?:\s+FROM\s+(?P<table>\S+))?'
            r'(?:\s+WHERE\s+(?P<where>.+?))?'
            r'(?:\s+GROUP\s+BY\s+(?P<groupby>\S+))?'
            r'(?:\s+HAVING\s+(?P<having>.+?))?'
            r'(?:\s+ORDER\s+BY\s+(?P<orderby>.+?))?'
            r'(?:\s+LIMIT\s+(?P<limit>\d+))?',
            re.IGNORECASE
        )

    def translate(self, sql: str, data_name: str = 'data') -> Tuple[str, Pipeline]:
        """Translate SQL to ja pipeline.

        Returns:
            Tuple of (description, pipeline)
        """
        match = self.parser.match(sql.strip())
        if not match:
            raise ValueError(f"Invalid SQL syntax: {sql}")

        ops = []
        desc = []

        # WHERE clause
        where = match.group('where')
        if where:
            # Convert SQL operators to ja operators
            where = where.replace(' AND ', ' and ')
            where = where.replace(' OR ', ' or ')
            where = where.replace(' NOT ', ' not ')
            where = where.replace(' IN ', ' in ')
            where = where.replace(' LIKE ', ' =~ ')
            where = re.sub(r"'([^']+)'", r'"\1"', where)  # Convert single quotes

            ops.append(Select(where))
            desc.append(f"Filter: {where}")

        # GROUP BY clause
        groupby = match.group('groupby')
        having = match.group('having')
        if groupby:
            # Detect aggregations in SELECT
            fields = match.group('fields') or '*'
            agg_pattern = r'(COUNT|SUM|AVG|MIN|MAX)\(([^)]+)\)'
            aggs = re.findall(agg_pattern, fields, re.IGNORECASE)

            if aggs:
                agg_spec = []
                for func, field in aggs:
                    func = func.lower()
                    if func == 'count' and field == '*':
                        agg_spec.append('count=count')
                    else:
                        agg_spec.append(f'{func}_{field}={func}({field})')

                from ja import GroupBy
                ops.append(GroupBy(groupby, ','.join(agg_spec)))
                desc.append(f"Group by {groupby}: {', '.join(agg_spec)}")
            else:
                from ja import GroupBy
                ops.append(GroupBy(groupby))
                desc.append(f"Group by {groupby}")

        # SELECT clause (projection)
        elif match.group('fields') and match.group('fields') != '*':
            fields = match.group('fields')
            # Parse field list
            field_list = [f.strip() for f in fields.split(',')]

            # Handle aliases (field AS alias)
            processed_fields = []
            for field in field_list:
                if ' AS ' in field.upper():
                    orig, alias = re.split(r'\s+AS\s+', field, flags=re.IGNORECASE)
                    processed_fields.append(f"{alias.strip()}={orig.strip()}")
                else:
                    processed_fields.append(field)

            ops.append(Project(processed_fields))
            desc.append(f"Select: {', '.join(processed_fields)}")

        # ORDER BY clause
        orderby = match.group('orderby')
        if orderby:
            # Parse ORDER BY (field [ASC|DESC], ...)
            order_parts = orderby.split(',')
            keys = []
            descending = False

            for part in order_parts:
                part = part.strip()
                if ' DESC' in part.upper():
                    descending = True
                    part = part[:part.upper().index(' DESC')].strip()
                elif ' ASC' in part.upper():
                    part = part[:part.upper().index(' ASC')].strip()
                keys.append(part)

            ops.append(Sort(keys, descending=descending))
            desc.append(f"Order by: {', '.join(keys)} {'DESC' if descending else 'ASC'}")

        # LIMIT clause
        limit = match.group('limit')
        if limit:
            from ja import Take
            ops.append(Take(int(limit)))
            desc.append(f"Limit: {limit}")

        pipeline = Pipeline(*ops)
        return ' | '.join(desc), pipeline


class DataProfiler:
    """Profile JSONL data to understand structure and statistics."""

    def profile(self, data: List[Dict]) -> Dict[str, Any]:
        """Generate comprehensive data profile."""
        if not data:
            return {'error': 'No data to profile'}

        profile = {
            'row_count': len(data),
            'fields': {},
            'sample': data[:5],
        }

        # Analyze each field
        fields = set()
        for row in data:
            fields.update(self._flatten_keys(row))

        for field in fields:
            field_profile = self._profile_field(data, field)
            profile['fields'][field] = field_profile

        # Detect potential keys
        profile['potential_keys'] = self._detect_keys(data)

        # Schema inference
        try:
            profile['schema'] = infer_schema(data)
        except:
            profile['schema'] = None

        return profile

    def _flatten_keys(self, obj: Dict, prefix: str = '') -> List[str]:
        """Get all keys including nested ones."""
        keys = []
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key
            keys.append(full_key)
            if isinstance(value, dict):
                keys.extend(self._flatten_keys(value, full_key))
        return keys

    def _profile_field(self, data: List[Dict], field: str) -> Dict:
        """Profile a single field."""
        values = []
        null_count = 0

        # Extract values
        from ja.expr import ExprEval
        parser = ExprEval()

        for row in data:
            value = parser.get_field_value(row, field)
            if value is None:
                null_count += 1
            else:
                values.append(value)

        if not values:
            return {
                'type': 'null',
                'null_count': len(data),
                'null_percentage': 100.0,
            }

        # Determine type and statistics
        types = Counter(type(v).__name__ for v in values)
        dominant_type = types.most_common(1)[0][0]

        field_info = {
            'type': dominant_type,
            'types': dict(types),
            'null_count': null_count,
            'null_percentage': (null_count / len(data)) * 100,
            'unique_count': len(set(str(v) for v in values)),
            'uniqueness': (len(set(str(v) for v in values)) / len(values)) * 100 if values else 0,
        }

        # Type-specific statistics
        if dominant_type in ['int', 'float']:
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            if numeric_values:
                field_info.update({
                    'min': min(numeric_values),
                    'max': max(numeric_values),
                    'mean': statistics.mean(numeric_values),
                    'median': statistics.median(numeric_values),
                    'stdev': statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0,
                })

        elif dominant_type == 'str':
            string_values = [v for v in values if isinstance(v, str)]
            if string_values:
                lengths = [len(v) for v in string_values]
                field_info.update({
                    'min_length': min(lengths),
                    'max_length': max(lengths),
                    'avg_length': statistics.mean(lengths),
                    'top_values': Counter(string_values).most_common(5),
                })

        elif dominant_type == 'list':
            list_values = [v for v in values if isinstance(v, list)]
            if list_values:
                lengths = [len(v) for v in list_values]
                field_info.update({
                    'min_items': min(lengths),
                    'max_items': max(lengths),
                    'avg_items': statistics.mean(lengths),
                })

        return field_info

    def _detect_keys(self, data: List[Dict]) -> List[str]:
        """Detect fields that could be primary keys."""
        if not data:
            return []

        potential_keys = []
        fields = set()
        for row in data:
            fields.update(row.keys())

        for field in fields:
            values = [row.get(field) for row in data]
            # Check if all values are unique and non-null
            if None not in values and len(values) == len(set(values)):
                potential_keys.append(field)

        return potential_keys


class DataExplorer:
    """Interactive JSONL data explorer."""

    def __init__(self, filename: str):
        self.filename = Path(filename)
        self.data = []
        self.current_result = []
        self.history = []
        self.sql_translator = SQLTranslator()
        self.profiler = DataProfiler()
        self.load_data()
        self.setup_completion()

    def load_data(self):
        """Load JSONL data from file."""
        try:
            self.data = list(read_jsonl(str(self.filename)))
            self.current_result = self.data
            print(f"Loaded {len(self.data)} rows from {self.filename}")
        except Exception as e:
            print(f"Error loading data: {e}")
            sys.exit(1)

    def setup_completion(self):
        """Setup tab completion for commands and fields."""
        # Extract all field names
        self.fields = set()
        for row in self.data[:100]:  # Sample first 100 rows
            self.fields.update(self._get_all_fields(row))

        # Commands
        self.commands = [
            'select', 'project', 'sort', 'group', 'join', 'distinct',
            'profile', 'schema', 'export', 'help', 'quit', 'SELECT',
            'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'FROM'
        ]

        # Setup readline
        readline.set_completer(self.complete)
        readline.parse_and_bind('tab: complete')

    def _get_all_fields(self, obj: Dict, prefix: str = '') -> List[str]:
        """Extract all field names including nested."""
        fields = []
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key
            fields.append(full_key)
            if isinstance(value, dict):
                fields.extend(self._get_all_fields(value, full_key))
        return fields

    def complete(self, text: str, state: int) -> Optional[str]:
        """Tab completion handler."""
        options = []

        # Complete commands
        if not self.history or self.history[-1].startswith(text):
            options = [cmd for cmd in self.commands if cmd.startswith(text)]

        # Complete field names
        options.extend([field for field in self.fields if field.startswith(text)])

        return options[state] if state < len(options) else None

    def execute_ja(self, command: str):
        """Execute ja command."""
        parts = command.split(maxsplit=1)
        if len(parts) < 2:
            print("Usage: <command> <arguments>")
            return

        cmd, args = parts
        cmd = cmd.lower()

        try:
            if cmd == 'select':
                self.current_result = select(self.current_result, args)
            elif cmd == 'project':
                fields = [f.strip() for f in args.split(',')]
                self.current_result = project(self.current_result, fields)
            elif cmd == 'sort':
                self.current_result = sort_by(self.current_result, args)
            elif cmd == 'group':
                # Parse group command: group <key> [agg_spec]
                group_parts = args.split(maxsplit=1)
                if len(group_parts) == 2:
                    key, agg = group_parts
                    self.current_result = groupby_agg(self.current_result, key, agg)
                else:
                    from ja import groupby_with_metadata
                    self.current_result = groupby_with_metadata(self.current_result, group_parts[0])
            elif cmd == 'distinct':
                self.current_result = distinct(self.current_result)
            else:
                print(f"Unknown command: {cmd}")
                return

            print(f"Result: {len(self.current_result)} rows")
            self.show_results()

        except Exception as e:
            print(f"Error: {e}")

    def execute_sql(self, query: str):
        """Execute SQL-like query."""
        try:
            desc, pipeline = self.sql_translator.translate(query)
            print(f"Translated to: {desc}")
            self.current_result = list(pipeline(self.data))
            print(f"Result: {len(self.current_result)} rows")
            self.show_results()
        except Exception as e:
            print(f"SQL Error: {e}")

    def show_results(self, limit: int = 10):
        """Display current results."""
        if not self.current_result:
            print("No results")
            return

        # Show first N results
        for i, row in enumerate(self.current_result[:limit], 1):
            print(f"{i}. {json.dumps(row, ensure_ascii=False)}")

        if len(self.current_result) > limit:
            print(f"... and {len(self.current_result) - limit} more rows")

    def show_profile(self):
        """Show data profile."""
        profile = self.profiler.profile(self.current_result)

        print(f"\n{'=' * 60}")
        print(f"Data Profile - {len(self.current_result)} rows")
        print(f"{'=' * 60}")

        # Field statistics
        for field, info in profile['fields'].items():
            print(f"\n{field}:")
            print(f"  Type: {info['type']}")
            print(f"  Unique: {info.get('unique_count', 0)} ({info.get('uniqueness', 0):.1f}%)")
            print(f"  Nulls: {info.get('null_count', 0)} ({info.get('null_percentage', 0):.1f}%)")

            if 'mean' in info:
                print(f"  Range: {info['min']} - {info['max']}")
                print(f"  Mean: {info['mean']:.2f}, Median: {info['median']:.2f}")

            if 'top_values' in info:
                print(f"  Top values: {info['top_values'][:3]}")

        if profile.get('potential_keys'):
            print(f"\nPotential keys: {', '.join(profile['potential_keys'])}")

    def export(self, format: str, filename: str):
        """Export current results."""
        try:
            if format == 'json':
                with open(filename, 'w') as f:
                    json.dump(self.current_result, f, indent=2, ensure_ascii=False)
            elif format == 'jsonl':
                with open(filename, 'w') as f:
                    for row in self.current_result:
                        f.write(json.dumps(row, ensure_ascii=False) + '\n')
            elif format == 'csv':
                import csv
                if not self.current_result:
                    print("No data to export")
                    return

                # Flatten nested structures
                flattened = []
                for row in self.current_result:
                    flat_row = self._flatten_dict(row)
                    flattened.append(flat_row)

                # Write CSV
                keys = set()
                for row in flattened:
                    keys.update(row.keys())

                with open(filename, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=sorted(keys))
                    writer.writeheader()
                    writer.writerows(flattened)

            print(f"Exported {len(self.current_result)} rows to {filename}")

        except Exception as e:
            print(f"Export error: {e}")

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)

    def show_help(self):
        """Show help information."""
        print("""
Data Explorer Commands:
====================

JA Commands:
  select <expression>     - Filter rows (e.g., select age > 25)
  project <fields>        - Select fields (e.g., project name, age)
  sort <fields>           - Sort by fields (e.g., sort age DESC)
  group <field> [agg]     - Group and aggregate (e.g., group city count=count)
  distinct                - Remove duplicates

SQL-like Queries:
  SELECT <fields> WHERE <condition> ORDER BY <field> LIMIT <n>

  Examples:
    SELECT name, age WHERE age > 25 ORDER BY age DESC LIMIT 10
    SELECT COUNT(*), AVG(age) GROUP BY city

Special Commands:
  profile                 - Show data statistics
  schema                  - Show inferred schema
  export <format> <file>  - Export results (json, jsonl, csv)
  reset                   - Reset to original data
  help                    - Show this help
  quit                    - Exit

Tips:
  - Use Tab for field name completion
  - Use arrow keys for command history
  - Pipe commands work on current results
""")

    def repl(self):
        """Run interactive REPL."""
        print(f"\nData Explorer - {self.filename}")
        print(f"Type 'help' for commands, 'quit' to exit\n")

        while True:
            try:
                # Prompt with current result count
                prompt = f"[{len(self.current_result)} rows]> "
                command = input(prompt).strip()

                if not command:
                    continue

                # Save to history
                self.history.append(command)

                # Check for special commands
                if command.lower() == 'quit':
                    break
                elif command.lower() == 'help':
                    self.show_help()
                elif command.lower() == 'profile':
                    self.show_profile()
                elif command.lower() == 'schema':
                    schema = infer_schema(self.current_result)
                    print(json.dumps(schema, indent=2))
                elif command.lower() == 'reset':
                    self.current_result = self.data
                    print(f"Reset to original data: {len(self.data)} rows")
                elif command.lower().startswith('export'):
                    parts = command.split()
                    if len(parts) != 3:
                        print("Usage: export <format> <filename>")
                    else:
                        self.export(parts[1], parts[2])
                elif command.upper().startswith('SELECT'):
                    self.execute_sql(command)
                else:
                    self.execute_ja(command)

            except KeyboardInterrupt:
                print("\nUse 'quit' to exit")
            except EOFError:
                break
            except Exception as e:
                print(f"Error: {e}")

        print("\nGoodbye!")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Interactive JSONL data explorer',
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('file', help='JSONL file to explore')
    parser.add_argument('--query', help='Initial query to execute')
    parser.add_argument('--profile', action='store_true', help='Show data profile and exit')

    args = parser.parse_args()

    # Check file exists
    if not Path(args.file).exists():
        print(f"Error: File '{args.file}' not found")
        sys.exit(1)

    # Create explorer
    explorer = DataExplorer(args.file)

    # Handle special modes
    if args.profile:
        explorer.show_profile()
    elif args.query:
        if args.query.upper().startswith('SELECT'):
            explorer.execute_sql(args.query)
        else:
            explorer.execute_ja(args.query)
        explorer.show_results(20)
    else:
        # Run interactive mode
        explorer.repl()


if __name__ == '__main__':
    main()