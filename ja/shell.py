#!/usr/bin/env python3
"""
Rich interactive shell for navigating JSON/JSONL files.

Provides a filesystem-like interface with:
- Command history and editing (using prompt_toolkit)
- Tab completion for paths and commands
- Pretty-printed output (using rich)
- Syntax highlighting
"""

import sys
import json
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory

except ImportError:
    print("Error: prompt_toolkit not installed. Install with: pip install prompt_toolkit", file=sys.stderr)
    sys.exit(1)

try:
    from rich.console import Console
    from rich.table import Table
    from rich.syntax import Syntax
    from rich.panel import Panel
    from rich.tree import Tree
    from rich import box
except ImportError:
    print("Error: rich not installed. Install with: pip install rich", file=sys.stderr)
    sys.exit(1)

from .vfs import JSONPath, NodeType, LazyJSONL
from .core import select as ja_select
from .expr import ExprEval
import re


class ShellCompleter(Completer):
    """Smart tab completion for shell commands and paths."""

    def __init__(self, vfs: JSONPath, commands: List[str]):
        self.vfs = vfs
        self.commands = commands

    def get_completions(self, document, complete_event):
        """Generate completions for the current input."""
        text = document.text_before_cursor
        words = text.split()

        # If no words or completing first word, offer commands
        if not words or (len(words) == 1 and not text.endswith(' ')):
            prefix = words[0] if words else ""
            for cmd in self.commands:
                if cmd.startswith(prefix):
                    yield Completion(
                        cmd,
                        start_position=-len(prefix),
                        display=cmd,
                        display_meta="command"
                    )
            # Also complete paths for first word
            if not words:
                return

        # Get the word being completed
        if text.endswith(' '):
            # Starting a new word
            path_to_complete = ""
        else:
            path_to_complete = words[-1]

        # Split into directory and prefix
        if '/' in path_to_complete:
            last_slash = path_to_complete.rfind('/')
            dir_path = path_to_complete[:last_slash] if last_slash > 0 else '/'
            prefix = path_to_complete[last_slash + 1:]
            base = path_to_complete[:last_slash + 1]
        else:
            dir_path = self.vfs.pwd()
            prefix = path_to_complete
            base = ""

        # Try to list directory
        try:
            entries = self.vfs.ls(dir_path)
            for name, is_dir in entries:
                if name.lower().startswith(prefix.lower()):
                    suffix = '/' if is_dir else ''
                    completion = base + name + suffix
                    yield Completion(
                        completion,
                        start_position=-len(path_to_complete),
                        display=name + suffix,
                        display_meta="dir" if is_dir else "file"
                    )
        except Exception:
            # Can't complete this path
            pass


class JAShell:
    """Rich interactive shell for JSON/JSONL navigation."""

    def __init__(self, root_dir: str = '.'):
        """Initialize the shell."""
        self.vfs = JSONPath(root_dir)
        self.console = Console()
        self.running = True

        # Setup prompt_toolkit session
        history_file = Path.home() / '.ja_shell_history'
        self.session: PromptSession = PromptSession(
            history=FileHistory(str(history_file)),
            auto_suggest=AutoSuggestFromHistory(),
            enable_history_search=True,
        )

        # Command registry
        self.commands = {
            'ls': self.cmd_ls,
            'cd': self.cmd_cd,
            'pwd': self.cmd_pwd,
            'cat': self.cmd_cat,
            'tree': self.cmd_tree,
            'stat': self.cmd_stat,
            'head': self.cmd_head,
            'tail': self.cmd_tail,
            'count': self.cmd_count,
            'grep': self.cmd_grep,
            'select': self.cmd_select,
            'help': self.cmd_help,
            'exit': self.cmd_exit,
            'quit': self.cmd_exit,
        }

        # Welcome message
        self.show_welcome()

    def show_welcome(self):
        """Display welcome message."""
        welcome = Panel(
            "[bold cyan]Welcome to ja-shell![/bold cyan]\n\n"
            "Navigate JSON/JSONL files like a filesystem.\n\n"
            "[dim]Type 'help' for available commands.[/dim]",
            box=box.ROUNDED,
            border_style="cyan"
        )
        self.console.print(welcome)
        self.console.print()

    def get_prompt(self) -> str:
        """Generate the shell prompt."""
        cwd = self.vfs.pwd()
        return f"[bold green]ja[/bold green]:[bold blue]{cwd}[/bold blue]$ "

    def run(self):
        """Main shell loop."""
        # Create path-aware completer
        path_completer = ShellCompleter(self.vfs, list(self.commands.keys()))

        while self.running:
            try:
                user_input = self.session.prompt(
                    self.get_prompt(),
                    completer=path_completer
                )

                # Parse and execute command
                self.execute(user_input.strip())

            except KeyboardInterrupt:
                self.console.print("\n[dim]Use 'exit' or 'quit' to exit.[/dim]")
            except EOFError:
                break
            except Exception as e:
                self.console.print(f"[bold red]Error:[/bold red] {e}")

        self.console.print("\n[cyan]Goodbye![/cyan]")

    def execute(self, line: str):
        """Execute a command line."""
        if not line:
            return

        parts = line.split()
        cmd = parts[0]
        args = parts[1:]

        if cmd in self.commands:
            try:
                self.commands[cmd](args)
            except Exception as e:
                self.console.print(f"[bold red]Error:[/bold red] {e}")
        else:
            self.console.print(f"[bold red]Unknown command:[/bold red] {cmd}")
            self.console.print("[dim]Type 'help' for available commands.[/dim]")

    # Command implementations

    def cmd_ls(self, args: List[str]):
        """List directory contents."""
        path = args[0] if args else None

        try:
            entries = self.vfs.ls(path)

            if not entries:
                self.console.print("[dim]Empty directory[/dim]")
                return

            # Create a pretty table
            table = Table(show_header=True, box=box.SIMPLE)
            table.add_column("Type", style="cyan", width=4)
            table.add_column("Name", style="green")
            table.add_column("Preview", style="dim", max_width=60)

            for name, is_dir in entries:
                icon = "📁" if is_dir else "📄"

                # Get preview
                if is_dir:
                    try:
                        target_path = f"{path or self.vfs.pwd()}/{name}".replace('//', '/')
                        sub_entries = self.vfs.ls(target_path)
                        count = len(sub_entries)
                        preview = f"[{count} items]"
                    except Exception:
                        preview = ""
                else:
                    try:
                        target_path = f"{path or self.vfs.pwd()}/{name}".replace('//', '/')
                        content = self.vfs.cat(target_path)
                        preview = content[:57] + "..." if len(content) > 60 else content
                        # Escape newlines
                        preview = preview.replace('\n', '\\n')
                    except Exception:
                        preview = ""

                table.add_row(icon, name, preview)

            self.console.print(table)

        except Exception:
            raise

    def cmd_cd(self, args: List[str]):
        """Change directory."""
        if not args:
            # Go to root
            path = "/"
        else:
            path = args[0]

        self.vfs.cd(path)

    def cmd_pwd(self, args: List[str]):
        """Print working directory."""
        self.console.print(self.vfs.pwd())

    def cmd_cat(self, args: List[str]):
        """Display file contents."""
        if not args:
            self.console.print("[bold red]Error:[/bold red] cat requires a path argument")
            return

        path = args[0]
        content = self.vfs.cat(path)

        # Try to syntax highlight if it's JSON
        try:
            # Check if it's JSON
            json_data = json.loads(content) if isinstance(content, str) else content
            formatted = json.dumps(json_data, indent=2)
            syntax = Syntax(formatted, "json", theme="monokai", line_numbers=False)
            self.console.print(syntax)
        except (json.JSONDecodeError, TypeError):
            # Plain text
            self.console.print(content)

    def cmd_tree(self, args: List[str]):
        """Display directory tree."""
        path = args[0] if args else None
        max_depth = int(args[1]) if len(args) > 1 else 3

        target_path = path or self.vfs.pwd()

        # Build tree
        tree = Tree(
            f"[bold blue]{target_path}[/bold blue]",
            guide_style="dim"
        )

        try:
            self._build_tree(tree, target_path, max_depth, 0)
            self.console.print(tree)
        except Exception:
            raise

    def _build_tree(self, tree: Tree, path: str, max_depth: int, current_depth: int):
        """Recursively build a tree structure."""
        if current_depth >= max_depth:
            tree.add("[dim]...[/dim]")
            return

        try:
            entries = self.vfs.ls(path)

            for name, is_dir in entries[:20]:  # Limit to first 20 entries
                child_path = f"{path}/{name}".replace('//', '/')

                if is_dir:
                    icon = "📁"
                    style = "bold cyan"
                else:
                    icon = "📄"
                    style = "green"

                label = f"{icon} [{style}]{name}[/{style}]"
                branch = tree.add(label)

                if is_dir:
                    self._build_tree(branch, child_path, max_depth, current_depth + 1)

            if len(entries) > 20:
                tree.add(f"[dim]... and {len(entries) - 20} more items[/dim]")

        except Exception:
            pass

    def cmd_stat(self, args: List[str]):
        """Show detailed information about a path."""
        if not args:
            path = self.vfs.pwd()
        else:
            path = args[0]

        info = self.vfs.stat(path)

        # Create a nice panel
        content = []
        for key, value in info.items():
            content.append(f"[cyan]{key}:[/cyan] {value}")

        panel = Panel(
            "\n".join(content),
            title=f"Info: {path}",
            border_style="cyan",
            box=box.ROUNDED
        )

        self.console.print(panel)

    def _get_records(self, path: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get records from a JSONL file or current directory."""
        target_path = path or self.vfs.pwd()
        node, data = self.vfs._resolve_path(target_path)

        if node.node_type == NodeType.JSONL_FILE:
            if isinstance(data, LazyJSONL):
                return list(data)
            return list(data) if not isinstance(data, list) else data
        elif node.node_type == NodeType.ARRAY:
            return list(data) if not isinstance(data, list) else data
        else:
            raise TypeError(f"Cannot get records from {node.node_type.value}")

    def cmd_head(self, args: List[str]):
        """Show first N records (default: 10)."""
        # Parse args: head [n] [path]
        n = 10
        path = None

        for arg in args:
            if arg.isdigit():
                n = int(arg)
            else:
                path = arg

        try:
            records = self._get_records(path)
            records = records[:n]

            for i, record in enumerate(records):
                formatted = json.dumps(record, indent=2)
                syntax = Syntax(formatted, "json", theme="monokai", line_numbers=False)
                self.console.print(f"[dim]--- [{i}] ---[/dim]")
                self.console.print(syntax)

            self.console.print(f"\n[dim]Showing {len(records)} records[/dim]")

        except Exception as e:
            self.console.print(f"[bold red]Error:[/bold red] {e}")

    def cmd_tail(self, args: List[str]):
        """Show last N records (default: 10)."""
        # Parse args: tail [n] [path]
        n = 10
        path = None

        for arg in args:
            if arg.isdigit():
                n = int(arg)
            else:
                path = arg

        try:
            records = self._get_records(path)
            total = len(records)
            records = records[-n:]
            start_idx = max(0, total - n)

            for i, record in enumerate(records):
                formatted = json.dumps(record, indent=2)
                syntax = Syntax(formatted, "json", theme="monokai", line_numbers=False)
                self.console.print(f"[dim]--- [{start_idx + i}] ---[/dim]")
                self.console.print(syntax)

            self.console.print(f"\n[dim]Showing last {len(records)} of {total} records[/dim]")

        except Exception as e:
            self.console.print(f"[bold red]Error:[/bold red] {e}")

    def cmd_count(self, args: List[str]):
        """Count records in a JSONL file or array."""
        path = args[0] if args else None

        try:
            records = self._get_records(path)
            count = len(records)
            self.console.print(f"[bold cyan]{count}[/bold cyan] records")

        except Exception as e:
            self.console.print(f"[bold red]Error:[/bold red] {e}")

    def cmd_grep(self, args: List[str]):
        """Search for pattern in records.

        Usage: grep <pattern> [path] [--field <field>]
        """
        if not args:
            self.console.print("[bold red]Error:[/bold red] grep requires a pattern")
            return

        pattern = args[0]
        path = None
        field = None

        # Parse remaining args
        i = 1
        while i < len(args):
            if args[i] == '--field' and i + 1 < len(args):
                field = args[i + 1]
                i += 2
            else:
                path = args[i]
                i += 1

        try:
            records = self._get_records(path)
            regex = re.compile(pattern, re.IGNORECASE)
            matches = []

            for idx, record in enumerate(records):
                # Search in specific field or all fields
                if field:
                    # Get field value using ExprEval
                    parser = ExprEval()
                    value = parser.get_field_value(record, field)
                    if value is not None and regex.search(str(value)):
                        matches.append((idx, record))
                else:
                    # Search in entire record
                    record_str = json.dumps(record)
                    if regex.search(record_str):
                        matches.append((idx, record))

            # Display results
            if not matches:
                self.console.print("[dim]No matches found[/dim]")
                return

            table = Table(show_header=True, box=box.SIMPLE)
            table.add_column("Index", style="cyan", width=6)
            table.add_column("Record", style="green")

            for idx, record in matches[:50]:  # Limit to 50 results
                # Highlight matches
                record_str = json.dumps(record)
                # Simple highlight by replacing matches
                highlighted = regex.sub(
                    lambda m: f"[bold red]{m.group()}[/bold red]",
                    record_str
                )
                if len(highlighted) > 100:
                    highlighted = highlighted[:100] + "..."
                table.add_row(f"[{idx}]", highlighted)

            self.console.print(table)
            if len(matches) > 50:
                self.console.print(f"[dim]... and {len(matches) - 50} more matches[/dim]")
            self.console.print(f"\n[dim]{len(matches)} matches found[/dim]")

        except Exception as e:
            self.console.print(f"[bold red]Error:[/bold red] {e}")

    def cmd_select(self, args: List[str]):
        """Filter records with an expression.

        Usage: select <expression> [path]
        Example: select "age > 25" users.jsonl
        """
        if not args:
            self.console.print("[bold red]Error:[/bold red] select requires an expression")
            self.console.print("[dim]Usage: select <expression> [path][/dim]")
            return

        # Join args to support expressions with spaces
        expr = args[0]
        path = args[1] if len(args) > 1 else None

        try:
            records = self._get_records(path)

            # Apply selection using ja's select function
            filtered = list(ja_select(records, expr))

            if not filtered:
                self.console.print("[dim]No records match the expression[/dim]")
                return

            # Display results
            for i, record in enumerate(filtered[:20]):  # Limit to 20
                formatted = json.dumps(record, indent=2)
                syntax = Syntax(formatted, "json", theme="monokai", line_numbers=False)
                self.console.print(f"[dim]--- Result {i + 1} ---[/dim]")
                self.console.print(syntax)

            if len(filtered) > 20:
                self.console.print(f"\n[dim]... and {len(filtered) - 20} more records[/dim]")

            self.console.print(f"\n[dim]{len(filtered)} records match the expression[/dim]")

        except Exception as e:
            self.console.print(f"[bold red]Error:[/bold red] {e}")

    def cmd_help(self, args: List[str]):
        """Show help information."""
        help_text = """
[bold cyan]Navigation Commands:[/bold cyan]

  [bold]ls[/bold] [path]           List directory contents
  [bold]cd[/bold] <path>           Change directory
  [bold]pwd[/bold]                 Print working directory
  [bold]cat[/bold] <path>          Display file contents
  [bold]tree[/bold] [path] [depth] Display directory tree
  [bold]stat[/bold] <path>         Show detailed path information

[bold cyan]Data Commands:[/bold cyan]

  [bold]head[/bold] [n] [path]     Show first N records (default: 10)
  [bold]tail[/bold] [n] [path]     Show last N records (default: 10)
  [bold]count[/bold] [path]        Count records
  [bold]grep[/bold] <pattern> [path] [--field <field>]
                        Search for pattern (regex) in records
  [bold]select[/bold] <expr> [path]  Filter records with expression

[bold cyan]Other:[/bold cyan]

  [bold]help[/bold]                Show this help message
  [bold]exit/quit[/bold]           Exit the shell

[bold cyan]Path Syntax:[/bold cyan]

  [bold]/[/bold]                   Root directory (physical files)
  [bold]file.jsonl[/bold]          JSONL file (collection of records)
  [bold]file.json[/bold]           JSON file
  [bold][0][/bold]                 Array/record index
  [bold]@[expr][/bold]             Filter (e.g., @[age>25])
  [bold]key[/bold]                 Object key

[bold cyan]Examples:[/bold cyan]

  ls                           # List current directory
  cd users.jsonl               # Navigate into JSONL file
  head 5 users.jsonl           # Show first 5 records
  tail 3                       # Show last 3 records (current path)
  count users.jsonl            # Count records in file
  grep "alice" users.jsonl     # Search for "alice"
  grep "NY" --field location   # Search in specific field
  select "age > 25"            # Filter with expression
  select "status == 'active'" users.jsonl
"""

        panel = Panel(
            help_text,
            title="ja-shell Help",
            border_style="cyan",
            box=box.ROUNDED
        )

        self.console.print(panel)

    def cmd_exit(self, args: List[str]):
        """Exit the shell."""
        self.running = False


def main():
    """Entry point for ja-shell."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Interactive shell for navigating JSON/JSONL files"
    )
    parser.add_argument(
        'root',
        nargs='?',
        default='.',
        help='Root directory containing JSON/JSONL files (default: current directory)'
    )

    args = parser.parse_args()

    shell = JAShell(args.root)
    shell.run()


if __name__ == '__main__':
    main()
