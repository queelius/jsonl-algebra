#!/usr/bin/env python3
"""
Rich interactive shell for navigating JSON/JSONL files.

Provides a filesystem-like interface with:
- Command history and editing (using prompt_toolkit)
- Tab completion for paths and commands
- Pretty-printed output (using rich)
- Syntax highlighting
"""

import os
import sys
import json
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion, WordCompleter
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    from prompt_toolkit.styles import Style
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

from .vfs import JSONPath, NodeType


class PathCompleter(Completer):
    """Tab completion for filesystem paths."""

    def __init__(self, vfs: JSONPath):
        self.vfs = vfs

    def get_completions(self, document, complete_event):
        """Generate completions for the current input."""
        text = document.text_before_cursor
        words = text.split()

        if not words:
            return

        # Get the path to complete (last word)
        path_to_complete = words[-1] if words else ""

        # Split into directory and prefix
        if '/' in path_to_complete:
            dir_path, prefix = path_to_complete.rsplit('/', 1)
            if not dir_path:
                dir_path = '/'
        else:
            dir_path = self.vfs.pwd()
            prefix = path_to_complete

        # Try to list directory
        try:
            entries = self.vfs.ls(dir_path)
            for name, is_dir in entries:
                if name.startswith(prefix):
                    suffix = '/' if is_dir else ''
                    yield Completion(
                        name + suffix,
                        start_position=-len(prefix),
                        display=name + suffix
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
        self.session = PromptSession(
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
        while self.running:
            try:
                # Get input with completion
                completer = WordCompleter(
                    list(self.commands.keys()) + [
                        name for name, _ in self.vfs.ls()
                    ],
                    ignore_case=True
                )

                user_input = self.session.prompt(
                    self.get_prompt(),
                    completer=completer
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

        except Exception as e:
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
        except Exception as e:
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

                label = f"{icon} [/]{style}]{name}[/{style}]"
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

    def cmd_help(self, args: List[str]):
        """Show help information."""
        help_text = """
[bold cyan]Available Commands:[/bold cyan]

  [bold]ls[/bold] [path]           List directory contents
  [bold]cd[/bold] <path>           Change directory
  [bold]pwd[/bold]                 Print working directory
  [bold]cat[/bold] <path>          Display file contents
  [bold]tree[/bold] [path] [depth] Display directory tree
  [bold]stat[/bold] <path>         Show detailed path information
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

  ls                      # List current directory
  cd users.jsonl          # Navigate into JSONL file
  ls                      # Shows [0], [1], [2]...
  cd [0]                  # Navigate into first record
  ls                      # Shows record keys
  cat name                # Show the 'name' field value
  cd /                    # Back to root
  tree users.jsonl 2      # Show tree view, depth 2
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
