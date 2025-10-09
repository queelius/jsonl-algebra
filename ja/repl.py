"""Interactive REPL (Read-Eval-Print Loop) for JSONL algebra operations.

This module provides a powerful, interactive shell for JSONL data manipulation
with named datasets, immediate execution, and a non-destructive design.

Key features:
- Named datasets: Load and manage multiple JSONL files by name
- Safe operations: All transformations require unique output names
- Immediate execution: See results right away, no pipeline building
- File-based streaming: Store paths, not data (memory efficient)
- Shell integration: Execute bash commands with !<command>
"""

import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional


class ReplSession:
    """Interactive REPL session for JSONL data manipulation.

    This class manages a workspace of named datasets (JSONL files) and provides
    commands for loading, transforming, and exploring data interactively.

    Design principles:
    - Non-destructive: Operations create new datasets, never modify originals
    - Explicit: All operations require unique output names
    - Streaming: Store file paths, not data in memory
    """

    def __init__(self):
        """Initialize the REPL session."""
        self.datasets: Dict[str, str] = {}  # name -> file_path
        self.current_dataset: Optional[str] = None
        self.settings = {
            "window_size": 20,  # Default preview limit
        }
        self.temp_dir = tempfile.mkdtemp(prefix="ja_repl_")
        self.temp_counter = 0

    def _get_temp_file(self, name: str) -> str:
        """Generate a unique temp file path for a dataset name."""
        self.temp_counter += 1
        return os.path.join(self.temp_dir, f"{name}_{self.temp_counter}.jsonl")

    def _check_name_conflict(self, name: str) -> None:
        """Raise error if dataset name already exists."""
        if name in self.datasets:
            raise ValueError(
                f"Dataset '{name}' already exists. Use a different name."
            )

    def _require_current(self) -> str:
        """Raise error if no current dataset is set."""
        if self.current_dataset is None:
            raise ValueError(
                "No current dataset. Use 'load <file>' or 'cd <name>' first."
            )
        return self.current_dataset

    def _execute_ja_command(self, cmd_parts: list) -> subprocess.CompletedProcess:
        """Execute a ja command and return the result."""
        cmd = shlex.join(cmd_parts)
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=False,
            )
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to execute command: {e}")

    # ==================== Command Handlers ====================

    def handle_load(self, args):
        """Load a JSONL file into the workspace.

        Usage: load <file> [name]

        If name is not provided, uses the file stem (filename without extension).
        The loaded dataset becomes the current dataset.
        """
        if not args:
            print("Error: 'load' requires a file path.")
            print("Usage: load <file> [name]")
            return

        file_path = args[0]
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return

        # Determine dataset name
        if len(args) > 1:
            name = args[1]
        else:
            name = Path(file_path).stem

        # Check for conflicts
        try:
            self._check_name_conflict(name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        # Register the dataset
        self.datasets[name] = os.path.abspath(file_path)
        self.current_dataset = name
        print(f"Loaded: {name} (current)")
        print(f"  Path: {self.datasets[name]}")

    def handle_cd(self, args):
        """Switch to a different dataset.

        Usage: cd <name>
        """
        if not args:
            print("Error: 'cd' requires a dataset name.")
            print("Usage: cd <name>")
            return

        name = args[0]
        if name not in self.datasets:
            print(f"Error: Unknown dataset '{name}'.")
            print(f"Available datasets: {', '.join(self.datasets.keys())}")
            return

        self.current_dataset = name
        print(f"Current dataset: {name}")

    def handle_pwd(self, args):
        """Show the current dataset name and path.

        Usage: pwd
        Alias: current
        """
        if self.current_dataset is None:
            print("No current dataset.")
            return

        print(f"Current dataset: {self.current_dataset}")
        print(f"  Path: {self.datasets[self.current_dataset]}")

    def handle_current(self, args):
        """Alias for pwd."""
        self.handle_pwd(args)

    def handle_datasets(self, args):
        """List all registered datasets.

        Usage: datasets

        Shows all loaded datasets with a marker for the current one.
        """
        if not self.datasets:
            print("No datasets loaded.")
            return

        print("Registered datasets:")
        for name in sorted(self.datasets.keys()):
            marker = " (current)" if name == self.current_dataset else ""
            print(f"  {name}{marker}")
            print(f"    {self.datasets[name]}")

    def handle_save(self, args):
        """Save the current dataset to a file.

        Usage: save <file>

        Writes the current dataset to the specified file path.
        Does not register the file as a new dataset.
        """
        if not args:
            print("Error: 'save' requires a file path.")
            print("Usage: save <file>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = args[0]
        input_file = self.datasets[current]

        # Copy the current dataset to the output file
        try:
            with open(input_file, 'r') as inf, open(output_file, 'w') as outf:
                outf.write(inf.read())
            print(f"Saved {current} to: {output_file}")
        except Exception as e:
            print(f"Error saving file: {e}")

    def handle_ls(self, args):
        """Preview a dataset.

        Usage: ls [name] [--limit N]

        Shows the first N lines of the dataset (default: window_size setting).
        If name is omitted, shows the current dataset.
        """
        # Parse arguments
        dataset_name = None
        limit = self.settings["window_size"]

        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "--limit":
                if i + 1 >= len(args):
                    print("Error: --limit requires a number.")
                    return
                try:
                    limit = int(args[i + 1])
                    i += 2
                except ValueError:
                    print(f"Error: Invalid limit value '{args[i + 1]}'.")
                    return
            elif arg.startswith("--limit="):
                try:
                    limit = int(arg.split("=", 1)[1])
                    i += 1
                except ValueError:
                    print(f"Error: Invalid limit value in '{arg}'.")
                    return
            else:
                dataset_name = arg
                i += 1

        # Determine which dataset to show
        if dataset_name is None:
            try:
                dataset_name = self._require_current()
            except ValueError as e:
                print(f"Error: {e}")
                return
        elif dataset_name not in self.datasets:
            print(f"Error: Unknown dataset '{dataset_name}'.")
            return

        file_path = self.datasets[dataset_name]

        # Use head to show first N lines
        try:
            result = subprocess.run(
                ["head", f"-n{limit}", file_path],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                print(result.stdout.rstrip())
            else:
                print(f"Error reading dataset: {result.stderr}")
        except Exception as e:
            print(f"Error: {e}")

    def handle_shell(self, args):
        """Execute a shell command.

        Usage: !<command>

        Passes the command directly to the shell.
        """
        # args is already the full command (without the !)
        cmd = " ".join(args)
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                check=False,
            )
        except Exception as e:
            print(f"Error executing shell command: {e}")

    def handle_window_size(self, args):
        """Get or set the window size setting.

        Usage: window-size [N]

        Without arguments, shows the current value.
        With a number, sets the value.
        """
        if not args:
            print(f"window-size: {self.settings['window_size']}")
            return

        try:
            new_size = int(args[0])
            if new_size <= 0:
                print("Error: window-size must be a positive integer.")
                return
            self.settings["window_size"] = new_size
            print(f"window-size set to: {new_size}")
        except ValueError:
            print(f"Error: Invalid number '{args[0]}'.")

    def handle_info(self, args):
        """Show statistics and information about a dataset.

        Usage: info [name]

        If name is omitted, shows info for the current dataset.
        Displays: row count, file size, fields, and a sample row.
        """
        # Determine which dataset to show info for
        dataset_name = None
        if args:
            dataset_name = args[0]
            if dataset_name not in self.datasets:
                print(f"Error: Unknown dataset '{dataset_name}'.")
                return
        else:
            try:
                dataset_name = self._require_current()
            except ValueError as e:
                print(f"Error: {e}")
                return

        file_path = self.datasets[dataset_name]

        try:
            import json
            import os

            # Get file size
            file_size = os.path.getsize(file_path)
            if file_size < 1024:
                size_str = f"{file_size} B"
            elif file_size < 1024 * 1024:
                size_str = f"{file_size / 1024:.1f} KB"
            else:
                size_str = f"{file_size / (1024 * 1024):.1f} MB"

            # Count rows and collect field names
            row_count = 0
            all_fields = set()
            first_row = None

            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        row_count += 1
                        try:
                            obj = json.loads(line)
                            if first_row is None:
                                first_row = obj
                            # Collect field names (flatten nested objects)
                            self._collect_fields(obj, all_fields)
                        except json.JSONDecodeError:
                            pass  # Skip malformed lines

            # Sort fields for consistent display
            fields = sorted(all_fields)

            # Display info
            print(f"\nDataset: {dataset_name}")
            print(f"Path: {file_path}")
            print(f"Rows: {row_count:,}")
            print(f"Size: {size_str}")

            if fields:
                # Limit field display to avoid clutter
                if len(fields) <= 20:
                    print(f"Fields: {', '.join(fields)}")
                else:
                    print(f"Fields ({len(fields)} total): {', '.join(fields[:20])}, ...")

            if first_row:
                print(f"\nSample (first row):")
                print(f"  {json.dumps(first_row, indent=2)}")

            print()

        except FileNotFoundError:
            print(f"Error: File not found: {file_path}")
        except Exception as e:
            print(f"Error reading dataset: {e}")

    def _collect_fields(self, obj, field_set, prefix=""):
        """Recursively collect field names from a JSON object.

        Nested fields are represented with dot notation (e.g., 'user.name').
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                full_key = f"{prefix}.{key}" if prefix else key
                field_set.add(full_key)
                if isinstance(value, dict):
                    self._collect_fields(value, field_set, full_key)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    # For arrays of objects, show the array field plus nested fields
                    self._collect_fields(value[0], field_set, full_key)
        elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
            self._collect_fields(obj[0], field_set, prefix)

    # ==================== Unary Operations ====================

    def handle_select(self, args):
        """Filter rows with an expression.

        Usage: select '<expr>' <output_name>

        Creates a new dataset with filtered rows.
        """
        if len(args) < 2:
            print("Error: 'select' requires an expression and output name.")
            print("Usage: select '<expr>' <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        expr = args[0]
        output_name = args[1]

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        # Create temp file for output
        output_file = self._get_temp_file(output_name)
        input_file = self.datasets[current]

        # Execute: ja select '<expr>' <input> > <output>
        cmd_parts = ["ja", "select", expr, input_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            # Save output to temp file
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_project(self, args):
        """Select specific fields.

        Usage: project <fields> <output_name>

        Creates a new dataset with only the specified fields.
        """
        if len(args) < 2:
            print("Error: 'project' requires fields and output name.")
            print("Usage: project <fields> <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        fields = args[0]
        output_name = args[1]

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        input_file = self.datasets[current]

        cmd_parts = ["ja", "project", fields, input_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_rename(self, args):
        """Rename fields.

        Usage: rename <mapping> <output_name>

        Example: rename old=new,foo=bar output
        """
        if len(args) < 2:
            print("Error: 'rename' requires a mapping and output name.")
            print("Usage: rename <old=new,...> <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        mapping = args[0]
        output_name = args[1]

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        input_file = self.datasets[current]

        cmd_parts = ["ja", "rename", mapping, input_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_distinct(self, args):
        """Remove duplicate rows.

        Usage: distinct <output_name>
        """
        if len(args) < 1:
            print("Error: 'distinct' requires an output name.")
            print("Usage: distinct <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_name = args[0]

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        input_file = self.datasets[current]

        cmd_parts = ["ja", "distinct", input_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_sort(self, args):
        """Sort rows by key(s).

        Usage: sort <keys> [--desc] <output_name>

        Example: sort age,name output
        Example: sort age --desc output
        """
        if len(args) < 2:
            print("Error: 'sort' requires keys and output name.")
            print("Usage: sort <keys> [--desc] <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        # Parse args: keys, optional --desc, output_name
        keys = args[0]
        desc = False
        output_name = args[-1]

        if "--desc" in args:
            desc = True

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        input_file = self.datasets[current]

        cmd_parts = ["ja", "sort", keys, input_file]
        if desc:
            cmd_parts.append("--desc")

        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_groupby(self, args):
        """Group rows by a key.

        Usage: groupby <key> [--agg <spec>] <output_name>

        Example: groupby region output
        Example: groupby region --agg count,sum(amount) output
        """
        if len(args) < 2:
            print("Error: 'groupby' requires a key and output name.")
            print("Usage: groupby <key> [--agg <spec>] <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        key = args[0]
        output_name = args[-1]

        # Check for --agg
        agg_spec = None
        if "--agg" in args:
            agg_idx = args.index("--agg")
            if agg_idx + 1 < len(args) - 1:  # -1 because last is output_name
                agg_spec = args[agg_idx + 1]

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        input_file = self.datasets[current]

        cmd_parts = ["ja", "groupby", key, input_file]
        if agg_spec:
            cmd_parts.extend(["--agg", agg_spec])

        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    # ==================== Binary Operations ====================

    def handle_join(self, args):
        """Join with another dataset.

        Usage: join <dataset_name> --on <mapping> <output_name>

        Example: join orders --on user_id=id user_orders
        """
        if len(args) < 4 or "--on" not in args:
            print("Error: 'join' requires a dataset, --on mapping, and output name.")
            print("Usage: join <dataset> --on <mapping> <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        right_name = args[0]
        if right_name not in self.datasets:
            print(f"Error: Unknown dataset '{right_name}'.")
            return

        on_idx = args.index("--on")
        on_mapping = args[on_idx + 1]
        output_name = args[-1]

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        left_file = self.datasets[current]
        right_file = self.datasets[right_name]

        cmd_parts = ["ja", "join", left_file, right_file, "--on", on_mapping]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_union(self, args):
        """Union with another dataset.

        Usage: union <dataset_name> <output_name>
        """
        if len(args) < 2:
            print("Error: 'union' requires a dataset name and output name.")
            print("Usage: union <dataset> <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        right_name = args[0]
        output_name = args[1]

        if right_name not in self.datasets:
            print(f"Error: Unknown dataset '{right_name}'.")
            return

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        left_file = self.datasets[current]
        right_file = self.datasets[right_name]

        cmd_parts = ["ja", "union", left_file, right_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_intersection(self, args):
        """Intersection with another dataset.

        Usage: intersection <dataset_name> <output_name>
        """
        if len(args) < 2:
            print("Error: 'intersection' requires a dataset name and output name.")
            print("Usage: intersection <dataset> <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        right_name = args[0]
        output_name = args[1]

        if right_name not in self.datasets:
            print(f"Error: Unknown dataset '{right_name}'.")
            return

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        left_file = self.datasets[current]
        right_file = self.datasets[right_name]

        cmd_parts = ["ja", "intersection", left_file, right_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_difference(self, args):
        """Difference with another dataset.

        Usage: difference <dataset_name> <output_name>
        """
        if len(args) < 2:
            print("Error: 'difference' requires a dataset name and output name.")
            print("Usage: difference <dataset> <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        right_name = args[0]
        output_name = args[1]

        if right_name not in self.datasets:
            print(f"Error: Unknown dataset '{right_name}'.")
            return

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        left_file = self.datasets[current]
        right_file = self.datasets[right_name]

        cmd_parts = ["ja", "difference", left_file, right_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_product(self, args):
        """Cartesian product with another dataset.

        Usage: product <dataset_name> <output_name>
        """
        if len(args) < 2:
            print("Error: 'product' requires a dataset name and output name.")
            print("Usage: product <dataset> <output_name>")
            return

        try:
            current = self._require_current()
        except ValueError as e:
            print(f"Error: {e}")
            return

        right_name = args[0]
        output_name = args[1]

        if right_name not in self.datasets:
            print(f"Error: Unknown dataset '{right_name}'.")
            return

        try:
            self._check_name_conflict(output_name)
        except ValueError as e:
            print(f"Error: {e}")
            return

        output_file = self._get_temp_file(output_name)
        left_file = self.datasets[current]
        right_file = self.datasets[right_name]

        cmd_parts = ["ja", "product", left_file, right_file]
        result = self._execute_ja_command(cmd_parts)

        if result.returncode == 0:
            with open(output_file, 'w') as f:
                f.write(result.stdout)

            self.datasets[output_name] = output_file
            self.current_dataset = output_name
            print(f"Created: {output_name} (current)")
        else:
            print(f"Error: {result.stderr}")

    def handle_help(self, args):
        """Display help message."""
        help_text = """
JSONL Algebra REPL - Interactive Data Manipulation

DATASET MANAGEMENT:
  load <file> [name]           Load a JSONL file (default name: file stem)
  cd <name>                    Switch to a dataset
  pwd / current                Show current dataset
  datasets                     List all registered datasets
  info [name]                  Show dataset statistics (rows, size, fields)
  save <file>                  Save current dataset to file

UNARY OPERATIONS (operate on current dataset):
  select '<expr>' <output>     Filter rows with expression
  project <fields> <output>    Select specific fields (comma-separated)
  rename <old=new> <output>    Rename fields
  distinct <output>            Remove duplicates
  sort <keys> [--desc] <out>   Sort by keys
  groupby <key> [--agg <spec>] <output>
                               Group and optionally aggregate

BINARY OPERATIONS (combine current with another dataset):
  join <dataset> --on <map> <output>
                               Join datasets on keys
  union <dataset> <output>     Union of datasets
  intersection <dataset> <out> Intersection of datasets
  difference <dataset> <out>   Difference of datasets
  product <dataset> <output>   Cartesian product

VIEWING & EXPLORATION:
  ls [name] [--limit N]        Preview dataset (default: current)
  !<command>                   Execute shell command

SETTINGS:
  window-size [N]              Get/set preview window size

META:
  help                         Show this help
  exit                         Quit REPL

NOTES:
- All operations create NEW datasets with unique names
- Use dot notation for nested fields (e.g., user.name)
- Current dataset is used as input for operations
- Operations automatically switch to the new output dataset

EXAMPLES:
  ja> load users.jsonl
  ja> select 'age > 30' adults
  ja> project name,email adults_contact
  ja> load orders.jsonl
  ja> cd adults_contact
  ja> join orders --on user_id=id final
  ja> ls --limit 5
  ja> save results.jsonl
"""
        print(help_text)

    def parse_command(self, line: str):
        """Parse a command line into command and arguments."""
        try:
            parts = shlex.split(line)
        except ValueError as e:
            print(f"Error parsing command: {e}")
            return None, None

        if not parts:
            return None, None

        command = parts[0].lower()
        args = parts[1:]
        return command, args

    def process(self, line: str):
        """Process a single command line."""
        if not line or line.strip() == "":
            return

        # Handle shell commands
        if line.startswith("!"):
            cmd = line[1:].strip()
            self.handle_shell(shlex.split(cmd))
            return

        command, args = self.parse_command(line)
        if command is None:
            return

        # Command routing
        handlers = {
            "load": self.handle_load,
            "cd": self.handle_cd,
            "pwd": self.handle_pwd,
            "current": self.handle_current,
            "datasets": self.handle_datasets,
            "info": self.handle_info,
            "save": self.handle_save,
            "ls": self.handle_ls,
            "window-size": self.handle_window_size,
            "select": self.handle_select,
            "project": self.handle_project,
            "rename": self.handle_rename,
            "distinct": self.handle_distinct,
            "sort": self.handle_sort,
            "groupby": self.handle_groupby,
            "join": self.handle_join,
            "union": self.handle_union,
            "intersection": self.handle_intersection,
            "difference": self.handle_difference,
            "product": self.handle_product,
            "help": self.handle_help,
            "exit": lambda _: sys.exit(0),
        }

        handler = handlers.get(command)
        if handler:
            try:
                handler(args)
            except Exception as e:
                print(f"Error: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"Unknown command: '{command}'. Type 'help' for available commands.")

    def run(self, initial_args=None):
        """Run the REPL main loop."""
        print("Welcome to ja REPL. Type 'help' for commands, 'exit' to quit.")

        # Handle initial args (e.g., ja repl data.jsonl)
        if initial_args and len(initial_args) > 0:
            # Auto-load the file
            initial_line = f"load {shlex.join(initial_args)}"
            self.process(initial_line)

        while True:
            try:
                line = input("ja> ").strip()
                self.process(line)
            except EOFError:
                print("\nExiting...")
                sys.exit(0)
            except KeyboardInterrupt:
                print("\nInterrupted. Type 'exit' or Ctrl-D to quit.")


def repl(parsed_cli_args):
    """Entry point for the ja repl command."""
    session = ReplSession()
    initial_args = getattr(parsed_cli_args, "initial_args", [])
    session.run(initial_args)
