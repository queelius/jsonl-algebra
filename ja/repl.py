"""Interactive REPL (Read-Eval-Print Loop) for JSONL algebra operations.

This module provides a friendly, interactive shell for chaining JSONL algebra
operations together. It's a great way to explore your data, build up complex
transformation pipelines step-by-step, and see the results instantly.

Think of it as a command-line laboratory for your JSONL data!
"""

import shlex
import subprocess
import sys


class ReplCompiler:
    """Compiles and executes a sequence of JSONL algebra commands.

    This class is the engine of the REPL. It manages the state of the command
    pipeline, parses user input, and translates the pipeline into a shell command
    that can be executed. It's designed to provide an intuitive, interactive
    experience for building data workflows.
    """

    def __init__(self):
        """Initialize the REPL compiler with an empty pipeline."""
        self.pipeline = []
        self.current_input_source = None  # Can be a file path or None (implying stdin)
        self.handlers = {}  # Command handlers are registered in the `run` method.

    def parse_command(self, line):
        """Parse a line of input into a command and its arguments.

        Uses `shlex` to handle quoted arguments correctly.

        Args:
            line (str): The raw input line from the user.

        Returns:
            A tuple of (command, args_list), or (None, None) if parsing fails.
        """
        try:
            parts = shlex.split(line)
        except ValueError as e:
            print(f"Error parsing command: {e}. Check your quotes.")
            return None, None
        if not parts:
            return None, None
        command = parts[0].lower()
        args = parts[1:]
        return command, args

    def handle_from(self, args):
        """Set the initial data source for the pipeline (e.g., a file).

        This command must be the first one used when starting a new pipeline
        with a file source.

        Args:
            args (list): A list containing the file path or "stdin".
        """
        if not args:
            print("Error: 'from' requires a file path (or 'stdin').")
            return
        if self.pipeline:
            print(
                "Error: 'from' can only be used at the beginning of a new pipeline. Use 'reset' first."
            )
            return
        self.current_input_source = args[0]
        if self.current_input_source.lower() == "stdin":
            self.current_input_source = None  # Internally, None means stdin for clarity
            print("Input source set to: stdin")
        else:
            print(f"Input source set to: {self.current_input_source}")

    def add_to_pipeline(self, command_name, args, cli_command_name=None):
        """Add a new command step to the current pipeline.

        Args:
            command_name (str): The name of the REPL command (e.g., "project").
            args (list): The list of arguments for the command.
            cli_command_name (str, optional): The corresponding `ja` CLI command name.
                                             Defaults to `command_name`.
        """
        if not cli_command_name:
            cli_command_name = command_name
        # Ensure 'from' is not added to the pipeline steps directly
        if command_name.lower() == "from":
            print(
                "Error: 'from' is a directive, not a pipeline step. Use 'reset' then 'from <file>'."
            )
            return
        self.pipeline.append(
            {
                "repl_command": command_name,
                "cli_command": cli_command_name,
                "args": args,
            }
        )
        print(f"Added: {command_name} {' '.join(shlex.quote(a) for a in args)}")

    def handle_select(self, args):
        """Handle the 'select' command by adding it to the pipeline."""
        if not args:
            print("Error: 'select' requires an expression.")
            return
        self.add_to_pipeline("select", args)

    def handle_project(self, args):
        """Handle the 'project' command by adding it to the pipeline."""
        if not args:
            print("Error: 'project' requires column names.")
            return
        self.add_to_pipeline("project", args)

    def handle_join(self, args):
        """Handle the 'join' command by adding it to the pipeline."""
        if len(args) < 3 or args[-2].lower() != "--on":
            print("Error: 'join' requires <right_file> --on <key_map>.")
            print("Example: join orders.jsonl --on user.id=customer_id")
            return
        self.add_to_pipeline("join", args)

    def handle_rename(self, args):
        """Handle the 'rename' command by adding it to the pipeline."""
        if not args:
            print("Error: 'rename' requires a mapping (e.g., old_name=new_name).")
            return
        self.add_to_pipeline("rename", args)

    def handle_distinct(self, args):
        """Handle the 'distinct' command by adding it to the pipeline."""
        if args:
            print("Warning: 'distinct' does not take arguments in REPL mode. Ignoring.")
        self.add_to_pipeline("distinct", [])

    def handle_sort(self, args):
        """Handle the 'sort' command by adding it to the pipeline."""
        if not args:
            print("Error: 'sort' requires column names.")
            return
        self.add_to_pipeline("sort", args)

    def handle_groupby(self, args):
        """Handle the 'groupby' command by adding it to the pipeline."""
        # Support both chained groupby (no --agg) and immediate aggregation (with --agg)
        if "--agg" in args:
            # Traditional groupby with immediate aggregation
            if len(args) < 3 or args[-2].lower() != "--agg":
                print("Error: 'groupby --agg' requires <key> --agg <spec>.")
                print("Example: groupby user.location --agg count,sum(amount)")
                return
        else:
            # Chained groupby mode
            if not args:
                print("Error: 'groupby' requires a key.")
                print("Example: groupby region")
                return
        self.add_to_pipeline("groupby", args)

    def handle_agg(self, args):
        """Handle the 'agg' command by adding it to the pipeline."""
        if not args:
            print("Error: 'agg' requires an aggregation specification.")
            print("Example: agg count,total=sum(amount)")
            return
        self.add_to_pipeline("agg", args)

    def handle_product(self, args):
        """Handle the 'product' command by adding it to the pipeline."""
        if not args:
            print("Error: 'product' requires a right file path.")
            return
        self.add_to_pipeline("product", args)

    def handle_union(self, args):
        """Handle the 'union' command by adding it to the pipeline."""
        if not args:
            print("Error: 'union' requires a file path.")
            return
        self.add_to_pipeline("union", args)

    def handle_intersection(self, args):
        """Handle the 'intersection' command by adding it to the pipeline."""
        if not args:
            print("Error: 'intersection' requires a file path.")
            return
        self.add_to_pipeline("intersection", args)

    def handle_difference(self, args):
        """Handle the 'difference' command by adding it to the pipeline."""
        if not args:
            print("Error: 'difference' requires a file path.")
            return
        self.add_to_pipeline("difference", args)

    def _generate_pipeline_command_string_and_segments(self):
        """Construct the full shell command string from the pipeline steps.

        This is the core logic that translates the user's interactive steps into
        a runnable `ja ... | ja ...` shell command.

        Returns:
            A tuple containing:
            - The full, executable shell command string.
            - A list of individual command segments for display.
            - An error message string, if any.
        """
        if not self.pipeline:
            return None, None, "Pipeline is empty."

        display_segments = []
        execution_segments = []

        for i, step in enumerate(self.pipeline):
            current_ja_cmd_parts = ["ja", step["cli_command"]]
            is_first_command_in_pipe = i == 0

            if step["cli_command"] in ["join", "product", "union", "intersection", "difference"]:
                # REPL args: <right_file> [--on <key_map>] for join
                # REPL args: <right_file> for product
                # CLI: ja join <left> <right> --on <key_map>
                # CLI: ja product <left> <right>
                right_file_repl_arg = step["args"][0]

                if is_first_command_in_pipe:
                    left_input_for_cli = (
                        self.current_input_source if self.current_input_source else "-"
                    )
                else:
                    left_input_for_cli = "-"

                current_ja_cmd_parts.append(left_input_for_cli)
                current_ja_cmd_parts.append(right_file_repl_arg)

                if step["cli_command"] == "join":
                    current_ja_cmd_parts.extend(step["args"][1:])  # --on <key_map>

            elif step["cli_command"] == "groupby":
                # REPL args: <key> [--agg <spec>]
                # CLI: ja groupby <key> <file_or_stdin> [--agg <spec>]
                key_repl_arg = step["args"][0]
                other_args = step["args"][1:]

                current_ja_cmd_parts.append(key_repl_arg)

                if is_first_command_in_pipe:
                    input_file_for_cli = (
                        self.current_input_source if self.current_input_source else "-"
                    )
                else:
                    input_file_for_cli = "-"
                current_ja_cmd_parts.append(input_file_for_cli)
                current_ja_cmd_parts.extend(other_args)

            elif step["cli_command"] == "agg":
                # REPL args: <spec>
                # CLI: ja agg <spec> [file_or_stdin]
                current_ja_cmd_parts.extend(step["args"])

                if is_first_command_in_pipe:
                    if self.current_input_source:
                        current_ja_cmd_parts.append(self.current_input_source)

            else:  # select, project, rename, distinct, sort
                # REPL args: <command_specific_args>
                # CLI: ja <command> [command_specific_args...] [file_if_first_and_not_stdin]
                current_ja_cmd_parts.extend(step["args"])

                if is_first_command_in_pipe:
                    if self.current_input_source:
                        current_ja_cmd_parts.append(self.current_input_source)

            joined_segment = shlex.join(current_ja_cmd_parts)
            display_segments.append(joined_segment)
            execution_segments.append(joined_segment)

        executable_command_string = " | ".join(execution_segments)
        return executable_command_string, display_segments, None

    def handle_compile(self, cmd_args):
        """Generate and print a bash script for the current pipeline."""
        _executable_cmd_str, display_segments, error_msg = (
            self._generate_pipeline_command_string_and_segments()
        )

        if error_msg:
            print(error_msg)
            return

        print("\n--- Compiled Bash Script ---")
        print("#!/bin/bash")
        print("# Generated by ja REPL")

        if not display_segments:
            print("# Pipeline is empty.")
        elif len(display_segments) == 1:
            print(display_segments[0])
        else:
            # Build the pretty-printed pipeline string
            script_str = display_segments[0]
            for i in range(1, len(display_segments)):
                script_str += f" | \\\n  {display_segments[i]}"
            print(script_str)
        print("--------------------------\n")

    def handle_execute(self, cmd_args):
        """Execute the current pipeline and display the output."""
        limit_lines = None
        if cmd_args:
            if cmd_args[0].startswith("--lines="):
                try:
                    limit_lines = int(cmd_args[0].split("=")[1])
                    if limit_lines <= 0:
                        print("Error: --lines must be a positive integer.")
                        return
                except (ValueError, IndexError):
                    print(
                        "Error: Invalid format for --lines. Use --lines=N (e.g., --lines=10)."
                    )
                    return
            else:
                print(
                    f"Warning: Unknown argument '{cmd_args[0]}' for execute. Ignoring. Did you mean --lines=N?"
                )

        command_to_execute, _display_segments, error_msg = (
            self._generate_pipeline_command_string_and_segments()
        )

        if error_msg:
            print(error_msg)
            return
        if not command_to_execute:
            print(
                "Internal error: No command to execute."
            )  # Should be caught by error_msg
            return

        print(f"Executing: {command_to_execute}")

        try:
            process = subprocess.run(
                command_to_execute,
                shell=True,  # Essential for pipes
                capture_output=True,
                text=True,
                check=False,  # Manually check returncode
            )

            print("\n--- Output ---")
            if process.stdout:
                output_lines_list = process.stdout.splitlines()
                if limit_lines is not None:
                    for i, line_content in enumerate(output_lines_list):
                        if i < limit_lines:
                            print(line_content)
                        else:
                            print(
                                f"... (output truncated to {limit_lines} lines, total {len(output_lines_list)})"
                            )
                            break
                else:
                    print(process.stdout.strip())
            elif process.returncode == 0:
                print("(No output produced)")

            if process.stderr:
                print("\n--- Errors ---")
                print(process.stderr.strip())

            if process.returncode != 0:
                print(f"\nCommand exited with status {process.returncode}")
            elif not process.stdout and not process.stderr and process.returncode == 0:
                print("(Execution successful: No output and no errors)")

            print("--------------\n")

        except FileNotFoundError:  # pragma: no cover
            print(f"Error: Command 'ja' not found. Make sure it's in your PATH.")
        except Exception as e:  # pragma: no cover
            print(
                f"An unexpected error occurred while trying to execute the command: {e}"
            )
            # import traceback
            # traceback.print_exc()

    def handle_reset(self, args):
        """Clear the current pipeline and reset the input source."""
        self.pipeline = []
        self.current_input_source = None
        print("Pipeline reset.")

    def handle_pipeline_show(self, args):
        """Display the steps in the current pipeline."""
        if not self.pipeline:
            print("Pipeline is empty.")
        else:
            print("Current pipeline:")
            if self.current_input_source:
                print(f"  Input: {self.current_input_source}")
            else:
                print(
                    f"  Input: stdin (assumed for the first command if 'from' not used)"
                )
            for idx, step in enumerate(self.pipeline):
                print(
                    f"  {idx + 1}. {step['repl_command']} {' '.join(shlex.quote(a) for a in step['args'])}"
                )
        print("")

    def handle_help(self, args):
        """Display the help message with all available REPL commands."""
        print("\nWelcome to the `ja` REPL! Build data pipelines interactively.")
        print("Here are the available commands:\n")
        print("  from <file|stdin>      : Start a new pipeline from a file or stdin.")
        print("                           Example: from users.jsonl")
        print("  select '<expr>'        : Filter rows with a Python expression.")
        print("                           Example: select 'user.age > 30'")
        print(
            "  project <cols>         : Pick columns, supporting nested data with dot notation."
        )
        print("                           Example: project id,user.name,user.location")
        print("  join <file> --on <L=R> : Join with another file on one or more keys.")
        print("                           Example: join orders.jsonl --on id=user_id")
        print("  rename <old=new,...>   : Rename columns. Supports dot notation.")
        print("                           Example: rename user.id=user_id,location=loc")
        print("  distinct               : Remove duplicate rows based on all columns.")
        print(
            "  sort <cols>            : Sort rows by one or more columns (supports dot notation)."
        )
        print("                           Example: sort user.age,id")
        print("  groupby <key> --agg <> : Group rows and aggregate data.")
        print(
            "                           Example: groupby cat --agg count,avg:user.score"
        )
        print(
            "  product <file>         : Create a Cartesian product with another file."
        )
        print("                           Example: product features.jsonl")
        print("  union <file>           : Combine rows from another file (deduplicated).")
        print("                           Example: union archived_users.jsonl")
        print("  intersection <file>    : Keep only rows present in another file.")
        print("                           Example: intersection active_users.jsonl")
        print("  difference <file>      : Remove rows present in another file.")
        print("                           Example: difference temp_users.jsonl")

        print("\n--- Pipeline Control ---")
        print(
            "  execute [--lines=N]    : Run the pipeline and see output (e.g., execute --lines=10)."
        )
        print(
            "  compile                : Show the bash script for the current pipeline."
        )
        print("  pipeline               : Show the steps in the current pipeline.")
        print("  reset                  : Clear the current pipeline.")
        print("  help                   : Show this help message.")
        print("  exit                   : Exit the REPL.\n")

        print("Tips:")
        print("- Use dot notation (e.g., `user.address.city`) for nested JSON fields.")
        print(
            """- Wrap arguments with spaces in quotes (e.g., select 'name == "John Doe"').\n"""
        )

    def process(self, line):
        """Process a single line of input from the REPL.

        This method parses the line, finds the appropriate handler for the
        command, and invokes it.

        Args:
            line (str): The line of input to process.
        """
        try:
            if not line:
                return

            command, cmd_args = self.parse_command(line)
            if command is None:  # Parsing error
                return

            if command in self.handlers:
                self.handlers[command](cmd_args)
            elif command:
                print(
                    f"Unknown command: '{command}'. Type 'help' for available commands."
                )

        except EOFError:
            print("\nExiting...")
        except KeyboardInterrupt:
            print("\nInterrupted. Use 'exit' to quit.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            import traceback

            traceback.print_exc()

    def run(self, initial_command_list=None):  # Renamed 'args' for clarity
        """Start the main REPL event loop.

        This method prints a welcome message, registers all command handlers,
        and enters an infinite loop to read and process user input.

        Args:
            initial_command_list (list, optional): A list of command-line arguments
                                                   to process before starting the
                                                   interactive loop.
        """
        print("Welcome to ja REPL. Type 'help' for commands, 'exit' to quit.")
        self.handlers = {  # Assign to self.handlers so self.process() can use it
            "from": self.handle_from,
            "select": self.handle_select,
            "project": self.handle_project,
            "join": self.handle_join,
            "rename": self.handle_rename,
            "distinct": self.handle_distinct,
            "sort": self.handle_sort,
            "groupby": self.handle_groupby,
            "product": self.handle_product,
            "union": self.handle_union,
            "intersection": self.handle_intersection,
            "difference": self.handle_difference,
            "compile": self.handle_compile,
            "execute": self.handle_execute,
            "agg": self.handle_agg,
            "reset": self.handle_reset,
            "pipeline": self.handle_pipeline_show,
            "help": self.handle_help,
            "exit": lambda _args: sys.exit(
                0
            ),  # Consistent signature with other handlers
        }

        if initial_command_list and len(initial_command_list) > 0:
            processed_initial_parts = list(initial_command_list)
            # If the first token of initial_command_list is not a known REPL command,
            # assume 'from' should be prepended.
            # This allows `ja repl myfile.jsonl` to be treated as `from myfile.jsonl`.
            if processed_initial_parts[0].lower() not in self.handlers:
                processed_initial_parts.insert(0, "from")

            initial_line = shlex.join(
                processed_initial_parts
            )  # Use shlex.join for safety
            self.process(initial_line)

        while True:
            try:
                line = input("ja> ").strip()
                if not line:
                    continue
                # 'exit' command will be handled by self.process -> self.handlers['exit']
                self.process(line)
            except EOFError:
                print("\nExiting...")
                sys.exit(0)  # Ensure clean exit
            except KeyboardInterrupt:
                print("\nInterrupted. Type 'exit' or Ctrl-D to quit.")
                # Loop continues, allowing user to recover or exit cleanly.


def repl(parsed_cli_args):  # Receives the argparse.Namespace object
    """Entry point for the `ja repl` command.

    Initializes and runs the ReplCompiler.

    Args:
        parsed_cli_args (argparse.Namespace): The parsed command-line arguments,
                                              which may include an initial file
                                              to load.
    """
    compiler = ReplCompiler()
    # Get the list of initial arguments passed to `ja repl ...`
    # getattr default to empty list if 'initial_args' is not present (it will be due to nargs="*")
    initial_repl_args_list = getattr(parsed_cli_args, "initial_args", [])
    compiler.run(initial_command_list=initial_repl_args_list)
