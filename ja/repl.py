import shlex
import sys
import subprocess

class ReplCompiler:
    def __init__(self):
        self.pipeline = []
        self.current_input_source = None # Can be a file path or None (implying stdin)
        self.handlers = {} # Initialize handlers

    def parse_command(self, line):
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
        if not args:
            print("Error: 'from' requires a file path (or 'stdin').")
            return
        if self.pipeline:
            print("Error: 'from' can only be used at the beginning of a new pipeline. Use 'reset' first.")
            return
        self.current_input_source = args[0]
        if self.current_input_source.lower() == 'stdin':
            self.current_input_source = None # Internally, None means stdin for clarity
            print("Input source set to: stdin")
        else:
            print(f"Input source set to: {self.current_input_source}")

    def add_to_pipeline(self, command_name, args, cli_command_name=None):
        if not cli_command_name:
            cli_command_name = command_name
        # Ensure 'from' is not added to the pipeline steps directly
        if command_name.lower() == 'from':
            print("Error: 'from' is a directive, not a pipeline step. Use 'reset' then 'from <file>'.")
            return
        self.pipeline.append({'repl_command': command_name, 'cli_command': cli_command_name, 'args': args})
        print(f"Added: {command_name} {' '.join(shlex.quote(a) for a in args)}")


    def handle_select(self, args):
        if not args:
            print("Error: 'select' requires an expression.")
            return
        self.add_to_pipeline('select', args)

    def handle_project(self, args):
        if not args:
            print("Error: 'project' requires column names.")
            return
        self.add_to_pipeline('project', args)

    def handle_join(self, args):
        if len(args) < 3 or args[-2].lower() != '--on':
            print("Error: 'join' requires <right_file> --on <key_map>.")
            print("Example: join orders.jsonl --on user_id=customer_id")
            return
        self.add_to_pipeline('join', args)

    def handle_rename(self, args):
        if not args:
            print("Error: 'rename' requires a mapping (e.g., old_name=new_name).")
            return
        self.add_to_pipeline('rename', args)

    def handle_distinct(self, args):
        if args:
            print("Warning: 'distinct' does not take arguments in REPL mode. Ignoring.")
        self.add_to_pipeline('distinct', [])

    def handle_sort(self, args):
        if not args:
            print("Error: 'sort' requires column names.")
            return
        self.add_to_pipeline('sort', args)

    def handle_groupby(self, args):
        if len(args) < 3 or args[-2].lower() != '--agg':
            print("Error: 'groupby' requires <key> --agg <spec>.")
            print("Example: groupby category --agg count,sum:amount")
            return
        self.add_to_pipeline('groupby', args)

    def handle_product(self, args):
        if not args:
            print("Error: 'product' requires a right file path.")
            return
        self.add_to_pipeline('product', args)

    def _generate_pipeline_command_string_and_segments(self):
        if not self.pipeline:
            return None, None, "Pipeline is empty."

        display_segments = [] 
        execution_segments = []

        for i, step in enumerate(self.pipeline):
            current_ja_cmd_parts = ["ja", step['cli_command']]
            is_first_command_in_pipe = (i == 0)

            if step['cli_command'] in ['join', 'product']:
                # REPL args: <right_file> [--on <key_map>] for join
                # REPL args: <right_file> for product
                # CLI: ja join <left> <right> --on <key_map>
                # CLI: ja product <left> <right>
                right_file_repl_arg = step['args'][0]

                if is_first_command_in_pipe:
                    left_input_for_cli = self.current_input_source if self.current_input_source else '-'
                else:
                    left_input_for_cli = '-' 
                
                current_ja_cmd_parts.append(left_input_for_cli)
                current_ja_cmd_parts.append(right_file_repl_arg)
                
                if step['cli_command'] == 'join':
                    current_ja_cmd_parts.extend(step['args'][1:]) # --on <key_map>
            
            elif step['cli_command'] == 'groupby':
                # REPL args: <key> --agg <spec>
                # CLI: ja groupby <key> <file_or_stdin> --agg <spec>
                key_repl_arg = step['args'][0]
                agg_spec_repl_args = step['args'][1:]

                current_ja_cmd_parts.append(key_repl_arg)

                if is_first_command_in_pipe:
                    input_file_for_cli = self.current_input_source if self.current_input_source else '-'
                else:
                    input_file_for_cli = '-'
                current_ja_cmd_parts.append(input_file_for_cli)
                current_ja_cmd_parts.extend(agg_spec_repl_args)
                
            else: # select, project, rename, distinct, sort
                # REPL args: <command_specific_args>
                # CLI: ja <command> [command_specific_args...] [file_if_first_and_not_stdin]
                current_ja_cmd_parts.extend(step['args'])
                
                if is_first_command_in_pipe:
                    if self.current_input_source:
                        current_ja_cmd_parts.append(self.current_input_source)
            
            joined_segment = shlex.join(current_ja_cmd_parts)
            display_segments.append(joined_segment)
            execution_segments.append(joined_segment)
        
        executable_command_string = " | ".join(execution_segments)
        return executable_command_string, display_segments, None

    def handle_compile(self, cmd_args):
        _executable_cmd_str, display_segments, error_msg = self._generate_pipeline_command_string_and_segments()

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
        limit_lines = None
        if cmd_args:
            if cmd_args[0].startswith("--lines="):
                try:
                    limit_lines = int(cmd_args[0].split("=")[1])
                    if limit_lines <= 0:
                        print("Error: --lines must be a positive integer.")
                        return
                except (ValueError, IndexError):
                    print("Error: Invalid format for --lines. Use --lines=N (e.g., --lines=10).")
                    return
            else:
                print(f"Warning: Unknown argument '{cmd_args[0]}' for execute. Ignoring. Did you mean --lines=N?")

        command_to_execute, _display_segments, error_msg = self._generate_pipeline_command_string_and_segments()

        if error_msg:
            print(error_msg)
            return
        if not command_to_execute:
            print("Internal error: No command to execute.") # Should be caught by error_msg
            return

        print(f"Executing: {command_to_execute}")
        
        try:
            process = subprocess.run(
                command_to_execute,
                shell=True, # Essential for pipes
                capture_output=True,
                text=True,
                check=False # Manually check returncode
            )
            
            print("\n--- Output ---")
            if process.stdout:
                output_lines_list = process.stdout.splitlines()
                if limit_lines is not None:
                    for i, line_content in enumerate(output_lines_list):
                        if i < limit_lines:
                            print(line_content)
                        else:
                            print(f"... (output truncated to {limit_lines} lines, total {len(output_lines_list)})")
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

        except FileNotFoundError: # pragma: no cover
            print(f"Error: Command 'ja' not found. Make sure it's in your PATH.")
        except Exception as e: # pragma: no cover
            print(f"An unexpected error occurred while trying to execute the command: {e}")
            # import traceback
            # traceback.print_exc()

    def handle_reset(self, args):
        self.pipeline = []
        self.current_input_source = None
        print("Pipeline reset.")

    def handle_pipeline_show(self, args):
        if not self.pipeline:
            print("Pipeline is empty.")
        else:
            print("Current pipeline:")
            if self.current_input_source:
                print(f"  Input: {self.current_input_source}")
            else:
                print(f"  Input: stdin (assumed for the first command if 'from' not used)")
            for idx, step in enumerate(self.pipeline):
                print(f"  {idx + 1}. {step['repl_command']} {' '.join(shlex.quote(a) for a in step['args'])}")
        print("")

    def handle_help(self, args):
        print("\nAvailable ja REPL commands:")
        print("  from <file_path|stdin>   : Set initial input (e.g., 'from data.jsonl', 'from stdin').")
        print("  select '<expr>'          : Filter rows (e.g., select 'amount > 100').")
        print("  project <cols>           : Select columns (e.g., project id,name).")
        print("  join <right_file> --on <L=R>: Join with another file (e.g., join orders.jsonl --on user_id=cust_id).")
        print("  rename <old=new,...>     : Rename columns (e.g., rename old_name=new_name).")
        print("  distinct                 : Remove duplicate rows.")
        print("  sort <cols>              : Sort by columns (e.g., sort timestamp,user_id).")
        print("  groupby <key> --agg <spec>: Group and aggregate (e.g., groupby cat --agg count,sum:val).")
        print("  product <right_file>     : Cartesian product (e.g., product categories.jsonl).")
        print("  compile                  : Generate the bash script for the current pipeline.")
        print("  execute [--lines=N]      : Execute pipeline & show output (optionally first N lines).")
        print("  pipeline                 : Show the current pipeline steps.")
        print("  reset                    : Clear the current pipeline and input source.")
        print("  help                     : Show this help message.")
        print("  exit                     : Exit the REPL.")
        print("\nNotes:")
        print("- Arguments with spaces or special characters should be quoted (e.g., select 'item == \"book\"').")
        print("- The 'compile' command generates a script that pipes ja CLI commands together.")
        print("- If 'from' is not used, the first command in the pipeline assumes input from stdin.\n")

    def process(self, line):    
        try:
            if not line:
                return
            
            command, cmd_args = self.parse_command(line)
            if command is None: # Parsing error
                return

            if command in self.handlers:
                self.handlers[command](cmd_args)
            elif command:
                print(f"Unknown command: '{command}'. Type 'help' for available commands.")

        except EOFError:
            print("\nExiting...")
        except KeyboardInterrupt:
            print("\nInterrupted. Use 'exit' to quit.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            import traceback
            traceback.print_exc()

    def run(self, initial_command_list=None): # Renamed 'args' for clarity
        print("Welcome to ja REPL. Type 'help' for commands, 'exit' to quit.")
        self.handlers = { # Assign to self.handlers so self.process() can use it
            "from": self.handle_from,
            "select": self.handle_select,
            "project": self.handle_project,
            "join": self.handle_join,
            "rename": self.handle_rename,
            "distinct": self.handle_distinct,
            "sort": self.handle_sort,
            "groupby": self.handle_groupby,
            "product": self.handle_product,
            "compile": self.handle_compile,
            "execute": self.handle_execute,
            "reset": self.handle_reset,
            "pipeline": self.handle_pipeline_show,
            "help": self.handle_help,
            "exit": lambda _args: sys.exit(0), # Consistent signature with other handlers
        }

        if initial_command_list and len(initial_command_list) > 0:
            processed_initial_parts = list(initial_command_list)
            # If the first token of initial_command_list is not a known REPL command,
            # assume 'from' should be prepended.
            # This allows `ja repl myfile.jsonl` to be treated as `from myfile.jsonl`.
            if processed_initial_parts[0].lower() not in self.handlers:
                processed_initial_parts.insert(0, 'from')
            
            initial_line = shlex.join(processed_initial_parts) # Use shlex.join for safety
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
                sys.exit(0) # Ensure clean exit
            except KeyboardInterrupt:
                print("\nInterrupted. Type 'exit' or Ctrl-D to quit.")
                # Loop continues, allowing user to recover or exit cleanly.

def repl(parsed_cli_args): # Receives the argparse.Namespace object
    compiler = ReplCompiler()
    # Get the list of initial arguments passed to `ja repl ...`
    # getattr default to empty list if 'initial_args' is not present (it will be due to nargs="*")
    initial_repl_args_list = getattr(parsed_cli_args, 'initial_args', [])
    compiler.run(initial_command_list=initial_repl_args_list)
