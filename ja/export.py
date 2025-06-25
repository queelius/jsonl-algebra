import json
import os
import pathlib
import re
import sys

def jsonl_to_json_array_string(jsonl_input_stream) -> str:
    """
    Reads JSONL from a stream and returns a string representing a JSON array.
    """
    records = []
    for line in jsonl_input_stream:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON line: {line.strip()} - Error: {e}", file=sys.stderr)
            continue
    return json.dumps(records, indent=2)

def json_array_to_jsonl_lines(json_array_input_stream):
    """
    Reads a JSON array from a stream and yields each element as a JSONL line.
    """
    try:
        json_string = "".join(json_array_input_stream)
        data = json.loads(json_string)
        if not isinstance(data, list):
            raise ValueError("Input is not a JSON array.")
        for record in data:
            yield json.dumps(record)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON array input: {e}")
    except ValueError as e:
        raise e


def jsonl_to_dir(jsonl_input_stream, output_dir_path_str: str, input_filename_stem: str = "data"):
    """
    Exports JSONL lines to individual JSON files in a directory.
    The output directory is named after input_filename_stem if output_dir_path_str is not specific.
    Files are named item-<index>.json.
    """
    output_dir = pathlib.Path(output_dir_path_str)
    
    # If output_dir_path_str was just a name (not a path), it might be used as the stem.
    # If it's a directory, we use the provided input_filename_stem for the sub-directory.
    if output_dir.is_dir() and not output_dir.exists(): # A path like "output/my_data" where "output" exists
        # This case is tricky. Let's assume output_dir_path_str is the target directory.
        pass
    elif not output_dir.name.endswith(('.jsonl', '.json')) and not output_dir.exists():
        # Treat as a new directory to be created directly
        pass
    else: # Default behavior: create a subdirectory based on the input stem
        output_dir = output_dir / input_filename_stem

    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for i, line in enumerate(jsonl_input_stream):
        try:
            record = json.loads(line)
            file_path = output_dir / f"item-{i}.json"
            with open(file_path, 'w') as f:
                json.dump(record, f, indent=2)
            count += 1
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON line during export: {line.strip()} - Error: {e}", file=sys.stderr)
            continue
    print(f"Exported {count} items to {output_dir.resolve()}", file=sys.stderr)


def _ensure_unique_key(data_dict, base_key):
    """Generates a unique key if base_key exists in data_dict."""
    if base_key not in data_dict:
        return base_key
    counter = 1
    while True:
        new_key = f"{base_key}_{counter}"
        if new_key not in data_dict:
            return new_key
        counter += 1

def _sort_files_for_implode(filenames_with_paths):
    """
    Sorts files: by index if all match 'item-<index>.json', otherwise lexicographically.
    Input is a list of pathlib.Path objects.
    """
    item_pattern = re.compile(r"item-(\d+)\.json$", re.IGNORECASE)
    indexed_files = []
    other_files = []
    
    all_match_pattern = True
    for path_obj in filenames_with_paths:
        match = item_pattern.match(path_obj.name)
        if match:
            indexed_files.append((int(match.group(1)), path_obj))
        else:
            all_match_pattern = False
            other_files.append(path_obj) # Collect for lexicographical sort if pattern fails

    if indexed_files and not other_files and all_match_pattern: # All files matched the pattern
        indexed_files.sort(key=lambda x: x[0])
        return [path_obj for _, path_obj in indexed_files]
    else:
        # Fallback to lexicographical sort for all .json files found
        all_json_files = [p for p in filenames_with_paths if p.name.lower().endswith('.json')]
        all_json_files.sort()
        return all_json_files


def dir_to_jsonl(input_dir_path_str: str, add_filename_key: str = None, recursive: bool = False):
    """
    Converts JSON files in a directory to JSONL lines.
    Files are sorted by 'item-<index>.json' pattern if applicable, otherwise lexicographically.
    Optionally adds filename as a key to each JSON object.
    """
    input_dir = pathlib.Path(input_dir_path_str)
    if not input_dir.is_dir():
        raise ValueError(f"Input path is not a directory: {input_dir_path_str}")

    json_files_paths = []
    if recursive:
        for root, _, files in os.walk(input_dir):
            for file in files:
                if file.lower().endswith(".json"):
                    json_files_paths.append(pathlib.Path(root) / file)
    else:
        for item in input_dir.iterdir():
            if item.is_file() and item.name.lower().endswith(".json"):
                json_files_paths.append(item)
    
    sorted_file_paths = _sort_files_for_implode(json_files_paths)

    for file_path in sorted_file_paths:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if add_filename_key:
                # Use relative path from the input_dir to keep it cleaner
                relative_filename = str(file_path.relative_to(input_dir))
                actual_key = _ensure_unique_key(data, add_filename_key)
                data[actual_key] = relative_filename
            
            yield json.dumps(data)
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON file: {file_path} - Error: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"Error processing file {file_path}: {e}", file=sys.stderr)
            continue