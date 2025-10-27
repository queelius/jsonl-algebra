#!/usr/bin/env python3
"""
Virtual Filesystem abstraction for JSON/JSONL files.

This module provides a filesystem-like interface for navigating and manipulating
JSON and JSONL data structures. It treats:
- JSONL files as directories of records
- JSON objects as directories of key-value pairs
- JSON arrays as directories of indexed elements
- Atomic values (strings, numbers, etc.) as files
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum


class NodeType(Enum):
    """Types of nodes in the virtual filesystem."""
    ROOT = "root"              # Root of the filesystem
    JSONL_FILE = "jsonl_file"  # A .jsonl file (collection)
    JSON_FILE = "json_file"    # A .json file
    RECORD = "record"          # A record in JSONL (object)
    OBJECT = "object"          # A JSON object
    ARRAY = "array"            # A JSON array
    VALUE = "value"            # An atomic value (file)


@dataclass
class PathSegment:
    """A single segment in a virtual path."""
    name: str
    is_index: bool = False     # True if this is an array/JSONL index like [0]
    is_filter: bool = False    # True if this is a filter like @[age>25]
    filter_expr: Optional[str] = None

    @classmethod
    def parse(cls, segment: str) -> 'PathSegment':
        """Parse a path segment string into a PathSegment."""
        if not segment or segment in ('.', '..'):
            return cls(segment)

        # Check for filter: @[expression]
        if segment.startswith('@[') and segment.endswith(']'):
            return cls(
                name=segment,
                is_filter=True,
                filter_expr=segment[2:-1]  # Extract expression
            )

        # Check for array index: [N]
        if segment.startswith('[') and segment.endswith(']'):
            try:
                idx = int(segment[1:-1])
                return cls(name=str(idx), is_index=True)
            except ValueError:
                # Not a valid index, treat as literal key
                return cls(name=segment)

        # Regular key
        return cls(name=segment)


@dataclass
class VFSNode:
    """A node in the virtual filesystem."""
    path: str                  # Virtual path
    node_type: NodeType        # Type of node
    physical_path: Optional[Path] = None  # Path to actual file (if any)
    data: Any = None          # Cached data
    parent: Optional['VFSNode'] = None

    def is_directory(self) -> bool:
        """Check if this node should be treated as a directory."""
        return self.node_type in (
            NodeType.ROOT,
            NodeType.JSONL_FILE,
            NodeType.JSON_FILE,
            NodeType.RECORD,
            NodeType.OBJECT,
            NodeType.ARRAY
        )

    def is_file(self) -> bool:
        """Check if this node should be treated as a file."""
        return self.node_type == NodeType.VALUE


class LazyJSONL:
    """Lazy loader for JSONL files to handle large datasets."""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.index = None  # Will build index on first access
        self._cache = {}   # Cache for recently accessed records
        self._max_cache = 100

    def _build_index(self):
        """Build an index of record positions in the file."""
        if self.index is not None:
            return

        self.index = []
        with open(self.file_path, 'rb') as f:
            pos = 0
            while True:
                line = f.readline()
                if not line:
                    break
                self.index.append(pos)
                pos = f.tell()

    def __len__(self):
        """Return number of records."""
        self._build_index()
        return len(self.index)

    def __getitem__(self, idx: int) -> dict:
        """Get a record by index."""
        if idx in self._cache:
            return self._cache[idx]

        self._build_index()
        if idx < 0 or idx >= len(self.index):
            raise IndexError(f"Record index {idx} out of range")

        # Seek to position and read record
        with open(self.file_path, 'r') as f:
            f.seek(self.index[idx])
            line = f.readline()
            record = json.loads(line)

        # Cache it
        if len(self._cache) >= self._max_cache:
            # Simple LRU: remove first item
            self._cache.pop(next(iter(self._cache)))
        self._cache[idx] = record

        return record

    def __iter__(self):
        """Iterate over all records."""
        with open(self.file_path, 'r') as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)


class JSONPath:
    """
    Virtual filesystem abstraction for JSON/JSONL files.

    Provides filesystem-like navigation and manipulation of JSON structures.
    """

    def __init__(self, root_dir: Union[str, Path] = '.'):
        """
        Initialize JSONPath with a physical root directory.

        Args:
            root_dir: Physical directory containing JSON/JSONL files
        """
        self.root_dir = Path(root_dir).resolve()
        self.current_path = "/"
        self._file_cache = {}  # Cache loaded files

    def _parse_path(self, path: str) -> List[PathSegment]:
        """
        Parse a virtual path into segments.

        Examples:
            "/users.jsonl/[0]/address/city" →
                ["users.jsonl", "[0]", "address", "city"]
        """
        if path == "/":
            return []

        # Remove leading/trailing slashes
        path = path.strip('/')

        # Split by / but preserve [] brackets
        segments = []
        current = ""
        bracket_depth = 0

        for char in path:
            if char == '[':
                bracket_depth += 1
                current += char
            elif char == ']':
                bracket_depth -= 1
                current += char
            elif char == '/' and bracket_depth == 0:
                if current:
                    segments.append(PathSegment.parse(current))
                    current = ""
            else:
                current += char

        if current:
            segments.append(PathSegment.parse(current))

        return segments

    def _resolve_path(self, path: str) -> Tuple[VFSNode, Any]:
        """
        Resolve a virtual path to a node and its data.

        Returns:
            (VFSNode, data) tuple
        """
        # Handle absolute vs relative paths
        if path.startswith('/'):
            abs_path = path
        else:
            abs_path = self._join_paths(self.current_path, path)

        # Normalize path (handle . and ..)
        abs_path = self._normalize_path(abs_path)

        # Parse into segments
        segments = self._parse_path(abs_path)

        if not segments:
            # Root directory
            return VFSNode(
                path="/",
                node_type=NodeType.ROOT,
                physical_path=self.root_dir
            ), None

        # First segment should be a physical file
        first_seg = segments[0]
        file_path = self.root_dir / first_seg.name

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {first_seg.name}")

        # Load the file
        if file_path.suffix == '.jsonl':
            data = self._load_jsonl(file_path)
            node = VFSNode(
                path=f"/{first_seg.name}",
                node_type=NodeType.JSONL_FILE,
                physical_path=file_path,
                data=data
            )
        elif file_path.suffix == '.json':
            data = self._load_json(file_path)
            node = VFSNode(
                path=f"/{first_seg.name}",
                node_type=NodeType.JSON_FILE,
                physical_path=file_path,
                data=data
            )
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

        # Navigate through remaining segments
        current_data = data
        current_path = f"/{first_seg.name}"

        for seg in segments[1:]:
            # Build path segment with proper formatting
            if seg.is_index:
                path_segment = f"[{seg.name}]"
            elif seg.is_filter:
                path_segment = f"@[{seg.filter_expr}]"
            else:
                path_segment = seg.name

            current_path += f"/{path_segment}"

            if seg.is_filter:
                # Apply filter
                current_data = self._apply_filter(current_data, seg.filter_expr)
                node = VFSNode(
                    path=current_path,
                    node_type=NodeType.ARRAY,
                    data=current_data,
                    parent=node
                )
            elif seg.is_index:
                # Array/JSONL index access
                idx = int(seg.name)
                if isinstance(current_data, LazyJSONL):
                    current_data = current_data[idx]
                elif isinstance(current_data, list):
                    current_data = current_data[idx]
                else:
                    raise TypeError(f"Cannot index into {type(current_data)}")

                node = VFSNode(
                    path=current_path,
                    node_type=self._get_node_type(current_data),
                    data=current_data,
                    parent=node
                )
            else:
                # Object key access
                if isinstance(current_data, dict):
                    if seg.name not in current_data:
                        raise KeyError(f"Key not found: {seg.name}")
                    current_data = current_data[seg.name]
                else:
                    raise TypeError(f"Cannot access key '{seg.name}' on {type(current_data)}")

                node = VFSNode(
                    path=current_path,
                    node_type=self._get_node_type(current_data),
                    data=current_data,
                    parent=node
                )

        return node, current_data

    def _get_node_type(self, data: Any) -> NodeType:
        """Determine the node type for a piece of data."""
        if isinstance(data, dict):
            return NodeType.OBJECT
        elif isinstance(data, list):
            return NodeType.ARRAY
        elif isinstance(data, LazyJSONL):
            return NodeType.JSONL_FILE
        else:
            return NodeType.VALUE

    def _load_jsonl(self, file_path: Path) -> LazyJSONL:
        """Load a JSONL file lazily."""
        if str(file_path) not in self._file_cache:
            self._file_cache[str(file_path)] = LazyJSONL(file_path)
        return self._file_cache[str(file_path)]

    def _load_json(self, file_path: Path) -> Any:
        """Load a JSON file."""
        if str(file_path) not in self._file_cache:
            with open(file_path, 'r') as f:
                self._file_cache[str(file_path)] = json.load(f)
        return self._file_cache[str(file_path)]

    def _apply_filter(self, data: Any, filter_expr: str) -> List:
        """Apply a filter expression to data."""
        # Import here to avoid circular dependency
        from .core import select

        # Convert to list if needed
        if isinstance(data, LazyJSONL):
            data = list(data)
        elif not isinstance(data, list):
            raise TypeError("Can only filter arrays/JSONL")

        # Apply filter using ja's select function
        return list(select(data, filter_expr))

    def _normalize_path(self, path: str) -> str:
        """Normalize a path (resolve . and ..)."""
        segments = path.split('/')
        normalized = []

        for seg in segments:
            if seg == '..':
                if normalized:
                    normalized.pop()
            elif seg and seg != '.':
                normalized.append(seg)

        return '/' + '/'.join(normalized) if normalized else '/'

    def _join_paths(self, base: str, relative: str) -> str:
        """Join two paths."""
        if relative.startswith('/'):
            return relative

        if base.endswith('/'):
            return base + relative
        else:
            return base + '/' + relative

    # Public API

    def cd(self, path: str):
        """Change current directory."""
        node, _ = self._resolve_path(path)
        if not node.is_directory():
            raise NotADirectoryError(f"{path} is not a directory")
        self.current_path = node.path

    def pwd(self) -> str:
        """Get current directory path."""
        return self.current_path

    def ls(self, path: Optional[str] = None) -> List[Tuple[str, bool]]:
        """
        List directory contents.

        Returns:
            List of (name, is_directory) tuples
        """
        target_path = path if path else self.current_path
        node, data = self._resolve_path(target_path)

        if not node.is_directory():
            raise NotADirectoryError(f"{target_path} is not a directory")

        entries = []

        if node.node_type == NodeType.ROOT:
            # List physical files in root directory
            for item in self.root_dir.iterdir():
                if item.suffix in ('.json', '.jsonl'):
                    entries.append((item.name, True))

        elif node.node_type == NodeType.JSONL_FILE:
            # List records as [0], [1], etc.
            count = len(data)
            for i in range(count):
                entries.append((f"[{i}]", True))

        elif node.node_type == NodeType.ARRAY:
            # List array indices
            arr = data if isinstance(data, list) else list(data)
            for i in range(len(arr)):
                is_dir = isinstance(arr[i], (dict, list))
                entries.append((f"[{i}]", is_dir))

        elif node.node_type in (NodeType.OBJECT, NodeType.RECORD):
            # List object keys
            if isinstance(data, dict):
                for key, value in data.items():
                    is_dir = isinstance(value, (dict, list))
                    entries.append((key, is_dir))

        return entries

    def cat(self, path: str) -> str:
        """Read file contents."""
        node, data = self._resolve_path(path)

        if node.is_directory():
            raise IsADirectoryError(f"{path} is a directory")

        # Return string representation
        if isinstance(data, str):
            return data
        elif isinstance(data, (int, float, bool, type(None))):
            return str(data)
        else:
            return json.dumps(data, indent=2)

    def stat(self, path: str) -> Dict[str, Any]:
        """Get information about a path."""
        node, data = self._resolve_path(path)

        info = {
            'path': node.path,
            'type': node.node_type.value,
            'is_directory': node.is_directory(),
            'is_file': node.is_file(),
        }

        if node.is_file():
            content = self.cat(path)
            info['size'] = len(content)
        elif node.node_type == NodeType.JSONL_FILE:
            info['count'] = len(data)
        elif node.node_type == NodeType.ARRAY:
            arr = data if isinstance(data, list) else list(data)
            info['count'] = len(arr)
        elif node.node_type in (NodeType.OBJECT, NodeType.RECORD):
            if isinstance(data, dict):
                info['count'] = len(data)

        return info
