#!/usr/bin/env python3
import sys
import re

if len(sys.argv) != 2:
    print("Usage: python bump_version.py <new_version>")
    sys.exit(1)

new_version = sys.argv[1]

# Update pyproject.toml
with open('pyproject.toml', 'r') as f:
    content = f.read()

content = re.sub(r'^version = "[^"]*"', f'version = "{new_version}"', content, flags=re.MULTILINE)

with open('pyproject.toml', 'w') as f:
    f.write(content)

print(f"Version bumped to {new_version}")
