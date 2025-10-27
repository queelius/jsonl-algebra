# Installation

Welcome! This guide will help you get **jsonl-algebra** installed and ready to use on your system.

## Prerequisites

Before installing jsonl-algebra, ensure you have:

- **Python 3.8 or higher** - Check with `python --version` or `python3 --version`
- **pip** - Python's package installer (usually comes with Python)
- **Terminal access** - Command-line interface

!!! tip "Python Version Check"
    ```bash
    python3 --version
    # Should show: Python 3.8.x or higher
    ```

## Quick Install (Recommended)

The easiest way to install jsonl-algebra is from PyPI using pip:

=== "Using pip"

    ```bash
    pip install jsonl-algebra
    ```

=== "Using pip3"

    ```bash
    pip3 install jsonl-algebra
    ```

=== "With User Install"

    ```bash
    pip install --user jsonl-algebra
    ```

This single command installs:

- The `ja` CLI tool
- The `ja-shell` interactive navigator
- The Python library for programmatic use
- All required dependencies

### Verify Installation

After installation, verify everything works:

```bash
# Check that ja is available
ja --version

# Should output something like: ja version 1.01

# Test basic functionality
echo '{"name": "Alice", "age": 30}' | ja project name
# Output: {"name": "Alice"}
```

!!! success "Installation Complete!"
    If you see the version number and the test command works, you're all set! Head to the [Quick Start](quickstart.md) guide.

## Installation Methods

### Method 1: Install from PyPI (Stable Release)

This is the recommended method for most users. It installs the latest stable version:

```bash
pip install jsonl-algebra
```

**What gets installed:**

- Core library (`ja` package)
- CLI tool (`ja` command)
- Interactive shell (`ja-shell` command)
- Dataset generator (`ja-generate-dataset` command)
- All dependencies (jmespath, jsonschema, prompt-toolkit, rich)

### Method 2: Install from Source (Latest Development Version)

For developers or users who want the latest features:

=== "Clone and Install"

    ```bash
    # Clone the repository
    git clone https://github.com/queelius/jsonl-algebra.git
    cd jsonl-algebra

    # Install in editable mode
    pip install -e .
    ```

=== "Install with Development Tools"

    ```bash
    # Clone the repository
    git clone https://github.com/queelius/jsonl-algebra.git
    cd jsonl-algebra

    # Install with dev dependencies
    pip install -e ".[dev]"
    ```

**Editable mode** (`-e`) means changes to the source code take effect immediately without reinstalling.

### Method 3: Install in a Virtual Environment (Recommended for Development)

Using a virtual environment keeps your project dependencies isolated:

=== "Using venv"

    ```bash
    # Create virtual environment
    python3 -m venv ja-env

    # Activate it
    source ja-env/bin/activate  # On Linux/Mac
    # or
    ja-env\Scripts\activate     # On Windows

    # Install jsonl-algebra
    pip install jsonl-algebra
    ```

=== "Using conda"

    ```bash
    # Create conda environment
    conda create -n ja-env python=3.10

    # Activate it
    conda activate ja-env

    # Install jsonl-algebra
    pip install jsonl-algebra
    ```

## Dependencies

jsonl-algebra comes with these dependencies (automatically installed):

| Package | Purpose | Required |
|---------|---------|----------|
| **jmespath** | Advanced query expressions | Yes |
| **jsonschema** | Schema validation | Yes |
| **prompt-toolkit** | Rich terminal input (for ja-shell) | Yes |
| **rich** | Beautiful terminal output (for ja-shell) | Yes |

### Optional Dependencies

For development or special features:

```bash
# Development tools (testing, linting, docs)
pip install jsonl-algebra[dev]

# Dataset generation
pip install jsonl-algebra[dataset]
```

## Platform-Specific Notes

### Linux

Most distributions work out of the box:

```bash
# Ubuntu/Debian
sudo apt install python3 python3-pip
pip3 install jsonl-algebra

# Fedora/RHEL
sudo dnf install python3 python3-pip
pip3 install jsonl-algebra

# Arch Linux
sudo pacman -S python python-pip
pip install jsonl-algebra
```

### macOS

Using Homebrew:

```bash
# Install Python if needed
brew install python3

# Install jsonl-algebra
pip3 install jsonl-algebra
```

### Windows

=== "Using Python Installer"

    1. Download Python from [python.org](https://www.python.org/downloads/)
    2. Run installer (make sure "Add Python to PATH" is checked)
    3. Open Command Prompt or PowerShell
    4. Run: `pip install jsonl-algebra`

=== "Using WSL2 (Recommended)"

    ```bash
    # In WSL2 Ubuntu terminal
    sudo apt update
    sudo apt install python3 python3-pip
    pip3 install jsonl-algebra
    ```

## Upgrading

To upgrade to the latest version:

```bash
pip install --upgrade jsonl-algebra
```

To upgrade to a specific version:

```bash
pip install jsonl-algebra==1.01
```

## Uninstalling

If you need to remove jsonl-algebra:

```bash
pip uninstall jsonl-algebra
```

## Troubleshooting

### Command Not Found

If `ja` command is not found after installation:

=== "Check PATH"

    ```bash
    # On Linux/Mac
    echo $PATH

    # The directory containing 'ja' should be in PATH
    # Usually: ~/.local/bin or /usr/local/bin
    ```

=== "Add to PATH"

    ```bash
    # Add to ~/.bashrc or ~/.zshrc
    export PATH="$HOME/.local/bin:$PATH"

    # Then reload
    source ~/.bashrc
    ```

=== "Use Full Path"

    ```bash
    # Find where ja was installed
    pip show -f jsonl-algebra | grep bin/ja

    # Use the full path
    /path/to/ja --version
    ```

### Permission Denied

If you get permission errors:

```bash
# Use --user flag
pip install --user jsonl-algebra

# Or use a virtual environment (recommended)
python3 -m venv myenv
source myenv/bin/activate
pip install jsonl-algebra
```

### SSL Certificate Errors

If pip has SSL issues:

```bash
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org jsonl-algebra
```

### Python Version Issues

If Python 3.8+ is not your default:

```bash
# Use python3 explicitly
python3 -m pip install jsonl-algebra

# Or specify the version
python3.10 -m pip install jsonl-algebra
```

## Verifying Installation

Run these commands to verify everything is working:

```bash
# 1. Check version
ja --version

# 2. List available commands
ja --help

# 3. Test basic operation
echo '{"x": 1}' | ja select 'x > 0'

# 4. Test ja-shell
ja-shell --version

# 5. Test Python import
python3 -c "from ja.core import select; print('Import successful')"
```

!!! success "Ready to Go!"
    If all commands work, you're ready to start using jsonl-algebra! Continue to the [Quick Start Guide](quickstart.md) for a hands-on tutorial.

## Next Steps

- [Quick Start Tutorial](quickstart.md) - Learn the basics in 5 minutes
- [Core Concepts](concepts.md) - Understand the fundamentals
- [CLI Reference](../cli/overview.md) - Explore all available commands
- [Examples](../cli/examples.md) - See real-world usage patterns

## Getting Help

If you encounter issues:

1. Check the [Troubleshooting Guide](../troubleshooting.md)
2. Search [existing issues](https://github.com/queelius/jsonl-algebra/issues)
3. Open a [new issue](https://github.com/queelius/jsonl-algebra/issues/new) with:
   - Your OS and Python version
   - Installation method used
   - Complete error message
   - Steps to reproduce
