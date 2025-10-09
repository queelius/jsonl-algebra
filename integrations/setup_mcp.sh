#!/bin/bash
# Setup script for JSONL Algebra MCP Server

echo "Setting up JSONL Algebra MCP Server..."

# Check if MCP SDK is installed
if ! python -c "import mcp" 2>/dev/null; then
    echo "Installing MCP SDK..."
    pip install mcp
fi

# Get the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$( dirname "$DIR" )"

# Create MCP configuration directory if it doesn't exist
MCP_CONFIG_DIR="$HOME/.config/mcp"
mkdir -p "$MCP_CONFIG_DIR"

# Copy configuration
echo "Copying MCP configuration..."
cp "$DIR/mcp_config.json" "$MCP_CONFIG_DIR/jsonl-algebra.json"

# Create a wrapper script for the MCP server
WRAPPER_SCRIPT="$HOME/.local/bin/jsonl-algebra-mcp"
mkdir -p "$HOME/.local/bin"

cat > "$WRAPPER_SCRIPT" << EOF
#!/bin/bash
cd "$PARENT_DIR"
exec python -m integrations.mcp_server "\$@"
EOF

chmod +x "$WRAPPER_SCRIPT"

echo "MCP Server setup complete!"
echo ""
echo "To use the JSONL Algebra MCP server:"
echo "1. Ensure $HOME/.local/bin is in your PATH"
echo "2. Configure your AI assistant to use the MCP server at: jsonl-algebra-mcp"
echo "3. Or run directly: python -m integrations.mcp_server"
echo ""
echo "Configuration file installed at: $MCP_CONFIG_DIR/jsonl-algebra.json"