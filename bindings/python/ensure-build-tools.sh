#!/bin/bash
# Ensure build tools are installed for all Python versions available

set -e

echo "Checking Python installations and installing build tools..."

# Check system python3
if command -v python3 &> /dev/null; then
    PYTHON_PATH=$(command -v python3)
    echo "Found python3 at: $PYTHON_PATH"

    # Try to install build in the current environment
    # This will fail silently if the environment is managed (like Homebrew)
    "$PYTHON_PATH" -m pip install build 2> /dev/null || echo "  ⚠️  Could not install in $PYTHON_PATH (managed environment)"
fi

# Also check common Homebrew Python 3.14 location
if [[ -f "/opt/homebrew/bin/python3.14" ]]; then
    echo "Found /opt/homebrew/bin/python3.14"
    /opt/homebrew/bin/python3.14 -m pip install build 2> /dev/null || echo "  ⚠️  Could not install in /opt/homebrew/bin/python3.14 (managed environment)"
fi

echo "✅ Done"
