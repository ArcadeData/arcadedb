#!/bin/bash
set -euo pipefail

# Local testing script for arcadedb-builder.sh
# Simulates GitHub releases by serving files locally

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Dynamically get version from pom.xml
PROJECT_VERSION=$(mvn -f ../pom.xml help:evaluate -Dexpression=project.version -q -DforceStdout)

echo "Local Builder Testing"
echo "===================="
echo ""

# Check if base distribution exists
BASE_DIST="${SCRIPT_DIR}/target/arcadedb-${PROJECT_VERSION}-base.tar.gz"
if [[ ! -f "$BASE_DIST" ]]; then
    echo "Error: Base distribution not found"
    echo "Run: mvn clean package -DskipTests"
    exit 1
fi

echo "Found base distribution: $BASE_DIST"
echo ""

# Test 1: Dry run with no modules
echo "Test 1: Dry run - base only"
./arcadedb-builder.sh \
    --version=${PROJECT_VERSION} \
    --dry-run \
    --output-dir=/tmp

echo ""
echo "Test 1: PASSED"
echo ""

# Test 2: Dry run with modules
echo "Test 2: Dry run - with modules"
./arcadedb-builder.sh \
    --version=${PROJECT_VERSION} \
    --modules=console,studio \
    --dry-run \
    --skip-docker

echo ""
echo "Test 2: PASSED"
echo ""

# Test 3: Local repository mode (dry run)
echo "Test 3: Local repository mode - dry run"
./arcadedb-builder.sh \
    --version=${PROJECT_VERSION} \
    --modules=console,studio \
    --local-repo=$HOME/.m2/repository \
    --dry-run \
    --skip-docker

echo ""
echo "Test 3: PASSED"
echo ""

# Test 4: Help message
echo "Test 4: Help message"
./arcadedb-builder.sh --help | head -5

echo ""
echo "Test 4: PASSED"
echo ""

# Test 5: Invalid version
echo "Test 5: Invalid version (should fail)"
if ./arcadedb-builder.sh --version=invalid 2>/dev/null; then
    echo "Test 5: FAILED (should have rejected invalid version)"
    exit 1
else
    echo "Test 5: PASSED"
fi

echo ""
echo "All tests passed!"
