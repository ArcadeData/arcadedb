#!/bin/bash
set -euo pipefail

# Helper script to prepare builder for GitHub release
# This copies the builder script and README to a release directory

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION="${1:-}"

if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 26.1.0"
    exit 1
fi

RELEASE_DIR="${SCRIPT_DIR}/target/release-${VERSION}"

echo "Preparing release artifacts for version $VERSION"
echo "Release directory: $RELEASE_DIR"

# Create release directory
mkdir -p "$RELEASE_DIR"

# Copy builder script
cp "${SCRIPT_DIR}/arcadedb-builder.sh" "${RELEASE_DIR}/"
chmod +x "${RELEASE_DIR}/arcadedb-builder.sh"

# Copy README
cp "${SCRIPT_DIR}/README-BUILDER.md" "${RELEASE_DIR}/"

echo ""
echo "Release artifacts prepared:"
echo "  - arcadedb-builder.sh"
echo "  - README-BUILDER.md"
echo ""
echo "Upload these files to GitHub releases for version $VERSION"
echo "Also upload: arcadedb-${VERSION}-base.tar.gz and arcadedb-${VERSION}-base.tar.gz.sha256"
