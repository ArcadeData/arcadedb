#!/bin/bash
# Unified build script for all ArcadeDB Python distributions
# Builds headless, minimal, and full distributions in Docker

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Parse command line arguments
DISTRIBUTION="${1:-all}"

print_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  🎮 ArcadeDB Python Bindings - Docker Build Script         ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_usage() {
    echo "Usage: $0 [DISTRIBUTION]"
    echo ""
    echo "DISTRIBUTION:"
    echo "  all         Build all three distributions (default)"
    echo "  headless    Build headless distribution only (recommended for production)"
    echo "  minimal     Build minimal distribution (includes Studio UI)"
    echo "  full        Build full distribution (includes Gremlin + GraphQL)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Build all distributions"
    echo "  $0 headless           # Build only headless"
    echo "  $0 full               # Build only full"
    echo ""
}

# Check for help flag
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    print_header
    print_usage
    exit 0
fi

print_header

# Validate distribution argument
if [[ ! "$DISTRIBUTION" =~ ^(all|headless|minimal|full)$ ]]; then
    echo -e "${RED}❌ Invalid distribution: $DISTRIBUTION${NC}"
    echo ""
    print_usage
    exit 1
fi

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed or not in PATH${NC}"
    echo -e "${YELLOW}💡 Please install Docker to build the Python bindings${NC}"
    exit 1
fi

echo -e "${CYAN}📋 Build Configuration:${NC}"
echo -e "   Distribution: ${YELLOW}$DISTRIBUTION${NC}"
echo -e "   Build Method: ${YELLOW}Docker${NC}"
echo ""

# Function to build a single distribution
build_distribution() {
    local dist=$1
    local dist_name=""
    
    case $dist in
        headless)
            dist_name="Headless (Recommended)"
            ;;
        minimal)
            dist_name="Minimal (with Studio)"
            ;;
        full)
            dist_name="Full (with Gremlin + GraphQL)"
            ;;
    esac
    
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Building: ${YELLOW}$dist_name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    echo -e "${YELLOW}🐳 Building in Docker...${NC}"
    
    # Build Docker image with distribution as build arg
    echo -e "${CYAN}📦 Building Docker image for $dist distribution...${NC}"
    docker build \
        --build-arg DISTRIBUTION=$dist \
        --target export \
        -t arcadedb-python-bindings-$dist-export \
        -f Dockerfile.build \
        ../..
    
    # Also build the tester stage (runs tests)
    echo -e "${CYAN}🧪 Running tests in Docker...${NC}"
    docker build \
        --build-arg DISTRIBUTION=$dist \
        --target tester \
        -t arcadedb-python-bindings-$dist \
        -f Dockerfile.build \
        ../..
    
    # Create dist directory if it doesn't exist
    mkdir -p dist
    
    # Extract the wheel from the export container
    echo -e "${CYAN}📋 Extracting wheel file...${NC}"
    CONTAINER_ID=$(docker create arcadedb-python-bindings-$dist-export)
    docker cp ${CONTAINER_ID}:/build/dist/. ./dist/
    docker rm ${CONTAINER_ID}
    
    # Verify wheel was extracted
    if ls dist/*.whl 1> /dev/null 2>&1; then
        echo -e "${GREEN}✅ Wheel file created successfully!${NC}"
    else
        echo -e "${RED}❌ Failed to extract wheel file${NC}"
        exit 1
    fi
    
    echo ""
}

# Main build logic
if [ "$DISTRIBUTION" = "all" ]; then
    echo -e "${CYAN}📦 Building all distributions...${NC}"
    echo ""
    
    build_distribution "headless"
    build_distribution "minimal"
    build_distribution "full"
    
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}🎉 All distributions built successfully!${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
else
    build_distribution "$DISTRIBUTION"
    
    echo -e "${GREEN}🎉 Build completed successfully!${NC}"
fi

echo ""
echo -e "${CYAN}📦 Built packages:${NC}"
if [ -d "dist" ]; then
    ls -lh dist/*.whl 2>/dev/null | awk '{print "   " $9 " (" $5 ")"}'
fi

echo ""
echo -e "${BLUE}💡 Next steps:${NC}"
echo -e "   📦 Install a package:"
echo -e "      ${YELLOW}pip install dist/arcadedb_embedded_headless-*.whl${NC}"
echo -e "      ${YELLOW}pip install dist/arcadedb_embedded_minimal-*.whl${NC}"
echo -e "      ${YELLOW}pip install dist/arcadedb_embedded_full-*.whl${NC}"
echo ""
echo -e "   🧪 Run tests:"
echo -e "      ${YELLOW}pytest tests/${NC}"
echo ""
echo -e "   📤 Publish to PyPI:"
echo -e "      ${YELLOW}twine upload dist/*.whl${NC}"
echo ""
