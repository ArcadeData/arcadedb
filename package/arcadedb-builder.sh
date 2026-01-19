#!/usr/bin/env bash
# Requires bash 4.0+ for associative arrays
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eo pipefail

VERSION="1.0.0"
SCRIPT_NAME="$(basename "$0")"

# URLs
MAVEN_CENTRAL_BASE="https://repo1.maven.org/maven2/com/arcadedb"
GITHUB_RELEASES_BASE="https://github.com/arcadedata/arcadedb/releases/download"

# Module metadata
SHADED_MODULES="gremlin redisw mongodbw postgresw grpcw metrics"
REGULAR_MODULES="console studio graphql"

# Module descriptions for interactive menu
# Note: Associative arrays require bash 4.0+
# Workaround for bash 3.2 compatibility: use functions instead
get_module_description() {
    case "$1" in
        console) echo "Interactive database console" ;;
        gremlin) echo "Apache Tinkerpop Gremlin support" ;;
        studio) echo "Web-based administration interface" ;;
        redisw) echo "Redis wire protocol compatibility" ;;
        mongodbw) echo "MongoDB wire protocol compatibility" ;;
        postgresw) echo "PostgreSQL wire protocol compatibility" ;;
        grpcw) echo "gRPC wire protocol support" ;;
        graphql) echo "GraphQL API support" ;;
        metrics) echo "Prometheus metrics integration" ;;
        *) echo "Unknown module" ;;
    esac
}

set -u

# Default values
ARCADEDB_VERSION=""
SELECTED_MODULES=""
OUTPUT_NAME=""
OUTPUT_DIR="$(pwd)"
DOCKER_TAG=""
SKIP_DOCKER=false
DOCKERFILE_ONLY=false
KEEP_TEMP=false
DRY_RUN=false
VERBOSE=false
QUIET=false

# Temp directory
TEMP_DIR=""

#===============================================================================
# Help and Usage Functions
#===============================================================================

show_help() {
    cat << EOF
Usage: ${SCRIPT_NAME} [OPTIONS]

Build custom ArcadeDB distributions with only the modules you need.

OPTIONS:
    --version VERSION       ArcadeDB version to build (required)
    --modules MODULES       Comma-separated list of modules (required)
                           Options: console,gremlin,studio,redisw,mongodbw,postgresw,grpcw,graphql,metrics
    --output-name NAME      Custom name for distribution (default: arcadedb-<version>-<modules>)
    --output-dir DIR        Output directory (default: current directory)
    --docker-tag TAG        Build Docker image with specified tag
    --skip-docker          Skip Docker image build
    --dockerfile-only      Only generate Dockerfile, don't build image
    --keep-temp            Keep temporary build directory
    --dry-run              Show what would be done without executing
    -v, --verbose          Enable verbose output
    -q, --quiet            Suppress non-error output
    -h, --help             Show this help message

OPTIONAL MODULES:
    console      Interactive database console
    gremlin      Apache Tinkerpop Gremlin support
    studio       Web-based administration interface
    redisw       Redis wire protocol compatibility
    mongodbw     MongoDB wire protocol compatibility
    postgresw    PostgreSQL wire protocol compatibility
    grpcw        gRPC wire protocol support
    graphql      GraphQL API support
    metrics      Prometheus metrics integration

EXAMPLES:
    # Build with Gremlin and Studio
    ${SCRIPT_NAME} --version=26.1.0 --modules=gremlin,studio

    # Build minimal distribution (server only)
    ${SCRIPT_NAME} --version=26.1.0 --modules=console

    # Build with custom name and Docker image
    ${SCRIPT_NAME} --version=26.1.0 --modules=gremlin,studio --output-name=my-arcade --docker-tag=myrepo/arcade:latest

    # Dry run to see what would be built
    ${SCRIPT_NAME} --version=26.1.0 --modules=gremlin,studio --dry-run

EOF
}

#===============================================================================
# Argument Parsing
#===============================================================================

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --version=*)
                ARCADEDB_VERSION="${1#*=}"
                shift
                ;;
            --modules=*)
                SELECTED_MODULES="${1#*=}"
                shift
                ;;
            --output-name=*)
                OUTPUT_NAME="${1#*=}"
                shift
                ;;
            --output-dir=*)
                OUTPUT_DIR="${1#*=}"
                shift
                ;;
            --docker-tag=*)
                DOCKER_TAG="${1#*=}"
                shift
                ;;
            --skip-docker)
                SKIP_DOCKER=true
                shift
                ;;
            --dockerfile-only)
                DOCKERFILE_ONLY=true
                shift
                ;;
            --keep-temp)
                KEEP_TEMP=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -q|--quiet)
                QUIET=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                echo "Error: Unknown option: $1" >&2
                echo "Use --help for usage information" >&2
                exit 1
                ;;
        esac
    done
}

#===============================================================================
# Main Entry Point
#===============================================================================

main() {
    echo "ArcadeDB Modular Distribution Builder v${VERSION}"
    echo "================================================"
    echo ""

    parse_args "$@"

    # Temporary: Print parsed arguments for testing
    echo "Parsed arguments:"
    echo "  Version: ${ARCADEDB_VERSION}"
    echo "  Modules: ${SELECTED_MODULES}"
    echo "  Output Name: ${OUTPUT_NAME}"
    echo "  Output Dir: ${OUTPUT_DIR}"
    echo "  Docker Tag: ${DOCKER_TAG}"
    echo "  Skip Docker: ${SKIP_DOCKER}"
    echo "  Dockerfile Only: ${DOCKERFILE_ONLY}"
    echo "  Keep Temp: ${KEEP_TEMP}"
    echo "  Dry Run: ${DRY_RUN}"
    echo "  Verbose: ${VERBOSE}"
    echo "  Quiet: ${QUIET}"
}

# Run main function
main "$@"
