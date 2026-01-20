#!/usr/bin/env bash
# Requires bash 3.2+ for [[ ]] conditionals and local variables
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
                error_exit "Unknown option: $1. Use --help for usage information"
                ;;
        esac
    done
}

#===============================================================================
# Logging and Error Handling Functions
#===============================================================================

# Logging functions
log_info() {
    if [[ "$QUIET" != true ]]; then
        echo "[INFO] $*"
    fi
}

log_verbose() {
    if [[ "$VERBOSE" == true ]]; then
        echo "[DEBUG] $*"
    fi
}

log_error() {
    echo "[ERROR] $*" >&2
}

log_success() {
    if [[ "$QUIET" != true ]]; then
        echo "[SUCCESS] $*"
    fi
}

log_warning() {
    if [[ "$QUIET" != true ]]; then
        echo "[WARNING] $*" >&2
    fi
}

# Error handler
error_exit() {
    log_error "$1"
    cleanup
    exit 1
}

# Cleanup function
cleanup() {
    if [[ -n "$TEMP_DIR" ]] && [[ -d "$TEMP_DIR" ]]; then
        if [[ "$KEEP_TEMP" == true ]]; then
            log_info "Keeping temporary directory: $TEMP_DIR"
        else
            log_verbose "Cleaning up temporary directory: $TEMP_DIR"
            rm -rf "$TEMP_DIR"
        fi
    fi
}

# Trap errors and interrupts
trap cleanup EXIT
trap 'log_error "Script interrupted"; exit 130' INT TERM

#===============================================================================
# Prerequisites Validation
#===============================================================================

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_tools=()

    # Check for download tool
    if ! command -v curl &> /dev/null && ! command -v wget &> /dev/null; then
        missing_tools+=("curl or wget")
    fi

    # Check for tar
    if ! command -v tar &> /dev/null; then
        missing_tools+=("tar")
    fi

    # Check for unzip
    if ! command -v unzip &> /dev/null; then
        missing_tools+=("unzip")
    fi

    # Check for checksum tool
    if ! command -v sha256sum &> /dev/null && ! command -v shasum &> /dev/null; then
        missing_tools+=("sha256sum or shasum")
    fi

    # Check for sha1sum (for Maven Central)
    if ! command -v sha1sum &> /dev/null && ! command -v shasum &> /dev/null; then
        missing_tools+=("sha1sum or shasum")
    fi

    # Check for Docker if needed
    if [[ "$SKIP_DOCKER" != true ]] && [[ "$DOCKERFILE_ONLY" != true ]]; then
        if ! command -v docker &> /dev/null; then
            missing_tools+=("docker (or use --skip-docker/--dockerfile-only)")
        fi
    fi

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        error_exit "Missing required tools: ${missing_tools[*]}"
    fi

    # Check write permissions
    # Ensure directory exists or can be created
    if [[ ! -d "$OUTPUT_DIR" ]]; then
        if ! mkdir -p "$OUTPUT_DIR" 2>/dev/null; then
            error_exit "Cannot create output directory: $OUTPUT_DIR"
        fi
    fi

    if [[ ! -w "$OUTPUT_DIR" ]]; then
        error_exit "Output directory not writable: $OUTPUT_DIR"
    fi

    # Check disk space (warn if < 500MB)
    local available_space
    if command -v df &> /dev/null; then
        available_space=$(df -k "$OUTPUT_DIR" | awk 'NR==2 {print $4}')
        if [[ $available_space -lt 512000 ]]; then
            log_warning "Less than 500MB available in $OUTPUT_DIR"
        fi
    fi

    log_success "All prerequisites satisfied"
}

# Validate version format
validate_version() {
    if [[ -z "$ARCADEDB_VERSION" ]]; then
        error_exit "Version not specified. Use --version=X.Y.Z or run in interactive mode"
    fi

    # Check version format (X.Y.Z or X.Y.Z-SNAPSHOT)
    if ! [[ "$ARCADEDB_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-SNAPSHOT)?$ ]]; then
        error_exit "Invalid version format: $ARCADEDB_VERSION. Expected format: X.Y.Z or X.Y.Z-SNAPSHOT"
    fi

    log_verbose "Version validated: $ARCADEDB_VERSION"
}

# Set default values based on inputs
set_defaults() {
    # Set default output name if not specified
    if [[ -z "$OUTPUT_NAME" ]]; then
        local timestamp=$(date +%Y%m%d-%H%M%S)
        OUTPUT_NAME="arcadedb-${ARCADEDB_VERSION}-custom-${timestamp}"
    fi

    # Set default Docker tag if not specified
    if [[ -z "$DOCKER_TAG" ]]; then
        DOCKER_TAG="arcadedb-custom:${ARCADEDB_VERSION}"
    fi

    log_verbose "Output name: $OUTPUT_NAME"
    log_verbose "Docker tag: $DOCKER_TAG"
}

#===============================================================================
# Main Entry Point
#===============================================================================

main() {
    parse_args "$@"

    log_info "Starting modular distribution builder"

    check_prerequisites
    validate_version
    set_defaults

    log_success "Configuration validated"
}

# Run main function
main "$@"
