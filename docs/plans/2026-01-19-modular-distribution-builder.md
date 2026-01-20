# Modular Distribution Builder Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a modular distribution builder that creates custom ArcadeDB packages with user-selected modules, generating zip/tar.gz archives and Docker images.

**Architecture:** Base distribution approach - create a new "base" Maven assembly with core modules (engine, server, network) and dependencies. A standalone bash script downloads the base from GitHub releases, adds optional modules from Maven Central, and generates archives and Docker images.

**Tech Stack:** Maven Assembly Plugin, Bash scripting, Maven Central API, GitHub Releases, Docker

---

## Task 1: Create Base Assembly Descriptor

**Files:**
- Create: `package/src/main/assembly/base.xml`

**Step 1: Create the base assembly XML file**

Create `package/src/main/assembly/base.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
    Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->
<assembly xmlns="http://maven.apache.org/ASSEMBLY/2.2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/ASSEMBLY/2.2.0 https://maven.apache.org/xsd/assembly-2.2.0.xsd">

    <id>base</id>

    <formats>
        <format>dir</format>
        <format>tar.gz</format>
        <format>zip</format>
    </formats>

    <fileSets>
        <!--
         | copy scripts
        -->
        <fileSet>
            <directory>${basedir}/src/main/scripts</directory>
            <outputDirectory>bin</outputDirectory>
            <includes>
                <include>*.sh</include>
                <include>*.bat</include>
            </includes>
            <fileMode>755</fileMode>
            <filtered>true</filtered>
        </fileSet>

        <fileSet>
            <directory>${basedir}/src/main/config</directory>
            <outputDirectory>config</outputDirectory>
            <includes>
                <include>*.yaml</include>
                <include>*.json</include>
                <include>*.groovy</include>
                <include>*.properties</include>
            </includes>
            <fileMode>755</fileMode>
            <filtered>true</filtered>
        </fileSet>
        <fileSet> <!-- Create empty directory -->
            <outputDirectory>databases</outputDirectory>
            <excludes>
                <exclude>**/*</exclude>
            </excludes>
        </fileSet>
        <fileSet> <!-- Create empty directory -->
            <outputDirectory>backups</outputDirectory>
            <excludes>
                <exclude>**/*</exclude>
            </excludes>
        </fileSet>
        <fileSet> <!-- Create empty directory -->
            <outputDirectory>replication</outputDirectory>
            <excludes>
                <exclude>**/*</exclude>
            </excludes>
        </fileSet>
        <fileSet> <!-- Create empty directory -->
            <outputDirectory>log</outputDirectory>
            <excludes>
                <exclude>**/*</exclude>
            </excludes>
        </fileSet>

    </fileSets>

    <!--
     | includes legals
    -->
    <files>
        <file>
            <source>${basedir}/../README.md</source>
            <fileMode>666</fileMode>
        </file>
        <file>
            <source>${basedir}/../LICENSE</source>
            <fileMode>666</fileMode>
        </file>
    </files>
    <!--
     | Core modules + dependencies only (engine, server, network)
     | Excludes all optional modules
    -->
    <dependencySets>
        <dependencySet>
            <outputDirectory>lib</outputDirectory>
            <includes>
                <include>*:jar:*</include>
            </includes>
            <excludes>
                <!-- Exclude optional modules -->
                <exclude>com.arcadedb:arcadedb-console</exclude>
                <exclude>com.arcadedb:arcadedb-gremlin</exclude>
                <exclude>com.arcadedb:arcadedb-redisw</exclude>
                <exclude>com.arcadedb:arcadedb-mongodbw</exclude>
                <exclude>com.arcadedb:arcadedb-graphql</exclude>
                <exclude>com.arcadedb:arcadedb-studio</exclude>
                <exclude>com.arcadedb:arcadedb-postgresw</exclude>
                <exclude>com.arcadedb:arcadedb-grpcw</exclude>
                <exclude>com.arcadedb:arcadedb-metrics</exclude>
            </excludes>
        </dependencySet>
    </dependencySets>

</assembly>
```

**Step 2: Verify file syntax**

Run: `xmllint --noout package/src/main/assembly/base.xml`
Expected: No output (success)

**Step 3: Commit**

```bash
git add package/src/main/assembly/base.xml
git commit -m "feat: add base assembly descriptor for modular builder

Add base.xml assembly descriptor that includes core modules
(engine, server, network) with all dependencies, scripts, and
configs. This base distribution will be used by the modular
builder to create custom distributions.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Update Maven POM for Base Assembly

**Files:**
- Modify: `package/pom.xml`

**Step 1: Add base assembly execution to pom.xml**

In `package/pom.xml`, add a new execution after the headless execution (around line 96):

```xml
                    <execution>
                        <id>base</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                        <configuration>
                            <appendAssemblyId>true</appendAssemblyId>
                            <finalName>arcadedb-${project.version}</finalName>
                            <descriptors>
                                <descriptor>./src/main/assembly/base.xml</descriptor>
                            </descriptors>
                            <ignoreDirFormatExtensions>false</ignoreDirFormatExtensions>
                            <tarLongFileMode>gnu</tarLongFileMode>
                        </configuration>
                    </execution>
```

**Step 2: Build package module to verify**

Run: `cd package && mvn clean package -DskipTests`
Expected: Should create `target/arcadedb-26.1.1-SNAPSHOT-base.tar.gz` and `target/arcadedb-26.1.1-SNAPSHOT-base.zip`

**Step 3: Verify base distribution contents**

Run:
```bash
cd package/target
tar -tzf arcadedb-26.1.1-SNAPSHOT-base.tar.gz | head -20
```

Expected: Should show bin/, config/, lib/ directories with core JARs only (no gremlin, redisw, etc.)

**Step 4: Commit**

```bash
git add package/pom.xml
git commit -m "feat: add base distribution build to package module

Add Maven assembly execution to build base distribution with
core modules only. This generates base.tar.gz and base.zip
for use by the modular distribution builder.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Add Checksum Generation Plugin

**Files:**
- Modify: `package/pom.xml`

**Step 1: Add checksum-maven-plugin to package/pom.xml**

Add this plugin configuration inside the `<build><plugins>` section of `package/pom.xml`:

```xml
            <plugin>
                <groupId>net.nicoulaj.maven.plugins</groupId>
                <artifactId>checksum-maven-plugin</artifactId>
                <version>1.11</version>
                <executions>
                    <execution>
                        <id>generate-checksums</id>
                        <phase>package</phase>
                        <goals>
                            <goal>files</goal>
                        </goals>
                        <configuration>
                            <fileSets>
                                <fileSet>
                                    <directory>${project.build.directory}</directory>
                                    <includes>
                                        <include>arcadedb-*.tar.gz</include>
                                        <include>arcadedb-*.zip</include>
                                    </includes>
                                </fileSet>
                            </fileSets>
                            <algorithms>
                                <algorithm>SHA-256</algorithm>
                            </algorithms>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
```

**Step 2: Build and verify checksums are generated**

Run: `cd package && mvn clean package -DskipTests`
Expected: Should create `.sha256` files for all archives

**Step 3: Verify checksum files exist**

Run: `ls -la package/target/*.sha256`
Expected: Should show `.sha256` files for base, full, minimal, and headless distributions

**Step 4: Commit**

```bash
git add package/pom.xml
git commit -m "feat: add SHA-256 checksum generation for distributions

Add checksum-maven-plugin to generate SHA-256 checksums for
all distribution archives. These checksums will be used by
the modular builder to verify downloads.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Create Builder Script Foundation

**Files:**
- Create: `package/arcadedb-builder.sh`

**Step 1: Create basic script structure with shebang and constants**

Create `package/arcadedb-builder.sh`:

```bash
#!/bin/bash
set -euo pipefail

# ArcadeDB Modular Distribution Builder
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
# Licensed under the Apache License, Version 2.0

VERSION="1.0.0"
SCRIPT_NAME="$(basename "$0")"

# URLs
MAVEN_CENTRAL_BASE="https://repo1.maven.org/maven2/com/arcadedb"
GITHUB_RELEASES_BASE="https://github.com/arcadedata/arcadedb/releases/download"

# Module metadata
SHADED_MODULES="gremlin redisw mongodbw postgresw grpcw metrics"
REGULAR_MODULES="console studio graphql"

# Module descriptions for interactive menu
declare -A MODULE_DESCRIPTIONS=(
    [console]="Interactive database console"
    [gremlin]="Apache Tinkerpop Gremlin support"
    [studio]="Web-based administration interface"
    [redisw]="Redis wire protocol compatibility"
    [mongodbw]="MongoDB wire protocol compatibility"
    [postgresw]="PostgreSQL wire protocol compatibility"
    [grpcw]="gRPC wire protocol support"
    [graphql]="GraphQL API support"
    [metrics]="Prometheus metrics integration"
)

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

echo "ArcadeDB Modular Distribution Builder v${VERSION}"
echo "================================================"
echo ""
```

**Step 2: Make script executable**

Run: `chmod +x package/arcadedb-builder.sh`

**Step 3: Test basic script execution**

Run: `./package/arcadedb-builder.sh`
Expected: Should print header and exit cleanly

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: create modular builder script foundation

Add basic script structure with constants, module metadata,
and configuration variables for the modular distribution builder.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Add Help and Usage Functions

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add show_help function**

Add after the constants section:

```bash
# Show help message
show_help() {
    cat << EOF
Usage: ${SCRIPT_NAME} [OPTIONS]

Build custom ArcadeDB distributions with selected optional modules.

OPTIONS:
    --version=X.Y.Z         ArcadeDB version to build (required for non-interactive)
    --modules=mod1,mod2     Comma-separated list of optional modules
    --output-name=NAME      Custom output name (default: arcadedb-{version}-custom-{timestamp})
    --output-dir=PATH       Output directory (default: current directory)
    --docker-tag=TAG        Docker image tag (default: arcadedb-custom:{version})
    --skip-docker           Skip Docker image generation
    --dockerfile-only       Generate Dockerfile without building image
    --keep-temp             Don't delete temporary working directory
    --dry-run               Show what would be downloaded without doing it
    -v, --verbose           Verbose output
    -q, --quiet             Quiet mode (errors only)
    -h, --help              Show this help message

OPTIONAL MODULES:
    console     - Interactive database console
    gremlin     - Apache Tinkerpop Gremlin support
    studio      - Web-based administration interface
    redisw      - Redis wire protocol compatibility
    mongodbw    - MongoDB wire protocol compatibility
    postgresw   - PostgreSQL wire protocol compatibility
    grpcw       - gRPC wire protocol support
    graphql     - GraphQL API support
    metrics     - Prometheus metrics integration

EXAMPLES:
    # Interactive mode
    ${SCRIPT_NAME}

    # Minimal build with PostgreSQL
    ${SCRIPT_NAME} --version=26.1.0 --modules=postgresw

    # Full custom build
    ${SCRIPT_NAME} --version=26.1.0 --modules=console,gremlin,studio,postgresw,mongodbw

    # CI/CD mode
    ${SCRIPT_NAME} --version=26.1.0 --modules=gremlin,studio --quiet --output-dir=/tmp/builds

EOF
}
```

**Step 2: Add argument parsing skeleton**

Add after show_help function:

```bash
# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -q|--quiet)
                QUIET=true
                shift
                ;;
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
            *)
                echo "Error: Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}
```

**Step 3: Add main entry point**

Add at the end of the script:

```bash
# Main entry point
main() {
    parse_args "$@"

    echo "Arguments parsed successfully"
    echo "Version: ${ARCADEDB_VERSION:-<not set>}"
    echo "Modules: ${SELECTED_MODULES:-<not set>}"
}

main "$@"
```

**Step 4: Test help message**

Run: `./package/arcadedb-builder.sh --help`
Expected: Should display help message and exit

**Step 5: Test argument parsing**

Run: `./package/arcadedb-builder.sh --version=26.1.0 --modules=gremlin,studio`
Expected: Should print parsed arguments

**Step 6: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add help and argument parsing to builder script

Add comprehensive help message and CLI argument parsing
with support for all planned options.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Add Logging and Error Handling Functions

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add logging functions**

Add after parse_args function:

```bash
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
trap 'error_exit "Script interrupted"' INT TERM
```

**Step 2: Test error handling**

Update main() to test:

```bash
main() {
    parse_args "$@"

    log_info "Starting modular distribution builder"
    log_verbose "Verbose mode enabled"
    log_success "Test successful"
}
```

**Step 3: Run tests**

Run: `./package/arcadedb-builder.sh --version=26.1.0 -v`
Expected: Should show info and debug messages

Run: `./package/arcadedb-builder.sh --version=26.1.0 -q`
Expected: Should show minimal output

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add logging and error handling to builder script

Add structured logging functions (info, verbose, error, success)
and error handling with cleanup on exit or interrupt.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Add Prerequisites Validation

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add check_prerequisites function**

Add after cleanup function:

```bash
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
    if [[ "$SKIP_DOCKER" != true ]] && ! command -v docker &> /dev/null; then
        if [[ "$DOCKERFILE_ONLY" != true ]]; then
            log_error "Docker not found. Install Docker or use --skip-docker or --dockerfile-only"
            missing_tools+=("docker")
        fi
    fi

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        error_exit "Missing required tools: ${missing_tools[*]}"
    fi

    # Check disk space (warn if < 500MB)
    local available_space
    if command -v df &> /dev/null; then
        available_space=$(df -k "$OUTPUT_DIR" | awk 'NR==2 {print $4}')
        if [[ $available_space -lt 512000 ]]; then
            log_error "Warning: Less than 500MB available in $OUTPUT_DIR"
        fi
    fi

    # Check write permissions
    if [[ ! -w "$OUTPUT_DIR" ]]; then
        error_exit "Output directory not writable: $OUTPUT_DIR"
    fi

    log_success "All prerequisites satisfied"
}
```

**Step 2: Call check_prerequisites from main**

Update main():

```bash
main() {
    parse_args "$@"

    log_info "Starting modular distribution builder"

    check_prerequisites

    log_success "Ready to build"
}
```

**Step 3: Test prerequisites check**

Run: `./package/arcadedb-builder.sh --version=26.1.0`
Expected: Should check and report all prerequisites

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add prerequisites validation to builder script

Check for required tools (curl/wget, tar, unzip, checksums, docker),
disk space, and write permissions before proceeding.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Add Version Validation

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add validate_version function**

Add after check_prerequisites:

```bash
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
```

**Step 2: Add set_defaults function**

Add after validate_version:

```bash
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
```

**Step 3: Update main to call validation**

Update main():

```bash
main() {
    parse_args "$@"

    log_info "Starting modular distribution builder"

    check_prerequisites
    validate_version
    set_defaults

    log_success "Configuration validated"
}
```

**Step 4: Test version validation**

Run: `./package/arcadedb-builder.sh --version=invalid`
Expected: Should fail with "Invalid version format"

Run: `./package/arcadedb-builder.sh --version=26.1.0`
Expected: Should validate successfully

**Step 5: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add version validation and defaults to builder script

Validate version format and set default values for output name
and Docker tag based on version and timestamp.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 9: Add Interactive Module Selection

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add interactive_select_modules function**

Add after set_defaults:

```bash
# Interactive module selection
interactive_select_modules() {
    echo ""
    echo "Select optional modules (space-separated numbers, e.g., 1 3 5):"
    echo "Press Enter without input to skip all optional modules"
    echo ""

    local all_modules=($SHADED_MODULES $REGULAR_MODULES)
    local counter=1
    local -A module_index

    for module in "${all_modules[@]}"; do
        printf "%2d. %-12s - %s\n" "$counter" "$module" "${MODULE_DESCRIPTIONS[$module]}"
        module_index[$counter]=$module
        ((counter++))
    done

    echo ""
    read -p "Enter module numbers: " -r selections

    # Parse selections
    local selected=()
    for num in $selections; do
        if [[ -n "${module_index[$num]}" ]]; then
            selected+=("${module_index[$num]}")
        else
            log_error "Invalid selection: $num"
        fi
    done

    # Convert to comma-separated string
    SELECTED_MODULES=$(IFS=,; echo "${selected[*]}")

    if [[ -z "$SELECTED_MODULES" ]]; then
        log_info "No optional modules selected. Building base distribution only."
    else
        log_info "Selected modules: $SELECTED_MODULES"
    fi
}
```

**Step 2: Add interactive mode detection**

Update main():

```bash
main() {
    parse_args "$@"

    log_info "Starting modular distribution builder"

    check_prerequisites

    # Interactive mode if version not specified
    if [[ -z "$ARCADEDB_VERSION" ]]; then
        echo ""
        read -p "Enter ArcadeDB version (e.g., 26.1.0): " ARCADEDB_VERSION
    fi

    validate_version
    set_defaults

    # Interactive module selection if not specified
    if [[ -z "$SELECTED_MODULES" ]] && [[ "$DRY_RUN" != true ]]; then
        interactive_select_modules
    fi

    log_success "Configuration complete"
}
```

**Step 3: Test interactive mode**

Run: `./package/arcadedb-builder.sh` (then enter version and select modules interactively)
Expected: Should prompt for version and modules

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add interactive module selection to builder script

Add interactive mode that prompts for version and displays
numbered module list for selection. Falls back to interactive
if version or modules not specified on command line.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 10: Add Download Helper Functions

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add download_file function**

Add after interactive_select_modules:

```bash
# Download file with curl or wget
download_file() {
    local url="$1"
    local output="$2"

    log_verbose "Downloading: $url"

    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would download: $url"
        return 0
    fi

    if command -v curl &> /dev/null; then
        if [[ "$VERBOSE" == true ]]; then
            curl -fL --progress-bar "$url" -o "$output"
        else
            curl -fsSL "$url" -o "$output"
        fi
    elif command -v wget &> /dev/null; then
        if [[ "$QUIET" == true ]]; then
            wget -q "$url" -O "$output"
        else
            wget "$url" -O "$output"
        fi
    else
        error_exit "No download tool available (curl or wget)"
    fi

    if [[ ! -f "$output" ]]; then
        error_exit "Failed to download: $url"
    fi

    log_verbose "Downloaded to: $output"
}

# Verify SHA-256 checksum
verify_sha256() {
    local file="$1"
    local checksum_file="$2"

    log_verbose "Verifying SHA-256 checksum for: $file"

    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would verify checksum: $file"
        return 0
    fi

    local expected_checksum
    expected_checksum=$(cat "$checksum_file" | awk '{print $1}')

    local actual_checksum
    if command -v sha256sum &> /dev/null; then
        actual_checksum=$(sha256sum "$file" | awk '{print $1}')
    elif command -v shasum &> /dev/null; then
        actual_checksum=$(shasum -a 256 "$file" | awk '{print $1}')
    else
        error_exit "No SHA-256 tool available"
    fi

    if [[ "$expected_checksum" != "$actual_checksum" ]]; then
        error_exit "Checksum verification failed for $file. Expected: $expected_checksum, Got: $actual_checksum"
    fi

    log_verbose "Checksum verified successfully"
}

# Verify SHA-1 checksum (for Maven Central)
verify_sha1() {
    local file="$1"
    local checksum_file="$2"

    log_verbose "Verifying SHA-1 checksum for: $file"

    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would verify checksum: $file"
        return 0
    fi

    local expected_checksum
    expected_checksum=$(cat "$checksum_file" | awk '{print $1}')

    local actual_checksum
    if command -v sha1sum &> /dev/null; then
        actual_checksum=$(sha1sum "$file" | awk '{print $1}')
    elif command -v shasum &> /dev/null; then
        actual_checksum=$(shasum -a 1 "$file" | awk '{print $1}')
    else
        error_exit "No SHA-1 tool available"
    fi

    if [[ "$expected_checksum" != "$actual_checksum" ]]; then
        error_exit "Checksum verification failed for $file. Expected: $expected_checksum, Got: $actual_checksum"
    fi

    log_verbose "Checksum verified successfully"
}
```

**Step 2: Test download functions with a simple file**

Add to main() for testing:

```bash
    # Test download (temporary)
    TEMP_DIR=$(mktemp -d)
    download_file "https://www.apache.org/licenses/LICENSE-2.0.txt" "$TEMP_DIR/test.txt"
    log_success "Download test successful"
```

**Step 3: Run test**

Run: `./package/arcadedb-builder.sh --version=26.1.0`
Expected: Should download test file successfully

**Step 4: Remove test code and commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add download and checksum verification functions

Add helper functions to download files using curl/wget and
verify SHA-256 and SHA-1 checksums. Includes dry-run support.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 11: Add Base Distribution Download

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add download_base_distribution function**

Add after verify_sha1:

```bash
# Download and extract base distribution
download_base_distribution() {
    log_info "Downloading base distribution for version $ARCADEDB_VERSION..."

    local base_filename="arcadedb-${ARCADEDB_VERSION}-base.tar.gz"
    local base_url="${GITHUB_RELEASES_BASE}/${ARCADEDB_VERSION}/${base_filename}"
    local checksum_url="${base_url}.sha256"

    local base_file="$TEMP_DIR/$base_filename"
    local checksum_file="${base_file}.sha256"

    # Download base distribution
    download_file "$base_url" "$base_file"

    # Download checksum
    download_file "$checksum_url" "$checksum_file"

    # Verify checksum
    verify_sha256 "$base_file" "$checksum_file"

    log_success "Base distribution downloaded and verified"

    # Extract base distribution
    log_info "Extracting base distribution..."

    if [[ "$DRY_RUN" != true ]]; then
        tar -xzf "$base_file" -C "$TEMP_DIR"

        # Find the extracted directory
        local extracted_dir="$TEMP_DIR/arcadedb-${ARCADEDB_VERSION}-base"
        if [[ ! -d "$extracted_dir" ]]; then
            error_exit "Extracted directory not found: $extracted_dir"
        fi

        log_verbose "Extracted to: $extracted_dir"
    else
        log_info "[DRY RUN] Would extract: $base_file"
    fi

    log_success "Base distribution extracted"
}
```

**Step 2: Update main to call download_base_distribution**

Update main():

```bash
main() {
    parse_args "$@"

    log_info "Starting modular distribution builder"

    check_prerequisites

    # Interactive mode if version not specified
    if [[ -z "$ARCADEDB_VERSION" ]]; then
        echo ""
        read -p "Enter ArcadeDB version (e.g., 26.1.0): " ARCADEDB_VERSION
    fi

    validate_version
    set_defaults

    # Interactive module selection if not specified
    if [[ -z "$SELECTED_MODULES" ]] && [[ "$DRY_RUN" != true ]]; then
        interactive_select_modules
    fi

    # Create temp directory
    TEMP_DIR=$(mktemp -d)
    log_verbose "Created temporary directory: $TEMP_DIR"

    # Download base distribution
    download_base_distribution

    log_success "Build complete"
}
```

**Step 3: Test with dry-run (won't actually download yet since base doesn't exist)**

Run: `./package/arcadedb-builder.sh --version=26.1.0 --dry-run --modules=gremlin`
Expected: Should show what would be downloaded

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add base distribution download to builder script

Download base distribution from GitHub releases, verify checksum,
and extract to temporary directory.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 12: Add Optional Module Downloads

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add download_optional_modules function**

Add after download_base_distribution:

```bash
# Download optional modules from Maven Central
download_optional_modules() {
    if [[ -z "$SELECTED_MODULES" ]]; then
        log_info "No optional modules selected, skipping download"
        return 0
    fi

    log_info "Downloading optional modules: $SELECTED_MODULES..."

    local extracted_dir="$TEMP_DIR/arcadedb-${ARCADEDB_VERSION}-base"
    local lib_dir="${extracted_dir}/lib"

    # Split modules by comma
    IFS=',' read -ra modules <<< "$SELECTED_MODULES"

    for module in "${modules[@]}"; do
        module=$(echo "$module" | xargs) # trim whitespace

        log_info "Downloading module: $module"

        # Determine if shaded or regular JAR
        local classifier=""
        if [[ " $SHADED_MODULES " =~ " $module " ]]; then
            classifier="-shaded"
        fi

        # Construct Maven Central URL
        local artifact_id="arcadedb-${module}"
        local jar_filename="${artifact_id}-${ARCADEDB_VERSION}${classifier}.jar"
        local jar_url="${MAVEN_CENTRAL_BASE}/${artifact_id}/${ARCADEDB_VERSION}/${jar_filename}"
        local checksum_url="${jar_url}.sha1"

        local jar_file="${lib_dir}/${jar_filename}"
        local checksum_file="${jar_file}.sha1"

        # Download JAR
        download_file "$jar_url" "$jar_file"

        # Download checksum
        download_file "$checksum_url" "$checksum_file"

        # Verify checksum
        verify_sha1 "$jar_file" "$checksum_file"

        # Clean up checksum file
        if [[ "$DRY_RUN" != true ]]; then
            rm -f "$checksum_file"
        fi

        log_success "Module downloaded: $module"
    done

    log_success "All optional modules downloaded"
}
```

**Step 2: Update main to call download_optional_modules**

Update main():

```bash
    # Download base distribution
    download_base_distribution

    # Download optional modules
    download_optional_modules

    log_success "Build complete"
```

**Step 3: Test with dry-run**

Run: `./package/arcadedb-builder.sh --version=26.1.0 --dry-run --modules=gremlin,studio`
Expected: Should show Maven Central URLs for selected modules

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add optional module download from Maven Central

Download selected optional modules (shaded or regular JARs)
from Maven Central, verify SHA-1 checksums, and add to lib directory.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 13: Add Archive Creation

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add create_archives function**

Add after download_optional_modules:

```bash
# Create zip and tar.gz archives
create_archives() {
    log_info "Creating distribution archives..."

    local extracted_dir="$TEMP_DIR/arcadedb-${ARCADEDB_VERSION}-base"
    local final_dir="$TEMP_DIR/$OUTPUT_NAME"

    # Rename extracted directory to final name
    if [[ "$DRY_RUN" != true ]]; then
        mv "$extracted_dir" "$final_dir"
    else
        log_info "[DRY RUN] Would rename: $extracted_dir -> $final_dir"
    fi

    local zip_file="${OUTPUT_DIR}/${OUTPUT_NAME}.zip"
    local targz_file="${OUTPUT_DIR}/${OUTPUT_NAME}.tar.gz"

    # Create tar.gz
    log_info "Creating tar.gz archive..."
    if [[ "$DRY_RUN" != true ]]; then
        tar -czf "$targz_file" -C "$TEMP_DIR" "$OUTPUT_NAME"
        log_success "Created: $targz_file"
    else
        log_info "[DRY RUN] Would create: $targz_file"
    fi

    # Create zip
    log_info "Creating zip archive..."
    if [[ "$DRY_RUN" != true ]]; then
        (cd "$TEMP_DIR" && zip -r -q "$zip_file" "$OUTPUT_NAME")
        log_success "Created: $zip_file"
    else
        log_info "[DRY RUN] Would create: $zip_file"
    fi

    log_success "Archives created successfully"
}
```

**Step 2: Update main to call create_archives**

Update main():

```bash
    # Download optional modules
    download_optional_modules

    # Create archives
    create_archives

    log_success "Build complete"
```

**Step 3: Test with dry-run**

Run: `./package/arcadedb-builder.sh --version=26.1.0 --dry-run --modules=gremlin`
Expected: Should show archive creation steps

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add archive creation to builder script

Create zip and tar.gz archives from the assembled distribution
directory with user-specified or timestamp-based naming.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 14: Add Docker Image Generation

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add generate_dockerfile function**

Add after create_archives:

```bash
# Generate Dockerfile
generate_dockerfile() {
    local dist_dir="$1"
    local dockerfile="${dist_dir}/Dockerfile"

    log_info "Generating Dockerfile..."

    if [[ "$DRY_RUN" != true ]]; then
        cat > "$dockerfile" << 'EOF'
FROM eclipse-temurin:21-jre-alpine

ARG ARCADEDB_USER=arcadedb
ARG ARCADEDB_HOME=/home/arcadedb

ENV JAVA_OPTS="-Xms1G -Xmx4G"

RUN addgroup -S ${ARCADEDB_USER} && adduser -S ${ARCADEDB_USER} -G ${ARCADEDB_USER}

WORKDIR ${ARCADEDB_HOME}

COPY --chown=${ARCADEDB_USER}:${ARCADEDB_USER} . ${ARCADEDB_HOME}

RUN chmod +x ${ARCADEDB_HOME}/bin/*.sh

USER ${ARCADEDB_USER}

EXPOSE 2480 2424

VOLUME ["${ARCADEDB_HOME}/databases", "${ARCADEDB_HOME}/backups", "${ARCADEDB_HOME}/log"]

CMD ["./bin/server.sh"]
EOF
        log_success "Dockerfile generated: $dockerfile"
    else
        log_info "[DRY RUN] Would generate Dockerfile"
    fi
}

# Build Docker image
build_docker_image() {
    if [[ "$SKIP_DOCKER" == true ]]; then
        log_info "Skipping Docker image generation (--skip-docker)"
        return 0
    fi

    local final_dir="$TEMP_DIR/$OUTPUT_NAME"

    # Generate Dockerfile
    generate_dockerfile "$final_dir"

    if [[ "$DOCKERFILE_ONLY" == true ]]; then
        log_info "Dockerfile generated. Skipping image build (--dockerfile-only)"
        # Copy Dockerfile to output directory
        if [[ "$DRY_RUN" != true ]]; then
            cp "${final_dir}/Dockerfile" "${OUTPUT_DIR}/${OUTPUT_NAME}-Dockerfile"
            log_success "Dockerfile saved to: ${OUTPUT_DIR}/${OUTPUT_NAME}-Dockerfile"
        fi
        return 0
    fi

    # Check Docker availability
    if ! command -v docker &> /dev/null; then
        error_exit "Docker not found. Install Docker or use --skip-docker"
    fi

    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        error_exit "Docker daemon not running. Start Docker or use --skip-docker"
    fi

    log_info "Building Docker image: $DOCKER_TAG"

    if [[ "$DRY_RUN" != true ]]; then
        if [[ "$VERBOSE" == true ]]; then
            docker build -t "$DOCKER_TAG" "$final_dir"
        else
            docker build -t "$DOCKER_TAG" "$final_dir" > /dev/null
        fi
        log_success "Docker image built: $DOCKER_TAG"
    else
        log_info "[DRY RUN] Would build Docker image: $DOCKER_TAG"
    fi
}
```

**Step 2: Update main to call build_docker_image**

Update main():

```bash
    # Create archives
    create_archives

    # Build Docker image
    build_docker_image

    log_success "Build complete"
```

**Step 3: Test with dry-run and --skip-docker**

Run: `./package/arcadedb-builder.sh --version=26.1.0 --dry-run --modules=gremlin --skip-docker`
Expected: Should skip Docker generation

Run: `./package/arcadedb-builder.sh --version=26.1.0 --dry-run --modules=gremlin --dockerfile-only`
Expected: Should only generate Dockerfile

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add Docker image generation to builder script

Generate Dockerfile and optionally build Docker image with
custom tag. Supports --skip-docker and --dockerfile-only flags.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 15: Add Final Summary and Testing

**Files:**
- Modify: `package/arcadedb-builder.sh`

**Step 1: Add print_summary function**

Add after build_docker_image:

```bash
# Print final summary
print_summary() {
    echo ""
    echo "========================================"
    echo "Build Summary"
    echo "========================================"
    echo "Version:        $ARCADEDB_VERSION"
    echo "Output Name:    $OUTPUT_NAME"
    echo "Output Dir:     $OUTPUT_DIR"

    if [[ -n "$SELECTED_MODULES" ]]; then
        echo "Modules:        $SELECTED_MODULES"
    else
        echo "Modules:        (base only)"
    fi

    echo ""
    echo "Generated Files:"

    if [[ "$DRY_RUN" != true ]]; then
        local zip_file="${OUTPUT_DIR}/${OUTPUT_NAME}.zip"
        local targz_file="${OUTPUT_DIR}/${OUTPUT_NAME}.tar.gz"

        if [[ -f "$zip_file" ]]; then
            local zip_size=$(du -h "$zip_file" | cut -f1)
            echo "  - $zip_file ($zip_size)"
        fi

        if [[ -f "$targz_file" ]]; then
            local targz_size=$(du -h "$targz_file" | cut -f1)
            echo "  - $targz_file ($targz_size)"
        fi

        if [[ "$SKIP_DOCKER" != true ]] && [[ "$DOCKERFILE_ONLY" != true ]]; then
            echo "  - Docker image: $DOCKER_TAG"
        elif [[ "$DOCKERFILE_ONLY" == true ]]; then
            echo "  - ${OUTPUT_DIR}/${OUTPUT_NAME}-Dockerfile"
        fi
    else
        echo "  [DRY RUN - no files created]"
    fi

    echo ""
    echo "Build completed successfully!"
    echo "========================================"
}
```

**Step 2: Update main to call print_summary**

Update main():

```bash
    # Build Docker image
    build_docker_image

    # Print summary
    print_summary
```

**Step 3: Test complete script with dry-run**

Run: `./package/arcadedb-builder.sh --version=26.1.0 --dry-run --modules=console,gremlin,studio --output-name=test-build`
Expected: Should complete full dry-run and show summary

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add build summary to builder script

Display comprehensive summary of build configuration and
generated artifacts at completion.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 16: Create README for Builder Script

**Files:**
- Create: `package/README-BUILDER.md`

**Step 1: Create comprehensive README**

Create `package/README-BUILDER.md`:

```markdown
# ArcadeDB Modular Distribution Builder

Build custom ArcadeDB distributions with only the modules you need.

## Overview

The modular distribution builder (`arcadedb-builder.sh`) creates custom ArcadeDB packages by:
1. Downloading a base distribution (engine, server, network + dependencies)
2. Adding user-selected optional modules from Maven Central
3. Generating zip, tar.gz, and optionally Docker images

## Prerequisites

- `curl` or `wget` - for downloading files
- `tar` - for extracting and creating archives
- `unzip` and `zip` - for creating zip archives
- `sha256sum` or `shasum` - for checksum verification
- `docker` (optional) - for Docker image generation

## Quick Start

### Interactive Mode

```bash
./arcadedb-builder.sh
```

The script will prompt for:
- ArcadeDB version
- Optional modules to include

### CLI Mode

```bash
./arcadedb-builder.sh --version=26.1.0 --modules=gremlin,postgresw,studio
```

## Available Modules

**Core (always included):**
- `engine` - Database engine
- `server` - HTTP/REST API, clustering
- `network` - Network communication

**Optional:**
- `console` - Interactive database console
- `gremlin` - Apache Tinkerpop Gremlin support
- `studio` - Web-based administration interface
- `redisw` - Redis wire protocol compatibility
- `mongodbw` - MongoDB wire protocol compatibility
- `postgresw` - PostgreSQL wire protocol compatibility
- `grpcw` - gRPC wire protocol support
- `graphql` - GraphQL API support
- `metrics` - Prometheus metrics integration

## Usage Examples

### Minimal Build (PostgreSQL only)

```bash
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw
```

### Development Build

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=console,gremlin,studio \
  --output-name=arcadedb-dev
```

### Production Build (no Studio)

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=postgresw,metrics \
  --output-name=arcadedb-prod
```

### CI/CD Build

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=gremlin,studio \
  --quiet \
  --skip-docker \
  --output-dir=/tmp/builds
```

### Dockerfile Only (no build)

```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=gremlin,studio \
  --dockerfile-only
```

## Command-Line Options

### Required (non-interactive mode)

- `--version=X.Y.Z` - ArcadeDB version to build

### Optional

- `--modules=mod1,mod2,...` - Comma-separated list of optional modules
- `--output-name=NAME` - Custom output name (default: arcadedb-{version}-custom-{timestamp})
- `--output-dir=PATH` - Output directory (default: current directory)
- `--docker-tag=TAG` - Docker image tag (default: arcadedb-custom:{version})
- `--skip-docker` - Skip Docker image generation
- `--dockerfile-only` - Generate Dockerfile without building image
- `--keep-temp` - Don't delete temporary working directory
- `--dry-run` - Show what would be downloaded without doing it
- `-v, --verbose` - Verbose output
- `-q, --quiet` - Quiet mode (errors only)
- `-h, --help` - Show help message

## Output Files

The builder creates:
- `{output-name}.zip` - Zip archive
- `{output-name}.tar.gz` - Compressed tarball
- Docker image with tag `{docker-tag}` (if not skipped)

## Directory Structure

```
arcadedb-{version}-custom-{timestamp}/
├── bin/              # Server and console scripts
├── config/           # Configuration files
├── lib/              # JARs (core + selected modules)
├── databases/        # Database storage (empty)
├── backups/          # Backup storage (empty)
├── log/              # Log files (empty)
├── replication/      # Replication data (empty)
├── README.md         # ArcadeDB README
└── LICENSE           # Apache 2.0 License
```

## How It Works

1. **Download Base**: Fetches base distribution from GitHub releases
2. **Verify Checksums**: Validates SHA-256 checksum for base
3. **Add Modules**: Downloads selected modules from Maven Central
4. **Verify Modules**: Validates SHA-1 checksums for each module
5. **Create Archives**: Generates zip and tar.gz files
6. **Build Docker**: Optionally creates Docker image

## Troubleshooting

### Error: Base distribution not found

The base distribution for the specified version doesn't exist on GitHub releases. Check that:
- Version number is correct
- Version has been released
- Base distribution was included in the release

### Error: Module not found on Maven Central

The specified module doesn't exist for that version. This can happen with:
- Older versions before a module was introduced
- Typos in module names
- Unreleased or snapshot versions

### Error: Docker daemon not running

Docker is installed but not running. Start Docker Desktop or the Docker daemon.

### Error: Checksum verification failed

Downloaded file is corrupted or doesn't match expected checksum. Try:
- Running the script again (download may have been interrupted)
- Checking network connection
- Verifying the version exists

## Contributing

Report issues or suggest improvements at:
https://github.com/arcadedata/arcadedb/issues

## License

Copyright © 2021-present Arcade Data Ltd

Licensed under the Apache License, Version 2.0
```

**Step 2: Commit**

```bash
git add package/README-BUILDER.md
git commit -m "docs: add comprehensive README for builder script

Add detailed documentation covering usage, examples, options,
troubleshooting, and how the builder works.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 17: Add Script to Release Preparation

**Files:**
- Create: `package/prepare-release.sh`

**Step 1: Create release preparation helper script**

Create `package/prepare-release.sh`:

```bash
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
```

**Step 2: Make executable**

Run: `chmod +x package/prepare-release.sh`

**Step 3: Test preparation script**

Run: `cd package && ./prepare-release.sh 26.1.0`
Expected: Should create release directory with builder files

**Step 4: Commit**

```bash
git add package/prepare-release.sh
git commit -m "build: add release preparation script for builder

Add helper script to prepare builder artifacts for GitHub
releases, including builder script and README.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 18: Test Complete Workflow (Integration Test)

**Files:**
- None (testing only)

**Step 1: Build the base distribution**

Run: `cd package && mvn clean package -DskipTests`
Expected: Should create `arcadedb-26.1.1-SNAPSHOT-base.tar.gz` with SHA-256 checksum

**Step 2: Verify base distribution contents**

Run:
```bash
cd package/target
tar -tzf arcadedb-26.1.1-SNAPSHOT-base.tar.gz | grep "lib/arcadedb" | head -10
```

Expected: Should show engine, server, network JARs but not optional modules

**Step 3: Test builder with current build (manual simulation)**

Since we can't upload to GitHub releases yet, we can test locally by:

1. Copy base distribution to a test location
2. Modify builder script temporarily to use local file
3. Run builder script

This will be done in the next task with proper local testing setup.

**Step 4: Document testing results**

Create notes on what was tested and what works.

---

## Task 19: Create Local Testing Script

**Files:**
- Create: `package/test-builder-local.sh`

**Step 1: Create local testing script**

Create `package/test-builder-local.sh`:

```bash
#!/bin/bash
set -euo pipefail

# Local testing script for arcadedb-builder.sh
# Simulates GitHub releases by serving files locally

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Local Builder Testing"
echo "===================="
echo ""

# Check if base distribution exists
BASE_DIST="${SCRIPT_DIR}/target/arcadedb-26.1.1-SNAPSHOT-base.tar.gz"
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
    --version=26.1.1-SNAPSHOT \
    --dry-run \
    --output-dir=/tmp

echo ""
echo "Test 1: PASSED"
echo ""

# Test 2: Dry run with modules
echo "Test 2: Dry run - with modules"
./arcadedb-builder.sh \
    --version=26.1.1-SNAPSHOT \
    --modules=console,studio \
    --dry-run \
    --skip-docker

echo ""
echo "Test 2: PASSED"
echo ""

# Test 3: Help message
echo "Test 3: Help message"
./arcadedb-builder.sh --help | head -5

echo ""
echo "Test 3: PASSED"
echo ""

# Test 4: Invalid version
echo "Test 4: Invalid version (should fail)"
if ./arcadedb-builder.sh --version=invalid 2>/dev/null; then
    echo "Test 4: FAILED (should have rejected invalid version)"
    exit 1
else
    echo "Test 4: PASSED"
fi

echo ""
echo "All tests passed!"
```

**Step 2: Make executable and run**

Run: `chmod +x package/test-builder-local.sh`
Run: `cd package && ./test-builder-local.sh`

Expected: All tests should pass

**Step 3: Commit**

```bash
git add package/test-builder-local.sh
git commit -m "test: add local testing script for builder

Add script to test builder functionality locally without
requiring GitHub releases or Maven Central.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 20: Update CLAUDE.md with Builder Information

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Add modular builder section to CLAUDE.md**

Add after the "Server Operations" section (around line 25):

```markdown
### Modular Distribution Builder
- **Build custom distribution**: `package/arcadedb-builder.sh --version=X.Y.Z --modules=mod1,mod2`
- **Interactive mode**: `package/arcadedb-builder.sh`
- **See options**: `package/arcadedb-builder.sh --help`
- **Local testing**: `package/test-builder-local.sh`
```

**Step 2: Add builder notes to development guidelines**

Add to the end of the "Important Notes" section:

```markdown
- **Modular Builder**: Script to create custom distributions with selected modules (see `package/README-BUILDER.md`)
```

**Step 3: Verify changes**

Run: `git diff CLAUDE.md`
Expected: Should show new sections added

**Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add modular builder information to CLAUDE.md

Document the modular distribution builder commands and
usage in the project documentation.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 21: Final Integration and Documentation

**Files:**
- Create: `docs/modular-builder-guide.md`

**Step 1: Create user guide**

Create `docs/modular-builder-guide.md`:

```markdown
# Modular Distribution Builder Guide

## For End Users

### What is the Modular Builder?

The ArcadeDB Modular Distribution Builder allows you to create custom ArcadeDB packages containing only the features you need. This results in smaller distributions, reduced dependencies, and simplified deployments.

### Getting Started

1. Download `arcadedb-builder.sh` from the [GitHub releases page](https://github.com/arcadedata/arcadedb/releases)
2. Make it executable: `chmod +x arcadedb-builder.sh`
3. Run it: `./arcadedb-builder.sh`

### Quick Examples

**Interactive Mode:**
```bash
./arcadedb-builder.sh
# Follow the prompts to select version and modules
```

**PostgreSQL-only Build:**
```bash
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw
```

**Full Custom Build:**
```bash
./arcadedb-builder.sh \
  --version=26.1.0 \
  --modules=console,gremlin,studio,postgresw \
  --output-name=my-arcadedb
```

### Module Selection Guide

Choose modules based on your needs:

- **console**: If you need the interactive command-line tool
- **gremlin**: If you're using Gremlin graph queries
- **studio**: If you want the web-based admin UI
- **postgresw**: If you're connecting via PostgreSQL protocol
- **mongodbw**: If you're connecting via MongoDB protocol
- **redisw**: If you're connecting via Redis protocol
- **grpcw**: If you're using gRPC
- **graphql**: If you're using GraphQL queries
- **metrics**: If you need Prometheus metrics

### Distribution Comparison

| Distribution | Size | Modules Included |
|--------------|------|------------------|
| Full | ~150MB | All modules |
| Minimal | ~100MB | No gremlin, redisw, mongodbw, graphql |
| Headless | ~90MB | Minimal without studio |
| Custom | Varies | Your selection |

## For Developers

### Building from Source

After building ArcadeDB:

```bash
cd package
mvn clean package -DskipTests
./arcadedb-builder.sh --version=26.1.1-SNAPSHOT --modules=gremlin
```

### Testing the Builder

```bash
cd package
./test-builder-local.sh
```

### Preparing a Release

```bash
cd package
./prepare-release.sh 26.1.0
```

This creates release artifacts in `target/release-26.1.0/`:
- `arcadedb-builder.sh`
- `README-BUILDER.md`

Upload these along with the base distribution to GitHub releases.

### Architecture

The builder works in phases:

1. **Download Base**: Gets core modules from GitHub releases
2. **Add Modules**: Downloads optional modules from Maven Central
3. **Verify**: Checks SHA-256 and SHA-1 checksums
4. **Package**: Creates zip and tar.gz archives
5. **Docker**: Optionally builds Docker image

### Adding New Optional Modules

To add a new optional module:

1. Update `package/pom.xml` dependencies
2. Update base.xml to exclude the new module
3. Update `arcadedb-builder.sh`:
   - Add to `SHADED_MODULES` or `REGULAR_MODULES`
   - Add description to `MODULE_DESCRIPTIONS`
4. Test with local builder
5. Update documentation

## Troubleshooting

See `package/README-BUILDER.md` for detailed troubleshooting guide.
```

**Step 2: Commit**

```bash
git add docs/modular-builder-guide.md
git commit -m "docs: add modular builder user guide

Add comprehensive guide for both end users and developers
covering usage, examples, architecture, and customization.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Summary

This implementation plan creates:

1. **Base assembly descriptor** (`base.xml`) - Maven assembly for core modules
2. **Build configuration** - Updated `pom.xml` with checksum generation
3. **Builder script** (`arcadedb-builder.sh`) - Full-featured bash script with:
   - Interactive and CLI modes
   - Base distribution download from GitHub
   - Optional module download from Maven Central
   - Checksum verification (SHA-256 and SHA-1)
   - Archive creation (zip, tar.gz)
   - Docker image generation
   - Comprehensive error handling
4. **Documentation** - README, user guide, and CLAUDE.md updates
5. **Testing** - Local testing script
6. **Release preparation** - Helper script for releases

The builder enables users to create custom ArcadeDB distributions with only the modules they need, reducing size and complexity while maintaining flexibility.
