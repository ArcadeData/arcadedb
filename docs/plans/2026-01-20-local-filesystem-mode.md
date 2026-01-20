# Local Filesystem Mode Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add local filesystem mode to arcadedb-builder.sh to load optional modules from local Maven repository or custom directory instead of downloading from Maven Central, enabling offline testing and faster development iteration.

**Architecture:** Add `--local-repo=PATH` flag that switches module acquisition from HTTP downloads to local filesystem copies. When enabled, locate JARs in Maven local repository structure (`~/.m2/repository/com/arcadedb/{artifact}/{version}/`) or custom directory, verify they exist, and copy them to the distribution lib directory. Maintain same checksum verification flow using local .sha1 files if available.

**Tech Stack:** Bash 3.2+, Maven local repository structure

---

## Task 1: Add Local Repository Flag and Variable

**Files:**
- Modify: `package/arcadedb-builder.sh:52-63` (default values section)
- Modify: `package/arcadedb-builder.sh:78-91` (help text)
- Modify: `package/arcadedb-builder.sh:126-175` (argument parsing)

**Step 1: Add LOCAL_REPO variable to defaults section**

In the "Default values" section (after line 63), add:

```bash
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
LOCAL_REPO=""  # New: path to local Maven repository or custom JAR directory
```

**Step 2: Update help text to document new flag**

Add to OPTIONS section in show_help() (after line 83):

```bash
OPTIONS:
    --version=VERSION       ArcadeDB version to build (required for non-interactive mode)
    --modules=MODULES       Comma-separated list of modules. If not provided, will be asked interactively.
                           Options: console,gremlin,studio,redisw,mongodbw,postgresw,grpcw,graphql,metrics
    --local-repo=PATH      Use local Maven repository or directory instead of downloading from Maven Central.
                           If PATH is not provided, defaults to ~/.m2/repository
    --output-name=NAME      Custom name for distribution (default: arcadedb-<version>-<modules>)
    --output-dir=DIR        Output directory (default: current directory)
```

Add to EXAMPLES section (after line 115):

```bash
    # Build using local Maven repository (offline mode)
    ${SCRIPT_NAME} --version=26.1.1-SNAPSHOT --modules=gremlin,studio --local-repo

    # Build using custom JAR directory
    ${SCRIPT_NAME} --version=26.1.1-SNAPSHOT --modules=console --local-repo=/path/to/jars
```

**Step 3: Add argument parsing for --local-repo flag**

In parse_arguments(), add case statement (after line 142):

```bash
            --docker-tag=*)
                DOCKER_TAG="${1#*=}"
                shift
                ;;
            --local-repo=*)
                LOCAL_REPO="${1#*=}"
                shift
                ;;
            --local-repo)
                # No value provided, use default Maven local repo
                LOCAL_REPO="${HOME}/.m2/repository"
                shift
                ;;
            --skip-docker)
                SKIP_DOCKER=true
```

**Step 4: Run shellcheck to verify syntax**

Run: `shellcheck package/arcadedb-builder.sh`
Expected: No new errors

**Step 5: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add --local-repo flag for local filesystem mode

- Add LOCAL_REPO global variable
- Support both --local-repo and --local-repo=PATH syntax
- Default to ~/.m2/repository when no path provided
- Update help text with examples"
```

---

## Task 2: Add Local Repository Path Validation

**Files:**
- Modify: `package/arcadedb-builder.sh:820-850` (main function validation section)

**Step 1: Add validation for local repository path**

In main() function, after the conflicting flags validation (around line 827), add:

```bash
    # Validate flag combinations
    if [[ "$SKIP_DOCKER" == true ]] && [[ "$DOCKERFILE_ONLY" == true ]]; then
        error_exit "Cannot use --skip-docker and --dockerfile-only together"
    fi

    # Validate local repository path if provided
    if [[ -n "$LOCAL_REPO" ]]; then
        if [[ ! -d "$LOCAL_REPO" ]]; then
            error_exit "Local repository directory does not exist: $LOCAL_REPO"
        fi

        # If it looks like a Maven repo, verify structure
        if [[ -d "$LOCAL_REPO/com/arcadedb" ]]; then
            log_info "Using Maven repository: $LOCAL_REPO"
        else
            log_info "Using custom JAR directory: $LOCAL_REPO"
            log_warning "Custom directories should contain JARs with naming: arcadedb-{module}-{version}[-shaded].jar"
        fi
    fi

    # Check prerequisites
    check_prerequisites
```

**Step 2: Test with non-existent path**

Run: `./arcadedb-builder.sh --version=26.1.0 --modules=console --local-repo=/nonexistent --dry-run`
Expected: Error message "Local repository directory does not exist: /nonexistent"

**Step 3: Test with valid Maven repo path**

Run: `./arcadedb-builder.sh --version=26.1.0 --modules=console --local-repo=$HOME/.m2/repository --dry-run`
Expected: Log message "Using Maven repository: ..." (should not error)

**Step 4: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add validation for local repository path

- Verify local repo directory exists before proceeding
- Detect Maven repository vs custom directory structure
- Provide helpful warning for custom directories"
```

---

## Task 3: Create Helper Function to Locate Local JARs

**Files:**
- Modify: `package/arcadedb-builder.sh:610-620` (before download_optional_modules function)

**Step 1: Add find_local_jar() helper function**

Before the `download_optional_modules()` function, add:

```bash
# Find JAR file in local repository
# Args: module name, version, classifier (optional)
# Returns: absolute path to JAR file, or empty string if not found
find_local_jar() {
    local module="$1"
    local version="$2"
    local classifier="$3"

    local artifact_id="arcadedb-${module}"
    local jar_filename="${artifact_id}-${version}${classifier}.jar"

    # Try Maven repository structure first
    if [[ -d "$LOCAL_REPO/com/arcadedb" ]]; then
        local maven_path="$LOCAL_REPO/com/arcadedb/${artifact_id}/${version}/${jar_filename}"
        if [[ -f "$maven_path" ]]; then
            echo "$maven_path"
            return 0
        fi
    fi

    # Try custom directory (flat structure)
    local custom_path="$LOCAL_REPO/${jar_filename}"
    if [[ -f "$custom_path" ]]; then
        echo "$custom_path"
        return 0
    fi

    # Not found
    echo ""
    return 1
}
```

**Step 2: Write test for Maven repository structure**

Create a temporary test structure:

```bash
mkdir -p /tmp/test-maven-repo/com/arcadedb/arcadedb-console/26.1.0
touch /tmp/test-maven-repo/com/arcadedb/arcadedb-console/26.1.0/arcadedb-console-26.1.0.jar
```

Source the function and test:
```bash
LOCAL_REPO=/tmp/test-maven-repo
jar_path=$(find_local_jar "console" "26.1.0" "")
echo "$jar_path"
```
Expected: `/tmp/test-maven-repo/com/arcadedb/arcadedb-console/26.1.0/arcadedb-console-26.1.0.jar`

**Step 3: Write test for custom directory structure**

Create a temporary flat structure:

```bash
mkdir -p /tmp/test-custom-repo
touch /tmp/test-custom-repo/arcadedb-gremlin-26.1.0-shaded.jar
```

Source and test:
```bash
LOCAL_REPO=/tmp/test-custom-repo
jar_path=$(find_local_jar "gremlin" "26.1.0" "-shaded")
echo "$jar_path"
```
Expected: `/tmp/test-custom-repo/arcadedb-gremlin-26.1.0-shaded.jar`

**Step 4: Clean up test files**

```bash
rm -rf /tmp/test-maven-repo /tmp/test-custom-repo
```

**Step 5: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: add find_local_jar helper function

- Support Maven repository structure (com/arcadedb/{artifact}/{version}/)
- Support custom flat directory structure
- Return absolute path or empty string if not found"
```

---

## Task 4: Modify download_optional_modules to Support Local Mode

**Files:**
- Modify: `package/arcadedb-builder.sh:620-669` (download_optional_modules function)

**Step 1: Add local repository code path**

Replace the download logic in `download_optional_modules()` with a conditional that checks `LOCAL_REPO`:

```bash
download_optional_modules() {
    if [[ -z "$SELECTED_MODULES" ]]; then
        log_info "No optional modules selected, skipping module download"
        return 0
    fi

    if [[ -n "$LOCAL_REPO" ]]; then
        log_info "Using local modules from: $LOCAL_REPO"
    else
        log_info "Downloading optional modules: $SELECTED_MODULES..."
    fi

    local extracted_dir="$TEMP_DIR/arcadedb-${ARCADEDB_VERSION}-base"
    local lib_dir="${extracted_dir}/lib"

    # Split modules by comma
    IFS=',' read -ra modules <<< "$SELECTED_MODULES"

    for module in "${modules[@]}"; do
        module=$(echo "$module" | xargs) # trim whitespace

        # Determine if shaded or regular JAR
        local classifier=""
        if [[ " $SHADED_MODULES " =~ " $module " ]]; then
            classifier="-shaded"
        fi

        local artifact_id="arcadedb-${module}"
        local jar_filename="${artifact_id}-${ARCADEDB_VERSION}${classifier}.jar"
        local jar_file="${lib_dir}/${jar_filename}"

        if [[ -n "$LOCAL_REPO" ]]; then
            # Local repository mode
            copy_local_module "$module" "$ARCADEDB_VERSION" "$classifier" "$jar_file"
        else
            # Download mode
            download_remote_module "$module" "$ARCADEDB_VERSION" "$classifier" "$jar_file"
        fi

        log_success "Module added: $module"
    done

    log_success "All optional modules processed"
}
```

**Step 2: Extract remote download logic into helper function**

Before `download_optional_modules()`, add:

```bash
# Download module from Maven Central
download_remote_module() {
    local module="$1"
    local version="$2"
    local classifier="$3"
    local dest_jar="$4"

    log_info "Downloading module: $module"

    local artifact_id="arcadedb-${module}"
    local jar_filename="${artifact_id}-${version}${classifier}.jar"
    local jar_url="${MAVEN_CENTRAL_BASE}/${artifact_id}/${version}/${jar_filename}"
    local checksum_url="${jar_url}.sha1"

    local checksum_file="${dest_jar}.sha1"

    # Download JAR
    download_file "$jar_url" "$dest_jar"

    # Download checksum
    download_file "$checksum_url" "$checksum_file"

    # Verify checksum
    verify_sha1 "$dest_jar" "$checksum_file"

    # Clean up checksum file
    if [[ "$DRY_RUN" != true ]]; then
        rm -f "$checksum_file"
    fi
}
```

**Step 3: Add copy_local_module helper function**

Before `download_optional_modules()`, add:

```bash
# Copy module from local repository
copy_local_module() {
    local module="$1"
    local version="$2"
    local classifier="$3"
    local dest_jar="$4"

    log_info "Locating local module: $module"

    local source_jar
    source_jar=$(find_local_jar "$module" "$version" "$classifier")

    if [[ -z "$source_jar" ]]; then
        error_exit "Module not found in local repository: $module (looking for arcadedb-${module}-${version}${classifier}.jar)"
    fi

    log_info "Found: $source_jar"

    # Copy JAR file
    if [[ "$DRY_RUN" != true ]]; then
        if ! cp "$source_jar" "$dest_jar"; then
            error_exit "Failed to copy module: $source_jar -> $dest_jar"
        fi
    else
        log_info "[DRY RUN] Would copy: $source_jar -> $dest_jar"
    fi

    # Check for checksum file and verify if it exists
    local source_checksum="${source_jar}.sha1"
    if [[ -f "$source_checksum" ]]; then
        log_info "Verifying checksum: ${source_checksum}"
        local temp_checksum="${dest_jar}.sha1"

        if [[ "$DRY_RUN" != true ]]; then
            cp "$source_checksum" "$temp_checksum"
            verify_sha1 "$dest_jar" "$temp_checksum"
            rm -f "$temp_checksum"
        else
            log_info "[DRY RUN] Would verify checksum"
        fi
    else
        log_warning "No checksum file found, skipping verification: ${source_checksum}"
    fi
}
```

**Step 4: Test dry-run with local repo**

Run: `./arcadedb-builder.sh --version=26.1.1-SNAPSHOT --modules=console --local-repo=$HOME/.m2/repository --dry-run --output-dir=/tmp`
Expected: Should show "[DRY RUN] Would copy: ..." messages

**Step 5: Commit**

```bash
git add package/arcadedb-builder.sh
git commit -m "feat: implement local filesystem mode for modules

- Add copy_local_module to copy from local repository
- Extract download_remote_module from download_optional_modules
- Support optional checksum verification for local files
- Maintain same error handling and logging patterns"
```

---

## Task 5: Update test-builder-local.sh with Local Repository Tests

**Files:**
- Modify: `package/test-builder-local.sh`

**Step 1: Add local repository test after Test 2**

After the "Test 2: PASSED" section (around line 47), add:

```bash
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
```

**Step 2: Renumber subsequent tests**

Update test numbers:
- Old Test 3 → Test 4
- Old Test 4 → Test 5

**Step 3: Run the updated test script**

Run: `cd package && ./test-builder-local.sh`
Expected: All 5 tests should pass

**Step 4: Commit**

```bash
git add package/test-builder-local.sh
git commit -m "test: add local repository mode test to test script

- Add Test 3 for local repository mode with dry-run
- Renumber subsequent tests (3->4, 4->5)
- Verify local repo flag works with default Maven path"
```

---

## Task 6: Update README-BUILDER.md Documentation

**Files:**
- Modify: `package/README-BUILDER.md`

**Step 1: Add Local Development section**

After the "Command-Line Options" section (around line 50), add:

```markdown
## Local Development Mode

For offline development and testing, use `--local-repo` to load modules from your local Maven repository instead of downloading from Maven Central:

### Using Default Maven Repository

```bash
./arcadedb-builder.sh \
    --version=26.1.1-SNAPSHOT \
    --modules=gremlin,studio \
    --local-repo
```

The script will automatically use `~/.m2/repository` and look for JARs in Maven structure:
```
~/.m2/repository/com/arcadedb/arcadedb-{module}/{version}/arcadedb-{module}-{version}[-shaded].jar
```

### Using Custom JAR Directory

```bash
./arcadedb-builder.sh \
    --version=26.1.1-SNAPSHOT \
    --modules=console \
    --local-repo=/path/to/custom/jars
```

Custom directories should contain JARs with naming: `arcadedb-{module}-{version}[-shaded].jar`

### Building Local Modules First

Before using local repository mode, build the modules you need:

```bash
# Build all modules
cd /path/to/arcadedb
mvn clean install -DskipTests

# Then build custom distribution
cd package
./arcadedb-builder.sh \
    --version=$(mvn -f ../pom.xml help:evaluate -Dexpression=project.version -q -DforceStdout) \
    --modules=gremlin,studio \
    --local-repo
```

**Benefits:**
- ✅ Offline builds (no internet required)
- ✅ Faster iteration (no download time)
- ✅ Test local changes before publishing
- ✅ Reproducible builds with fixed dependencies
```

**Step 2: Update command-line options table**

Find the options table and add:

```markdown
| Option | Description |
|--------|-------------|
| `--version=VERSION` | ArcadeDB version to build (required for non-interactive) |
| `--modules=MODULES` | Comma-separated list of optional modules |
| `--local-repo[=PATH]` | Use local Maven repository (default: `~/.m2/repository`) or custom directory |
| `--output-name=NAME` | Custom name for distribution |
```

**Step 3: Review the changes**

Run: `cat package/README-BUILDER.md | grep -A 10 "Local Development Mode"`
Expected: Should show the new section

**Step 4: Commit**

```bash
git add package/README-BUILDER.md
git commit -m "docs: add local repository mode documentation

- Add Local Development Mode section
- Document default and custom repository paths
- Provide workflow for building and testing local modules
- Update command-line options table"
```

---

## Task 7: Update modular-builder-guide.md

**Files:**
- Modify: `docs/modular-builder-guide.md`

**Step 1: Add Development Workflow section**

After the "Testing" section (around line 85), add:

```markdown
## Development Workflow

### Local Testing Without Publishing

When developing new modules or testing changes, use local repository mode to avoid publishing to Maven Central:

#### Step 1: Build Modules Locally

```bash
cd /path/to/arcadedb
mvn clean install -DskipTests
```

This installs JARs to your local Maven repository (`~/.m2/repository`).

#### Step 2: Build Custom Distribution

```bash
cd package

# Get current version dynamically
VERSION=$(mvn -f ../pom.xml help:evaluate -Dexpression=project.version -q -DforceStdout)

# Build with local modules
./arcadedb-builder.sh \
    --version=$VERSION \
    --modules=gremlin,studio,postgresw \
    --local-repo \
    --skip-docker
```

#### Step 3: Test the Distribution

```bash
cd arcadedb-$VERSION-gremlin-studio-postgresw/bin
./server.sh
```

### Custom JAR Directory

For advanced scenarios, you can use a custom directory with hand-picked JAR versions:

```bash
# Prepare custom directory
mkdir -p /tmp/custom-arcade-jars
cp ~/.m2/repository/com/arcadedb/arcadedb-gremlin/26.1.0/arcadedb-gremlin-26.1.0-shaded.jar /tmp/custom-arcade-jars/
cp ~/.m2/repository/com/arcadedb/arcadedb-console/26.1.0/arcadedb-console-26.1.0.jar /tmp/custom-arcade-jars/

# Build with custom JARs
./arcadedb-builder.sh \
    --version=26.1.0 \
    --modules=gremlin,console \
    --local-repo=/tmp/custom-arcade-jars
```

### Checksum Verification

The builder automatically verifies checksums when available:

- **Maven repository**: If `.sha1` files exist alongside JARs, they are verified
- **Custom directory**: If `.sha1` files exist, they are verified; otherwise skipped with warning

Generate checksums for custom JARs:

```bash
cd /tmp/custom-arcade-jars
shasum -a 1 arcadedb-gremlin-26.1.0-shaded.jar > arcadedb-gremlin-26.1.0-shaded.jar.sha1
```
```

**Step 2: Update Architecture section with local mode details**

Find the "How It Works" section and add a paragraph:

```markdown
### How It Works

1. Downloads minimal base distribution from GitHub releases (or uses local file)
2. **Downloads optional modules from Maven Central (or copies from local repository)**
3. Verifies checksums (SHA-256 for base, SHA-1 for modules)
4. Creates custom distribution archives (tar.gz, zip)
5. Optionally generates Docker images
```

**Step 3: Commit**

```bash
git add docs/modular-builder-guide.md
git commit -m "docs: add development workflow for local repository mode

- Document local testing without publishing workflow
- Add custom JAR directory usage examples
- Explain checksum verification behavior
- Update architecture overview"
```

---

## Task 8: Manual Integration Test

**Files:**
- No file changes

**Step 1: Build ArcadeDB modules locally**

```bash
cd /Users/frank/projects/arcade/worktrees/package-cleanup
mvn clean install -DskipTests -pl engine,server,network,console,gremlin,studio -am
```

Expected: All specified modules build successfully and install to `~/.m2/repository`

**Step 2: Get current project version**

```bash
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "Current version: $VERSION"
```

Expected: Displays version (e.g., "26.1.1-SNAPSHOT")

**Step 3: Test local repository mode with dry-run**

```bash
cd package
./arcadedb-builder.sh \
    --version=$VERSION \
    --modules=console,gremlin \
    --local-repo \
    --dry-run \
    --output-dir=/tmp
```

Expected:
- Shows "[DRY RUN] Would copy: ..." for each module
- No download attempts
- No errors

**Step 4: Test actual build with local repository**

```bash
./arcadedb-builder.sh \
    --version=$VERSION \
    --modules=console,gremlin,studio \
    --local-repo \
    --skip-docker \
    --output-dir=/tmp
```

Expected:
- Creates `/tmp/arcadedb-$VERSION-console-gremlin-studio.tar.gz`
- Creates `/tmp/arcadedb-$VERSION-console-gremlin-studio.zip`
- No network access required

**Step 5: Verify distribution contents**

```bash
cd /tmp
tar -tzf arcadedb-$VERSION-console-gremlin-studio.tar.gz | grep -E '\.jar$' | head -20
```

Expected: Should show base JARs plus console, gremlin (shaded), and studio JARs

**Step 6: Test with non-existent module**

```bash
cd /Users/frank/projects/arcade/worktrees/package-cleanup/package
./arcadedb-builder.sh \
    --version=$VERSION \
    --modules=nonexistent \
    --local-repo \
    --dry-run
```

Expected: Error message "Module not found in local repository: nonexistent"

**Step 7: Clean up test artifacts**

```bash
rm -f /tmp/arcadedb-$VERSION-console-gremlin-studio.*
```

Expected: Test files removed

---

## Task 9: Update CLAUDE.md Project Documentation

**Files:**
- Modify: `/Users/frank/projects/arcade/worktrees/package-cleanup/CLAUDE.md`

**Step 1: Add local repository testing to Build Commands section**

Find the "### Server Operations" section and add after it:

```markdown
### Distribution Builder

The modular distribution builder (`package/arcadedb-builder.sh`) creates custom ArcadeDB distributions:

**Production builds** (download from releases):
```bash
cd package
./arcadedb-builder.sh --version=26.1.0 --modules=gremlin,studio
```

**Development builds** (use local Maven repository):
```bash
# Build modules first
mvn clean install -DskipTests

# Create distribution with local modules
cd package
VERSION=$(mvn -f ../pom.xml help:evaluate -Dexpression=project.version -q -DforceStdout)
./arcadedb-builder.sh \
    --version=$VERSION \
    --modules=console,gremlin,studio \
    --local-repo \
    --skip-docker
```

**Testing the builder**:
```bash
cd package
./test-builder-local.sh
```
```

**Step 2: Review changes**

Run: `grep -A 15 "Distribution Builder" CLAUDE.md`
Expected: Shows the new section

**Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add local repository mode to CLAUDE.md

- Document development workflow for builder script
- Show how to build and test with local modules
- Distinguish production vs development builds"
```

---

## Execution Summary

**Total Tasks:** 9
**Estimated Duration:** 60-90 minutes for full implementation and testing

**Key Implementation Points:**
- Maintain backward compatibility (local mode is optional)
- Support both Maven repository structure and custom flat directories
- Preserve checksum verification when .sha1 files are available locally
- Provide clear error messages when local modules not found
- Update all documentation (README, guide, CLAUDE.md)

**Testing Strategy:**
- Unit-level testing with dry-run mode
- Integration testing with actual local Maven repository
- Error case testing (missing modules, invalid paths)
- Documentation testing (verify all examples work)

**Success Criteria:**
- ✅ Can build distributions entirely offline using `--local-repo`
- ✅ Works with default Maven repository (`~/.m2/repository`)
- ✅ Works with custom JAR directories
- ✅ Verifies checksums when available
- ✅ Provides helpful error messages
- ✅ All existing tests continue to pass
- ✅ Documentation updated and accurate
