# Modular Distribution Builder Design

**Date:** 2026-01-19
**Status:** Design Phase
**Authors:** Design Session

## Overview

The modular distribution builder enables users to create custom ArcadeDB packages containing only the modules they need. This reduces distribution size, minimizes attack surface, and simplifies deployments where only specific protocols are required.

### Motivation

ArcadeDB currently ships three fixed distributions:
- **Full**: All modules (all protocols + studio)
- **Minimal**: Excludes gremlin, redisw, mongodbw, graphql (keeps postgresw, grpcw + studio)
- **Headless**: Like minimal but excludes studio

Many users need custom combinations (e.g., only PostgreSQL protocol, or only Gremlin + Studio). A modular builder allows users to create exactly the distribution they need.

### Key Capabilities

- Interactive module selection or CLI-driven automation
- Downloads artifacts from Maven Central for any released version
- Generates zip, tar.gz, and optionally Docker images
- Verifies artifact integrity via SHA-256 checksums
- Works standalone without requiring the ArcadeDB source repository

### Target Users

- DevOps teams creating minimal production images
- CI/CD pipelines building custom distributions
- Users deploying ArcadeDB in constrained environments
- Organizations with specific compliance/security requirements

## Architecture

### Base Distribution Approach

To avoid complex dependency resolution in bash, the build system creates a **base distribution** containing:
- Core modules: engine, server, network
- All transitive dependencies for core modules
- Scripts (bin/), configs (config/), directory structure
- README.md, LICENSE

The modular builder script then:
1. Downloads the base distribution from GitHub releases
2. Adds user-selected optional modules from Maven Central
3. Creates final archives and Docker images

### Module Categories

**Mandatory (included in base):**
- `arcadedb-engine` - Core database engine
- `arcadedb-server` - HTTP/REST API, clustering, HA
- `arcadedb-network` - Network communication layer

**Optional (user selects):**
- `arcadedb-console` - Interactive database console
- `arcadedb-gremlin` - Apache Tinkerpop Gremlin support
- `arcadedb-studio` - Web-based administration interface
- `arcadedb-redisw` - Redis wire protocol compatibility
- `arcadedb-mongodbw` - MongoDB wire protocol compatibility
- `arcadedb-postgresw` - PostgreSQL wire protocol compatibility
- `arcadedb-grpcw` - gRPC wire protocol support
- `arcadedb-graphql` - GraphQL API support
- `arcadedb-metrics` - Prometheus metrics integration

### Execution Phases

The script executes in six sequential phases:

#### 1. Input & Validation Phase
- Parse CLI arguments or launch interactive mode
- Validate version format (e.g., 26.1.0, 24.11.1)
- Collect module selections
- Determine output name (user-specified or `arcadedb-{version}-custom-{timestamp}`)
- Verify prerequisites (curl/wget, tar/unzip, sha256sum/shasum, Docker if needed)

#### 2. Base Distribution Download Phase
- Download `arcadedb-{version}-base.tar.gz` from GitHub releases
- Download corresponding `.sha256` checksum file
- Verify checksum
- Extract to temporary working directory
- Result: core JARs + dependencies + scripts + configs + directory structure

#### 3. Optional Modules Download Phase
- For each selected optional module, download from Maven Central:
  - Shaded JARs: gremlin, redisw, mongodbw, postgresw, grpcw, metrics
  - Regular JARs: console, studio, graphql
- Download and verify SHA-1 checksums from Maven Central
- Add JARs to the base distribution's `lib/` directory

#### 4. Final Assembly Phase
- Base distribution already has correct structure
- Optional module JARs added to `lib/`
- Directory structure complete

#### 5. Archive Creation Phase
- Create `arcadedb-{version}-custom-{timestamp}.zip`
- Create `arcadedb-{version}-custom-{timestamp}.tar.gz`
- Place in output directory
- Clean up temp files (unless `--keep-temp`)

#### 6. Docker Image Generation Phase (optional)
- If not `--skip-docker`:
  - Check Docker availability
  - Generate Dockerfile
  - Build image with tag `arcadedb-custom:{version}` or user-specified
- If `--dockerfile-only`: generate Dockerfile but skip build

## Distribution Structure

```
arcadedb-{version}-custom-{timestamp}/
├── bin/
│   ├── server.sh
│   ├── server.bat
│   ├── console.sh
│   └── console.bat
├── config/
│   ├── server-config.json
│   ├── server-config.yaml
│   ├── server-plugins.groovy
│   └── server.properties
├── lib/
│   ├── arcadedb-engine-{version}.jar
│   ├── arcadedb-server-{version}.jar
│   ├── arcadedb-network-{version}.jar
│   ├── [core module dependencies]
│   └── [selected optional module JARs]
├── databases/ (empty)
├── backups/ (empty)
├── log/ (empty)
├── replication/ (empty)
├── README.md
└── LICENSE
```

## CLI Interface

### Script Name
`arcadedb-builder.sh`

### Basic Usage

```bash
# Interactive mode (default)
./arcadedb-builder.sh

# Automated mode with flags
./arcadedb-builder.sh --version=26.1.0 --modules=gremlin,postgresw,studio
```

### Flags

**Required (for non-interactive mode):**
- `--version=X.Y.Z` - ArcadeDB version to build

**Optional:**
- `--modules=mod1,mod2,...` - Comma-separated list of optional modules
- `--output-name=NAME` - Custom output name (default: `arcadedb-{version}-custom-{timestamp}`)
- `--output-dir=PATH` - Output directory (default: current directory)
- `--docker-tag=TAG` - Docker image tag (default: `arcadedb-custom:{version}`)
- `--skip-docker` - Skip Docker image generation
- `--dockerfile-only` - Generate Dockerfile without building image
- `--keep-temp` - Don't delete temporary working directory
- `--dry-run` - Show what would be downloaded/built without doing it
- `-v, --verbose` - Verbose output
- `-q, --quiet` - Quiet mode (errors only)
- `-h, --help` - Show help message

### Examples

```bash
# Interactive
./arcadedb-builder.sh

# Minimal build with just PostgreSQL wire protocol
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw

# Full custom build
./arcadedb-builder.sh --version=26.1.0 \
  --modules=console,gremlin,studio,postgresw,mongodbw \
  --output-name=arcadedb-full

# CI/CD mode
./arcadedb-builder.sh --version=26.1.0 \
  --modules=gremlin,studio \
  --quiet \
  --output-dir=/tmp/builds
```

## Implementation Details

### Maven Central URL Construction

Maven Central artifact URLs:
```
https://repo1.maven.org/maven2/com/arcadedb/{artifactId}/{version}/{artifactId}-{version}[-classifier].jar
```

For ArcadeDB modules:
- Regular JAR: `arcadedb-{module}-{version}.jar`
- Shaded JAR: `arcadedb-{module}-{version}-shaded.jar`
- Checksum: append `.sha1` to JAR URL

### GitHub Release URL Construction

```
https://github.com/arcadedata/arcadedb/releases/download/{version}/arcadedb-{version}-base.tar.gz
https://github.com/arcadedata/arcadedb/releases/download/{version}/arcadedb-{version}-base.tar.gz.sha256
```

### Module Metadata

Embedded in script:

```bash
# Modules with shaded JARs
SHADED_MODULES="gremlin redisw mongodbw postgresw grpcw metrics"

# Modules with regular JARs
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
```

### Checksum Verification

- Base distribution: SHA-256 (from GitHub releases)
- Maven Central artifacts: SHA-1 (from Maven Central .sha1 files)
- Use `sha256sum` (Linux) or `shasum -a 256` (macOS)
- Use `sha1sum` (Linux) or `shasum -a 1` (macOS)
- Fail immediately on mismatch

## Error Handling

### Fail-Fast Strategy

The script stops immediately on errors with clear messages:

**Download failures:**
- Base distribution not found: `Error: Base distribution for version {version} not found on GitHub releases`
- Maven module not found: `Error: Module {module} version {version} not found on Maven Central`
- Network errors: `Error: Failed to download {artifact}. Check network connection`

**Validation failures:**
- Checksum mismatch: `Error: Checksum verification failed for {artifact}. Download may be corrupted`
- Invalid version: `Error: Invalid version '{version}'. Expected format: X.Y.Z`
- Docker not installed: `Error: Docker not found. Install Docker or use --skip-docker`

**Prerequisites check:**
- Verify required commands exist: `curl`/`wget`, `unzip`/`tar`, `sha256sum`/`shasum`
- Check disk space before download (warn if < 500MB available)
- Check write permissions on output directory

## User Experience

### Interactive Mode

- Display welcome message with version being built
- Show numbered module list with descriptions
- Confirm selections before downloading
- Progress indicators for downloads
- Summary at end showing output files created

Example:
```
ArcadeDB Modular Distribution Builder
======================================
Version: 26.1.0

Select optional modules (space-separated numbers):
 1. console    - Interactive database console
 2. gremlin    - Apache Tinkerpop Gremlin support
 3. studio     - Web-based administration interface
 4. redisw     - Redis wire protocol compatibility
 5. mongodbw   - MongoDB wire protocol compatibility
 6. postgresw  - PostgreSQL wire protocol compatibility
 7. grpcw      - gRPC wire protocol support
 8. graphql    - GraphQL API support
 9. metrics    - Prometheus metrics integration

Enter modules: 2 3 6

Building custom distribution with:
  - Core: engine, server, network
  - Optional: gremlin, studio, postgresw

Proceed? (y/n):
```

### CLI Mode

- Verbose flag (`-v`) for detailed logging
- Quiet mode (`-q`) for CI/CD (only errors printed)
- Help text (`--help`) with examples
- Dry-run mode (`--dry-run`) to preview without executing

## Build System Integration

### New Assembly Descriptor

Create `package/src/main/assembly/base.xml`:
- Include engine, server, network modules
- Include all transitive dependencies
- Include bin/, config/, and directory structure
- Include README.md, LICENSE
- Generate base.tar.gz and base.zip

### Maven POM Updates

Update `package/pom.xml`:
- Add new execution for base assembly
- Generate SHA-256 checksums for base archives
- Similar structure to existing full/minimal/headless executions

### Release Process

**Artifacts to publish on GitHub releases:**
1. `arcadedb-builder.sh` - The modular builder script
2. `arcadedb-{version}-base.tar.gz` - Base distribution
3. `arcadedb-{version}-base.tar.gz.sha256` - Checksum
4. `arcadedb-{version}-base.zip` - Base distribution (zip format)
5. `arcadedb-{version}-base.zip.sha256` - Checksum

**Release notes should mention:**
- Custom builder availability
- Link to builder documentation
- Examples of common use cases

## Testing Strategy

### Functional Tests

- Interactive mode with various module combinations
- CLI mode with all flags
- Edge cases: no optional modules (base only)
- Edge cases: all optional modules
- Invalid version numbers
- Non-existent modules
- Network failures (simulated)
- Checksum mismatches
- Docker available vs not available

### Platform Testing

- Linux (Ubuntu, CentOS/RHEL)
- macOS (Intel and ARM)
- Windows WSL

### Version Testing

- Build distributions for multiple ArcadeDB versions
- Verify modules that didn't exist in older versions fail correctly

## Security Considerations

- All artifacts verified with checksums (SHA-256 for base, SHA-1 for Maven Central)
- Downloads only from trusted sources (GitHub releases, Maven Central)
- No code execution from downloaded content (only JARs added to distribution)
- Clear error messages on verification failures
- Optional dry-run mode to inspect what would be downloaded

## Future Enhancements

Potential future improvements (not in initial scope):

- GPG signature verification
- Caching downloaded artifacts locally
- Support for custom Maven repositories
- Configuration file support (YAML/JSON) for repeatable builds
- Integration with package managers (Homebrew, apt, yum)
- Web-based configurator with download links

## Implementation Checklist

- [ ] Create `package/src/main/assembly/base.xml` assembly descriptor
- [ ] Update `package/pom.xml` to build base distribution
- [ ] Add SHA-256 checksum generation to Maven build
- [ ] Write `arcadedb-builder.sh` script
  - [ ] CLI argument parsing
  - [ ] Interactive mode
  - [ ] Prerequisites validation
  - [ ] Base distribution download & verification
  - [ ] Maven Central module download & verification
  - [ ] Archive creation (zip, tar.gz)
  - [ ] Docker image generation
  - [ ] Error handling
- [ ] Test on Linux, macOS, Windows WSL
- [ ] Test with multiple ArcadeDB versions
- [ ] Update release process documentation
- [ ] Write user documentation for the builder
- [ ] Add builder to CI/CD pipeline for testing
