# Feature Request: Modular Distribution Builder

## Summary

Add a modular distribution builder that allows users to create custom ArcadeDB packages containing only the modules they need, resulting in smaller distributions, reduced dependencies, and simplified deployments.

## Problem Statement

ArcadeDB currently ships three fixed distributions:
- **Full** (~195MB): All modules included
- **Minimal** (~100MB): Excludes gremlin, redisw, mongodbw, graphql
- **Headless** (~90MB): Minimal without studio

Many users need custom combinations (e.g., only PostgreSQL protocol, or only Gremlin + Studio) but cannot create them without building from source and modifying Maven configuration. This creates unnecessary complexity and larger-than-needed deployments.

## Proposed Solution

Create a standalone builder script (`arcadedb-builder.sh`) that:
1. Downloads a minimal base distribution (engine, server, network + dependencies) from GitHub releases
2. Adds user-selected optional modules from Maven Central
3. Generates zip, tar.gz, and optionally Docker images

### Available Optional Modules

- `console` - Interactive database console
- `gremlin` - Apache Tinkerpop Gremlin support
- `studio` - Web-based administration interface
- `redisw` - Redis wire protocol compatibility
- `mongodbw` - MongoDB wire protocol compatibility
- `postgresw` - PostgreSQL wire protocol compatibility
- `grpcw` - gRPC wire protocol support
- `graphql` - GraphQL API support
- `metrics` - Prometheus metrics integration

## User Experience

### Interactive Mode
```bash
./arcadedb-builder.sh
```
Prompts for version and modules.

### CLI Mode
```bash
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw,metrics
```

### Example Use Cases

**PostgreSQL-only deployment:**
```bash
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw
```
Result: ~60MB distribution (vs 195MB full)

**Development environment:**
```bash
./arcadedb-builder.sh --version=26.1.0 --modules=console,gremlin,studio
```

**Production with monitoring:**
```bash
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw,metrics --skip-docker
```

## Benefits

### For Users
- **Smaller distributions**: 50-70% size reduction for minimal builds
- **Reduced attack surface**: Only include needed protocols
- **Simplified deployments**: No unnecessary dependencies
- **Faster downloads**: Smaller packages download faster
- **Custom Docker images**: Build images with only required modules

### For DevOps
- **CI/CD friendly**: Fully automated via CLI flags
- **Reproducible builds**: Version and module selection in scripts
- **Security compliance**: Minimal installations for regulated environments
- **Bandwidth savings**: Smaller distributions for edge deployments

## Technical Implementation

### Architecture

**Base Distribution Approach:**
- Maven builds a `base` distribution containing core modules + transitive dependencies
- Published to GitHub releases as `arcadedb-{version}-base.tar.gz` (with SHA-256 checksum)
- Base is ~52MB (vs 195MB full distribution)

**Builder Script:**
- Bash 3.2+ compatible (works on macOS and Linux)
- Downloads base from GitHub releases
- Downloads optional modules from Maven Central
- Verifies checksums (SHA-256 for base, SHA-1 for modules)
- Creates zip, tar.gz archives
- Optionally generates Docker images

**Security:**
- URL validation (http/https only)
- Path traversal protection
- Checksum verification before extraction
- Tar bomb prevention
- Download timeouts

### Command-Line Options

**Required (non-interactive):**
- `--version=X.Y.Z` - ArcadeDB version to build

**Optional:**
- `--modules=mod1,mod2,...` - Comma-separated modules
- `--output-name=NAME` - Custom output name
- `--output-dir=PATH` - Output directory
- `--docker-tag=TAG` - Docker image tag
- `--skip-docker` - Skip Docker image generation
- `--dockerfile-only` - Generate Dockerfile without building
- `--keep-temp` - Keep temporary files
- `--dry-run` - Show what would be done
- `-v, --verbose` - Verbose output
- `-q, --quiet` - Quiet mode
- `-h, --help` - Help message

## Implementation Deliverables

### Maven Build Changes
1. **New assembly descriptor** (`package/src/main/assembly/base.xml`):
   - Includes engine, server, network modules
   - Includes all transitive dependencies
   - Excludes all 9 optional modules

2. **Updated `package/pom.xml`**:
   - New `base` assembly execution
   - SHA-256 checksum generation for all distributions

### Builder Script
- **File**: `package/arcadedb-builder.sh` (~900 lines)
- **Features**: Interactive mode, CLI mode, dry-run, Docker support
- **Error handling**: Strict mode, validation, clear error messages
- **Platform support**: macOS, Linux, Windows WSL

### Documentation
- **User README**: `package/README-BUILDER.md` - Usage guide with examples
- **Developer guide**: `docs/modular-builder-guide.md` - Architecture and customization
- **Project docs**: Updated `CLAUDE.md` with builder commands

### Testing & Release Tools
- **Local testing**: `package/test-builder-local.sh` - Test without GitHub/Maven
- **Release prep**: `package/prepare-release.sh` - Prepare artifacts for GitHub releases

## Release Process

### Build Time
```bash
cd package
mvn clean package -DskipTests
```
Generates:
- `arcadedb-{version}-base.tar.gz` + `.sha256`
- `arcadedb-{version}-base.zip` + `.sha256`
- Standard distributions (full, minimal, headless)

### GitHub Release
Upload to releases:
- `arcadedb-builder.sh` - The builder script
- `README-BUILDER.md` - User documentation
- `arcadedb-{version}-base.tar.gz` + checksum
- Optional: `arcadedb-{version}-base.zip` + checksum

### User Download
Users download `arcadedb-builder.sh` and run:
```bash
chmod +x arcadedb-builder.sh
./arcadedb-builder.sh --version=26.1.0 --modules=postgresw
```

## Testing

### Automated Tests
- 4 dry-run tests validating different scenarios
- Version validation (accepts X.Y.Z and X.Y.Z-SNAPSHOT)
- Invalid input rejection
- Help message display

### Manual Testing
- Built and verified base distribution (~52MB)
- Tested with multiple module combinations
- Verified all 9 optional modules download correctly
- Confirmed Docker image generation works

## Backward Compatibility

- **No breaking changes**: Existing distributions (full, minimal, headless) unchanged
- **Optional feature**: Users can continue using pre-built distributions
- **Additive only**: New base distribution and builder script are additions

## Dependencies

### Runtime (for builder script)
- `curl` or `wget` - File downloads
- `tar` - Archive operations
- `zip`/`unzip` - Zip operations
- `sha256sum` or `shasum` - Checksum verification
- `docker` (optional) - Docker image generation

### Build Time (for base distribution)
- Maven 3.6+
- Java 21+
- Existing ArcadeDB build dependencies

## Size Comparison

| Distribution | Size | Reduction |
|--------------|------|-----------|
| Full | 195MB | baseline |
| Minimal | 100MB | 49% smaller |
| Headless | 90MB | 54% smaller |
| **Base** | **52MB** | **73% smaller** |
| PostgreSQL-only* | ~60MB | 69% smaller |
| Gremlin+Studio* | ~110MB | 44% smaller |

*Custom builds (base + selected modules)

## Future Enhancements

Potential future improvements (not in initial scope):
- GPG signature verification
- Local artifact caching
- Custom Maven repository support
- Configuration file support (YAML/JSON) for repeatable builds
- Integration with package managers (Homebrew, apt, yum)
- Web-based configurator

## Implementation Status

✅ **COMPLETE** - All 21 implementation tasks finished:
- Maven build configuration
- Base assembly descriptor
- Builder script (907 lines, fully functional)
- Security hardening
- Docker support
- Comprehensive documentation
- Testing scripts
- Release preparation tools

Ready for integration into ArcadeDB release process.

---

**Issue Labels**: `enhancement`, `distribution`, `build`, `documentation`
**Assignee**: Build/Release team
**Milestone**: Next release (26.2.0 or 27.0.0)
