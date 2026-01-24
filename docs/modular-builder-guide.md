# Modular Distribution Builder Guide

## For End Users

### What is the Modular Builder?

The ArcadeDB Modular Distribution Builder allows you to create custom ArcadeDB packages containing only the features you need. This results in smaller distributions, reduced dependencies, and simplified deployments.

### Getting Started

**Option 1: Run directly with curl**
```bash
curl -fsSL https://github.com/ArcadeData/arcadedb/releases/download/26.1.1/arcadedb-builder.sh | bash -s -- --version=26.1.1 --modules=gremlin,studio
```

**Option 2: Download and run locally**
```bash
curl -fsSLO https://github.com/ArcadeData/arcadedb/releases/download/26.1.1/arcadedb-builder.sh
chmod +x arcadedb-builder.sh
./arcadedb-builder.sh
```

### Quick Examples

**Interactive Mode:**
```bash
./arcadedb-builder.sh
# Follow the prompts to select version and modules
```

**PostgreSQL-only Build:**
```bash
./arcadedb-builder.sh --version=26.1.1 --modules=postgresw
```

**Full Custom Build:**
```bash
./arcadedb-builder.sh \
  --version=26.1.1 \
  --modules=console,gremlin,studio,postgresw \
  --output-name=my-arcadedb
```

**Dry Run (preview without building):**
```bash
./arcadedb-builder.sh --version=26.1.1 --modules=gremlin,studio --dry-run
```

### Command-Line Options

| Option | Description |
|--------|-------------|
| `--version=VERSION` | ArcadeDB version to build (required for non-interactive mode) |
| `--modules=MODULES` | Comma-separated list of modules |
| `--output-name=NAME` | Custom name for distribution (default: arcadedb-VERSION-custom-TIMESTAMP) |
| `--output-dir=DIR` | Output directory (default: current directory) |
| `--local-repo[=PATH]` | Use local Maven repository or custom JAR directory |
| `--local-base=FILE` | Use local base distribution file |
| `--docker-tag=TAG` | Build Docker image with specified tag |
| `--skip-docker` | Skip Docker image build |
| `--dockerfile-only` | Only generate Dockerfile, don't build image |
| `--keep-temp` | Keep temporary build directory |
| `--dry-run` | Show what would be done without executing |
| `-v, --verbose` | Enable verbose output |
| `-q, --quiet` | Suppress non-error output |
| `-h, --help` | Show help message |

### Module Selection Guide

Choose modules based on your needs:

| Module | Type | Description |
|--------|------|-------------|
| **console** | Regular | Interactive database console |
| **studio** | Regular | Web-based administration interface |
| **graphql** | Regular | GraphQL API support |
| **gremlin** | Shaded | Apache Tinkerpop Gremlin support |
| **postgresw** | Shaded | PostgreSQL wire protocol compatibility |
| **mongodbw** | Shaded | MongoDB wire protocol compatibility |
| **redisw** | Shaded | Redis wire protocol compatibility |
| **grpcw** | Shaded | gRPC wire protocol support |
| **metrics** | Shaded | Prometheus metrics integration |

*Shaded modules include all dependencies in a single JAR to avoid conflicts.*

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
./arcadedb-builder.sh --version=26.1.2-SNAPSHOT --modules=gremlin
```

### Testing the Builder

```bash
cd package
./test-builder-local.sh
```

### Preparing a Release

```bash
cd package
./prepare-release.sh 26.1.1
```

This creates release artifacts in `target/release-26.1.1/`:
- `arcadedb-builder.sh`
- `README-BUILDER.md`

Upload these along with the base distribution to GitHub releases.

## Development Workflow

### Local Testing Without Publishing

When developing new modules or testing changes, use `--local-base` and `--local-repo` for fully offline builds:

#### Step 1: Build Base Distribution and Modules

```bash
cd /path/to/arcadedb
mvn clean package -DskipTests
```

This:
- Installs module JARs to your local Maven repository (`~/.m2/repository`)
- Creates base distribution in `package/target/arcadedb-*-base.tar.gz`

#### Step 2: Build Custom Distribution (Fully Offline)

```bash
cd package

# Get current version dynamically
VERSION=$(mvn -f ../pom.xml help:evaluate -Dexpression=project.version -q -DforceStdout)

# Build with local base and local modules (no internet required)
./arcadedb-builder.sh \
    --version=$VERSION \
    --modules=gremlin,studio,postgresw \
    --local-base=target/arcadedb-$VERSION-base.tar.gz \
    --local-repo \
    --skip-docker
```

This mode downloads nothing from the internet - it uses only local files.

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
cp ~/.m2/repository/com/arcadedb/arcadedb-gremlin/26.1.1/arcadedb-gremlin-26.1.1-shaded.jar /tmp/custom-arcade-jars/
cp ~/.m2/repository/com/arcadedb/arcadedb-console/26.1.1/arcadedb-console-26.1.1.jar /tmp/custom-arcade-jars/

# Build with custom JARs
./arcadedb-builder.sh \
    --version=26.1.1 \
    --modules=gremlin,console \
    --local-repo=/tmp/custom-arcade-jars
```

### Checksum Verification

The builder automatically verifies checksums when available:

- **GitHub releases**: SHA-256 checksums for base distribution
- **Maven repository**: SHA-1 checksums for module JARs
- **Custom directory**: If `.sha1` files exist, they are verified; otherwise skipped with warning

Generate checksums for custom JARs:

```bash
cd /tmp/custom-arcade-jars
shasum -a 1 arcadedb-gremlin-26.1.1-shaded.jar > arcadedb-gremlin-26.1.1-shaded.jar.sha1
```

### Architecture

The builder works in phases:

1. **Download Base**: Gets core modules from GitHub releases (or uses `--local-base`)
2. **Add Modules**: Downloads optional modules from Maven Central (or uses `--local-repo`)
3. **Verify**: Checks SHA-256 (base) and SHA-1 (modules) checksums
4. **Package**: Creates zip and tar.gz archives
5. **Docker**: Optionally builds Docker image

### Adding New Optional Modules

To add a new optional module:

1. Update `package/pom.xml` dependencies
2. Update base.xml to exclude the new module
3. Update `arcadedb-builder.sh`:
   - Add to `SHADED_MODULES` or `REGULAR_MODULES`
   - Add description to `get_module_description()` function
4. Test with local builder
5. Update documentation

## Troubleshooting

See `package/README-BUILDER.md` for detailed troubleshooting guide.
