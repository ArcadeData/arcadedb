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

### Architecture

The builder works in phases:

1. **Download Base**: Gets core modules from GitHub releases
2. **Add Modules**: Downloads optional modules from Maven Central (or copies from local repository)
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
