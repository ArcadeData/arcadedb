# Installation

## Choose Your Distribution

All three packages are **embedded** - they run ArcadeDB directly in your Python process. Choose based on which features you need:

=== "Headless (Recommended)"

    **Core database functionality - perfect for production**

    ```bash
    pip install arcadedb-embedded-headless
    ```

    - ✅ Size: ~94MB
    - ✅ SQL, Cypher queries
    - ✅ PostgreSQL wire protocol
    - ✅ HTTP REST API
    - ❌ No Studio UI

    **Best for:**

    - Production applications
    - Python applications where you don't need the web UI
    - Minimal dependencies

=== "Minimal"

    **Adds Studio web UI for development**

    ```bash
    pip install arcadedb-embedded-minimal
    ```

    - ✅ Size: ~97MB
    - ✅ Everything in Headless
    - ✅ **Studio web UI** for visual debugging

    **Best for:**

    - Development and learning
    - Visual database exploration
    - Debugging with the Studio UI

=== "Full"

    **Adds Gremlin + GraphQL support**

    ```bash
    pip install arcadedb-embedded
    ```

    !!! warning "Coming Soon"
        Full distribution is pending PyPI size limit approval. Will be available soon!

    - ✅ Size: ~158MB
    - ✅ Everything in Minimal
    - ✅ **Gremlin** query language
    - ✅ **GraphQL** support
    - ✅ MongoDB & Redis wire protocols

    **Best for:**

    - Applications using Gremlin graph queries
    - GraphQL integration
    - MongoDB/Redis compatibility

## Same Import for All Distributions

Regardless of which distribution you install, the import is always:

```python
import arcadedb_embedded as arcadedb
```

This means you can switch between distributions without changing your code!

## Requirements

### Java Runtime Environment (JRE)

!!! warning "Java Required"
    You need Java Runtime Environment (JRE) 21+ installed. The wheels bundle all JAR files, but need a JVM to run them.

=== "Ubuntu/Debian"

    ```bash
    sudo apt-get update
    sudo apt-get install default-jre-headless
    ```

    Verify installation:

    ```bash
    java -version
    # Should show: openjdk version "21.0.x" or higher
    ```

=== "macOS"

    ```bash
    brew install openjdk
    ```

    Verify installation:

    ```bash
    java -version
    # Should show: openjdk version "21.0.x" or higher
    ```

=== "Windows"

    1. Download OpenJDK from [Adoptium](https://adoptium.net/)
    2. Run the installer (choose JRE, not full JDK)
    3. Verify installation in Command Prompt:

    ```cmd
    java -version
    ```

### Python Version

- **Supported**: Python 3.8, 3.9, 3.10, 3.11, 3.12
- **Recommended**: Python 3.10 or higher

### Dependencies

All Python dependencies are automatically installed:

- **JPype1** >= 1.5.0 (Java-Python bridge)
- **typing-extensions** (for Python < 3.10)

## Verify Installation

After installation, verify everything works:

```python
import arcadedb_embedded as arcadedb
print(f"ArcadeDB Python bindings version: {arcadedb.__version__}")
```

Expected output (version will match what you installed):

```
ArcadeDB Python bindings version: X.Y.Z
```

## Eliminate Polyglot Warnings (Optional)

!!! tip "GraalVM Eliminates JVMCI Warnings"
    If you see warnings about "JVMCI is not enabled", install **GraalVM**. This warning appears when using ArcadeDB's polyglot scripting features (e.g., JavaScript in queries). GraalVM may also provide modest performance improvements for some workloads.

**Installation (Linux/macOS):**

```bash
# Install via SDKMAN
sdk install java 21.0.9-graal
sdk use java 21.0.9-graal

# Verify
java -version  # Should show: Oracle GraalVM
```

**What it fixes:** OpenJDK lacks JVMCI (JVM Compiler Interface), which GraalVM's Polyglot engine needs for JIT compilation of embedded scripts. Core database operations work fine either way.

## Building from Source

If you want to build the wheels yourself:

!!! info "Docker Required"
    Building requires Docker - it handles all dependencies (Java, Maven, Python build tools).

```bash
cd bindings/python/

# Build all three distributions
./build-all.sh

# Or build specific distribution
./build-all.sh headless    # ~94 MB
./build-all.sh minimal     # ~97 MB
./build-all.sh full        # ~158 MB
```

Built wheels will be in `dist/`:

```
dist/
├── arcadedb_embedded_headless-X.Y.Z-py3-none-any.whl
├── arcadedb_embedded_minimal-X.Y.Z-py3-none-any.whl
└── arcadedb_embedded_full-X.Y.Z-py3-none-any.whl
```

Install locally (version extracted from `pom.xml`):

```bash
pip install dist/arcadedb_embedded_headless-*.whl
```

## Troubleshooting

### Java Not Found

If you get `Java runtime not found` error:

1. Install JRE (see requirements above)
2. Set `JAVA_HOME` environment variable:

```bash
# Linux/macOS
export JAVA_HOME=/usr/lib/jvm/default-java

# Windows
set JAVA_HOME=C:\Program Files\Java\jdk-21
```

### Import Errors

If `import arcadedb_embedded` fails:

```bash
# Uninstall all distributions first
pip uninstall arcadedb-embedded arcadedb-embedded-headless arcadedb-embedded-minimal

# Reinstall chosen distribution
pip install arcadedb-embedded-headless
```

### Version Conflicts

If you see version conflicts with JPype:

```bash
# Upgrade JPype
pip install --upgrade JPype1

# Reinstall ArcadeDB
pip install --force-reinstall arcadedb-embedded-headless
```

## Next Steps

- [Quick Start Guide](quickstart.md) - Get started in 5 minutes
- [Distribution Comparison](distributions.md) - Detailed comparison
- [User Guide](../guide/core/database.md) - Learn all features
