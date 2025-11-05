# ArcadeDB Python Bindings

Native Python bindings for ArcadeDB - the multi-model database that supports Graph, Document, Key/Value, Search Engine, Time Series, and Vector models.

**Status**: ✅ Production Ready | **Tests**: 107/107 Passing (100%) | **Platforms**: 6 Supported

---

## 📚 Documentation

**[📖 Read the Full Documentation →](https://humemai.github.io/arcadedb-embedded-python/latest)**

Quick links:
- [Installation Guide](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/installation/)
- [Quick Start Tutorial](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/quickstart/)

- [User Guide](https://humemai.github.io/arcadedb-embedded-python/latest/guide/core/database/)
- [API Reference](https://humemai.github.io/arcadedb-embedded-python/latest/api/database/)
- [Examples](https://humemai.github.io/arcadedb-embedded-python/latest/examples/)

---

## 🚀 Quick Start

### Installation

```bash
# Temporarily install from GitHub Pages (awaiting PyPI size limit approval)
pip install arcadedb-embedded \
  --index-url https://humemai.github.io/arcadedb-embedded-python/simple/ \
  --extra-index-url https://pypi.org/simple/

# Once PyPI approves our size limit request, installation will be simpler:
# pip install arcadedb-embedded

# Development version (latest features, may be unstable)
pip install --pre arcadedb-embedded \
  --index-url https://humemai.github.io/arcadedb-embedded-python/simple/ \
  --extra-index-url https://pypi.org/simple/
```

!!! note "PyPI Size Limit"
    We're temporarily hosting wheels on GitHub Pages while awaiting PyPI size limit approval (our wheels are ~160MB, default limit is 100MB). The `--index-url` tells pip to use our GitHub Pages index for arcadedb-embedded, while `--extra-index-url` ensures dependencies like JPype1 are installed from PyPI.

**Requirements:**

- **Python 3.8+ only** - No Java installation required!
- **Supported Platforms**: Prebuilt wheels for **6 platforms**
  - Linux: x86_64, ARM64
  - macOS: Intel (x86_64), Apple Silicon (ARM64)
  - Windows: x86_64, ARM64
- **Development version**: Use `--pre` flag to install `.devN` versions

!!! tip "Development Releases"
    We publish development versions (`X.Y.Z.devN`) for every push to main when `pom.xml` contains a SNAPSHOT version. These are great for testing new features but may be unstable. [Learn more](DEV_RELEASE_STRATEGY.md)

### 5-Minute Example

```python
import arcadedb_embedded as arcadedb

# Create database (context manager for automatic cleanup)
with arcadedb.create_database("/tmp/mydb") as db:
    # Create schema
    db.command("sql", "CREATE DOCUMENT TYPE Person")

    # Insert data (requires transaction)
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

    # Query data
    result = db.query("sql", "SELECT FROM Person WHERE age > 25")
    for record in result:
        print(f"Name: {record.get_property('name')}")
```

**[👉 See full tutorial](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/quickstart/)**

---

## ✨ Features

- ☕ **No Java Installation Required**: Bundled JRE (47-63MB per platform)
- 🌍 **6 Platforms Supported**: Linux, macOS, Windows (x86_64 + ARM64)
- 🚀 **Embedded Mode**: Direct database access in Python process (no network)
- 🌐 **Server Mode**: Optional HTTP server with Studio web interface
- 📦 **Self-contained**: All dependencies bundled (~155-161MB wheel)
- 🔄 **Multi-model**: Graph, Document, Key/Value, Vector, Time Series
- 🔍 **Multiple query languages**: SQL, Cypher, Gremlin, MongoDB
- ⚡ **High performance**: Direct JVM integration via JPype
- 🔒 **ACID transactions**: Full transaction support
- 🎯 **Vector storage**: Store and query vector embeddings with HNSW indexing
- 📥 **Data import**: Built-in CSV, JSON, Neo4j importers

---

## 📦 What's Inside

The `arcadedb-embedded` package is platform-specific and self-contained:

**Package Contents (all platforms):**

- **ArcadeDB JARs**: 167.4MB (identical across all platforms)
- **Bundled JRE**: 47-63MB (platform-specific Java 21 runtime via jlink)
- **Total Size**: ~155-161MB compressed wheel, ~215-230MB installed

**Platform Details:**

| Platform | Wheel Size | JRE Size | Installed Size | Tests |
|----------|-----------|----------|----------------|-------|
| Windows ARM64 | 155.1M | 47.3M | ~215M | 107/107 ✅ |
| macOS ARM64 | 156.7M | 53.9M | ~221M | 107/107 ✅ |
| macOS Intel | 157.8M | 55.3M | ~223M | 107/107 ✅ |
| Windows x64 | 157.4M | 51.5M | ~219M | 107/107 ✅ |
| Linux ARM64 | 159.9M | 61.8M | ~229M | 107/107 ✅ |
| Linux x64 | 160.9M | 62.7M | ~230M | 107/107 ✅ |

**Note**: Some JARs are excluded to optimize package size (e.g., gRPC wire protocol). See `jar_exclusions.txt` for details.

Import: `import arcadedb_embedded as arcadedb`

---

## 🧪 Testing

**Status**: 107/107 tests passing (100%)

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_core.py -v
```

See [tests/README.md](tests/README.md) for detailed test documentation.

---

## 🔧 Building from Source

**Requirements vary by platform:**

- **Linux**: Docker (handles all dependencies)
- **macOS/Windows**: Java 21+ JDK with jlink (to build the bundled JRE)

### Setup Virtual Environment

First, create and activate a Python virtual environment:

```bash
cd bindings/python/

# Create virtual environment
python3 -m venv .venv

# Activate it
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
.venv\Scripts\activate

# Install build and test dependencies
pip install build
pip install -e ".[test]"
```

### Build the Package

```bash
# Build for your current platform (auto-detected)
./build.sh

# Build for specific platform
./build.sh linux/amd64    # Requires Docker
./build.sh darwin/arm64   # Requires Java JDK (native build)
./build.sh windows/arm64  # Requires Java JDK (native build)
# etc.
```

### Run Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_core.py -v
```

Built wheels will be in `dist/`. **[Build instructions](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/installation/#building-from-source)**

**Supported platforms:**

- `linux/amd64` (Docker build on native x64 runner)
- `linux/arm64` (Docker build on native ARM64 runner)
- `darwin/amd64` (Native build on macOS Intel)
- `darwin/arm64` (Native build on macOS Apple Silicon)
- `windows/amd64` (Native build on Windows x64)
- `windows/arm64` (Native build on Windows ARM64)


> **Developer Note:** See [docs/development/build-architecture.md](docs/development/build-architecture.md) for comprehensive documentation of the multi-platform build architecture, including how we achieve platform-specific JRE bundling across all 6 platforms on GitHub Actions.

## Development

!!! note "Package Contents"
    The package includes optimized ArcadeDB JARs. Some components are excluded for size optimization - see `jar_exclusions.txt` for details.

!!! note "Versioning"
    Versions are automatically extracted from the main ArcadeDB `pom.xml`. See [versioning strategy](https://humemai.github.io/arcadedb-embedded-python/latest/development/release/#python-versioning-strategy) for details on development vs release mode handling.

---

## 📋 Package Structure

```text
arcadedb_embedded/
├── __init__.py          # Public API exports
├── core.py              # Database and DatabaseFactory
├── server.py            # ArcadeDBServer for HTTP mode
├── results.py           # ResultSet and Result wrappers
├── transactions.py      # TransactionContext manager
├── vector.py            # Vector search and HNSW indexing
├── importer.py          # Data import (CSV, JSON, Neo4j)
├── exceptions.py        # ArcadeDBError exception
└── jvm.py              # JVM lifecycle management
```

**[Architecture details](https://humemai.github.io/arcadedb-embedded-python/latest/development/architecture/)**

---

## 🤝 Contributing

Contributions are welcome! See [CONTRIBUTING.md](../../CONTRIBUTING.md) and our [development guide](https://humemai.github.io/arcadedb-embedded-python/latest/development/contributing/).

---

## 📄 License

Apache License 2.0 - see [LICENSE](../../LICENSE)

---

## 🔗 Links

- **Documentation**: <https://humemai.github.io/arcadedb-embedded-python/latest/>
- **PyPI**: <https://pypi.org/project/arcadedb-embedded/>
- **GitHub**: <https://github.com/humemai/arcadedb-embedded-python>
- **ArcadeDB Main Docs**: <https://docs.arcadedb.com>
- **Issues**: <https://github.com/humemai/arcadedb-embedded-python/issues>

---

## Community

Made with ❤️ by the ArcadeDB community
