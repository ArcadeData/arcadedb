# ArcadeDB Python Bindings

Native Python bindings for ArcadeDB - the multi-model database that supports Graph, Document, Key/Value, Search Engine, Time Series, and Vector models.

**Status**: ✅ Production Ready | **Tests**: 43/43 Passing (100%)

---

## 📚 Documentation

**[📖 Read the Full Documentation →](https://humemai.github.io/arcadedb-embedded-python/latest)**

Quick links:
- [Installation Guide](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/installation/)
- [Quick Start Tutorial](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/quickstart/)
- [Distribution Comparison](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/distributions/)
- [User Guide](https://humemai.github.io/arcadedb-embedded-python/latest/guide/core/database/)
- [API Reference](https://humemai.github.io/arcadedb-embedded-python/latest/api/database/)
- [Examples](https://humemai.github.io/arcadedb-embedded-python/latest/examples/)

---

## 🚀 Quick Start

### Installation

Choose based on your needs:

```bash
# Headless (recommended) - Core database, no Studio UI
pip install arcadedb-embedded-headless

# Minimal - Adds Studio web UI for development
pip install arcadedb-embedded-minimal

# Full - Adds Gremlin + GraphQL (coming soon)
pip install arcadedb-embedded
```

**Requirements**: Java 21+ must be installed ([details](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/installation/#java-runtime-environment-jre))

!!! tip "Eliminate JVMCI Warnings"
    See warnings about "JVMCI is not enabled"? Install [GraalVM](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/installation/#eliminate-polyglot-warnings-optional) to fix them.

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

- 🚀 **Embedded Mode**: Direct database access in Python process (no network)
- 🌐 **Server Mode**: Optional HTTP server with Studio web interface
- 📦 **Self-contained**: All JAR files bundled (no external dependencies)
- 🔄 **Multi-model**: Graph, Document, Key/Value, Vector, Time Series
- 🔍 **Multiple query languages**: SQL, Cypher, Gremlin (full only), MongoDB
- ⚡ **High performance**: Direct JVM integration via JPype
- 🔒 **ACID transactions**: Full transaction support
- 🎯 **Vector storage**: Store and query vector embeddings with HNSW indexing
- 📥 **Data import**: Built-in CSV, JSON, Neo4j importers

---

## 📦 Distribution Options

All three packages are **embedded** - they run ArcadeDB in your Python process:

| Distribution | Package | Size | Studio UI | Gremlin | Status |
|-------------|---------|------|-----------|---------|--------|
| **Headless** | `arcadedb-embedded-headless` | ~94MB | ❌ | ❌ | ✅ Available |
| **Minimal** | `arcadedb-embedded-minimal` | ~97MB | ✅ | ❌ | ✅ Available |
| **Full** | `arcadedb-embedded` | ~158MB | ✅ | ✅ | ⏳ Coming Soon |

All use the same import: `import arcadedb_embedded as arcadedb`

**[📊 Detailed comparison](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/distributions/)**

---

## 🧪 Testing

**Status**: 43/43 tests passing (100%)

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_core.py -v
```

See [tests/README.md](tests/README.md) for detailed test documentation.

---

## 🔧 Building from Source

**Docker required** - it handles all dependencies (Java, Maven, Python build tools):

```bash
cd bindings/python/

# Build all three distributions
./build-all.sh

# Or build specific distribution
./build-all.sh headless    # ~94 MB
./build-all.sh minimal     # ~97 MB
./build-all.sh full        # ~158 MB
```

Built wheels will be in `dist/`. **[Build instructions](https://humemai.github.io/arcadedb-embedded-python/latest/getting-started/installation/#building-from-source)**

---

## 📋 Package Structure

```
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

- **Documentation**: https://humemai.github.io/arcadedb-embedded-python/latest/
- **PyPI (Headless)**: https://pypi.org/project/arcadedb-embedded-headless/
- **PyPI (Minimal)**: https://pypi.org/project/arcadedb-embedded-minimal/
- **GitHub**: https://github.com/humemai/arcadedb
- **ArcadeDB Main Docs**: https://docs.arcadedb.com
- **Issues**: https://github.com/humemai/arcadedb/issues

---

**Made with ❤️ by the ArcadeDB community**
