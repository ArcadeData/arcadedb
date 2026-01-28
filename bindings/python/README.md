# ArcadeDB Python Bindings

Native Python bindings for ArcadeDB - the multi-model database that supports Graph, Document, Key/Value, Search Engine, Time Series, and Vector models.

**Status**: ✅ Production Ready | **Tests**: 258 Passing | **Platforms**: 3 Supported

---

## 📚 Documentation

**[📖 Read the Full Documentation →](https://docs.humem.ai/arcadedb/latest)**

---

## 🚀 Quick Start

### Installation

```bash
uv pip install arcadedb-embedded
```

**Requirements:**

- **Python 3.10–3.14** (packaged/tested on CPython 3.12) - No Java installation required!
- **Supported Platforms**: Prebuilt wheels for **3 platforms**
  - Linux: x86_64, ARM64
  - macOS: Apple Silicon (ARM64)

### 5-Minute Example

```python
import arcadedb_embedded as arcadedb

# Create database (context manager for automatic open and close)
with arcadedb.create_database("./mydb") as db:
  # Create schema (schema ops are auto-transactional)
  db.schema.create_document_type("Person")
  db.schema.create_property("Person", "name", "STRING")
  db.schema.create_property("Person", "age", "INTEGER")

    # Insert data (requires transaction)
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

    # Query data
    result = db.query("sql", "SELECT FROM Person WHERE age > 25")
    for record in result:
        print(f"Name: {record.get('name')}")

  # SQL also works (useful when talking to a remote server),
  # but the embedded API above is preferred for local use.
```

**[👉 See full tutorial](https://docs.humem.ai/arcadedb/latest/getting-started/quickstart/)**

---

## ✨ Features

- ☕ **No Java Installation Required**: Bundled JRE (~249MB uncompressed)
- 🌍 **3 Platforms Supported**: Linux (x86_64, ARM64), macOS (ARM64)
- 🚀 **Embedded Mode**: Direct database access in Python process (no network)
- 🌐 **Server Mode**: Optional HTTP server with Studio web interface
- 📦 **Self-contained**: All dependencies bundled (~116MB wheel)
- 🔄 **Multi-model**: Graph, Document, Key/Value, Vector, Time Series
- 🔍 **Multiple query languages**: SQL, OpenCypher, MongoDB
- ⚡ **High performance**: Direct JVM integration via JPype
- 🔒 **ACID transactions**: Full transaction support
- 🎯 **Vector storage**: Store and query vector embeddings with HNSW (JVector) indexing
- 📥 **Data import**: Built-in CSV and ArcadeDB JSONL import

---

## 📦 What's Inside

The `arcadedb-embedded` package is platform-specific and self-contained:

**Package Contents (all platforms):**

- **Wheel size (compressed)**: ~116MB
- **ArcadeDB JARs (uncompressed)**: ~32MB
- **Bundled JRE (uncompressed)**: ~249MB (platform-specific Java 25 runtime via jlink)
- **Total uncompressed size**: ~281MB

**Note**: Some JARs are excluded to optimize package size (e.g., gRPC wire protocol). See [`jar_exclusions.txt`](https://github.com/humemai/arcadedb-embedded-python/blob/main/bindings/python/jar_exclusions.txt) for details.

Import: `import arcadedb_embedded as arcadedb`

---

## 🧪 Testing

**Status**: 258 tests + example scripts passing on all 3 platforms

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_core.py -v
```

See [testing documentation](https://docs.humem.ai/arcadedb/latest/development/testing/) for detailed test documentation.

---

## 🔧 Building from Source (Advanced)

Linux uses Docker. macOS uses a native Java 25+ JDK with jlink.

```bash
cd bindings/python/

# Install uv (one-time)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment with uv
uv venv .venv
source .venv/bin/activate

# Install build and test dependencies
uv pip install build
uv pip install -e ".[test]"

# Build for your current platform (auto-detected)
./build.sh
```

Built wheels will be in `dist/`.

**[Build instructions](https://docs.humem.ai/arcadedb/latest/getting-started/installation/#building-from-source)**

> **Developer Note:** See [build architecture docs](https://docs.humem.ai/arcadedb/latest/development/build-architecture/) for comprehensive documentation of the multi-platform build architecture, including how we achieve platform-specific JRE bundling across the supported platforms on GitHub Actions.

## Development

Versions are automatically extracted from the main ArcadeDB `pom.xml`. See [versioning strategy](https://docs.humem.ai/arcadedb/latest/development/release/#python-versioning-strategy) for details on development vs release mode handling.

---

## 📋 Package Structure

```bash
arcadedb_embedded/
├── __init__.py          # Public API exports
├── core.py              # Database and DatabaseFactory
├── server.py            # ArcadeDBServer for HTTP mode
├── results.py           # ResultSet and Result wrappers
├── transactions.py      # TransactionContext manager
├── schema.py            # Schema management API
├── vector.py            # Vector search and HNSW (JVector) indexing
├── importer.py          # Data import (CSV, JSONL)
├── exporter.py          # Data export (JSONL, GraphML, GraphSON, CSV)
├── graph.py             # Graph API helpers
├── batch.py             # Batch operations context
├── async_executor.py    # Asynchronous query execution
├── type_conversion.py   # Python-Java type conversion utilities
├── exceptions.py        # ArcadeDBError exception
├── jvm.py               # JVM lifecycle management
└── _version.py          # Package version info
```

**[Architecture details](https://docs.humem.ai/arcadedb/latest/development/architecture/)**

---

## 🤝 Contributing

[See our contributing guidelines](https://docs.humem.ai/arcadedb/latest/development/contributing/)

## 📄 License

Both upstream ArcadeDB (Java) and this ArcadeDB Embedded Python project are licensed under Apache 2.0, fully open and free for everyone, including commercial use.

---

## 🔗 Links

- **Documentation**: <https://docs.humem.ai/arcadedb/latest/>
- **PyPI**: <https://pypi.org/project/arcadedb-embedded/>
- **GitHub**: <https://github.com/humemai/arcadedb-embedded-python>
- **ArcadeDB Main Docs**: <https://docs.arcadedb.com>
- **Issues**: <https://github.com/humemai/arcadedb-embedded-python/issues>
