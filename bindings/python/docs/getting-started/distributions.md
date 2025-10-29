# Distribution Comparison

All three ArcadeDB Python packages are **embedded** - they run the database directly in your Python process via JPype. The difference is which Java JARs are bundled.

## Quick Comparison

| Feature | Headless | Minimal | Full |
|---------|----------|---------|------|
| **Package Name** | `arcadedb-embedded-headless` | `arcadedb-embedded-minimal` | `arcadedb-embedded` |
| **Wheel Size** | ~94 MB | ~97 MB | ~158 MB |
| **Studio Web UI** | ❌ No | ✅ Yes | ✅ Yes |
| **SQL** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Cypher** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Gremlin** | ❌ No | ❌ No | ✅ Yes |
| **GraphQL** | ❌ No | ❌ No | ✅ Yes |
| **PostgreSQL Wire** | ✅ Yes | ✅ Yes | ✅ Yes |
| **MongoDB Wire** | ❌ No | ❌ No | ✅ Yes |
| **Redis Wire** | ❌ No | ❌ No | ✅ Yes |
| **HTTP REST API** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Test Results** | 36/43 passed | 40/43 passed | 43/43 passed |
| **PyPI Status** | ✅ Available | ✅ Available | ⏳ Coming Soon |

## Headless Distribution

**Best for:** Production applications, minimal dependencies

```bash
pip install arcadedb-embedded-headless
```

### What's Included

- **Core Database**: SQL and Cypher query engines
- **PostgreSQL Wire Protocol**: Connect with PostgreSQL clients
- **HTTP REST API**: Programmatic access via HTTP
- **All Database Models**: Graph, Document, Key/Value, Vector, Time Series

### What's NOT Included

- ❌ Studio web UI (use code/API only)
- ❌ Gremlin query language
- ❌ GraphQL support
- ❌ MongoDB/Redis wire protocols

### Test Results

**36 out of 43 tests pass** (7 tests skipped):

- ✅ All core database operations work
- ✅ SQL and Cypher queries work
- ⏭️ Cypher tests skipped (Cypher engine not in headless)
- ⏭️ Gremlin tests skipped (Gremlin not available)
- ⏭️ Server tests skipped (HTTP server not included)

### Use Cases

- Production Python applications
- Headless servers and containers
- Applications that don't need visual debugging
- Minimal dependency footprint

### Example

```python
import arcadedb_embedded as arcadedb

# Direct database access - no UI needed
with arcadedb.create_database("/tmp/mydb") as db:
    db.command("sql", "CREATE DOCUMENT TYPE User")
    with db.transaction():
        db.command("sql", "INSERT INTO User SET name = 'Alice'")

    result = db.query("sql", "SELECT FROM User")
    print(f"Users: {len(result)}")
```

## Minimal Distribution

**Best for:** Development, learning, visual debugging

```bash
pip install arcadedb-embedded-minimal
```

### What's Included

Everything in **Headless** plus:

- ✅ **Studio Web UI** (~2 MB): Visual database explorer
  - Query editor with syntax highlighting
  - Schema visualization
  - Data browsing and editing
  - Graph visualization

### Test Results

**40 out of 43 tests pass** (3 tests skipped):

- ✅ All core database operations work
- ✅ SQL and Cypher queries work
- ✅ HTTP server and Studio UI work
- ⏭️ Gremlin tests skipped (Gremlin not available)

### Use Cases

- Development and testing
- Learning ArcadeDB features
- Visual database exploration
- Debugging queries and data

### Accessing Studio UI

```python
from arcadedb_embedded import create_server

# Start HTTP server with Studio UI
server = create_server("./databases")
server.start()

# Studio UI available at: http://localhost:2480
# Create databases, run queries, visualize data

# When done
server.stop()
```

!!! tip "Studio in Browser"
    Once the server starts, open your browser to `http://localhost:2480` to access the Studio UI.

## Full Distribution

**Best for:** Gremlin graphs, GraphQL APIs, MongoDB/Redis compatibility

```bash
pip install arcadedb-embedded
```

!!! warning "Coming Soon"
    The full distribution is pending PyPI size limit approval. Will be available soon!

### What's Included

Everything in **Minimal** plus:

- ✅ **Gremlin** (~60 MB): Apache TinkerPop graph traversal language
- ✅ **GraphQL** (~4 MB): GraphQL query endpoint
- ✅ **MongoDB Wire Protocol**: Connect with MongoDB clients
- ✅ **Redis Wire Protocol**: Connect with Redis clients

### Test Results

**43 out of 43 tests pass** (0 tests skipped):

- ✅ All core database operations work
- ✅ SQL, Cypher, and Gremlin queries work
- ✅ HTTP server and Studio UI work
- ✅ All features available

### Use Cases

- Applications using Gremlin graph traversals
- GraphQL API integration
- MongoDB client compatibility
- Redis client compatibility
- Complete feature set

### Gremlin Example

```python
import arcadedb_embedded as arcadedb

with arcadedb.create_database("/tmp/graphdb") as db:
    # Create vertices and edges
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE EDGE TYPE Knows")

    with db.transaction():
        db.command("sql", "CREATE VERTEX Person SET name = 'Alice'")
        db.command("sql", "CREATE VERTEX Person SET name = 'Bob'")
        db.command("sql", """
            CREATE EDGE Knows FROM
                (SELECT FROM Person WHERE name = 'Alice')
            TO
                (SELECT FROM Person WHERE name = 'Bob')
        """)

    # Use Gremlin for graph traversals
    result = db.query("gremlin", "g.V().has('name', 'Alice').out('Knows').values('name')")
    print(f"Alice knows: {list(result)}")  # ['Bob']
```

## Same Import for All

Regardless of distribution, the import is always:

```python
import arcadedb_embedded as arcadedb
```

This means you can:

- **Develop** with Minimal (Studio UI for debugging)
- **Deploy** with Headless (smaller, production-ready)
- **Upgrade** to Full (when you need Gremlin/GraphQL)

No code changes required!

## Which Distribution Should You Choose?

<div class="grid" markdown>

!!! success "Start with Headless"
    For most Python applications:

    - Production-ready and tested
    - Smallest size (~94 MB)
    - All core features included
    - Available now on PyPI

!!! info "Upgrade to Minimal for Development"
    If you need visual debugging:

    - Only ~3 MB larger
    - Studio UI for exploration
    - Great for learning
    - Available now on PyPI

!!! warning "Wait for Full if Needed"
    Only if you specifically need:

    - Gremlin graph traversals
    - GraphQL endpoint
    - MongoDB/Redis compatibility
    - Coming soon to PyPI!

</div>

## Size Breakdown

### Headless (~94 MB)

- Core Database: ~60 MB
- SQL/Cypher Engines: ~15 MB
- PostgreSQL Wire: ~5 MB
- HTTP Server: ~10 MB
- Dependencies: ~4 MB

### Minimal (~97 MB)

- Everything in Headless: ~94 MB
- Studio UI: ~3 MB

### Full (~158 MB)

- Everything in Minimal: ~97 MB
- Gremlin (TinkerPop): ~60 MB
- GraphQL: ~4 MB
- MongoDB/Redis Wire: ~2 MB

## Installation Tips

### Switch Distributions

Uninstall the current distribution first:

```bash
# Uninstall any existing distribution
pip uninstall arcadedb-embedded arcadedb-embedded-headless arcadedb-embedded-minimal

# Install the one you want
pip install arcadedb-embedded-headless
```

### Check Installed Distribution

```python
import arcadedb_embedded as arcadedb
print(f"Version: {arcadedb.__version__}")

# Check which JARs are available
from arcadedb_embedded.jvm import get_jvm
jvm = get_jvm()
# JVM will load JARs from your installed distribution
```

## Next Steps

- [Installation Guide](installation.md) - Detailed install instructions
- [Quick Start](quickstart.md) - Get started in 5 minutes
- [Server Mode](../guide/server.md) - Using the HTTP server with Studio UI
- [Gremlin Guide](../guide/graphs.md) - Graph traversals (Full distribution)
