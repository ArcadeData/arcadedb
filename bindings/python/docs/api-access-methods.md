# API Access Methods

ArcadeDB Python bindings provide **two distinct access methods** that can be used independently or together.

## Java API (Embedded Mode)

Direct JVM method calls via JPype - **recommended for most Python applications**.

### Characteristics
- **Transport**: Direct JVM method calls (no network)
- **Performance**: Fastest (no serialization/network overhead)
- **Use Cases**: Single-process applications, high-performance scenarios
- **Setup**: No server required (can be standalone or server-managed)

### Example

**Standalone Database (Most Common):**

```python
import arcadedb_embedded as arcadedb

# Direct database access - NO server needed
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

**Server-Managed Database (Optional):**

```python
import arcadedb_embedded as arcadedb

# Server manages databases (still Java API calls)
server = arcadedb.create_server("/tmp/server_data", "password123")
server.start()

try:
    db = server.create_database("mydb")

    # Same Java API calls as standalone
    with db.transaction():
        db.command("sql", "CREATE DOCUMENT TYPE Person")
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

    result = db.query("sql", "SELECT FROM Person WHERE age > 25")
    for record in result:
        print(f"Name: {record.get_property('name')}")

finally:
    server.stop()
```

## HTTP API (Server Mode)

REST requests over HTTP - **enables remote access and multi-language support**.

### Characteristics
- **Transport**: HTTP requests with JSON payloads
- **Performance**: Moderate (network + JSON serialization overhead)
- **Use Cases**: Multi-process applications, web services, remote access
- **Setup**: Requires server running

### Example

```python
import arcadedb_embedded as arcadedb
import requests
from requests.auth import HTTPBasicAuth

# Start server (using Java API)
server = arcadedb.create_server("/tmp/server_data", "password123")
server.start()

try:
    # Get server details
    base_url = f"http://localhost:{server.get_http_port()}"
    auth = HTTPBasicAuth("root", "password123")

    # Create database via HTTP
    response = requests.post(
        f"{base_url}/api/v1/command",
        auth=auth,
        json={"language": "sql", "command": "CREATE DATABASE mydb"}
    )
    assert response.status_code == 200

    # Create schema via HTTP
    response = requests.post(
        f"{base_url}/api/v1/command/mydb",
        auth=auth,
        json={"language": "sql", "command": "CREATE DOCUMENT TYPE Person"}
    )
    assert response.status_code == 200

    # Insert data via HTTP
    response = requests.post(
        f"{base_url}/api/v1/command/mydb",
        auth=auth,
        json={
            "language": "sql",
            "command": "INSERT INTO Person SET name = 'Alice', age = 30"
        }
    )
    assert response.status_code == 200

    # Query data via HTTP
    response = requests.post(
        f"{base_url}/api/v1/query/mydb",
        auth=auth,
        json={"language": "sql", "command": "SELECT FROM Person WHERE age > 25"}
    )
    result = response.json()

    for record in result["result"]:
        print(f"Name: {record['name']}")

finally:
    server.stop()
```

## Hybrid Usage

Both APIs can be used **simultaneously** on the same server:

```python
import arcadedb_embedded as arcadedb
import requests
from requests.auth import HTTPBasicAuth

# Start server
server = arcadedb.create_server("/tmp/hybrid", "password123")
server.start()

try:
    # Create database using Java API (fastest)
    db = server.create_database("hybriddb")

    with db.transaction():
        db.command("sql", "CREATE DOCUMENT TYPE Person")
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

    # Query same data using HTTP API (remote access)
    auth = HTTPBasicAuth("root", "password123")
    response = requests.post(
        f"http://localhost:{server.get_http_port()}/api/v1/query/hybriddb",
        auth=auth,
        json={"language": "sql", "command": "SELECT FROM Person"}
    )

    result = response.json()
    print(f"HTTP API found {len(result['result'])} records")
    print(f"Record from HTTP: {result['result'][0]}")

finally:
    server.stop()
```

## Performance Comparison

| Aspect | Java API | HTTP API | Notes |
|--------|----------|----------|-------|
| **Initialization** | ~0.5s | ~0.8s | HTTP has connection overhead |
| **Insert Rate** | ~1000/s | ~300/s | Network + JSON serialization |
| **Query Rate** | ~500/s | ~200/s | Result serialization overhead |
| **Memory** | Lower | Higher | JSON serialization |
| **Latency** | ~1ms | ~5ms | Network round-trip |

*Performance numbers are approximate and depend on hardware, data size, and network conditions.*

## When to Use Each

### Use Java API When:
- Single Python process application
- Maximum performance required
- Complex data manipulation
- Batch processing
- Local development/testing

### Use HTTP API When:
- Multi-process architecture
- Remote database access
- Web applications/APIs
- Multiple programming languages
- Microservices architecture
- Cross-network access

### Use Both When:
- Local high-performance operations + remote monitoring
- Hybrid applications with embedded + web components
- Development (Java API) + production monitoring (HTTP API)

## Common Misconceptions

❌ **"Java API is only for Java"**
✅ Java API is Python calling Java via JPype - fully Pythonic

❌ **"HTTP API is inferior"**
✅ HTTP API enables remote access - different purpose

❌ **"Must choose one or the other"**
✅ Both can be used simultaneously on same server

❌ **"Performance difference means HTTP is broken"**
✅ Performance difference is expected (network vs direct calls)
