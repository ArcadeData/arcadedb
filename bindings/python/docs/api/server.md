# Server API

The `ArcadeDBServer` class enables HTTP API access and the Studio web interface for managing and querying ArcadeDB databases. Perfect for interactive exploration, debugging, and web applications.

## Overview

The server provides:

- **HTTP REST API**: Query databases via HTTP endpoints
- **Studio Web UI**: Visual database exploration and query editor
- **Multi-database Management**: Create and access multiple databases
- **Remote Access**: Access from other applications via HTTP
- **GraphQL/Gremlin Support**: Extended query languages (with full distribution)

**When to Use Server Mode:**

- Interactive development and debugging
- Web applications needing HTTP API
- Visual data exploration via Studio
- Remote database access
- GraphQL or Gremlin queries

**When to Use Embedded Mode:**

- Python-only applications
- Maximum performance (no HTTP overhead)
- Simplified deployment
- Reduced memory footprint

## Module Function

### `create_server(root_path="./databases", root_password=None, config=None)`

Create an ArcadeDB server instance.

**Parameters:**

- `root_path` (str): Root directory for databases (default: `"./databases"`)
  - All databases will be created under this directory
  - Automatically created if it doesn't exist
- `root_password` (Optional[str]): Root user password (default: `None`)
  - **Strongly recommended for production**
  - If `None`, uses default password (insecure!)
- `config` (Optional[Dict[str, Any]]): Configuration dictionary (default: `None`)
  - `http_port` (int): HTTP API port (default: 2480)
  - `binary_port` (int): Binary protocol port (default: 2424)
  - `host` (str): Host to bind to (default: "0.0.0.0")
  - `mode` (str): Server mode - "development" or "production" (default: "development")
  - Additional ArcadeDB configuration keys (see Advanced Configuration)

**Returns:**

- `ArcadeDBServer`: Server instance (not started)

**Example:**

```python
import arcadedb_embedded as arcadedb

# Basic server (development)
server = arcadedb.create_server()

# Custom root path and password
server = arcadedb.create_server(
    root_path="./my_databases",
    root_password="my_secure_password123"
)

# Custom configuration
server = arcadedb.create_server(
    root_path="./dbs",
    root_password="secret",
    config={
        "http_port": 8080,
        "host": "127.0.0.1",
        "mode": "production"
    }
)
```

---

## ArcadeDBServer Class

### Constructor

```python
ArcadeDBServer(
    root_path: str = "./databases",
    root_password: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
)
```

**Prefer using `create_server()` function instead.**

**Parameters:** Same as `create_server()`

---

### `start()`

Start the ArcadeDB server and begin listening for connections.

**Raises:**

- `ArcadeDBError`: If server is already started or fails to start

**Example:**

```python
import arcadedb_embedded as arcadedb

server = arcadedb.create_server()
server.start()

print(f"Server running at: {server.get_studio_url()}")

# ... do work ...

server.stop()
```

---

### `stop()`

Stop the ArcadeDB server and release resources.

**Note:** It's safe to call even if server isn't started.

**Example:**

```python
server = arcadedb.create_server()
server.start()

try:
    # Do work
    pass
finally:
    server.stop()  # Always stop to clean up
```

---

### `is_started() -> bool`

Check if the server is currently running.

**Returns:**

- `bool`: `True` if server is running, `False` otherwise

**Example:**

```python
server = arcadedb.create_server()
print(server.is_started())  # False

server.start()
print(server.is_started())  # True

server.stop()
print(server.is_started())  # False
```

---

### `get_database(name: str) -> Database`

Get an existing database from the server.

**Parameters:**

- `name` (str): Database name

**Returns:**

- `Database`: Database instance

**Raises:**

- `ArcadeDBError`: If server not started or database doesn't exist

**Example:**

```python
server = arcadedb.create_server()
server.start()

# Get existing database
db = server.get_database("mydb")

# Use database
result = db.query("sql", "SELECT FROM Person")
for record in result:
    print(record.to_dict())

db.close()
server.stop()
```

---

### `create_database(name: str) -> Database`

Create a new database on the server.

**Parameters:**

- `name` (str): Database name
  - Alphanumeric and underscores recommended
  - Will be created under `root_path/{name}/`

**Returns:**

- `Database`: New database instance

**Raises:**

- `ArcadeDBError`: If server not started or creation fails

**Example:**

```python
server = arcadedb.create_server()
server.start()

# Create new database
db = server.create_database("products_db")

# Create schema
with db.transaction():
    db.command("sql", "CREATE DOCUMENT TYPE Product")
    db.command("sql", "CREATE PROPERTY Product.name STRING")
    db.command("sql", "CREATE PROPERTY Product.price DECIMAL")

db.close()
server.stop()
```

**Important:** This method properly registers the database with the server, making it immediately visible in Studio UI.

---

### `get_http_port() -> int`

Get the HTTP port the server is listening on.

**Returns:**

- `int`: HTTP port number

**Example:**

```python
server = arcadedb.create_server(config={"http_port": 8080})
print(server.get_http_port())  # 8080
```

---

### `get_studio_url() -> str`

Get the full URL for the Studio web interface.

**Returns:**

- `str`: Studio URL (e.g., `"http://localhost:2480/"`)

**Example:**

```python
server = arcadedb.create_server()
server.start()

print(f"Open Studio at: {server.get_studio_url()}")
# Open Studio at: http://localhost:2480/
```

---

### Context Manager Support

The server supports Python context managers for automatic start/stop:

```python
import arcadedb_embedded as arcadedb

with arcadedb.create_server() as server:
    # Server automatically started
    db = server.create_database("temp_db")

    # Use database
    with db.transaction():
        doc = db.new_document("Test")
        doc.set("value", 42)
        doc.save()

    db.close()

# Server automatically stopped when exiting 'with' block
```

---

## Configuration Options

### Basic Configuration

```python
config = {
    "http_port": 2480,           # HTTP API port
    "binary_port": 2424,         # Binary protocol port (for Java clients)
    "host": "0.0.0.0",           # Bind address (0.0.0.0 = all interfaces)
    "mode": "development",       # "development" or "production"
}

server = arcadedb.create_server(config=config)
```

### Mode Comparison

| Setting | Development | Production |
|---------|-------------|------------|
| CORS | Enabled | Disabled |
| Debug Logging | Verbose | Minimal |
| Error Details | Full stack traces | Generic messages |
| Performance Checks | Enabled | Disabled |

**Recommendation:** Use `"development"` for local dev, `"production"` for deployment.

---

### Advanced Configuration

You can pass any ArcadeDB configuration via the `config` dict:

```python
config = {
    "http_port": 8080,
    "mode": "production",
    # Additional ArcadeDB settings (with _ instead of .)
    "server_database_directory": "./custom_dbs",
    "server_http_session_expire": "30m",
    "profile_default": "high-performance"
}

server = arcadedb.create_server(config=config)
```

**Note:** Python uses underscores (`_`), which are automatically converted to dots (`.`) for Java config keys:

- `server_http_session_expire` → `arcadedb.server.http.session.expire`

---

## Logging Configuration

ArcadeDB writes logs to multiple locations:

### 1. Application Logs

**Location:** `./log/arcadedb.log.*` (relative to working directory)

**Content:** Server startup, database operations, errors

**Cannot be changed** (hardcoded in Java)

### 2. Server Event Logs

**Location:** `{root_path}/log/server-event-log-*.jsonl`

**Content:** HTTP requests, connections, events

**Example:** `./databases/log/server-event-log-2024-01-15.jsonl`

### 3. JVM Crash Logs

**Location:** `./log/hs_err_pid*.log` (default)

**Customize BEFORE importing:**

```python
import os

# Set custom crash log location
os.environ["ARCADEDB_JVM_ERROR_FILE"] = "/var/log/arcade/errors.log"

# Now import and use
import arcadedb_embedded as arcadedb

server = arcadedb.create_server()
# Crash logs will go to /var/log/arcade/errors.log
```

**Important:** Must be set before importing `arcadedb_embedded`.

---

## Complete Examples

### Basic Server with Studio

```python
import arcadedb_embedded as arcadedb

# Create and start server
server = arcadedb.create_server(
    root_path="./databases",
    root_password="admin123"
)
server.start()

print(f"Studio available at: {server.get_studio_url()}")
print("Press Ctrl+C to stop...")

try:
    # Keep server running
    import time
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping server...")
    server.stop()
```

**Usage:**

1. Run the script
2. Open browser to `http://localhost:2480/`
3. Login with username `root` and password `admin123`
4. Create and query databases via Studio UI

---

### Web Application Pattern

```python
import arcadedb_embedded as arcadedb
from flask import Flask, jsonify

app = Flask(__name__)

# Create server (singleton)
server = arcadedb.create_server(
    root_path="./app_databases",
    root_password="secure_password",
    config={"http_port": 2480, "mode": "production"}
)

@app.before_first_request
def startup():
    """Start ArcadeDB server on first request."""
    server.start()

    # Create database if needed
    try:
        db = server.get_database("app_db")
    except:
        db = server.create_database("app_db")
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE User")
            db.command("sql", "CREATE PROPERTY User.email STRING")
            db.command("sql", "CREATE INDEX ON User (email) UNIQUE")
    finally:
        db.close()

@app.route("/users")
def get_users():
    """Get all users via ArcadeDB Python API."""
    db = server.get_database("app_db")

    try:
        result_set = db.query("sql", "SELECT * FROM User")
        users = [result.to_dict() for result in result_set]
        return jsonify(users)
    finally:
        db.close()

@app.route("/studio")
def studio_link():
    """Provide link to Studio for admins."""
    return jsonify({"studio_url": server.get_studio_url()})

if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=5000)
    finally:
        server.stop()
```

---

### Multi-Database Server

```python
import arcadedb_embedded as arcadedb

# Context manager for automatic cleanup
with arcadedb.create_server() as server:
    # Create multiple databases
    users_db = server.create_database("users")
    products_db = server.create_database("products")
    analytics_db = server.create_database("analytics")

    # Initialize schemas
    with users_db.transaction():
        users_db.command("sql", "CREATE VERTEX TYPE User")
        users_db.command("sql", "CREATE PROPERTY User.email STRING")

    with products_db.transaction():
        products_db.command("sql", "CREATE DOCUMENT TYPE Product")
        products_db.command("sql", "CREATE PROPERTY Product.sku STRING")

    # Use databases
    with users_db.transaction():
        user = users_db.new_vertex("User")
        user.set("email", "alice@example.com")
        user.save()

    with products_db.transaction():
        product = products_db.new_document("Product")
        product.set("sku", "PROD-001")
        product.save()

    # Query different databases
    print("Users:")
    result = users_db.query("sql", "SELECT FROM User")
    for r in result:
        print(f"  {r.get_property('email')}")

    print("Products:")
    result = products_db.query("sql", "SELECT FROM Product")
    for r in result:
        print(f"  {r.get_property('sku')}")

    # Close databases
    users_db.close()
    products_db.close()
    analytics_db.close()

# Server automatically stopped
print("All databases shut down cleanly")
```

---

### Development Server with Auto-Reload

```python
import arcadedb_embedded as arcadedb
import time
import signal
import sys

# Global server instance
server = None

def shutdown_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    print("\nShutting down server...")
    if server:
        server.stop()
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, shutdown_handler)

# Start server
server = arcadedb.create_server(
    root_path="./dev_databases",
    root_password="dev",
    config={
        "http_port": 2480,
        "mode": "development"
    }
)
server.start()

# Create/open dev database
try:
    db = server.get_database("dev")
except:
    db = server.create_database("dev")
    print("Created new dev database")

# Initialize schema
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE TestNode IF NOT EXISTS")
    db.command("sql", "CREATE EDGE TYPE TestEdge IF NOT EXISTS")

db.close()

print(f"\n{'=' * 60}")
print(f"Development Server Running")
print(f"{'=' * 60}")
print(f"Studio UI:  {server.get_studio_url()}")
print(f"HTTP API:   http://localhost:{server.get_http_port()}/api/v1/")
print(f"Database:   'dev' (password: 'dev')")
print(f"\nPress Ctrl+C to stop")
print(f"{'=' * 60}\n")

# Keep running
while True:
    time.sleep(1)
```

---

### Production Deployment

```python
import arcadedb_embedded as arcadedb
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Production configuration
ROOT_PASSWORD = os.environ.get("ARCADEDB_ROOT_PASSWORD", "changeme")
ROOT_PATH = os.environ.get("ARCADEDB_ROOT_PATH", "/var/lib/arcadedb")
HTTP_PORT = int(os.environ.get("ARCADEDB_HTTP_PORT", "2480"))

if ROOT_PASSWORD == "changeme":
    logger.warning("Using default password - INSECURE for production!")

# Create production server
server = arcadedb.create_server(
    root_path=ROOT_PATH,
    root_password=ROOT_PASSWORD,
    config={
        "http_port": HTTP_PORT,
        "host": "0.0.0.0",
        "mode": "production",
        "server_http_session_expire": "30m",
        "profile_default": "high-performance"
    }
)

try:
    server.start()
    logger.info(f"Production server started on port {HTTP_PORT}")
    logger.info(f"Database path: {ROOT_PATH}")

    # Keep running
    import time
    while True:
        time.sleep(60)
        logger.debug("Server heartbeat - still running")

except KeyboardInterrupt:
    logger.info("Shutdown signal received")
except Exception as e:
    logger.error(f"Server error: {e}")
finally:
    server.stop()
    logger.info("Server stopped")
```

**Deployment:**

```bash
# Set environment variables
export ARCADEDB_ROOT_PASSWORD="super_secure_password_123"
export ARCADEDB_ROOT_PATH="/data/arcadedb"
export ARCADEDB_HTTP_PORT="8080"

# Run server
python production_server.py
```

---

## HTTP REST API

Once the server is running, you can access databases via HTTP:

### Query via HTTP

```bash
# POST query
curl -X POST http://localhost:2480/api/v1/query/mydb \
  -u root:password \
  -H "Content-Type: application/json" \
  -d '{
    "language": "sql",
    "command": "SELECT FROM Person WHERE age > 25"
  }'

# GET simple query
curl "http://localhost:2480/api/v1/query/mydb/sql/SELECT%20*%20FROM%20Person" \
  -u root:password
```

### Create Record via HTTP

```bash
curl -X POST http://localhost:2480/api/v1/command/mydb \
  -u root:password \
  -H "Content-Type: application/json" \
  -d '{
    "language": "sql",
    "command": "INSERT INTO Person SET name = '\''Alice'\'', age = 30"
  }'
```

### Full REST API Documentation

See the [official ArcadeDB HTTP API documentation](https://docs.arcadedb.com/#HTTP-API) for complete endpoint reference.

---

## Error Handling

```python
from arcadedb_embedded import ArcadeDBError

try:
    server = arcadedb.create_server()
    server.start()

    # May fail if database doesn't exist
    db = server.get_database("nonexistent")

except ArcadeDBError as e:
    print(f"Error: {e}")
finally:
    if server.is_started():
        server.stop()
```

**Common Errors:**

- **Port already in use**: Another process using port 2480
  - Solution: Change `http_port` in config or stop conflicting process
- **Permission denied**: Cannot write to `root_path`
  - Solution: Check directory permissions
- **Database not found**: `get_database()` on non-existent DB
  - Solution: Use `create_database()` first

---

## See Also

- [Server Mode Guide](../guide/server.md) - Comprehensive server usage guide
- [Database API](database.md) - Database operations
- [Getting Started](../index.md) - Quick start tutorial
- [ArcadeDB HTTP API Docs](https://docs.arcadedb.com/#HTTP-API) - Official REST API reference
