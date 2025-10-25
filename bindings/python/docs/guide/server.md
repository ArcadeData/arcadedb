# Server Mode

ArcadeDB Python bindings include a full HTTP server with the Studio web UI. This guide covers server setup, configuration, and management.

## Overview

Server mode provides:

- **HTTP REST API**: Access your database via HTTP
- **Studio Web UI**: Visual database explorer and query editor
- **Multi-database Management**: Host multiple databases
- **Authentication**: User management and security
- **Development & Production**: Suitable for both environments

## Quick Start

### Basic Server

Start a server with default configuration:

```python
import arcadedb_embedded as arcadedb

# Create and start server
server = arcadedb.create_server("./databases")
server.start()

print(f"🚀 Server started at: {server.get_studio_url()}")
print("📊 Access Studio UI in your browser")

# Keep server running
input("Press Enter to stop server...")
server.stop()
```

### Context Manager

Use a context manager for automatic cleanup:

```python
with arcadedb.create_server("./databases") as server:
    print(f"🚀 Server running at: {server.get_studio_url()}")

    # Server automatically stops on exit
    input("Press Enter to stop...")
```

## Server Configuration

### Basic Configuration

```python
server = arcadedb.create_server(
    root_path="./databases",
    root_password="my_secure_password",
    config={
        "http_port": 2480,
        "host": "0.0.0.0",
        "mode": "development"
    }
)
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `root_path` | `"./databases"` | Directory for database storage |
| `root_password` | None | Root user password (recommended) |
| `http_port` | 2480 | HTTP API/Studio port |
| `host` | `"0.0.0.0"` | Host to bind to |
| `mode` | `"development"` | Server mode (`development` or `production`) |

## Next Steps

- **[Graph Operations](graphs.md)**: Visualize graphs in Studio
- **[Vector Search](vectors.md)**: Add vector search to your server
- **[Data Import](import.md)**: Bulk import data into server databases
