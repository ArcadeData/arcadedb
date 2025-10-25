# Operations Guide

Understanding ArcadeDB operations, file structure, logging, and maintenance for the Python bindings.

## Overview

ArcadeDB creates various files and directories during operation for different purposes:

- **Database files**: Core data storage (embedded mode)
- **Server files**: Multi-user configuration (server mode)
- **Log files**: Three types of logs in different locations
- **Transaction logs**: Write-ahead logging for durability
- **Backup files**: Database snapshots

Understanding this structure helps with monitoring, debugging, and maintenance.

## File Structure

### Typical Directory Layout

When running ArcadeDB applications, you'll see this structure:

```
your_project/
├── your_app.py
├── log/                          # JVM and ArcadeDB logs
│   ├── arcadedb.log.0           # Current ArcadeDB log
│   ├── arcadedb.log.1           # Rotated log file
│   ├── arcadedb.log.2           # Older rotated file
│   ├── arcadedb.log.0.lck       # Log lock file
│   ├── hs_err_pid12345.log      # JVM crash dump
│   └── hs_err_pid67890.log      # Another crash dump
├── my_databases/                 # Your databases root
│   ├── production_db/           # Embedded database
│   │   ├── configuration.json   # Database config
│   │   ├── schema.json          # Schema definition
│   │   ├── statistics.json      # Performance stats
│   │   ├── dictionary.*.dict    # String compression
│   │   ├── MyType_*.bucket      # Data files
│   │   ├── txlog_*.wal          # Transaction logs
│   │   └── database.lck         # Database lock
│   ├── backups/                 # Backup directory
│   ├── config/                  # Server configuration
│   │   ├── server-users.jsonl   # User accounts
│   │   └── server-groups.json   # Permissions
│   └── log/                     # Server event logs
│       ├── server-event-log-*.jsonl
│       └── server-event-log-*.jsonl
```

## Logging System

ArcadeDB has **three distinct types of logs** stored in **two different locations**:

### 1. JVM and ArcadeDB Application Logs

**Location**: `./log/` (relative to your Python script)

**Files**:
- `arcadedb.log.0` - Current application log
- `arcadedb.log.1`, `arcadedb.log.2`, etc. - Rotated logs
- `arcadedb.log.0.lck` - Log rotation lock file
- `hs_err_pid*.log` - JVM crash dumps

**Content**: Application-level events, database operations, performance info
```
2025-10-22 10:23:21.560 INFO  [ArcadeDBServer] ArcadeDB Server v25.10.1 starting up...
2025-10-22 10:23:21.563 INFO  [ArcadeDBServer] Running on Linux - OpenJDK 64-Bit Server VM 21.0.4
2025-10-22 10:23:21.628 INFO  [ArcadeDBServer] Server root path: /path/to/databases
2025-10-22 10:23:21.928 INFO  [HttpServer] HTTP Server started (port=2480)
```

### 2. JVM Crash Dumps

**Location**: `./log/` (same directory as application logs)

**Files**: `hs_err_pid<process_id>.log`

**Content**: JVM fatal error information (crashes, segfaults)
```
# A fatal error has been detected by the Java Runtime Environment:
# SIGSEGV (0xb) at pc=0x000000000056950b, pid=1194752
# JRE version: OpenJDK Runtime Environment Temurin-21.0.4+7
# Problematic frame: C  [python+0x16950b]  PyObject_RichCompareBool+0x3b
```

### 3. Server Event Logs (Server Mode Only)

**Location**: `<server_root>/log/` (inside server root directory)

**Files**: `server-event-log-<timestamp>.<sequence>.jsonl`

**Content**: Structured server lifecycle events in JSON Lines format
```json
{"time":"2025-10-22 10:22:47.888","type":"INFO","component":"Server","db":null,"message":"ArcadeDB Server started in 'development' mode"}
{"time":"2025-10-22 10:22:49.059","type":"INFO","component":"Server","db":null,"message":"Server shutdown correctly"}
```

### Log Configuration

**Rotation**: Application logs rotate automatically:
- Current: `arcadedb.log.0`
- Previous: `arcadedb.log.1`, `arcadedb.log.2`, etc.
- Default: 5 files, 10MB each

**Verbosity**: Controlled by Java system properties:
```python
import jpype

# Set before importing arcadedb_embedded
jpype.startJVM(
    "-Djava.util.logging.level=DEBUG",
    "-Darcadedb.log.level=FINE"
)

import arcadedb_embedded as arcadedb
```

## Database Files (Embedded Mode)

### Core Database Files

**configuration.json** - Database settings
```json
{
  "configuration": {
    "timezone": "UTC",
    "dateFormat": "yyyy-MM-dd"
  }
}
```

**schema.json** - Schema definition with types, properties, indexes
```json
{
  "schemaVersion": 19,
  "dbmsVersion": "25.10.1-SNAPSHOT",
  "types": {
    "User": {
      "type": "d",
      "parents": [],
      "buckets": ["User_0"],
      "properties": {
        "email": {"type": "STRING", "mandatory": true}
      },
      "indexes": {
        "User.email": {"type": "LSM_TREE", "unique": true}
      }
    }
  }
}
```

**statistics.json** - Performance and storage statistics
```json
{
  "User_0": {
    "pages": [
      {"id": 0, "free": 57314},
      {"id": 1, "free": 45123}
    ]
  }
}
```

### Data Storage Files

**Type_N.M.P.vQ.bucket** - Actual data storage
- `Type`: Document/Vertex/Edge type name
- `N`: Bucket number
- `M`: Sub-bucket
- `P`: Page size
- `Q`: Version

Example: `User_0.1.65536.v0.bucket`

**dictionary.X.Y.vZ.dict** - String compression dictionary
- Frequently used strings stored once
- Referenced by ID in data files
- Reduces storage space significantly

### Transaction Files

**txlog_N.wal** - Write-Ahead Log files
- Ensures ACID properties
- Transaction durability
- Crash recovery
- Auto-rotation when full

**database.lck** - Database lock file
- Prevents concurrent access
- Contains process information
- Removed on clean shutdown

## Server Files (Server Mode Only)

### Configuration Files

**server-users.jsonl** - User accounts (JSON Lines format)
```jsonl
{"name":"root","databases":{"*":["admin"]},"password":"PBKDF2WithHmacSHA256$..."}
{"name":"user1","databases":{"mydb":["read"]},"password":"PBKDF2WithHmacSHA256$..."}
```

**server-groups.json** - Permission groups
```json
{
  "databases": {
    "*": {
      "groups": {
        "admin": {
          "resultSetLimit": -1,
          "access": ["updateSecurity", "updateSchema"],
          "types": {
            "*": {
              "access": ["createRecord", "readRecord", "updateRecord", "deleteRecord"]
            }
          }
        }
      }
    }
  }
}
```

### Directory Structure

**backups/** - Database backup storage
- Created by backup operations
- Timestamped directories
- Full database copies

**databases/** - Individual database directories
- Each database in separate subdirectory
- Same structure as embedded databases

## Monitoring and Maintenance

### Log Monitoring

**Watch application logs**:
```bash
# Follow current log
tail -f log/arcadedb.log.0

# Check for errors
grep -i error log/arcadedb.log.*

# Monitor server events (server mode)
tail -f my_databases/log/server-event-log-*.jsonl
```

**Python log monitoring**:
```python
import logging

# Enable Python logging to see ArcadeDB events
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

import arcadedb_embedded as arcadedb
# Now you'll see database operations in Python logs
```

### Disk Space Monitoring

```python
import os
import shutil

def get_database_size(db_path):
    """Get total database size in MB."""
    total_size = 0
    for root, dirs, files in os.walk(db_path):
        for file in files:
            file_path = os.path.join(root, file)
            total_size += os.path.getsize(file_path)
    return total_size / (1024 * 1024)

def get_log_size():
    """Get log directory size in MB."""
    if os.path.exists("./log"):
        return get_database_size("./log")
    return 0

# Monitor usage
db_size = get_database_size("./my_database")
log_size = get_log_size()

print(f"Database: {db_size:.1f} MB")
print(f"Logs: {log_size:.1f} MB")

# Alert if too large
if db_size > 1000:  # 1GB
    print("⚠️  Database size is large, consider archiving")
```

### Log Rotation and Cleanup

**Automatic rotation**: ArcadeDB rotates application logs automatically

**Manual cleanup**:
```python
import os
import glob
from datetime import datetime, timedelta

def cleanup_old_logs(days_to_keep=30):
    """Remove old log files."""
    cutoff = datetime.now() - timedelta(days=days_to_keep)

    # Clean JVM crash dumps
    for log_file in glob.glob("./log/hs_err_pid*.log"):
        if os.path.getctime(log_file) < cutoff.timestamp():
            os.remove(log_file)
            print(f"Removed old crash dump: {log_file}")

    # Clean old server event logs
    for log_file in glob.glob("./my_databases/log/server-event-log-*.jsonl"):
        if os.path.getctime(log_file) < cutoff.timestamp():
            os.remove(log_file)
            print(f"Removed old server log: {log_file}")

# Run weekly
cleanup_old_logs(days_to_keep=7)
```

### Database Maintenance

**Check database integrity**:
```python
def check_database_health(db_path):
    """Basic database health check."""
    health = {
        "exists": os.path.exists(db_path),
        "has_schema": os.path.exists(f"{db_path}/schema.json"),
        "has_config": os.path.exists(f"{db_path}/configuration.json"),
        "is_locked": os.path.exists(f"{db_path}/database.lck"),
        "wal_files": len(glob.glob(f"{db_path}/txlog_*.wal")),
        "data_files": len(glob.glob(f"{db_path}/*_*.bucket"))
    }

    print(f"Database Health Check: {db_path}")
    for key, value in health.items():
        status = "✅" if value else "❌"
        print(f"  {status} {key}: {value}")

    return health

# Check database
health = check_database_health("./my_database")

if health["is_locked"]:
    print("⚠️  Database is currently in use")
```

## Backup and Recovery

### Database Backup

```python
import shutil
from datetime import datetime

def backup_database(source_path, backup_dir):
    """Create timestamped database backup."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{backup_dir}/backup_{timestamp}"

    # Database must be closed for consistent backup
    if os.path.exists(f"{source_path}/database.lck"):
        raise Exception("Database is open - close before backup")

    shutil.copytree(source_path, backup_path)
    print(f"Backup created: {backup_path}")
    return backup_path

# Usage
backup_path = backup_database("./my_database", "./backups")
```

### Log Archiving

```python
def archive_logs(archive_dir):
    """Archive log files."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = f"{archive_dir}/logs_{timestamp}"

    if os.path.exists("./log"):
        shutil.copytree("./log", f"{archive_path}/application_logs")

    if os.path.exists("./my_databases/log"):
        shutil.copytree("./my_databases/log", f"{archive_path}/server_logs")

    print(f"Logs archived to: {archive_path}")
    return archive_path

# Archive before cleanup
archive_path = archive_logs("./archives")
cleanup_old_logs(days_to_keep=7)
```

## Troubleshooting Common Issues

### Database Won't Start

**Check lock file**:
```python
lock_file = "./my_database/database.lck"
if os.path.exists(lock_file):
    print("Database is locked")

    # Read lock file to see which process
    with open(lock_file, 'r') as f:
        content = f.read()
        print(f"Lock content: {content}")

    # If process is dead, remove lock
    # Only do this if you're sure!
    # os.remove(lock_file)
```

### High Disk Usage

**Check component sizes**:
```python
def analyze_disk_usage(db_path):
    """Break down disk usage by component."""
    usage = {
        "data_files": 0,
        "transaction_logs": 0,
        "schema_files": 0,
        "other": 0
    }

    for root, dirs, files in os.walk(db_path):
        for file in files:
            file_path = os.path.join(root, file)
            size = os.path.getsize(file_path)

            if ".bucket" in file:
                usage["data_files"] += size
            elif ".wal" in file:
                usage["transaction_logs"] += size
            elif file in ["schema.json", "configuration.json", "statistics.json"]:
                usage["schema_files"] += size
            else:
                usage["other"] += size

    # Convert to MB and display
    for component, size in usage.items():
        size_mb = size / (1024 * 1024)
        print(f"{component}: {size_mb:.1f} MB")

analyze_disk_usage("./my_database")
```

### Performance Issues

**Monitor transaction log growth**:
```python
def monitor_wal_files(db_path):
    """Monitor write-ahead log file growth."""
    wal_files = glob.glob(f"{db_path}/txlog_*.wal")

    total_size = 0
    file_count = len(wal_files)

    for wal_file in wal_files:
        size = os.path.getsize(wal_file)
        total_size += size
        print(f"{os.path.basename(wal_file)}: {size / 1024:.1f} KB")

    avg_size = total_size / file_count if file_count > 0 else 0

    print(f"\nSummary:")
    print(f"  Total WAL files: {file_count}")
    print(f"  Total size: {total_size / (1024 * 1024):.1f} MB")
    print(f"  Average size: {avg_size / 1024:.1f} KB")

    if file_count > 50:
        print("⚠️  Many WAL files - consider checkpoint")

monitor_wal_files("./my_database")
```

## Best Practices

### 1. Log Management

- Monitor log directory size regularly
- Archive old logs before deletion
- Set up log rotation alerts
- Keep crash dumps for debugging

### 2. Database Maintenance

- Backup before major operations
- Monitor WAL file growth
- Check database health regularly
- Clean up test databases

### 3. Development vs Production

**Development**:
```python
# Relaxed settings for development
import logging
logging.basicConfig(level=logging.DEBUG)

# Keep logs for debugging
cleanup_old_logs(days_to_keep=30)
```

**Production**:
```python
# Minimal logging for production
import logging
logging.basicConfig(level=logging.WARNING)

# Aggressive cleanup to save space
cleanup_old_logs(days_to_keep=7)

# Monitor disk usage
if get_database_size("./production_db") > 5000:  # 5GB
    send_alert("Database size warning")
```

### 4. File System Layout

**Recommended structure**:
```
/app/
├── src/
│   └── my_app.py
├── data/
│   ├── databases/
│   │   ├── production/
│   │   └── staging/
│   ├── backups/
│   └── archives/
├── logs/                  # Symlink to ./log for clarity
└── config/
```

**Benefits**:
- Clear separation of concerns
- Easy to backup data directory
- Logs isolated from application
- Configuration external to code

## See Also

- [Database Management](core/database.md) - Database lifecycle and configuration
- [Server Mode](server.md) - Multi-user server setup
- [Troubleshooting](../development/troubleshooting.md) - Common issues and solutions
- [Architecture](../development/architecture.md) - System design overview
