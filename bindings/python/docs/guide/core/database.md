# Database Management

Comprehensive guide to database lifecycle, configuration, and resource management in ArcadeDB Python bindings.

## Overview

ArcadeDB databases are embedded, file-based databases stored on your local filesystem. The database lifecycle includes creation, opening, operations, and cleanup.

**Database Modes:**

- **Embedded**: Database runs in your Python process
- **In-Process**: Direct Java API access via JPype
- **File-Based**: Data persisted to disk

## Quick Start

### Create New Database

```python
import arcadedb_embedded as arcadedb

# Simple creation
db = arcadedb.create_database("./mydb")

# Use the database
with db.transaction():
    vertex = db.new_vertex("User")
    vertex.set("name", "Alice")
    vertex.save()

# Always close when done
db.close()
```

### Open Existing Database

```python
# Open existing database
db = arcadedb.open_database("./mydb")

# Query data
result = db.query("sql", "SELECT FROM User")
for user in result:
    print(user.get("name"))

db.close()
```

### Check if Database Exists

```python
if arcadedb.database_exists("./mydb"):
    db = arcadedb.open_database("./mydb")
else:
    db = arcadedb.create_database("./mydb")
```

## Database Lifecycle

### 1. Creation

```python
import arcadedb_embedded as arcadedb

# Create new database
db = arcadedb.create_database("./mydb")

# Database is ready to use
print(f"Database created at: {db.get_name()}")

db.close()
```

**What happens during creation:**

1. Directory `./mydb` created on filesystem
2. Schema files initialized
3. System files created (dictionary, configuration)
4. Database opened in READ_WRITE mode
5. Ready for operations

### 2. Opening

```python
# Open existing database
db = arcadedb.open_database("./mydb")

# Database is ready for operations
print(f"Database opened: {db.get_name()}")

db.close()
```

**Opening modes:**

- `READ_WRITE` (default): Full access
- `READ_ONLY`: Query-only access (coming soon to Python API)

### 3. Using

```python
db = arcadedb.open_database("./mydb")

# Create schema
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE PROPERTY Person.name STRING")

# Insert data
with db.transaction():
    person = db.new_vertex("Person")
    person.set("name", "Bob")
    person.save()

# Query data
result = db.query("sql", "SELECT FROM Person")
for person in result:
    print(person.get("name"))

db.close()
```

### 4. Closing

```python
# Explicit close
db.close()

# Or use context manager (recommended)
with arcadedb.open_database("./mydb") as db:
    # Use database
    pass
# Automatically closed
```

**What happens during close:**

1. Active transactions rolled back
2. Buffers flushed to disk
3. Files closed
4. Resources released
5. Database removed from active instances

### 5. Dropping

```python
db = arcadedb.open_database("./mydb")

# Drop database (deletes all files)
db.drop()

# Database and all files are permanently deleted
```

⚠️ **Warning**: `drop()` is irreversible and deletes all data!

## DatabaseFactory Class

For advanced use cases, use `DatabaseFactory` directly:

```python
import arcadedb_embedded as arcadedb

# Create factory
factory = arcadedb.DatabaseFactory("./mydb")

# Check existence
if factory.exists():
    print("Database exists")
    db = factory.open()
else:
    print("Creating new database")
    db = factory.create()

# Use database
with db.transaction():
    # Operations
    pass

db.close()
```

### Factory Pattern Benefits

- **Explicit control**: Clear separation of creation/opening logic
- **Reusability**: One factory for multiple operations
- **Configuration**: Set options before creating/opening

## Context Managers

### Database Context Manager

```python
# Automatic resource cleanup
with arcadedb.open_database("./mydb") as db:
    # Use database
    result = db.query("sql", "SELECT FROM User")
    for row in result:
        print(row.get("name"))
# Database automatically closed on exit
```

**Benefits:**

- ✅ Automatic close on normal exit
- ✅ Automatic close on exception
- ✅ Guaranteed resource cleanup
- ✅ Pythonic style

### Nested Context Managers

```python
# Open database and use transaction
with arcadedb.open_database("./mydb") as db:
    with db.transaction():
        vertex = db.new_vertex("User")
        vertex.set("name", "Charlie")
        vertex.save()
# Transaction committed, database closed
```

## Configuration

### Database Directory Structure

```
./mydb/
├── configuration.json       # Database configuration
├── schema.json             # Schema definition
├── schema.prev.json        # Schema backup
├── dictionary.*.dict       # String dictionary
├── statistics.json         # Database statistics
├── User_0.*.bucket        # User type data files
├── HasFriend_0.*.bucket   # Edge type data files
└── .lock                  # Lock file (when open)
```

### Database Location

```python
import os

# Relative path
db = arcadedb.create_database("./mydb")

# Absolute path
db = arcadedb.create_database("/var/data/mydb")

# User home directory
home = os.path.expanduser("~")
db = arcadedb.create_database(f"{home}/databases/mydb")
```

### Database Naming

**Rules:**

- ✅ Use alphanumeric characters
- ✅ Use underscores and hyphens
- ✅ Use forward slashes for paths
- ❌ Avoid spaces in names
- ❌ Avoid special characters

```python
# Good names
arcadedb.create_database("./my_database")
arcadedb.create_database("./project-data")
arcadedb.create_database("./data/production/main")

# Bad names
arcadedb.create_database("./my database")      # Space
arcadedb.create_database("./data@2024")        # Special char
```

## Resource Management

### Proper Cleanup

```python
# ✓ Recommended: Context manager
with arcadedb.open_database("./mydb") as db:
    # Use database
    pass

# ✓ Acceptable: Explicit close
db = arcadedb.open_database("./mydb")
try:
    # Use database
    pass
finally:
    db.close()

# ✗ Bad: No cleanup
db = arcadedb.open_database("./mydb")
# Operations...
# Database never closed!
```

### Multiple Databases

```python
# Open multiple databases simultaneously
db1 = arcadedb.open_database("./database1")
db2 = arcadedb.open_database("./database2")

try:
    # Use both databases
    result1 = db1.query("sql", "SELECT FROM User")
    result2 = db2.query("sql", "SELECT FROM Product")
finally:
    db1.close()
    db2.close()

# Or with context managers
with arcadedb.open_database("./database1") as db1, \
     arcadedb.open_database("./database2") as db2:
    # Use both databases
    pass
```

### Database Locking

**Lock File:**

- Created when database opens: `.lock`
- Prevents concurrent access from same/different processes
- Automatically removed on clean close
- Manual removal only if process crashed

```python
# If database locked by another process
try:
    db = arcadedb.open_database("./mydb")
except Exception as e:
    print(f"Database locked: {e}")

    # Check if process is still running
    # If not, remove lock file
    import os
    lock_file = "./mydb/.lock"
    if os.path.exists(lock_file):
        os.remove(lock_file)
```

## Common Patterns

### Database Initialization

```python
def init_database(path: str):
    """Initialize database with schema."""
    # Create if doesn't exist
    if not arcadedb.database_exists(path):
        db = arcadedb.create_database(path)

        # Create schema
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE User")
            db.command("sql", "CREATE PROPERTY User.email STRING")
            db.command("sql", "CREATE INDEX ON User (email) UNIQUE")

            db.command("sql", "CREATE VERTEX TYPE Post")
            db.command("sql", "CREATE PROPERTY Post.title STRING")

            db.command("sql", "CREATE EDGE TYPE Authored")

        print(f"Database initialized at {path}")
        return db
    else:
        return arcadedb.open_database(path)

# Usage
db = init_database("./myapp")
```

### Database Reset

```python
def reset_database(path: str):
    """Drop and recreate database."""
    if arcadedb.database_exists(path):
        db = arcadedb.open_database(path)
        db.drop()
        print(f"Database dropped: {path}")

    db = arcadedb.create_database(path)
    print(f"Database created: {path}")
    return db

# Usage
db = reset_database("./mydb")
```

### Database Backup Pattern

```python
import shutil
import datetime

def backup_database(db_path: str, backup_dir: str):
    """Backup database files."""
    # Close database first
    if arcadedb.database_exists(db_path):
        db = arcadedb.open_database(db_path)
        db.close()  # Ensure clean state

    # Create backup with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{backup_dir}/backup_{timestamp}"

    # Copy database directory
    shutil.copytree(db_path, backup_path)
    print(f"Backup created: {backup_path}")

    return backup_path

# Usage
backup_path = backup_database("./mydb", "./backups")
```

### Database Migration

```python
def migrate_database(old_path: str, new_path: str):
    """Migrate data from old to new database."""
    # Open old database
    old_db = arcadedb.open_database(old_path)

    # Create new database
    new_db = arcadedb.create_database(new_path)

    try:
        # Copy schema
        with new_db.transaction():
            # Create types
            new_db.command("sql", "CREATE VERTEX TYPE User")
            new_db.command("sql", "CREATE VERTEX TYPE Post")
            new_db.command("sql", "CREATE EDGE TYPE Authored")

        # Copy data
        batch_size = 1000

        # Migrate users
        result = old_db.query("sql", "SELECT FROM User")
        users_batch = []
        for user in result:
            users_batch.append(user)

            if len(users_batch) >= batch_size:
                with new_db.transaction():
                    for u in users_batch:
                        new_user = new_db.new_vertex("User")
                        new_user.set("name", u.get("name"))
                        new_user.set("email", u.get("email"))
                        new_user.save()
                users_batch = []

        # Flush remaining
        if users_batch:
            with new_db.transaction():
                for u in users_batch:
                    new_user = new_db.new_vertex("User")
                    new_user.set("name", u.get("name"))
                    new_user.set("email", u.get("email"))
                    new_user.save()

        print("Migration complete")
    finally:
        old_db.close()
        new_db.close()

# Usage
migrate_database("./old_db", "./new_db")
```

### Singleton Database Pattern

```python
class DatabaseManager:
    """Singleton database manager."""

    _instance = None
    _db = None

    def __new__(cls, path: str):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._path = path
        return cls._instance

    def get_database(self):
        """Get or create database connection."""
        if self._db is None:
            if arcadedb.database_exists(self._path):
                self._db = arcadedb.open_database(self._path)
            else:
                self._db = arcadedb.create_database(self._path)
        return self._db

    def close(self):
        """Close database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None

# Usage
manager = DatabaseManager("./mydb")
db = manager.get_database()

# Use database
result = db.query("sql", "SELECT FROM User")

# Later...
manager.close()
```

## Database Information

### Get Database Name

```python
db = arcadedb.open_database("./mydb")
print(f"Database: {db.get_name()}")  # "mydb"
```

### Get Database Path

```python
db = arcadedb.open_database("./mydb")
print(f"Path: {db.get_name()}")
# Note: Currently returns name, full path API coming soon
```

### Check Transaction Status

```python
db = arcadedb.open_database("./mydb")

print(db.is_transaction_active())  # False

with db.transaction():
    print(db.is_transaction_active())  # True

print(db.is_transaction_active())  # False
```

## Error Handling

### Database Already Exists

```python
try:
    db = arcadedb.create_database("./mydb")
except Exception as e:
    print(f"Error: {e}")
    # Database already exists
    db = arcadedb.open_database("./mydb")
```

### Database Not Found

```python
try:
    db = arcadedb.open_database("./nonexistent")
except Exception as e:
    print(f"Database not found: {e}")
    # Create it
    db = arcadedb.create_database("./nonexistent")
```

### Database Locked

```python
import time

def open_with_retry(path, max_retries=3):
    """Open database with retry logic."""
    for attempt in range(max_retries):
        try:
            return arcadedb.open_database(path)
        except Exception as e:
            if "locked" in str(e).lower():
                if attempt < max_retries - 1:
                    print(f"Database locked, retry {attempt + 1}/{max_retries}")
                    time.sleep(1)
                    continue
            raise
    raise Exception("Failed to open database after retries")

# Usage
db = open_with_retry("./mydb")
```

### Graceful Shutdown

```python
import atexit
import signal

db = None

def cleanup():
    """Cleanup on exit."""
    global db
    if db is not None:
        try:
            if db.is_transaction_active():
                db.rollback()
            db.close()
            print("Database closed cleanly")
        except:
            pass

# Register cleanup handlers
atexit.register(cleanup)
signal.signal(signal.SIGTERM, lambda s, f: cleanup())
signal.signal(signal.SIGINT, lambda s, f: cleanup())

# Use database
db = arcadedb.open_database("./mydb")
```

## Best Practices

### 1. Always Close Databases

```python
# ✓ Use context managers
with arcadedb.open_database("./mydb") as db:
    pass

# ✓ Or explicit close in finally
db = arcadedb.open_database("./mydb")
try:
    pass
finally:
    db.close()
```

### 2. Check Existence Before Creating

```python
# ✓ Check first
if arcadedb.database_exists("./mydb"):
    db = arcadedb.open_database("./mydb")
else:
    db = arcadedb.create_database("./mydb")

# ✗ Don't blindly create
db = arcadedb.create_database("./mydb")  # Error if exists!
```

### 3. Use Absolute Paths in Production

```python
import os

# ✓ Absolute path
db_path = os.path.abspath("./mydb")
db = arcadedb.open_database(db_path)

# ✗ Relative paths can be ambiguous
db = arcadedb.open_database("./mydb")  # Depends on CWD
```

### 4. Initialize Schema on Creation

```python
def get_or_create_database(path):
    if arcadedb.database_exists(path):
        return arcadedb.open_database(path)

    # Create with schema
    db = arcadedb.create_database(path)

    with db.transaction():
        # Define schema here
        db.command("sql", "CREATE VERTEX TYPE User")
        db.command("sql", "CREATE INDEX ON User (email) UNIQUE")

    return db
```

### 5. Handle Concurrent Access

```python
# Only one process can open database at a time
# For multi-process: use server mode instead

# ✓ Single process, multiple threads
db = arcadedb.open_database("./mydb")
# Each thread uses same db instance

# ✗ Multiple processes opening same database
# Process 1: arcadedb.open_database("./mydb")
# Process 2: arcadedb.open_database("./mydb")  # ERROR!
```

### 6. Backup Before Dropping

```python
import shutil

def safe_drop(db_path, backup_path):
    """Drop database with backup."""
    # Backup first
    shutil.copytree(db_path, backup_path)

    # Then drop
    db = arcadedb.open_database(db_path)
    db.drop()

    print(f"Database dropped, backup at {backup_path}")
```

### 7. Monitor Database Size

```python
import os

def get_database_size(db_path):
    """Get database size in MB."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(db_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)
    return total_size / (1024 * 1024)  # MB

# Usage
size_mb = get_database_size("./mydb")
print(f"Database size: {size_mb:.2f} MB")
```

## Advanced Topics

### JVM Lifecycle and Databases

```python
# JVM starts on first import
import arcadedb_embedded as arcadedb  # JVM starts here

# JVM runs for entire Python process
db1 = arcadedb.create_database("./db1")
db1.close()

db2 = arcadedb.open_database("./db1")  # Same JVM
db2.close()

# JVM shuts down when Python exits
```

### Multiple Databases, One JVM

```python
# All databases share the same JVM
db1 = arcadedb.open_database("./database1")
db2 = arcadedb.open_database("./database2")
db3 = arcadedb.open_database("./database3")

# Efficient: shared JVM resources
# Remember to close all!
db1.close()
db2.close()
db3.close()
```

### Database Path Normalization

```python
# ArcadeDB normalizes paths
db1 = arcadedb.open_database("./mydb")
db2 = arcadedb.open_database("./mydb/")  # Same as above
db3 = arcadedb.open_database("mydb")     # Different! (no ./)

# Use consistent paths
db1.close()
```

## Troubleshooting

### "Database already exists"

```python
# Check first
if arcadedb.database_exists("./mydb"):
    db = arcadedb.open_database("./mydb")
else:
    db = arcadedb.create_database("./mydb")
```

### "Database not found"

```python
# Verify path
import os
db_path = "./mydb"
if not os.path.exists(db_path):
    print(f"Path doesn't exist: {db_path}")
    db = arcadedb.create_database(db_path)
```

### "Database is locked"

```python
# Another process has database open
# Solution 1: Close other process
# Solution 2: Remove .lock file if process crashed
import os
lock_file = "./mydb/.lock"
if os.path.exists(lock_file):
    os.remove(lock_file)
```

### Memory Usage

```python
import gc

# Force garbage collection after closing
db.close()
gc.collect()  # Clean up Java objects
```

## See Also

- [Database API Reference](../../api/database.md) - Complete API documentation
- [Transactions](transactions.md) - Transaction management
- [Quick Start](../../getting-started/quickstart.md) - Getting started guide
- [Server Mode](../../api/server.md) - Multi-process database access
