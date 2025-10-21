# Exceptions API

The `ArcadeDBError` exception is the base class for all errors raised by the ArcadeDB Python bindings. It wraps underlying Java exceptions and provides Pythonic error handling.

## Overview

All errors from ArcadeDB operations raise `ArcadeDBError` or its subclasses (currently just the base class). This provides a single exception type to catch for all ArcadeDB-related errors.

**Error Sources:**

- **Schema violations**: Type mismatches, constraint failures, missing properties
- **Transaction errors**: Concurrent modifications, commit failures, rollbacks
- **Query errors**: Syntax errors, invalid queries, type errors
- **Database errors**: File I/O errors, corruption, not found
- **Server errors**: Connection failures, authentication errors, port conflicts
- **Resource errors**: Out of memory, too many open files

## ArcadeDBError Class

```python
class ArcadeDBError(Exception):
    """Base exception for ArcadeDB errors."""
    pass
```

**Inheritance:** `Exception` → `ArcadeDBError`

**Usage:**

```python
from arcadedb_embedded import ArcadeDBError

try:
    # ArcadeDB operation
    db.query("sql", "INVALID SYNTAX")
except ArcadeDBError as e:
    print(f"Database error: {e}")
```

---

## Common Error Patterns

### Database Not Found

```python
from arcadedb_embedded import ArcadeDBError, open_database

try:
    db = open_database("./nonexistent_db")
except ArcadeDBError as e:
    print(f"Error: {e}")
    # Error: Database does not exist: ./nonexistent_db
```

**Solution:** Use `database_exists()` to check first, or use `create_database()`.

---

### Query Syntax Error

```python
try:
    result = db.query("sql", "SELCT FROM Person")  # Typo: SELCT
except ArcadeDBError as e:
    print(f"Query error: {e}")
    # Query error: ... syntax error near 'SELCT'
```

**Solution:** Check query syntax, use proper SQL/Cypher/Gremlin.

---

### Schema Constraint Violation

```python
# Assuming User.email has UNIQUE constraint
try:
    with db.transaction():
        user1 = db.new_document("User")
        user1.set("email", "alice@example.com")
        user1.save()

        user2 = db.new_document("User")
        user2.set("email", "alice@example.com")  # Duplicate!
        user2.save()

except ArcadeDBError as e:
    print(f"Constraint violation: {e}")
    # Constraint violation: ... duplicate key ... email
```

**Solution:** Check for existing records before insert, handle duplicates gracefully.

---

### Type Mismatch

```python
# Assuming Person.age is INTEGER
try:
    with db.transaction():
        person = db.new_document("Person")
        person.set("age", "not a number")  # Wrong type!
        person.save()

except ArcadeDBError as e:
    print(f"Type error: {e}")
    # Type error: ... cannot convert 'not a number' to INTEGER
```

**Solution:** Ensure data types match schema definitions.

---

### Transaction Error

```python
try:
    # Start transaction
    db.begin()

    doc = db.new_document("Test")
    doc.set("data", "value")
    doc.save()

    # Try to start another (not allowed)
    db.begin()  # Error!

except ArcadeDBError as e:
    print(f"Transaction error: {e}")
    db.rollback()
```

**Solution:** Use context managers (`with db.transaction()`) to avoid manual transaction management errors.

---

### Property Not Found

```python
try:
    result = db.query("sql", "SELECT FROM Person LIMIT 1")
    person = result.next()

    # Property might not exist
    phone = person.get_property("phone_number")  # Typo or missing

except ArcadeDBError as e:
    print(f"Property error: {e}")
```

**Solution:** Use `has_property()` before accessing, or handle exceptions.

---

### Server Already Running

```python
from arcadedb_embedded import create_server, ArcadeDBError

server = create_server()
server.start()

try:
    server.start()  # Already started!
except ArcadeDBError as e:
    print(f"Server error: {e}")
    # Server error: Server is already started
finally:
    server.stop()
```

**Solution:** Check `server.is_started()` before calling `start()`.

---

### Port Already in Use

```python
try:
    server = create_server(config={"http_port": 2480})
    server.start()
except ArcadeDBError as e:
    if "bind" in str(e).lower() or "port" in str(e).lower():
        print("Port 2480 is already in use")
        print("Try a different port or stop conflicting process")
    else:
        print(f"Server error: {e}")
```

**Solution:** Use a different port or stop the process using port 2480.

---

## Error Handling Best Practices

### Specific Error Handling

```python
from arcadedb_embedded import ArcadeDBError

try:
    db = open_database("./mydb")
    result = db.query("sql", "SELECT FROM Person WHERE age > 25")

except ArcadeDBError as e:
    error_msg = str(e).lower()

    if "does not exist" in error_msg:
        print("Database not found - creating new one")
        db = create_database("./mydb")

    elif "syntax" in error_msg:
        print("Query syntax error - check your SQL")

    elif "constraint" in error_msg:
        print("Constraint violation - check your data")

    else:
        print(f"Unknown error: {e}")
        raise
```

---

### Transaction Error Handling

```python
from arcadedb_embedded import ArcadeDBError

def safe_insert(db, record_data):
    """Insert with automatic retry on concurrent modification."""
    max_retries = 3

    for attempt in range(max_retries):
        try:
            with db.transaction():
                doc = db.new_document("Record")
                for key, value in record_data.items():
                    doc.set(key, value)
                doc.save()

            return True  # Success

        except ArcadeDBError as e:
            if "concurrent" in str(e).lower() and attempt < max_retries - 1:
                # Retry on concurrent modification
                import time
                time.sleep(0.1 * (attempt + 1))
                continue
            else:
                # Other error or max retries exceeded
                raise

    return False

# Usage
try:
    success = safe_insert(db, {"name": "Alice", "age": 30})
    if success:
        print("Insert successful")
except ArcadeDBError as e:
    print(f"Insert failed: {e}")
```

---

### Graceful Degradation

```python
from arcadedb_embedded import ArcadeDBError

def get_user_safely(db, email):
    """Get user with fallback on error."""
    try:
        result = db.query("sql", f"SELECT FROM User WHERE email = '{email}'")
        if result.has_next():
            return result.next()
        else:
            return None

    except ArcadeDBError as e:
        print(f"Warning: Database query failed: {e}")
        # Return None or default value instead of crashing
        return None

# Usage
user = get_user_safely(db, "alice@example.com")
if user:
    print(f"Found user: {user.get_property('name')}")
else:
    print("User not found or error occurred")
```

---

### Cleanup on Error

```python
from arcadedb_embedded import ArcadeDBError, create_server

server = None
db = None

try:
    # Start server
    server = create_server()
    server.start()

    # Create database
    db = server.create_database("temp_db")

    # Do work
    with db.transaction():
        doc = db.new_document("Test")
        doc.set("data", "value")
        doc.save()

except ArcadeDBError as e:
    print(f"Error: {e}")

finally:
    # Always cleanup
    if db:
        db.close()
    if server and server.is_started():
        server.stop()
    print("Cleanup complete")
```

---

### Logging Errors

```python
import logging
from arcadedb_embedded import ArcadeDBError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    with db.transaction():
        # Complex operation
        result = db.query("sql", "SELECT FROM LargeTable")
        for record in result:
            process(record)

except ArcadeDBError as e:
    logger.error(f"Database operation failed: {e}", exc_info=True)
    # Log includes full stack trace
    raise
```

---

## Complete Error Handling Example

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded import ArcadeDBError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_database_operation():
    """Comprehensive error handling example."""
    db = None

    try:
        # Check if database exists
        if not arcadedb.database_exists("./mydb"):
            logger.info("Database not found - creating")
            db = arcadedb.create_database("./mydb")

            # Initialize schema
            with db.transaction():
                db.command("sql", "CREATE DOCUMENT TYPE Person")
                db.command("sql", "CREATE PROPERTY Person.name STRING")
                db.command("sql", "CREATE PROPERTY Person.email STRING")
                db.command("sql", "CREATE INDEX ON Person (email) UNIQUE")
        else:
            db = arcadedb.open_database("./mydb")

        # Insert records with error handling
        people = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "email": "alice@example.com"},  # Duplicate!
        ]

        success_count = 0
        error_count = 0

        for person in people:
            try:
                with db.transaction():
                    doc = db.new_document("Person")
                    doc.set("name", person["name"])
                    doc.set("email", person["email"])
                    doc.save()

                success_count += 1
                logger.info(f"Inserted: {person['name']}")

            except ArcadeDBError as e:
                error_count += 1
                error_msg = str(e).lower()

                if "duplicate" in error_msg or "unique" in error_msg:
                    logger.warning(f"Skipped duplicate: {person['email']}")
                else:
                    logger.error(f"Failed to insert {person['name']}: {e}")

        logger.info(f"Completed: {success_count} success, {error_count} errors")

        # Query with error handling
        try:
            result = db.query("sql", "SELECT * FROM Person")
            count = len(list(result))
            logger.info(f"Total people in database: {count}")
        except ArcadeDBError as e:
            logger.error(f"Query failed: {e}")

        return True

    except ArcadeDBError as e:
        logger.error(f"Database operation failed: {e}", exc_info=True)
        return False

    finally:
        if db:
            try:
                db.close()
                logger.info("Database closed successfully")
            except ArcadeDBError as e:
                logger.error(f"Error closing database: {e}")

# Run
if __name__ == "__main__":
    success = safe_database_operation()
    if success:
        print("Operation completed successfully")
    else:
        print("Operation failed - check logs")
```

---

## Debugging Tips

### Print Full Exception Details

```python
import traceback
from arcadedb_embedded import ArcadeDBError

try:
    db.query("sql", "INVALID QUERY")
except ArcadeDBError as e:
    print("Error occurred:")
    print(f"  Message: {e}")
    print(f"  Type: {type(e).__name__}")
    print("\nFull traceback:")
    traceback.print_exc()
```

---

### Check Java Exception

```python
from arcadedb_embedded import ArcadeDBError

try:
    # Operation that might fail
    db.query("sql", "SELECT FROM NonExistentType")
except ArcadeDBError as e:
    print(f"Python error: {e}")

    # The __cause__ attribute contains the original Java exception
    if e.__cause__:
        print(f"Java cause: {e.__cause__}")
        print(f"Java type: {type(e.__cause__).__name__}")
```

---

### Validate Before Operations

```python
from arcadedb_embedded import ArcadeDBError

def validate_schema(db, type_name, properties):
    """Validate schema before operations."""
    try:
        # Check if type exists
        schema_info = db.command("sql", f"SELECT FROM schema:types WHERE name = '{type_name}'")

        if not schema_info:
            raise ValueError(f"Type {type_name} does not exist")

        # Check if properties exist
        for prop in properties:
            result = db.command("sql",
                f"SELECT FROM schema:properties WHERE type = '{type_name}' AND name = '{prop}'")
            if not result:
                raise ValueError(f"Property {type_name}.{prop} does not exist")

        return True

    except ArcadeDBError as e:
        print(f"Schema validation error: {e}")
        return False

# Usage
if validate_schema(db, "Person", ["name", "email"]):
    # Safe to proceed
    with db.transaction():
        person = db.new_document("Person")
        person.set("name", "Alice")
        person.set("email", "alice@example.com")
        person.save()
```

---

## Error Categories

### Schema Errors

- Type doesn't exist
- Property doesn't exist
- Type mismatch (e.g., string → integer)
- Constraint violation (unique, mandatory, etc.)

**Prevention:**

- Define schema upfront with `CREATE TYPE`
- Use schema validation before operations
- Handle type conversion explicitly

---

### Transaction Errors

- Transaction already active
- No active transaction
- Concurrent modification
- Commit failure
- Rollback failure

**Prevention:**

- Use context managers (`with db.transaction()`)
- Keep transactions short
- Implement retry logic for concurrent modifications

---

### Query Errors

- Syntax errors
- Invalid language
- Type not found
- Invalid property reference
- Invalid function usage

**Prevention:**

- Validate queries with test data first
- Use parameterized queries when possible
- Test query syntax in Studio UI

---

### Resource Errors

- Database not found
- Database already exists
- Cannot create database
- File I/O errors
- Out of memory

**Prevention:**

- Check `database_exists()` before operations
- Ensure sufficient disk space
- Use appropriate batch sizes for imports

---

### Server Errors

- Server already running
- Port in use
- Cannot bind to address
- Database not accessible

**Prevention:**

- Check `is_started()` before operations
- Use available ports
- Ensure proper permissions

---

## See Also

- [Database API](database.md) - Database operations that may raise errors
- [Transactions API](transactions.md) - Transaction error handling
- [Server API](server.md) - Server-related errors
- [Troubleshooting Guide](../development/troubleshooting.md) - Common issues and solutions
