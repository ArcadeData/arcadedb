# Transactions

Comprehensive guide to transaction management in ArcadeDB Python bindings.

## Overview

ArcadeDB provides ACID-compliant transactions with automatic lifecycle management through Python context managers.

**Transaction Lifecycle:**
```
BEGIN → OPERATIONS → COMMIT (success) or ROLLBACK (failure)
```

**Key Features:**

- ✅ **Atomicity**: All operations commit together or none commit
- ✅ **Consistency**: Schema validation and constraint enforcement
- ✅ **Isolation**: Read committed isolation level
- ✅ **Durability**: Changes persisted to WAL (Write-Ahead Log) on commit

## Quick Start

### Context Manager (Recommended)

```python
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./mydb")

# Automatic transaction management
with db.transaction():
    vertex = db.new_vertex("User")
    vertex.set("name", "Alice")
    vertex.set("email", "alice@example.com")
    vertex.save()

    # If this block completes successfully → COMMIT
    # If exception occurs → ROLLBACK
```

**Why Context Manager?**

- ✅ Automatic commit on success
- ✅ Automatic rollback on exception
- ✅ Clean, Pythonic syntax
- ✅ Prevents transaction leaks

### Manual Control

```python
db.begin()
try:
    vertex = db.new_vertex("User")
    vertex.set("name", "Bob")
    vertex.save()
    db.commit()
except Exception as e:
    db.rollback()
    raise
```

## ACID Guarantees

### Atomicity

All operations in a transaction are treated as a single unit:

```python
with db.transaction():
    # Create user
    user = db.new_vertex("User")
    user.set("name", "Charlie")
    user.save()

    # Create user's profile
    profile = db.new_document("Profile")
    profile.set("bio", "Software Engineer")
    profile.save()

    # Link them
    edge = db.new_edge("HasProfile", user, profile)
    edge.save()

    # Either ALL three operations succeed, or NONE do

# If any operation fails, entire transaction rolls back
```

### Consistency

Schema validation enforced at transaction commit:

```python
# Create schema with constraints
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE User")
    db.command("sql", "CREATE PROPERTY User.email STRING")
    db.command("sql", "CREATE INDEX ON User (email) UNIQUE")

# Constraint violation causes rollback
try:
    with db.transaction():
        user1 = db.new_vertex("User")
        user1.set("email", "same@example.com")
        user1.save()

        user2 = db.new_vertex("User")
        user2.set("email", "same@example.com")  # Duplicate!
        user2.save()  # This will fail
except Exception:
    print("Transaction rolled back - unique constraint violated")
```

### Isolation

**Isolation Level**: READ_COMMITTED (default)

- Transactions see only committed data
- Dirty reads are prevented
- Non-repeatable reads possible (data can change between reads)

```python
# Transaction A
with db.transaction():
    result = db.query("sql", "SELECT FROM User WHERE name = 'Alice'")
    user = list(result)[0]

    # Transaction B commits changes to Alice here

    result2 = db.query("sql", "SELECT FROM User WHERE name = 'Alice'")
    user2 = list(result2)[0]

    # user and user2 may have different data (non-repeatable read)
```

**Available Isolation Levels:**

- `READ_COMMITTED` (default) - Prevents dirty reads
- `REPEATABLE_READ` - Prevents dirty and non-repeatable reads

Currently, Python bindings use the default (`READ_COMMITTED`). Custom isolation levels coming soon.

### Durability

Changes are persisted to Write-Ahead Log (WAL) on commit:

```python
with db.transaction():
    vertex = db.new_vertex("Data")
    vertex.set("value", 42)
    vertex.save()
    # On successful exit, changes written to WAL
    # Crash recovery uses WAL to restore state
```

**WAL Configuration:**

ArcadeDB uses WAL for crash recovery. Configuration handled at database level:

- WAL entries written before commit
- Automatic recovery on database open
- Configurable flush strategy (sync, async)

## Error Handling

### Automatic Rollback

Context manager automatically rolls back on any exception:

```python
try:
    with db.transaction():
        vertex = db.new_vertex("User")
        vertex.set("name", "Dave")
        vertex.save()

        raise ValueError("Something went wrong!")
        # ROLLBACK happens automatically
except ValueError:
    print("Transaction rolled back")
```

### Explicit Rollback

```python
with db.transaction():
    vertex = db.new_vertex("User")
    vertex.set("name", "Eve")
    vertex.save()

    # Check some condition
    if not validate_user(vertex):
        # Force rollback without raising exception
        db.rollback()
        return

    # Continue with transaction
```

### Nested Transactions Not Supported

```python
# ✗ This will fail!
with db.transaction():
    with db.transaction():  # ERROR: Transaction already active
        pass

# ✓ Use sequential transactions instead
with db.transaction():
    # First transaction
    pass

with db.transaction():
    # Second transaction
    pass
```

### Transaction Conflicts

Concurrent modifications can cause conflicts:

```python
import time

def update_with_retry(db, max_retries=3):
    for attempt in range(max_retries):
        try:
            with db.transaction():
                result = db.query("sql", "SELECT FROM Counter WHERE name = 'global'")
                counter = list(result)[0]

                count = counter.get("value")
                counter.set("value", count + 1)
                counter.save()

                return count + 1
        except Exception as e:
            if "concurrent modification" in str(e).lower():
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
            raise

    raise Exception("Max retries exceeded")
```

## Transaction Patterns

### Batch Operations

Process multiple items in batches:

```python
def import_users(db, users, batch_size=1000):
    for i in range(0, len(users), batch_size):
        batch = users[i:i+batch_size]

        with db.transaction():
            for user_data in batch:
                user = db.new_vertex("User")
                for key, value in user_data.items():
                    user.set(key, value)
                user.save()

        print(f"Imported batch {i//batch_size + 1}")
```

### Conditional Commit

```python
def create_user_if_valid(db, name, email):
    with db.transaction():
        # Check if email exists
        result = db.query("sql",
            "SELECT FROM User WHERE email = :email",
            {"email": email}
        )

        if result.has_next():
            # Email exists - rollback
            db.rollback()
            return None

        # Create user
        user = db.new_vertex("User")
        user.set("name", name)
        user.set("email", email)
        user.save()

        return user
```

### Long-Running Operations

Split into smaller transactions to avoid locks:

```python
def process_large_dataset(db, records):
    batch_size = 1000
    total = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]

        # Each batch in separate transaction
        with db.transaction():
            for record in batch:
                process_record(db, record)

        total += len(batch)
        print(f"Processed {total}/{len(records)}")

        # Transaction committed, locks released
```

### Upsert Pattern

Insert or update based on existence:

```python
def upsert_user(db, email, name):
    with db.transaction():
        result = db.query("sql",
            "SELECT FROM User WHERE email = :email",
            {"email": email}
        )

        if result.has_next():
            # Update existing
            user = list(result)[0]
            user.set("name", name)
            user.set("updated", int(time.time()))
            user.save()
        else:
            # Create new
            user = db.new_vertex("User")
            user.set("email", email)
            user.set("name", name)
            user.set("created", int(time.time()))
            user.save()

        return user
```

## Performance Considerations

### Transaction Size

**Rule of Thumb:**

- Small transactions: < 1,000 operations
- Medium transactions: 1,000 - 10,000 operations
- Large transactions: 10,000+ operations (use batching)

```python
# ✗ Too large - single transaction
with db.transaction():
    for i in range(1_000_000):
        vertex = db.new_vertex("Data")
        vertex.set("value", i)
        vertex.save()

# ✓ Better - batched transactions
batch_size = 10_000
for i in range(0, 1_000_000, batch_size):
    with db.transaction():
        for j in range(batch_size):
            vertex = db.new_vertex("Data")
            vertex.set("value", i + j)
            vertex.save()
```

### Lock Contention

Minimize time holding locks:

```python
# ✗ External operations inside transaction
with db.transaction():
    vertex = db.new_vertex("Data")
    vertex.set("value", expensive_computation())  # Slow!
    vertex.save()

# ✓ Compute before transaction
value = expensive_computation()
with db.transaction():
    vertex = db.new_vertex("Data")
    vertex.set("value", value)
    vertex.save()
```

### Read vs Write Transactions

Queries outside transactions are read-only and more efficient:

```python
# ✓ Read without transaction
result = db.query("sql", "SELECT FROM User WHERE active = true")
for user in result:
    print(user.get("name"))

# ✗ Unnecessary transaction for reads
with db.transaction():
    result = db.query("sql", "SELECT FROM User WHERE active = true")
    for user in result:
        print(user.get("name"))
```

### Transaction Overhead

Each transaction has overhead (~100μs). For high-throughput scenarios, batch operations:

```python
import time

# Measure overhead
def benchmark_transactions(db, count):
    # Individual transactions
    start = time.time()
    for i in range(count):
        with db.transaction():
            vertex = db.new_vertex("Data")
            vertex.set("value", i)
            vertex.save()
    individual_time = time.time() - start

    # Batched transaction
    start = time.time()
    with db.transaction():
        for i in range(count):
            vertex = db.new_vertex("Data")
            vertex.set("value", i)
            vertex.save()
    batched_time = time.time() - start

    print(f"Individual: {individual_time:.2f}s")
    print(f"Batched: {batched_time:.2f}s")
    print(f"Speedup: {individual_time/batched_time:.1f}x")

benchmark_transactions(db, 1000)
# Individual: 0.85s
# Batched: 0.08s
# Speedup: 10.6x
```

## Advanced Patterns

### Transaction Checkpoint

Save progress during long operations:

```python
def import_with_checkpoints(db, records, checkpoint_file="checkpoint.txt"):
    batch_size = 1000

    # Load last checkpoint
    start_idx = 0
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            start_idx = int(f.read())

    for i in range(start_idx, len(records), batch_size):
        batch = records[i:i+batch_size]

        try:
            with db.transaction():
                for record in batch:
                    import_record(db, record)

            # Save checkpoint
            with open(checkpoint_file, "w") as f:
                f.write(str(i + batch_size))

        except Exception as e:
            print(f"Failed at batch {i}: {e}")
            raise
```

### Transaction Hooks

Execute logic on commit/rollback:

```python
class TransactionWithHooks:
    def __init__(self, db, on_commit=None, on_rollback=None):
        self.db = db
        self.on_commit = on_commit
        self.on_rollback = on_rollback

    def __enter__(self):
        self.db.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.db.commit()
            if self.on_commit:
                self.on_commit()
        else:
            self.db.rollback()
            if self.on_rollback:
                self.on_rollback()

# Usage
def notify_success():
    print("Transaction committed!")

def notify_failure():
    print("Transaction rolled back!")

with TransactionWithHooks(db, on_commit=notify_success, on_rollback=notify_failure):
    # Operations here
    pass
```

### Distributed Transaction Pattern

For operations across multiple databases (use multiprocessing):

```python
from multiprocessing import Process, Queue

def transactional_operation(db_path, data, result_queue):
    db = arcadedb.open_database(db_path)
    try:
        with db.transaction():
            # Perform operations
            result = process_data(db, data)
            result_queue.put(("success", result))
    except Exception as e:
        result_queue.put(("error", str(e)))
    finally:
        db.close()

def distributed_transaction(db_paths, data_list):
    result_queue = Queue()
    processes = []

    # Start all processes
    for db_path, data in zip(db_paths, data_list):
        p = Process(target=transactional_operation,
                   args=(db_path, data, result_queue))
        p.start()
        processes.append(p)

    # Wait for all
    for p in processes:
        p.join()

    # Check results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    return results
```

## Best Practices

### 1. Always Use Context Managers

```python
# ✓ Recommended
with db.transaction():
    # Operations

# ✗ Avoid manual control unless necessary
db.begin()
try:
    # Operations
    db.commit()
except:
    db.rollback()
```

### 2. Keep Transactions Short

```python
# ✓ Short transaction
with db.transaction():
    vertex = db.new_vertex("User")
    vertex.set("name", name)
    vertex.save()

# ✗ Long-running transaction
with db.transaction():
    time.sleep(10)  # Holds locks!
```

### 3. Don't Ignore Exceptions

```python
# ✗ Silently catch exceptions
try:
    with db.transaction():
        # Operations
except:
    pass  # Don't do this!

# ✓ Log and handle properly
import logging

try:
    with db.transaction():
        # Operations
except Exception as e:
    logging.error(f"Transaction failed: {e}")
    raise
```

### 4. Batch Operations

```python
# ✓ Batch for efficiency
batch_size = 1000
for i in range(0, len(items), batch_size):
    with db.transaction():
        for item in items[i:i+batch_size]:
            process_item(db, item)
```

### 5. Retry on Conflicts

```python
def retry_transaction(db, operation, max_retries=3):
    for attempt in range(max_retries):
        try:
            with db.transaction():
                return operation(db)
        except Exception as e:
            if "concurrent" in str(e).lower() and attempt < max_retries - 1:
                time.sleep(0.1 * (2 ** attempt))
                continue
            raise
```

### 6. Validate Before Commit

```python
with db.transaction():
    vertex = db.new_vertex("User")
    vertex.set("email", email)

    # Validate
    if not is_valid_email(email):
        db.rollback()
        raise ValueError("Invalid email")

    vertex.save()
```

### 7. Monitor Transaction Duration

```python
import time

def monitored_transaction(db, operation, max_duration=5.0):
    start = time.time()
    with db.transaction():
        result = operation(db)
        duration = time.time() - start

        if duration > max_duration:
            logging.warning(f"Long transaction: {duration:.2f}s")

        return result
```

## Common Pitfalls

### ❌ Nested Transactions

```python
# ✗ Will fail!
with db.transaction():
    with db.transaction():
        pass
```

### ❌ Transaction Leaks

```python
# ✗ Transaction never committed/rolled back
db.begin()
# ... operations ...
# Forgot to commit or rollback!
```

### ❌ Large Transactions

```python
# ✗ Too much in single transaction
with db.transaction():
    for i in range(10_000_000):  # Too many!
        vertex = db.new_vertex("Data")
        vertex.save()
```

### ❌ External Operations Inside Transaction

```python
# ✗ Network call while holding locks
with db.transaction():
    vertex = db.new_vertex("Data")
    vertex.set("value", requests.get("http://api.example.com/data").json())
    vertex.save()
```

## Debugging

### Enable Transaction Logging

```python
import logging

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

with db.transaction():
    # Transaction events will be logged
    pass
```

### Transaction Statistics

```python
def transaction_stats(db, operation):
    import time

    start = time.time()

    with db.transaction():
        result = operation(db)

    duration = time.time() - start

    print(f"Transaction completed in {duration:.3f}s")
    return result
```

### Monitor Active Transactions

```python
# Check if transaction is active
if db.is_transaction_active():
    print("Transaction in progress")
else:
    print("No active transaction")
```

## See Also

- [Transactions API Reference](../../api/transactions.md) - Complete API documentation
- [Database API](../../api/database.md) - Database operations
- [Error Handling](../../api/exceptions.md) - Exception handling
- [Performance Guide](../import.md#performance-optimization) - Optimization strategies
