# Results API

The `ResultSet` and `Result` classes provide Python-friendly interfaces for working with query results from ArcadeDB. They handle iteration, property access, and type conversion automatically.

## Overview

When you execute a query, ArcadeDB returns a `ResultSet` that can be iterated to access individual `Result` objects. Each `Result` represents one row/record from your query.

**Key Features:**

- **Pythonic iteration**: Use `for` loops or iterators
- **Property access**: Get values by property name
- **Type conversion**: Automatic conversion from Java to Python types
- **Multiple access patterns**: Dict-like access, JSON export, direct properties

## ResultSet Class

Iterable wrapper for query results. Supports both Python iteration (`for`) and manual iteration (`has_next()`/`next()`).

### Creation

`ResultSet` objects are returned by query operations:

```python
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./mydb")

# query() returns ResultSet
result_set = db.query("sql", "SELECT FROM Person WHERE age > 25")

# command() also returns ResultSet for SELECT queries
result_set = db.command("sql", "SELECT * FROM Person LIMIT 10")
```

---

### Iteration

**Python-style iteration (recommended):**

```python
# Using for loop (most Pythonic)
for result in result_set:
    name = result.get_property("name")
    age = result.get_property("age")
    print(f"{name}: {age}")

# As iterator
result_set = db.query("sql", "SELECT FROM Product")
results = list(result_set)  # Convert to list
```

**Manual iteration:**

```python
# Check and iterate manually
while result_set.has_next():
    result = result_set.next()
    print(result.to_dict())
```

---

### `has_next() -> bool`

Check if there are more results available.

**Returns:**

- `bool`: `True` if more results exist, `False` otherwise

**Example:**

```python
result_set = db.query("sql", "SELECT FROM User LIMIT 5")

count = 0
while result_set.has_next():
    result = result_set.next()
    count += 1

print(f"Found {count} results")
```

---

### `next() -> Result`

Get the next result.

**Returns:**

- `Result`: Next result object

**Raises:**

- `StopIteration`: When no more results available

**Example:**

```python
result_set = db.query("sql", "SELECT FROM Document")

if result_set.has_next():
    first = result_set.next()
    print(first.to_dict())
```

---

### `close()`

Close the result set and release resources.

**Example:**

```python
result_set = db.query("sql", "SELECT FROM LargeTable")

try:
    for result in result_set:
        process(result)
finally:
    result_set.close()  # Ensure cleanup
```

**Note:** In most cases, result sets are automatically cleaned up when they go out of scope. Explicit closing is only needed for long-running operations or very large result sets.

---

## Result Class

Represents a single result row/record from a query.

### Creation

`Result` objects are created automatically when iterating a `ResultSet`:

```python
result_set = db.query("sql", "SELECT FROM Person")

# Each iteration gives you a Result
for result in result_set:
    # result is a Result object
    pass
```

---

### `get_property(name: str) -> Any`

Get the value of a property by name.

**Parameters:**

- `name` (str): Property name

**Returns:**

- `Any`: Property value (type depends on the data)
  - Automatically converts Java types to Python types
  - Java `Boolean` → Python `bool`
  - Java `Integer`/`Long` → Python `int`
  - Java `Float`/`Double` → Python `float`
  - Java `String` → Python `str`
  - Java collections → Python lists/dicts

**Raises:**

- `ArcadeDBError`: If property doesn't exist or access fails

**Example:**

```python
result_set = db.query("sql", "SELECT name, age, active FROM User")

for result in result_set:
    name = result.get_property("name")     # str
    age = result.get_property("age")       # int
    active = result.get_property("active") # bool (converted from Java Boolean)

    print(f"{name} is {age} years old, active: {active}")
```

---

### `has_property(name: str) -> bool`

Check if a property exists in the result.

**Parameters:**

- `name` (str): Property name to check

**Returns:**

- `bool`: `True` if property exists, `False` otherwise

**Example:**

```python
result_set = db.query("sql", "SELECT * FROM Person")

for result in result_set:
    if result.has_property("email"):
        email = result.get_property("email")
        print(f"Email: {email}")
    else:
        print("No email address")
```

---

### `get_property_names() -> List[str]`

Get list of all property names in the result.

**Returns:**

- `List[str]`: List of property names

**Example:**

```python
result_set = db.query("sql", "SELECT * FROM Document LIMIT 1")

for result in result_set:
    properties = result.get_property_names()
    print(f"Properties: {', '.join(properties)}")

    for prop in properties:
        value = result.get_property(prop)
        print(f"  {prop}: {value}")
```

---

### `to_dict() -> Dict[str, Any]`

Convert the result to a Python dictionary.

**Returns:**

- `Dict[str, Any]`: Dictionary with property names as keys

**Example:**

```python
result_set = db.query("sql", "SELECT name, age, city FROM Person")

# Convert all results to list of dicts
people = [result.to_dict() for result in result_set]

for person in people:
    print(person)
    # {'name': 'Alice', 'age': 30, 'city': 'NYC'}
```

**Use Cases:**

- Converting to pandas DataFrame
- Serialization to JSON (via `json.dumps()`)
- Passing data to other libraries
- Debugging/inspection

---

### `to_json() -> str`

Convert the result to a JSON string.

**Returns:**

- `str`: JSON representation of the result

**Example:**

```python
result_set = db.query("sql", "SELECT * FROM Product WHERE price > 100")

for result in result_set:
    json_str = result.to_json()
    print(json_str)
    # {"@rid":"#1:0","@type":"Product","name":"Laptop","price":999.99}
```

**Note:** The JSON includes ArcadeDB metadata like `@rid` (record ID) and `@type` (type name).

---

## Common Patterns

### Converting to Lists and Dicts

```python
# List of dictionaries (most common)
result_set = db.query("sql", "SELECT FROM User")
users = [result.to_dict() for result in result_set]

# List of specific property values
result_set = db.query("sql", "SELECT name FROM User")
names = [result.get_property("name") for result in result_set]

# Dictionary keyed by ID
result_set = db.query("sql", "SELECT id, name FROM User")
user_map = {
    result.get_property("id"): result.get_property("name")
    for result in result_set
}
```

---

### Pandas Integration

```python
import pandas as pd
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./mydb")

# Query and convert to DataFrame
result_set = db.query("sql", "SELECT name, age, city FROM Person")
df = pd.DataFrame([result.to_dict() for result in result_set])

print(df.head())
#      name  age    city
# 0   Alice   30     NYC
# 1     Bob   25      LA
# 2 Charlie   35  Boston
```

---

### Processing Large Result Sets

For memory efficiency with large datasets:

```python
# Process in batches
result_set = db.query("sql", "SELECT FROM LargeTable")

batch = []
batch_size = 1000

for result in result_set:
    batch.append(result.to_dict())

    if len(batch) >= batch_size:
        # Process batch
        process_batch(batch)
        batch = []

# Process remaining
if batch:
    process_batch(batch)
```

---

### Conditional Property Access

```python
result_set = db.query("sql", "SELECT * FROM Product")

for result in result_set:
    # Safely get optional properties
    discount = (
        result.get_property("discount")
        if result.has_property("discount")
        else 0.0
    )

    price = result.get_property("price")
    final_price = price * (1 - discount)
    print(f"Price: ${final_price:.2f}")
```

---

### Extracting RIDs and Types

```python
result_set = db.query("sql", "SELECT FROM Person")

for result in result_set:
    # Get ArcadeDB metadata
    rid = result.get_property("@rid")      # Record ID (e.g., "#1:0")
    rec_type = result.get_property("@type") # Type name (e.g., "Person")

    # Get user properties
    name = result.get_property("name")

    print(f"[{rid}] {rec_type}: {name}")
```

---

## Complete Examples

### User Search and Display

```python
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./users_db")

def search_users(name_pattern):
    """Search users by name pattern."""
    query = f"""
        SELECT name, email, created_at
        FROM User
        WHERE name LIKE '%{name_pattern}%'
        ORDER BY name
    """

    result_set = db.query("sql", query)
    users = []

    for result in result_set:
        user = {
            'name': result.get_property("name"),
            'email': result.get_property("email"),
            'created_at': result.get_property("created_at")
        }
        users.append(user)

    return users

# Search
results = search_users("John")

print(f"Found {len(results)} users:")
for user in results:
    print(f"  {user['name']} <{user['email']}>")

db.close()
```

---

### Graph Traversal Results

```python
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./social_graph")

# Find friends of friends
query = """
    SELECT
        @rid as person_rid,
        name,
        out('Follows').out('Follows').name as friends_of_friends
    FROM Person
    WHERE name = 'Alice'
"""

result_set = db.query("sql", query)

for result in result_set:
    person_name = result.get_property("name")
    friends_of_friends = result.get_property("friends_of_friends")

    print(f"{person_name}'s extended network:")

    # friends_of_friends is a Java collection, convert to Python
    if friends_of_friends:
        fof_list = list(friends_of_friends)
        for friend in fof_list:
            print(f"  - {friend}")

db.close()
```

---

### Aggregation Results

```python
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./analytics_db")

# Group by and aggregation
query = """
    SELECT
        category,
        COUNT(*) as product_count,
        AVG(price) as avg_price,
        MAX(price) as max_price
    FROM Product
    GROUP BY category
    ORDER BY product_count DESC
"""

result_set = db.query("sql", query)

print("Product Statistics by Category:")
print("-" * 60)

for result in result_set:
    category = result.get_property("category")
    count = result.get_property("product_count")
    avg_price = result.get_property("avg_price")
    max_price = result.get_property("max_price")

    print(f"{category}:")
    print(f"  Products: {count}")
    print(f"  Avg Price: ${avg_price:.2f}")
    print(f"  Max Price: ${max_price:.2f}")
    print()

db.close()
```

---

### Export to JSON File

```python
import arcadedb_embedded as arcadedb
import json

db = arcadedb.open_database("./mydb")

# Export query results to JSON file
result_set = db.query("sql", "SELECT * FROM Document")

# Method 1: Using to_dict()
documents = [result.to_dict() for result in result_set]

with open("export.json", "w") as f:
    json.dump(documents, f, indent=2, default=str)

# Method 2: Using to_json() directly
result_set = db.query("sql", "SELECT * FROM Document")

with open("export_raw.jsonl", "w") as f:
    for result in result_set:
        f.write(result.to_json() + "\n")

db.close()
```

---

### Cypher Query Results

```python
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./graph_db")

# Cypher queries also return ResultSet
cypher_query = """
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    WHERE c.name = 'TechCorp'
    RETURN p.name AS employee, p.role AS position
"""

result_set = db.query("cypher", cypher_query)

print("TechCorp Employees:")
for result in result_set:
    employee = result.get_property("employee")
    position = result.get_property("position")
    print(f"  {employee} - {position}")

db.close()
```

---

## Error Handling

```python
from arcadedb_embedded import ArcadeDBError

result_set = db.query("sql", "SELECT * FROM Person")

for result in result_set:
    try:
        # Safe property access
        name = result.get_property("name")

        # May not exist
        if result.has_property("phone"):
            phone = result.get_property("phone")
        else:
            phone = "N/A"

        print(f"{name}: {phone}")

    except ArcadeDBError as e:
        print(f"Error accessing properties: {e}")
        continue
```

---

## Type Handling

ArcadeDB returns Java types that are automatically converted:

| Java Type | Python Type | Notes |
|-----------|-------------|-------|
| `java.lang.String` | `str` | Direct conversion |
| `java.lang.Integer`, `Long` | `int` | Numeric conversion |
| `java.lang.Float`, `Double` | `float` | Numeric conversion |
| `java.lang.Boolean` | `bool` | **Explicitly converted** |
| `java.util.ArrayList` | `list` | Iterable conversion |
| `java.util.HashMap` | `dict` | Key-value conversion |
| `null` | `None` | Direct mapping |

**Boolean Conversion:**

The `Result` class explicitly converts Java `Boolean` to Python `bool`:

```python
# This is handled automatically
result_set = db.query("sql", "SELECT active FROM User")

for result in result_set:
    active = result.get_property("active")  # Python bool
    if active:  # Works as expected
        print("User is active")
```

---

## Performance Tips

### Minimize Property Access

```python
# Less efficient: Multiple property accesses
for result in result_set:
    if result.get_property("age") > 25:
        name = result.get_property("name")
        age = result.get_property("age")
        print(f"{name}: {age}")

# More efficient: Access once, reuse
for result in result_set:
    age = result.get_property("age")
    if age > 25:
        name = result.get_property("name")
        print(f"{name}: {age}")
```

### Use to_dict() for Multiple Properties

```python
# When accessing many properties, convert to dict once
for result in result_set:
    data = result.to_dict()

    # Now access from Python dict (faster)
    process(
        data["name"],
        data["age"],
        data["email"],
        data["phone"]
    )
```

### Stream Processing

```python
# Don't collect all results if you can process incrementally
result_set = db.query("sql", "SELECT * FROM LargeTable")

# Process as you iterate (memory efficient)
total = 0
for result in result_set:
    value = result.get_property("amount")
    total += value

# Better than:
# results = list(result_set)  # Loads everything into memory
```

---

## See Also

- [Database API](database.md) - Query and command methods
- [Query Guide](../guide/core/queries.md) - Writing effective queries
- [Transaction API](transactions.md) - Transaction context
- [Graph Operations Guide](../guide/graphs.md) - Working with graph results
