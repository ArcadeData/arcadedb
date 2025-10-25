# Graph Operations

ArcadeDB is a native multi-model database with first-class support for property graphs. This guide covers working with vertices, edges, and graph traversals using the Python bindings.

## Overview

ArcadeDB's graph model consists of:

- **Vertices (Nodes)**: Entities in your graph with properties
- **Edges (Relationships)**: Connections between vertices with optional properties
- **Types**: Schema definitions for vertices and edges

## Creating Graph Schema

Before creating vertices and edges, define their types:

```python
import arcadedb_embedded as arcadedb

with arcadedb.create_database("/tmp/social") as db:
    # Create vertex types
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE VERTEX TYPE Company")

    # Create edge types
    db.command("sql", "CREATE EDGE TYPE Knows")
    db.command("sql", "CREATE EDGE TYPE WorksFor")

    # Add properties to vertices
    db.command("sql", "ALTER TYPE Person CREATE PROPERTY name STRING")
    db.command("sql", "ALTER TYPE Person CREATE PROPERTY age INTEGER")
    db.command("sql", "ALTER TYPE Person CREATE PROPERTY email STRING")

    # Add properties to edges
    db.command("sql", "ALTER TYPE Knows CREATE PROPERTY since DATE")
    db.command("sql", "ALTER TYPE WorksFor CREATE PROPERTY role STRING")

    print("✅ Graph schema created")
```

## Creating Vertices

### Using SQL

The simplest way to create vertices is with SQL:

```python
with db.transaction():
    # Create vertices with properties
    db.command("sql", """
        CREATE VERTEX Person
        SET name = 'Alice', age = 30, email = 'alice@example.com'
    """)

    db.command("sql", """
        CREATE VERTEX Person
        SET name = 'Bob', age = 25, email = 'bob@example.com'
    """)
```

### Using the API

You can also create vertices programmatically using the `new_vertex()` method:

```python
with db.transaction():
    # Create a vertex using the API
    vertex = db.new_vertex("Person")
    vertex.set("name", "Charlie")
    vertex.set("age", 35)
    vertex.set("email", "charlie@example.com")
    vertex.save()

    print(f"✅ Created vertex with RID: {vertex.getIdentity()}")
```

!!! tip "When to Use Each Approach"
    - **SQL**: Best for simple inserts, batch operations, and declarative code
    - **API**: Best when you need programmatic control, access to the vertex object, or complex logic

## Creating Edges

**Important**: In ArcadeDB, edges are created **from vertices**, not from the database directly. This is the proper graph model - edges represent connections between existing vertices.

### Why No `db.new_edge()` Method?

Unlike `db.new_vertex()` and `db.new_document()`, there is no `db.new_edge()` method. This is by design in the Java API and reflected in Python:

- Edges conceptually **belong to vertices** - they represent relationships
- You must have both vertices before creating a connection
- Edges are created by calling `vertex.newEdge(edgeType, toVertex, properties)`

### Creating Edges with SQL

The most straightforward way to create edges is with SQL:

```python
with db.transaction():
    # Create vertices first
    db.command("sql", """
        CREATE VERTEX Person SET name = 'Alice', id = 1
    """)
    db.command("sql", """
        CREATE VERTEX Person SET name = 'Bob', id = 2
    """)

    # Create edge between them
    db.command("sql", """
        CREATE EDGE Knows
        FROM (SELECT FROM Person WHERE id = 1)
        TO (SELECT FROM Person WHERE id = 2)
        SET since = date('2020-01-15')
    """)
```

### Creating Edges with the API

To create edges programmatically, you must:

1. Create or retrieve both vertices
2. Call `newEdge()` on the source vertex
3. Save the edge

```python
with db.transaction():
    # Create vertices
    alice = db.new_vertex("Person")
    alice.set("name", "Alice")
    alice.set("id", 1)
    alice.save()

    bob = db.new_vertex("Person")
    bob.set("name", "Bob")
    bob.set("id", 2)
    bob.save()

    # Create edge FROM alice TO bob
    # Note: Called on the vertex, not the database!
    edge = alice.newEdge("Knows", bob)
    edge.set("since", "2020-01-15")
    edge.save()

    print(f"✅ Created edge: {alice.get('name')} -> {bob.get('name')}")
```

### Creating Edges with Retrieved Vertices

Often you'll create edges between existing vertices:

```python
# Query for existing vertices
result_alice = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
result_bob = db.query("sql", "SELECT FROM Person WHERE name = 'Bob'")

if result_alice.has_next() and result_bob.has_next():
    alice = result_alice.next()._java_result
    bob = result_bob.next()._java_result

    with db.transaction():
        # Create edge between existing vertices
        edge = alice.newEdge("Knows", bob)
        edge.set("since", "2020-01-15")
        edge.save()
```

!!! warning "Vertex Must Be Saved First"
    Before creating an edge, both vertices must be saved (have a valid RID).
    Attempting to create an edge with unsaved vertices will raise an error:
    ```
    IllegalArgumentException: Current vertex is not persistent. Call save() first
    ```

### Creating Edges with Cypher

Cypher provides a clean syntax for creating edges:

```python
with db.transaction():
    # Create vertices and edge in one statement
    db.command("cypher", """
        CREATE (alice:Person {name: 'Alice', age: 30})
        CREATE (bob:Person {name: 'Bob', age: 25})
        CREATE (alice)-[:Knows {since: '2020-01-15'}]->(bob)
    """)
```

Or connect existing vertices:

```python
with db.transaction():
    db.command("cypher", """
        MATCH (alice:Person {name: 'Alice'})
        MATCH (bob:Person {name: 'Bob'})
        CREATE (alice)-[:Knows {since: '2020-01-15'}]->(bob)
    """)
```

## Complete Example: Social Network

Here's a complete example building a small social network:

```python
import arcadedb_embedded as arcadedb

def create_social_network():
    with arcadedb.create_database("/tmp/social_network") as db:
        # 1. Create schema
        print("Creating schema...")
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE EDGE TYPE Knows")
        db.command("sql", "ALTER TYPE Person CREATE PROPERTY name STRING")
        db.command("sql", "ALTER TYPE Person CREATE PROPERTY age INTEGER")
        db.command("sql", "ALTER TYPE Knows CREATE PROPERTY since INTEGER")

        # 2. Create vertices and edges
        print("Creating graph data...")
        with db.transaction():
            # Create people
            alice = db.new_vertex("Person")
            alice.set("name", "Alice")
            alice.set("age", 30)
            alice.save()

            bob = db.new_vertex("Person")
            bob.set("name", "Bob")
            bob.set("age", 25)
            bob.save()

            charlie = db.new_vertex("Person")
            charlie.set("name", "Charlie")
            charlie.set("age", 35)
            charlie.save()

            # Create relationships
            edge1 = alice.newEdge("Knows", bob)
            edge1.set("since", 2020)
            edge1.save()

            edge2 = bob.newEdge("Knows", charlie)
            edge2.set("since", 2019)
            edge2.save()

            edge3 = charlie.newEdge("Knows", alice)
            edge3.set("since", 2021)
            edge3.save()

        print("✅ Graph created\n")

        # 3. Query the graph
        print("Finding Alice's friends:")
        result = db.query("cypher", """
            MATCH (person:Person {name: 'Alice'})-[:Knows]->(friend)
            RETURN friend.name AS name, friend.age AS age
        """)

        for record in result:
            name = record.get_property('name')
            age = record.get_property('age')
            print(f"  - {name}, age {age}")

        print("\nFinding friends of friends:")
        result = db.query("cypher", """
            MATCH (person:Person {name: 'Alice'})-[:Knows*2]->(fof)
            WHERE fof.name <> 'Alice'
            RETURN DISTINCT fof.name AS name
        """)

        for record in result:
            print(f"  - {record.get_property('name')}")

if __name__ == "__main__":
    create_social_network()
```

## Best Practices

### 1. Always Use Transactions for Writes

```python
# ✅ Good - wrapped in transaction
with db.transaction():
    vertex = db.new_vertex("Person")
    vertex.set("name", "Alice")
    vertex.save()

# ❌ Bad - will fail
vertex = db.new_vertex("Person")  # Error: No transaction!
```

### 2. Create Indexes for Frequent Lookups

```python
# Index properties used in WHERE clauses
db.command("sql", "CREATE INDEX ON Person (name) NOTUNIQUE")
db.command("sql", "CREATE INDEX ON Person (email) UNIQUE")
```

### 3. Use Cypher for Complex Graph Queries

Cypher is more expressive and readable for graph operations than SQL.

### 4. Save Vertices Before Creating Edges

```python
with db.transaction():
    v1 = db.new_vertex("Person")
    v1.set("name", "Alice")
    v1.save()  # ✅ Must save first!

    v2 = db.new_vertex("Person")
    v2.set("name", "Bob")
    v2.save()  # ✅ Must save first!

    # Now can create edge
    edge = v1.newEdge("Knows", v2)
    edge.save()
```

## Next Steps

- **[Vector Search](vectors.md)**: Add vector embeddings to vertices for similarity search
- **[Data Import](import.md)**: Import graph data from CSV, JSON, or Neo4j
- **[Server Mode](server.md)**: Visualize your graph in Studio UI
