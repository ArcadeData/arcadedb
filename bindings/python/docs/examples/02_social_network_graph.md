# Social Network Graph Example ✅

**Status: Complete and Fully Functional**

## Overview

This example demonstrates how to use ArcadeDB as a graph database to model and query social networks. It showcases the power of graph databases for representing and traversing complex relationships between entities.

**File:** `examples/02_social_network_graph.py`

**What You'll Learn:**
- Creating vertex and edge types (schema definition)
- Modeling entities (Person) and relationships (FRIEND_OF) with properties
- NULL value handling for optional vertex properties (email, phone, reputation)
- Graph traversal patterns (friends, friends-of-friends, mutual connections)
- Comparing SQL MATCH vs Cypher query languages for graph operations
- Variable-length path queries (`*1..3` syntax in Cypher)
- Working with relationship properties and bidirectional edges
- Filtering by NULL values (finding people without email/phone)
- Proper transaction handling and property access patterns
- Real-world graph database implementation techniques

## Real-World Use Case

Social networks are perfect examples of graph data structures where:
- **Entities** (people, companies, places) become vertices
- **Relationships** (friendships, follows, likes) become edges
- **Properties** store additional information about both entities and relationships
- **Graph queries** efficiently find patterns like "friends of friends" or "mutual connections"

This pattern applies to many domains:
- Social media platforms
- Professional networks (LinkedIn-style)
- Recommendation systems
- Fraud detection networks
- Knowledge graphs

## Key Graph Concepts

### Vertices (Nodes)
Represent entities in your domain:
```python
# Person vertex with properties (using embedded ArcadeDB)
with db.transaction():
    db.command("sql", "CREATE VERTEX Person SET name = ?, age = ?, city = ?",
               "Alice Johnson", 28, "New York")
```

### Edges (Relationships)
Connect vertices with optional properties:
```python
# Bidirectional friendship with relationship metadata
with db.transaction():
    # Create edge using property-based lookups (recommended approach)
    db.command("sql", """
        CREATE EDGE FRIEND_OF
        FROM (SELECT FROM Person WHERE name = 'Alice Johnson')
        TO (SELECT FROM Person WHERE name = 'Bob Smith')
        SET since = date('2020-05-15'), closeness = 'close'
    """)
```

### Schema Definition
Define types and properties upfront for consistency:
```python
# Create vertex type with properties (using transactions)
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE PROPERTY Person.name STRING")
    db.command("sql", "CREATE PROPERTY Person.age INTEGER")
    db.command("sql", "CREATE PROPERTY Person.city STRING")
    db.command("sql", "CREATE PROPERTY Person.joined_date DATE")
    db.command("sql", "CREATE PROPERTY Person.email STRING")      # Optional (can be NULL)
    db.command("sql", "CREATE PROPERTY Person.phone STRING")      # Optional (can be NULL)
    db.command("sql", "CREATE PROPERTY Person.verified BOOLEAN")
    db.command("sql", "CREATE PROPERTY Person.reputation FLOAT")  # Optional (can be NULL)

    # Create edge type with properties
    db.command("sql", "CREATE EDGE TYPE FRIEND_OF")
    db.command("sql", "CREATE PROPERTY FRIEND_OF.since DATE")
    db.command("sql", "CREATE PROPERTY FRIEND_OF.closeness STRING")

    # Create indexes for performance
    db.command("sql", "CREATE INDEX ON Person (name) NOTUNIQUE")
```

## Query Language Comparison

One of ArcadeDB's strengths is supporting multiple query languages for graph operations:

### SQL Approach
Traditional SQL with subqueries for graph traversal:
```sql
-- Find all friends of Alice (using SQL subqueries)
SELECT name, city FROM Person
WHERE name IN (
    SELECT p2.name
    FROM Person p1, FRIEND_OF f, Person p2
    WHERE p1.name = 'Alice Johnson'
      AND f.out = p1
      AND f.in = p2
)
ORDER BY name
```

### Cypher Syntax
Neo4j-compatible graph query language:
```cypher
-- Same query in Cypher (more intuitive for graph patterns)
MATCH (alice:Person {name: 'Alice Johnson'})-[:FRIEND_OF]->(friend:Person)
RETURN friend.name as name, friend.city as city
ORDER BY friend.name
```

### When to Use Each

**SQL Approach:**
- Familiar to SQL developers
- Good for mixing graph and relational queries
- Powerful for aggregations and data transformations
- Works well with traditional reporting tools

**Cypher:**
- More intuitive for graph patterns
- Shorter syntax for complex traversals
- Natural expression of graph relationships
- Better for pure graph operations

### Property Access in Python
When processing query results, use the property access API:
```python
# Process query results with proper property access
result = db.query("cypher", """
    MATCH (alice:Person {name: 'Alice Johnson'})-[:FRIEND_OF]->(friend:Person)
    RETURN friend.name as name, friend.city as city
""")

for row in result:
    name = row.get_property('name')  # Use .get_property() not ['name']
    city = row.get_property('city')
    print(f"Friend: {name} from {city}")
```

## NULL Value Handling in Graphs

Graph vertices can have optional properties with NULL values:

```python
# Insert person with NULL email and phone
with db.transaction():
    db.command("sql", """
        CREATE VERTEX Person SET
            name = 'Eve Brown',
            age = 29,
            city = 'Seattle',
            joined_date = date('2020-08-22'),
            email = NULL,
            phone = NULL,
            verified = false,
            reputation = 3.2
    """)
```

### Querying for NULL Values

Find vertices with missing optional properties:

```python
# Find people without email addresses
result = db.query("sql", """
    SELECT name, phone, verified
    FROM Person
    WHERE email IS NULL
""")

# Find verified people with reputation scores (exclude NULLs)
result = db.query("sql", """
    SELECT name, reputation, city
    FROM Person
    WHERE verified = true AND reputation IS NOT NULL
    ORDER BY reputation DESC
""")
```

This pattern is useful for:
- Finding incomplete profiles
- Identifying missing contact information
- Filtering by data completeness
- Quality checks and data validation

## Advanced Graph Patterns

### Friends of Friends
Finding second-degree connections:
```sql
-- SQL MATCH
MATCH {type: Person, as: alice, where: (name = 'Alice Johnson')}
      -FRIEND_OF->
      {type: Person, as: friend}
      -FRIEND_OF->
      {type: Person, as: friend_of_friend, where: (name <> 'Alice Johnson')}
RETURN DISTINCT friend_of_friend.name as name, friend.name as through_friend
```

```cypher
-- Cypher
MATCH (alice:Person {name: 'Alice Johnson'})
      -[:FRIEND_OF]->(friend:Person)
      -[:FRIEND_OF]->(fof:Person)
WHERE fof.name <> 'Alice Johnson'
RETURN DISTINCT fof.name as name, friend.name as through_friend
```

### Mutual Friends
Finding common connections between two people:
```sql
-- SQL MATCH
MATCH {type: Person, as: alice, where: (name = 'Alice Johnson')}
      -FRIEND_OF->
      {type: Person, as: mutual}
      <-FRIEND_OF-
      {type: Person, as: bob, where: (name = 'Bob Smith')}
RETURN mutual.name as mutual_friend
```

```cypher
-- Cypher
MATCH (alice:Person {name: 'Alice Johnson'})
      -[:FRIEND_OF]->(mutual:Person)
      <-[:FRIEND_OF]-(bob:Person {name: 'Bob Smith'})
RETURN mutual.name as mutual_friend
```

### Variable-Length Paths
Finding all connections within a certain distance:
```cypher
-- Cypher (SQL MATCH also supports this with different syntax)
MATCH (alice:Person {name: 'Alice Johnson'})
      -[:FRIEND_OF*1..3]-(connected:Person)
WHERE connected.name <> 'Alice Johnson'
RETURN DISTINCT connected.name as name
```

## Working with Relationship Properties

Edges can store metadata about relationships:
```python
# Create friendship with properties
db.command("sql", """
    CREATE EDGE FRIEND_OF FROM ? TO ?
    SET since = date(?), closeness = ?, interaction_frequency = ?
""", alice_rid, bob_rid, "2020-05-15", "close", "daily")

# Query based on relationship properties
result = db.query("cypher", """
    MATCH (p1:Person)-[f:FRIEND_OF {closeness: 'close'}]->(p2:Person)
    RETURN p1.name as person1, p2.name as person2, f.since as since
    ORDER BY f.since
""")
```

## Bidirectional Relationships

For symmetric relationships like friendship:
```python
# Create both directions
db.command("sql", "CREATE EDGE FRIEND_OF FROM ? TO ? SET since = date(?), closeness = ?",
           alice_rid, bob_rid, "2020-05-15", "close")
db.command("sql", "CREATE EDGE FRIEND_OF FROM ? TO ? SET since = date(?), closeness = ?",
           bob_rid, alice_rid, "2020-05-15", "close")
```

This allows queries to work in either direction without specifying directionality.

## Performance Considerations

### Indexing
Create indexes on frequently queried properties:
```python
# Index on person names for fast lookups
db.command("sql", "CREATE INDEX ON Person (name) IF NOT EXISTS")

# Composite indexes for complex queries
db.command("sql", "CREATE INDEX ON Person (city, age) IF NOT EXISTS")
```

### Batch Operations
For large datasets, use batch operations:
```python
# Batch vertex creation
batch_vertices = []
for person_data in large_dataset:
    batch_vertices.append(f"CREATE VERTEX Person SET name = '{person_data['name']}', ...")

# Execute as transaction
db.begin()
try:
    for statement in batch_vertices:
        db.command("sql", statement)
    db.commit()
except Exception as e:
    db.rollback()
    raise
```

## Expected Output

When you run the example, you'll see comprehensive output showing all graph operations:

```
🌐 ArcadeDB Python - Social Network Graph Example
=======================================================
🔌 Creating/connecting to database...
✅ Database created at: ./my_test_databases/social_network_db
💡 Using embedded mode - no server needed!

📊 Creating social network schema...
  ✓ Created Person vertex type
  ✓ Created Person properties
  ✓ Created FRIEND_OF edge type
  ✓ Created FRIEND_OF properties
  ✓ Created index on Person.name

👥 Creating sample social network data...
  📝 Creating people...
    ✓ Created person: Alice Johnson (28, New York)
    ✓ Created person: Bob Smith (32, San Francisco)
    ✓ Created person: Carol Davis (26, Chicago)
    ... (8 people total)
  🤝 Creating friendships...
    ✓ Connected Alice Johnson ↔ Bob Smith (close)
    ✓ Connected Alice Johnson ↔ Carol Davis (casual)
    ... (12 bidirectional friendships = 24 edges)
  ✅ Created 8 people and 24 friendship connections

🔍 Demonstrating graph queries...

  📊 SQL MATCH Queries:
    1️⃣ Find all friends of Alice (SQL MATCH):
      👥 Bob Smith from San Francisco
      👥 Carol Davis from Chicago
      👥 Eve Brown from Seattle

    2️⃣ Find friends of friends of Alice (SQL MATCH):
      🔗 David Wilson (through Bob Smith)
      🔗 Frank Miller (through Bob Smith)
      🔗 Grace Lee (through Carol Davis)
      ... (7 results showing network expansion)

  🎯 Cypher Queries:
    1️⃣ Find all friends of Alice (Cypher):
      👥 Bob Smith from San Francisco
      👥 Carol Davis from Chicago
      👥 Eve Brown from Seattle

    4️⃣ Find close friendships (Cypher):
      💙 Alice Johnson → Bob Smith (since 2020-05-15)
      💙 Carol Davis → Eve Brown (since 2021-09-12)
      ... (8 close relationships)

    5️⃣ Find connections within 3 steps from Alice (Cypher):
      🌐 Bob Smith from San Francisco
      🌐 Carol Davis from Chicago
      🌐 David Wilson from Boston
      ... (7 people reachable within 3 hops)

  🆚 SQL vs Cypher Comparison:
    ✅ Cypher query returned 3 results
    💡 SQL and Cypher would yield equivalent results
    🎯 Key Differences: SQL uses subqueries, Cypher uses pattern matching

✅ Social network graph example completed successfully!
✅ Database connection closed
💡 Database files preserved at: ./my_test_databases/social_network_db
```

## Try It Yourself

1. **Run the example:**
   ```bash
   # Important: Navigate to examples directory first
   cd bindings/python/examples
   python 02_social_network_graph.py
   ```

2. **Explore the database:**
   - Database files are created in `./my_test_databases/social_network_db/`
   - Inspect the console output to understand each operation
   - Try modifying the sample data in the code

3. **Experiment with queries:**
   - Modify the Cypher queries in `demonstrate_cypher_queries()`
   - Add new relationship types (WORKS_WITH, LIVES_NEAR)
   - Try different traversal patterns and depths
   - Add relationship scoring (strength, trust level)

4. **Scale it up:**
   - Import larger datasets from CSV files
   - Add more vertex types (Company, Location, Interest)
   - Implement recommendation algorithms
   - Add temporal aspects (friendship start/end dates)

## Key Implementation Notes

### Property Access Pattern
```python
# Always use .get_property() for query results
for row in result:
    name = row.get_property('name')  # ✅ Correct
    city = row.get_property('city')  # ✅ Correct
    # name = row['name']             # ❌ Wrong - will fail
```

### Transaction Handling
```python
# Wrap all write operations in transactions
with db.transaction():
    db.command("sql", "CREATE VERTEX Person SET name = ?", "Alice")
    db.command("sql", "CREATE EDGE FRIEND_OF FROM (...) TO (...)")
```

### Edge Creation Best Practice
```python
# Use property-based lookups instead of RIDs
db.command("sql", """
    CREATE EDGE FRIEND_OF
    FROM (SELECT FROM Person WHERE name = 'Alice')
    TO (SELECT FROM Person WHERE name = 'Bob')
    SET since = date('2020-05-15')
""")
```

## Common Patterns

### Recommendation System
```cypher
-- Find people you might know (friends of friends who aren't already friends)
MATCH (me:Person {name: 'Alice Johnson'})
      -[:FRIEND_OF]->(:Person)
      -[:FRIEND_OF]->(recommended:Person)
WHERE NOT (me)-[:FRIEND_OF]-(recommended) AND me <> recommended
RETURN recommended.name, COUNT(*) as mutual_friends
ORDER BY mutual_friends DESC
```

### Social Distance
```cypher
-- Find shortest path between two people
MATCH path = shortestPath((start:Person {name: 'Alice Johnson'})
                         -[:FRIEND_OF*]-(end:Person {name: 'Henry Clark'}))
RETURN length(path) as degrees_of_separation
```

### Influencer Detection
```sql
-- Find most connected people
SELECT name, in('FRIEND_OF').size() as friend_count
FROM Person
ORDER BY friend_count DESC
LIMIT 5
```

## Related Examples

- **01_simple_document_store.py** - Basic database operations and schema
- **04_csv_import_to_graph.py** - Importing graph data from files
- **05_ecommerce_multimodel.py** - Combining graph with document storage

## Next Steps

- Learn about [Vector Embeddings](03_vector_embeddings_search.md) for AI-powered recommendations
- Explore [CSV Import](04_csv_import_to_graph.md) for real-world data migration
- See [Multi-Model](05_ecommerce_multimodel.md) for combining graph with document storage

---

*This example demonstrates core graph database concepts that apply across many domains. The patterns shown here scale from small applications to enterprise social platforms.*
