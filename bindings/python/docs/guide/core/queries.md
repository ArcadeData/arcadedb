# Query Languages Guide

ArcadeDB supports multiple query languages, each with different strengths. This guide helps you choose the right language and write effective queries.

## Overview

Supported Query Languages:

- **SQL**: Primary language, full-featured, best for most use cases
- **Cypher**: OpenCypher for graph pattern matching
- **Gremlin**: Apache TinkerPop for graph traversals (requires full distribution)
- **MongoDB**: MongoDB-compatible syntax (requires mongodbw plugin)
- **GraphQL**: GraphQL queries (requires graphql plugin)

## Quick Reference

| Language | Best For | Distribution | Learning Curve |
|----------|----------|--------------|----------------|
| SQL      | General queries, analytics | All | Easy |
| Cypher   | Graph patterns, relationships | All | Medium |
| Gremlin  | Complex graph algorithms | Full only | Hard |
| MongoDB  | Document queries | Full only | Easy |
| GraphQL  | API queries | Full only | Medium |

## SQL

### Why SQL?

**Primary language** for ArcadeDB with the most complete feature set.

**Strengths:**
- Full CRUD operations
- Schema management
- Indexes and constraints
- Aggregations
- Graph traversal extensions
- Best performance

**Use SQL when:**
- You need full control
- Building schema or indexes
- Doing analytics
- Writing production queries

---

### Basic SELECT

```python
# Simple select
result = db.query("sql", "SELECT FROM User")
for vertex in result:
    print(vertex.get("name"))

# With WHERE clause
result = db.query("sql", "SELECT FROM User WHERE age > 25")

# Projection
result = db.query("sql", "SELECT name, email FROM User")

# LIMIT and SKIP
result = db.query("sql", "SELECT FROM User LIMIT 10 SKIP 20")

# ORDER BY
result = db.query("sql", "SELECT FROM User ORDER BY name ASC")
```

---

### Graph Traversal

```python
# Outgoing edges
result = db.query("sql", """
    SELECT expand(out('Follows'))
    FROM User
    WHERE name = 'Alice'
""")

# Incoming edges
result = db.query("sql", """
    SELECT expand(in('Purchased'))
    FROM Product
    WHERE id = 'PROD123'
""")

# Both directions
result = db.query("sql", """
    SELECT expand(both('FriendOf'))
    FROM User
    WHERE name = 'Bob'
""")

# Multi-hop traversal
result = db.query("sql", """
    SELECT expand(out('Follows').out('Follows'))
    FROM User
    WHERE name = 'Alice'
""")
# Friends of friends

# Filtered traversal
result = db.query("sql", """
    SELECT expand(out('Purchased')[price > 100])
    FROM User
    WHERE name = 'Alice'
""")
```

---

### Aggregations

```python
# COUNT
result = db.query("sql", "SELECT count(*) as total FROM User")
total = result.next().get("total")

# AVG, SUM, MIN, MAX
result = db.query("sql", """
    SELECT
        avg(age) as avg_age,
        min(age) as min_age,
        max(age) as max_age
    FROM User
""")

# GROUP BY
result = db.query("sql", """
    SELECT category, count(*) as count
    FROM Product
    GROUP BY category
    ORDER BY count DESC
""")
```

---

### JOINs and Subqueries

```python
# Implicit JOIN via edges
result = db.query("sql", """
    SELECT
        User.name,
        out('Purchased').name as products
    FROM User
    WHERE User.name = 'Alice'
""")

# Subquery in WHERE
result = db.query("sql", """
    SELECT FROM Product
    WHERE @rid IN (
        SELECT out('Purchased')
        FROM User
        WHERE name = 'Alice'
    )
""")

# WITH clause (CTE)
result = db.query("sql", """
    WITH $active_users = (SELECT FROM User WHERE last_login > sysdate() - 7)
    SELECT FROM $active_users
    WHERE age > 25
""")
```

---

### Full-Text Search

```python
# Create full-text index
with db.transaction():
    db.command("sql", """
        CREATE INDEX Product.description_idx
        ON Product (description) FULLTEXT ENGINE LUCENE
    """)

# Search
result = db.query("sql", """
    SELECT FROM Product
    WHERE SEARCH_INDEX('Product.description_idx', 'laptop computer') = true
""")
```

---

### Parameters

```python
# Named parameters (RECOMMENDED)
result = db.query("sql",
    "SELECT FROM User WHERE name = :name AND age > :min_age",
    {
        "name": "Alice",
        "min_age": 25
    }
)

# Positional parameters
result = db.query("sql",
    "SELECT FROM User WHERE name = ? AND age > ?",
    ["Alice", 25]
)
```

**Always use parameters** to prevent SQL injection!

---

### Advanced SQL

```python
# MATCH (graph pattern)
result = db.query("sql", """
    MATCH {type: User, as: u}
        -[:Follows]->
        {as: f}
        -[:Purchased]->
        {type: Product, as: p}
    WHERE u.name = 'Alice'
    RETURN f.name, p.name
""")

# TRAVERSE
result = db.query("sql", """
    TRAVERSE out('Knows')
    FROM (SELECT FROM User WHERE name = 'Alice')
    MAXDEPTH 3
""")

# LET (variables)
result = db.query("sql", """
    SELECT name, $friends_count
    FROM User
    LET $friends_count = out('Knows').size()
    WHERE $friends_count > 10
""")
```

## Cypher

### Why Cypher?

**Graph-first** query language from Neo4j, now OpenCypher standard.

**Strengths:**
- Intuitive graph patterns
- Easy relationship queries
- Readable syntax
- Industry standard for graphs

**Use Cypher when:**
- Your team knows Neo4j
- Focus on graph patterns
- Migrating from Neo4j
- Prefer declarative style

---

### Basic MATCH

```python
# Match all users
result = db.query("cypher", "MATCH (u:User) RETURN u")

# Match with properties
result = db.query("cypher", """
    MATCH (u:User {name: 'Alice'})
    RETURN u
""")

# Match with WHERE
result = db.query("cypher", """
    MATCH (u:User)
    WHERE u.age > 25
    RETURN u.name, u.email
""")
```

---

### Relationship Patterns

```python
# Outgoing relationship
result = db.query("cypher", """
    MATCH (u:User {name: 'Alice'})-[:FOLLOWS]->(f)
    RETURN f.name
""")

# Incoming relationship
result = db.query("cypher", """
    MATCH (p:Product)<-[:PURCHASED]-(u:User)
    WHERE p.id = 'PROD123'
    RETURN u.name
""")

# Bidirectional
result = db.query("cypher", """
    MATCH (u1:User {name: 'Alice'})-[:FRIEND_OF]-(u2:User)
    RETURN u2.name
""")

# Variable length paths
result = db.query("cypher", """
    MATCH (u1:User {name: 'Alice'})-[:FOLLOWS*1..3]->(u2:User)
    RETURN u2.name
""")
# 1 to 3 hops

# Path with properties
result = db.query("cypher", """
    MATCH (u:User)-[r:PURCHASED {verified: true}]->(p:Product)
    RETURN u.name, p.name, r.date
""")
```

---

### CREATE and MERGE

```python
# Create node
result = db.query("cypher", """
    CREATE (u:User {name: 'Alice', age: 30})
    RETURN u
""")

# Create relationship
result = db.query("cypher", """
    MATCH (u1:User {name: 'Alice'}), (u2:User {name: 'Bob'})
    CREATE (u1)-[:FOLLOWS]->(u2)
""")

# MERGE (create if not exists)
result = db.query("cypher", """
    MERGE (u:User {email: 'alice@example.com'})
    ON CREATE SET u.created_at = timestamp()
    ON MATCH SET u.last_seen = timestamp()
    RETURN u
""")
```

---

### Aggregations

```python
# COUNT
result = db.query("cypher", """
    MATCH (u:User)
    RETURN count(u) as total
""")

# AVG, MIN, MAX
result = db.query("cypher", """
    MATCH (u:User)
    RETURN avg(u.age) as avg_age,
           min(u.age) as min_age,
           max(u.age) as max_age
""")

# GROUP BY (implicit)
result = db.query("cypher", """
    MATCH (u:User)-[:PURCHASED]->(p:Product)
    RETURN p.category, count(u) as buyers
    ORDER BY buyers DESC
""")

# COLLECT
result = db.query("cypher", """
    MATCH (u:User {name: 'Alice'})-[:FOLLOWS]->(f)
    RETURN collect(f.name) as following
""")
```

---

### Parameters

```python
# Named parameters
result = db.query("cypher",
    "MATCH (u:User {name: $name}) WHERE u.age > $min_age RETURN u",
    {
        "name": "Alice",
        "min_age": 25
    }
)
```

---

### Advanced Patterns

```python
# Optional match (like LEFT JOIN)
result = db.query("cypher", """
    MATCH (u:User)
    OPTIONAL MATCH (u)-[:PURCHASED]->(p:Product)
    RETURN u.name, p.name
""")

# Multiple patterns
result = db.query("cypher", """
    MATCH (u:User {name: 'Alice'})-[:FOLLOWS]->(f)
    MATCH (f)-[:PURCHASED]->(p:Product)
    RETURN DISTINCT p.name
""")

# Path
result = db.query("cypher", """
    MATCH path = (u1:User {name: 'Alice'})-[:KNOWS*]-(u2:User {name: 'Bob'})
    RETURN path
""")

# Shortest path
result = db.query("cypher", """
    MATCH path = shortestPath(
        (u1:User {name: 'Alice'})-[:KNOWS*]-(u2:User {name: 'Bob'})
    )
    RETURN length(path) as degrees_of_separation
""")
```

## Gremlin

### Why Gremlin?

**Traversal-based** graph query language from Apache TinkerPop.

**Requirements:**
- Full distribution only
- More complex setup

**Strengths:**
- Powerful graph algorithms
- Imperative traversal style
- Industry standard
- Rich graph analytics

**Use Gremlin when:**
- Need complex graph algorithms
- PageRank, community detection
- Your team knows TinkerPop
- Building graph analytics

---

### Basic Traversals

```python
# Get all vertices
result = db.query("gremlin", "g.V()")

# Filter by label
result = db.query("gremlin", "g.V().hasLabel('User')")

# Filter by property
result = db.query("gremlin", "g.V().has('User', 'name', 'Alice')")

# Range filter
result = db.query("gremlin", "g.V().has('User', 'age', gt(25))")
```

---

### Graph Traversals

```python
# Outgoing edges
result = db.query("gremlin", """
    g.V().has('User', 'name', 'Alice').out('Follows')
""")

# Incoming edges
result = db.query("gremlin", """
    g.V().has('Product', 'id', 'PROD123').in('Purchased')
""")

# Multi-hop
result = db.query("gremlin", """
    g.V().has('User', 'name', 'Alice')
        .out('Follows')
        .out('Follows')
""")

# Repeat
result = db.query("gremlin", """
    g.V().has('User', 'name', 'Alice')
        .repeat(out('Knows'))
        .times(3)
""")
```

---

### Aggregations

```python
# Count
result = db.query("gremlin", "g.V().hasLabel('User').count()")

# Group by
result = db.query("gremlin", """
    g.V().hasLabel('Product')
        .group()
        .by('category')
        .by(count())
""")

# Statistics
result = db.query("gremlin", """
    g.V().hasLabel('User')
        .values('age')
        .mean()
""")
```

---

### Path Queries

```python
# Find paths
result = db.query("gremlin", """
    g.V().has('User', 'name', 'Alice')
        .repeat(out('Knows'))
        .until(has('name', 'Bob'))
        .path()
""")

# Shortest path
result = db.query("gremlin", """
    g.V().has('User', 'name', 'Alice')
        .repeat(out('Knows').simplePath())
        .until(has('name', 'Bob'))
        .path()
        .limit(1)
""")
```

## MongoDB Syntax

### Why MongoDB?

**Document-oriented** query syntax for document databases.

**Requirements:**
- Full distribution only
- mongodbw plugin

**Strengths:**
- Familiar to MongoDB users
- Good for documents
- JSON-like syntax

**Use MongoDB syntax when:**
- Migrating from MongoDB
- Team knows MongoDB
- Primarily document operations

---

### Basic Queries

```python
# Find all
result = db.query("mongodb", """
    db.User.find()
""")

# Find with filter
result = db.query("mongodb", """
    db.User.find({name: 'Alice'})
""")

# Find with operators
result = db.query("mongodb", """
    db.User.find({age: {$gt: 25}})
""")

# Projection
result = db.query("mongodb", """
    db.User.find({}, {name: 1, email: 1})
""")
```

---

### Aggregation Pipeline

```python
# Group and count
result = db.query("mongodb", """
    db.Product.aggregate([
        {$group: {
            _id: '$category',
            count: {$sum: 1}
        }}
    ])
""")

# Match, group, sort
result = db.query("mongodb", """
    db.User.aggregate([
        {$match: {age: {$gt: 25}}},
        {$group: {
            _id: '$city',
            avg_age: {$avg: '$age'}
        }},
        {$sort: {avg_age: -1}}
    ])
""")
```

## Language Comparison

### Finding Users

**SQL:**
```python
db.query("sql", "SELECT FROM User WHERE age > 25")
```

**Cypher:**
```python
db.query("cypher", "MATCH (u:User) WHERE u.age > 25 RETURN u")
```

**Gremlin:**
```python
db.query("gremlin", "g.V().hasLabel('User').has('age', gt(25))")
```

**MongoDB:**
```python
db.query("mongodb", "db.User.find({age: {$gt: 25}})")
```

---

### Following Relationships

**SQL:**
```python
db.query("sql", """
    SELECT expand(out('Follows'))
    FROM User
    WHERE name = 'Alice'
""")
```

**Cypher:**
```python
db.query("cypher", """
    MATCH (u:User {name: 'Alice'})-[:FOLLOWS]->(f)
    RETURN f
""")
```

**Gremlin:**
```python
db.query("gremlin", """
    g.V().has('User', 'name', 'Alice').out('Follows')
""")
```

---

### Aggregation

**SQL:**
```python
db.query("sql", """
    SELECT category, count(*) as count
    FROM Product
    GROUP BY category
""")
```

**Cypher:**
```python
db.query("cypher", """
    MATCH (p:Product)
    RETURN p.category, count(p) as count
""")
```

**Gremlin:**
```python
db.query("gremlin", """
    g.V().hasLabel('Product')
        .group()
        .by('category')
        .by(count())
""")
```

**MongoDB:**
```python
db.query("mongodb", """
    db.Product.aggregate([
        {$group: {_id: '$category', count: {$sum: 1}}}
    ])
""")
```

## Query Optimization

### Use Indexes

```python
# Create index first
with db.transaction():
    db.command("sql", "CREATE INDEX ON User (email) UNIQUE")
    db.command("sql", "CREATE INDEX ON Product (category) NOTUNIQUE")

# Query with indexed fields
result = db.query("sql", """
    SELECT FROM User
    WHERE email = 'alice@example.com'
""")
# Uses index - fast!

result = db.query("sql", """
    SELECT FROM Product
    WHERE category = 'Electronics'
""")
# Uses index - fast!
```

---

### EXPLAIN Plans

```python
# Analyze query execution
result = db.query("sql", """
    EXPLAIN
    SELECT FROM User
    WHERE email = 'alice@example.com'
""")

for row in result:
    print(row.to_dict())
# Shows: index usage, estimated cost, execution plan
```

---

### Limit Results

```python
# Bad: Load everything
result = db.query("sql", "SELECT FROM User")
# Could be millions of rows!

# Good: Use LIMIT
result = db.query("sql", "SELECT FROM User LIMIT 100")

# Pagination
result = db.query("sql", "SELECT FROM User LIMIT 100 SKIP 200")
```

---

### Projection (Select Specific Fields)

```python
# Bad: Load all properties
result = db.query("sql", "SELECT FROM User")
# Loads everything!

# Good: Project only needed fields
result = db.query("sql", "SELECT name, email FROM User")
# Much faster!
```

---

### Batch Operations

```python
# Bad: Many small queries
for user_id in user_ids:
    result = db.query("sql", f"SELECT FROM User WHERE id = '{user_id}'")
    # 100 round trips!

# Good: Single query with IN
ids_str = "', '".join(user_ids)
result = db.query("sql", f"SELECT FROM User WHERE id IN ['{ids_str}']")
# 1 round trip!

# Better: Use parameters
result = db.query("sql",
    "SELECT FROM User WHERE id IN :ids",
    {"ids": user_ids}
)
```

---

### Transaction Size

```python
# Bad: Huge transaction
with db.transaction():
    for i in range(1000000):
        # Memory exhaustion!
        pass

# Good: Batch transactions
batch_size = 10000
for i in range(0, 1000000, batch_size):
    with db.transaction():
        for j in range(i, min(i + batch_size, 1000000)):
            # Process batch
            pass
```

## Best Practices

1. **Use SQL for Most Queries**: Most complete, best performance
2. **Use Cypher for Graph Patterns**: More readable for relationships
3. **Always Use Parameters**: Prevent SQL injection
4. **Create Indexes**: For frequently queried fields
5. **Use EXPLAIN**: Analyze slow queries
6. **Limit Results**: Always use LIMIT in production
7. **Project Fields**: Don't select * unless you need everything
8. **Batch Operations**: Reduce round trips
9. **Right-Size Transactions**: Not too big, not too small
10. **Test Performance**: Profile with realistic data

## Common Patterns

### Pagination

```python
def paginate(db, page, page_size=20):
    """Paginate query results."""
    skip = page * page_size
    result = db.query("sql",
        f"SELECT FROM User ORDER BY name LIMIT {page_size} SKIP {skip}")
    return list(result)

# Usage
page_1 = paginate(db, 0)
page_2 = paginate(db, 1)
```

---

### Search with Filters

```python
def search_users(db, name_filter=None, min_age=None, max_age=None):
    """Flexible search with optional filters."""
    conditions = []
    params = {}

    if name_filter:
        conditions.append("name LIKE :name")
        params["name"] = f"%{name_filter}%"

    if min_age:
        conditions.append("age >= :min_age")
        params["min_age"] = min_age

    if max_age:
        conditions.append("age <= :max_age")
        params["max_age"] = max_age

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    query = f"SELECT FROM User WHERE {where_clause}"

    return db.query("sql", query, params)

# Usage
results = search_users(db, name_filter="Ali", min_age=25, max_age=35)
```

---

### Graph Recommendations

```python
def recommend_products(db, user_name, k=5):
    """Recommend products based on friends' purchases."""
    result = db.query("sql", """
        SELECT
            product.name,
            count(*) as friend_purchases
        FROM (
            SELECT expand(out('Follows'))
            FROM User
            WHERE name = :name
        ) as friends
        LET product = friends.out('Purchased')
        WHERE product NOT IN (
            SELECT out('Purchased')
            FROM User
            WHERE name = :name
        )
        GROUP BY product
        ORDER BY friend_purchases DESC
        LIMIT :k
    """, {"name": user_name, "k": k})

    return [(row.get("name"), row.get("friend_purchases"))
            for row in result]

# Usage
recommendations = recommend_products(db, "Alice", k=5)
for product, count in recommendations:
    print(f"{product} ({count} friends bought this)")
```

## See Also

- [Database API Reference](../../api/database.md) - query() and command() methods
- [Results API Reference](../../api/results.md) - Working with query results
- [Transactions](../../api/transactions.md) - Transaction management
- [ArcadeDB SQL Reference](https://docs.arcadedb.com/#SQL) - Official SQL documentation
