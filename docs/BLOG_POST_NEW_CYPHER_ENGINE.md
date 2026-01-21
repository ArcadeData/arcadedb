# Native OpenCypher: A Game-Changer for ArcadeDB Graph Queries

We're thrilled to announce a major milestone for ArcadeDB: a brand-new, native OpenCypher engine that replaces the legacy Cypher-to-Gremlin translation layer. This isn't just an incremental improvement—it's a complete reimplementation that brings massive performance gains, a smaller footprint, and full support for modern Cypher syntax.

## What Changed?

For years, ArcadeDB supported Cypher queries through a translation layer developed by Neo4j that converted Cypher syntax to Gremlin traversals. While this approach worked, it had significant limitations:

- **Abandoned Technology**: The Cypher-to-Gremlin translator was abandoned by Neo4j years ago, leaving us with outdated Cypher grammar support
- **Heavy Dependencies**: The old implementation required over 100MB of additional JAR files
- **Performance Overhead**: Every query had to be translated from Cypher to Gremlin, adding latency and preventing optimal query planning
- **Limited Distribution Options**: Due to its size and dependencies, Cypher couldn't be included in our minimal distribution

**The new native OpenCypher engine changes everything.**

## A True Native Implementation

Our new Cypher engine is built from the ground up as a first-class citizen in ArcadeDB:

### 1. **Direct Query Execution**
Cypher queries are now parsed and executed directly against ArcadeDB's graph engine—no translation layer, no intermediate representation, no unnecessary overhead. The result? Query execution that's **3.5x to 187x faster** depending on the query pattern.

### 2. **Minimal Dependencies**
The entire implementation requires only the ANTLR library—the same parser generator we use for our SQL engine. We've gone from **over 100MB of dependencies to just a few megabytes**. This dramatic reduction means:
- Faster application startup
- Smaller container images
- Lower memory footprint
- Cleaner dependency trees

### 3. **Part of the Minimal Distribution**
Because of its tiny footprint, Cypher is now included in ArcadeDB's minimal distribution. You no longer need to choose between a lightweight deployment and Cypher support—you get both.

### 4. **Modern Cypher Grammar (Version 25)**
The new engine implements the latest OpenCypher grammar specification (Cypher 25), giving you access to modern Cypher features and syntax that weren't available in the old translator. This brings ArcadeDB's Cypher support in line with current industry standards.

## Performance: The Numbers Speak for Themselves

We ran comprehensive benchmarks comparing the legacy Cypher-to-Gremlin approach against the new native engine. The results exceeded our expectations:

```
╔═════════════════════════════════════════════════════════════════════════════════════════════════╗
║                               BENCHMARK RESULTS SUMMARY                                         ║
╠═════════════════════════════════════════════════════════════════════════════════════════════════╣
║ Query Type                          │  Legacy (µs) │  Native (µs) │    Diff (µs) │    Speedup   ║
╟─────────────────────────────────────┼──────────────┼──────────────┼──────────────┼──────────────╢
║ Join Ordering                       │       18,116 │        1,326 │       16,790 │     13.66x ⚡ ║
║ Multi-Hop Pattern (2-hop traversal) │       40,586 │       11,574 │       29,012 │      3.51x ⚡ ║
║ Index Seek (Selective Query)        │        5,079 │           49 │        5,030 │    102.89x ⚡ ║
║ Full Scan (Non-Selective Query)     │       11,136 │        1,734 │        9,402 │      6.42x ⚡ ║
║ Cross-Type Relationship             │        4,834 │           25 │        4,809 │    186.95x ⚡ ║
║ Relationship Traversal              │        3,568 │          134 │        3,434 │     26.46x ⚡ ║
╚═════════════════════════════════════════════════════════════════════════════════════════════════╝

Overall: Native OpenCypher is 25.74x faster than Legacy Cypher (Gremlin-based)
```

**Key Highlights:**

- **Cross-Type Relationship Queries**: 187x faster (4,834µs → 25µs)
  - Traversing from Person to Company nodes shows the power of native graph operations

- **Index Seek Performance**: 103x faster (5,079µs → 49µs)
  - Direct index access without translation overhead

- **Relationship Traversal**: 26x faster (3,568µs → 134µs)
  - Native edge navigation is dramatically more efficient

- **Join Ordering**: 14x faster (18,116µs → 1,326µs)
  - Cost-based optimizer makes intelligent decisions about query execution

- **Full Scans**: 6.4x faster (11,136µs → 1,734µs)
  - Even simple scans benefit from eliminating the translation layer

- **Multi-Hop Patterns**: 3.5x faster (40,586µs → 11,574µs)
  - Complex graph traversals show consistent improvement

**Bottom line**: Across all query patterns, the native engine is **25.74x faster on average**. For queries that benefit from index usage and optimal query planning, we're seeing improvements of **100x or more**.

## What This Means for You

### For New Users
- **Modern Cypher Support**: Use the latest Cypher syntax and features
- **Excellent Performance**: Graph queries execute at blazing speed
- **Smaller Deployments**: Include Cypher in even the most space-constrained environments

### For Existing Users
If you've been using the legacy Cypher implementation, here's what to expect:

**✅ Compatibility**: Most Cypher queries should work as-is. The new engine implements standard OpenCypher semantics.

**✅ Performance Boost**: Your queries will run dramatically faster—especially those involving indexes, joins, and relationship traversals.

**✅ Smaller Footprint**: Reduce your application's dependency size by over 100MB.

### Migration Path

The new native Cypher engine is accessible via the `opencypher` language identifier:

```java
// New native OpenCypher engine (recommended)
database.query("opencypher", "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p");
```

The legacy Cypher-to-Gremlin translator remains available via the `cypher` identifier for backward compatibility, but we strongly recommend migrating to the native engine:

```java
// Legacy Cypher (deprecated, for backward compatibility only)
database.query("cypher", "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p");
```

**We plan to deprecate and eventually remove the legacy translator in a future release**, so please start testing your applications with `opencypher` as soon as possible.

## Cost-Based Query Optimization

One of the most powerful features of the native engine is its cost-based optimizer. It analyzes your query, considers available indexes and data statistics, and chooses the most efficient execution plan. This is especially impactful for:

- **Index Selection**: Automatically uses indexes when they provide better performance
- **Join Ordering**: Starts from the most selective side of a join, dramatically reducing intermediate results
- **Predicate Pushdown**: Applies filters as early as possible in query execution

This isn't just theory—the benchmark results above show the real-world impact of intelligent query optimization.

## OpenCypher Grammar 25

The new engine implements the OpenCypher Grammar version 25, which includes modern Cypher features like:

- Advanced pattern matching capabilities
- Improved expression syntax
- Better type handling
- Enhanced aggregation functions
- Map and list projections

This brings ArcadeDB's Cypher implementation in line with current industry standards and ensures compatibility with modern Cypher tooling and documentation.

## We Need Your Feedback!

This is a major release, and while we've conducted extensive testing, real-world usage always reveals scenarios we haven't anticipated. We need your help to make the native Cypher engine even better!

**If you encounter any issues, unexpected behavior, or have suggestions for improvement, please reach out:**

- **GitHub Issues**: [https://github.com/ArcadeData/arcadedb/issues](https://github.com/ArcadeData/arcadedb/issues)
  - Include your Cypher query
  - Describe the expected vs. actual behavior
  - Mention the ArcadeDB version you're using

- **Discord Community**: Join our Discord server for real-time discussions
  - Get help from the community
  - Share your experiences with the new engine
  - Discuss performance optimizations

Your feedback is essential in ensuring the native Cypher engine meets the needs of the ArcadeDB community.

## Technical Implementation

For those interested in the technical details, the new engine is built on:

- **ANTLR Parser**: Uses the ANTLR parser generator with the official OpenCypher grammar specification
- **Cost-Based Optimizer**: Analyzes query patterns and data statistics to choose optimal execution plans
- **Native Graph Operations**: Direct execution against ArcadeDB's graph engine without intermediate translation
- **LSM-Tree Indexes**: Full integration with ArcadeDB's index system for fast lookups

The implementation is clean, maintainable, and positions us well for future Cypher language enhancements.

## Looking Forward

The native OpenCypher engine represents a major step forward for ArcadeDB's graph capabilities. It demonstrates our commitment to:

- **Performance**: Delivering the fastest possible graph query execution
- **Modern Standards**: Supporting current industry specifications
- **Lightweight Design**: Keeping ArcadeDB lean and efficient
- **Developer Experience**: Providing powerful tools with minimal complexity

We're excited to see what you'll build with these new capabilities. The combination of ArcadeDB's multi-model architecture, high-performance storage engine, and now native OpenCypher support creates a compelling platform for modern graph applications.

## Get Started Today

To try the native OpenCypher engine:

1. **Update to ArcadeDB 26.1.1 or later**
2. **Use the `opencypher` language identifier** in your queries
3. **Monitor performance improvements** in your application
4. **Report any issues** you encounter

Here's a simple example to get you started:

```java
// Create some graph data
database.transaction(() -> {
  database.command("opencypher",
    "CREATE (p:Person {name: 'Alice', age: 30})");
  database.command("opencypher",
    "CREATE (p:Person {name: 'Bob', age: 35})");
  database.command("opencypher",
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
    "CREATE (a)-[:KNOWS]->(b)");
});

// Query the graph
ResultSet rs = database.query("opencypher",
  "MATCH (a:Person)-[:KNOWS]->(b:Person) " +
  "WHERE a.age > 25 " +
  "RETURN a.name, b.name");

while (rs.hasNext()) {
  Result result = rs.next();
  System.out.println(result.getProperty("a.name") + " knows " +
                     result.getProperty("b.name"));
}
```

Thank you for being part of the ArcadeDB community. We can't wait to hear about your experiences with the native OpenCypher engine!

---

*The ArcadeDB Team*
