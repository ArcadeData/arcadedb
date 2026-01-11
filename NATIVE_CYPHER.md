# Native OpenCypher Query Language Implementation Plan

**Status**: Ready for Implementation
**Language Name**: `opencypher` (coexists with current `cypher`)
**Scope**: Comprehensive Cypher 2.5 implementation
**Performance Focus**: Graph traversals, index utilization, large result sets

> **Note**: This plan should be copied to `/Users/luca/Documents/GitHub/arcadedb/NATIVE_CYPHER.md` after exiting plan mode for persistent reference across iterations.

---

## 1. Architecture Overview

### 1.1 Core Design Principles

**Direct Database Access**
- No TinkerPop/Gremlin translation layer
- Direct use of ArcadeDB APIs: `LocalDatabase`, `LocalBucket`, `LocalSchema`, `Index`
- Native graph traversal using ArcadeDB's vertex/edge API

**SQL-Inspired Execution Model**
- ANTLR4 for parsing (Neo4j's Cypher25Lexer.g4 and Cypher25Parser.g4)
- AST → Execution Plan → Execution Steps pattern
- Lazy evaluation through iterator-based steps
- Execution plan caching for performance

**Integration Pattern**
- QueryEngine interface implementation
- Auto-discovery via QueryEngineManager
- HTTP endpoints automatically supported
- Module uses `provided` scope for arcadedb-server dependency

### 1.2 Module Structure

```
/Users/luca/Documents/GitHub/arcadedb/opencypher/
├── pom.xml
└── src/
    ├── main/
    │   ├── antlr4/com/arcadedb/opencypher/grammar/
    │   │   ├── Cypher25Lexer.g4
    │   │   └── Cypher25Parser.g4
    │   ├── java/com/arcadedb/opencypher/
    │   │   ├── query/                  # Query engine entry point
    │   │   ├── parser/                 # ANTLR integration
    │   │   ├── ast/                    # AST representation
    │   │   ├── planner/                # Execution planning
    │   │   ├── executor/               # Execution steps
    │   │   ├── traversal/              # Graph traversal logic
    │   │   ├── functions/              # Cypher functions
    │   │   └── result/                 # Result transformation
    │   └── resources/
    │       └── META-INF/services/
    │           └── com.arcadedb.query.QueryEngine$QueryEngineFactory
    └── test/
        ├── java/com/arcadedb/opencypher/
        │   ├── OpenCypherBasicTest.java
        │   ├── OpenCypherMatchTest.java
        │   ├── OpenCypherCreateTest.java
        │   ├── OpenCypherComplexQueryTest.java
        │   └── OpenCypherPerformanceTest.java
        └── resources/test-queries/
```

---

## 2. Critical Components

### 2.1 Query Engine (Entry Point)

**File**: `/opencypher/src/main/java/com/arcadedb/opencypher/query/OpenCypherQueryEngine.java`

```java
public class OpenCypherQueryEngine implements QueryEngine {
    public static final String ENGINE_NAME = "opencypher";
    private final DatabaseInternal database;
    private final AntlrCypherParser parser;
    private final CypherFunctionRegistry functions;

    public String getLanguage() { return ENGINE_NAME; }

    public AnalyzedQuery analyze(String query) {
        // Parse and analyze for idempotency and DDL
    }

    public ResultSet query(String query, ContextConfiguration config, Map<String, Object> params) {
        // Parse → Plan → Execute (read-only validation)
    }

    public ResultSet command(String query, ContextConfiguration config, Map<String, Object> params) {
        // Parse → Plan → Execute (supports writes)
    }
}
```

**File**: `/opencypher/src/main/java/com/arcadedb/opencypher/query/OpenCypherQueryEngineFactory.java`

```java
public class OpenCypherQueryEngineFactory implements QueryEngine.QueryEngineFactory {
    public String getLanguage() { return "opencypher"; }

    public QueryEngine getInstance(DatabaseInternal database) {
        // Cache engine instance in database wrappers
    }
}
```

### 2.2 Parser Integration

**File**: `/opencypher/src/main/java/com/arcadedb/opencypher/parser/AntlrCypherParser.java`

**Responsibilities**:
- Use ANTLR-generated lexer/parser from Neo4j grammars
- Transform parse tree to ArcadeDB AST
- Handle syntax errors with clear messages
- Support parameter binding ($param1, $param2)
- Cache parsed statements for reuse

**Key Methods**:
```java
public CypherStatement parse(String query)
public CypherStatement parseWithParameters(String query, Map<String, Object> params)
```

### 2.3 AST Representation

**Package**: `/opencypher/src/main/java/com/arcadedb/opencypher/ast/`

**Core Classes**:
- `CypherStatement` - Base statement interface
- `MatchClause` - MATCH pattern representation
- `WhereClause` - Filter expressions
- `ReturnClause` - Projection specifications
- `CreateClause` - Node/relationship creation
- `MergeClause` - MERGE semantics
- `WithClause` - Query composition
- `OrderByClause` - Sorting specifications
- `SkipLimitClause` - Pagination

**Pattern Representation**:
- `NodePattern` - (n:Label {prop: value})
- `RelationshipPattern` - -[r:TYPE {prop: value}]->
- `PathPattern` - Complete path patterns
- `PatternElement` - Base pattern element

**Expression Representation**:
- `Expression` - Base expression interface
- `PropertyExpression` - Property access (n.prop)
- `LiteralExpression` - Constants
- `ParameterExpression` - $param references
- `FunctionExpression` - Function calls
- `ComparisonExpression` - Comparisons and logical ops

### 2.4 Execution Planner

**File**: `/opencypher/src/main/java/com/arcadedb/opencypher/planner/CypherExecutionPlanner.java`

**Responsibilities**:
- Analyze MATCH patterns for optimal strategy
- Identify index usage opportunities
- Plan filter pushdown (WHERE optimization)
- Determine join order for multiple patterns
- Build execution step chain

**Planning Steps**:
1. **Index Selection**: Detect indexed properties in WHERE clauses
2. **Pattern Analysis**: Determine starting points (smallest cardinality)
3. **Traversal Planning**: BFS vs DFS for variable-length paths
4. **Filter Optimization**: Push filters down to earliest steps
5. **Step Chain Construction**: Build execution pipeline

**File**: `/opencypher/src/main/java/com/arcadedb/opencypher/planner/PatternMatchPlanner.java`

**Pattern Matching Strategies**:
- **Index-based start**: Use index for properties with equality/range conditions
- **Type scan**: Iterate all vertices/edges of specified label
- **Expansion**: Follow relationships from matched nodes
- **Variable-length**: Use specialized traverser for `-[*min..max]->`

### 2.5 Execution Steps

**Package**: `/opencypher/src/main/java/com/arcadedb/opencypher/executor/steps/`

All steps extend `AbstractExecutionStep` from SQL executor package.

**Pattern Matching Steps**:
- `MatchNodeStep` - Fetch vertices by label/type (uses TypeIndex)
- `MatchRelationshipStep` - Expand following relationship patterns
- `ExpandPathStep` - Variable-length paths with BFS/DFS
- `FilterPropertiesStep` - WHERE clause evaluation

**Write Operation Steps**:
- `CreateNodeStep` - CREATE vertices with properties
- `CreateRelationshipStep` - CREATE edges between vertices
- `MergeNodeStep` - MERGE = MATCH or CREATE logic

**Projection & Aggregation Steps**:
- `ProjectReturnStep` - RETURN clause projection
- `AggregationStep` - COUNT, SUM, AVG, MAX, MIN, COLLECT
- `OrderByStep` - Sort by expressions (reuse SQL's if possible)
- `SkipStep` / `LimitStep` - Pagination (reuse SQL's if possible)
- `WithClauseStep` - Query composition and scope management
- `UnwindStep` - List expansion

**Example Step Chain for**: `MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = 'John' RETURN a, r, b`

```
1. MatchNodeStep(Person) or FetchFromIndexStep(Person[name])
2. FilterPropertiesStep(a.name = 'John')
3. ExpandRelationshipStep(KNOWS, OUT)
4. FilterByTypeStep(b:Person)
5. ProjectReturnStep(a, r, b)
```

### 2.6 Graph Traversal

**Package**: `/opencypher/src/main/java/com/arcadedb/opencypher/traversal/`

**Core Classes**:
- `GraphTraverser` (abstract) - Base traversal interface
- `BreadthFirstTraverser` - Level-by-level expansion (shortest paths)
- `DepthFirstTraverser` - Deep path exploration
- `VariableLengthPathTraverser` - Specialized for `-[*min..max]->`

**Traversal Features**:
- Cycle detection (track visited nodes)
- Hop counting for bounded depth
- Path accumulation for path results
- Direction support (OUT, IN, BOTH)
- Relationship type filtering

### 2.7 Functions Registry

**Package**: `/opencypher/src/main/java/com/arcadedb/opencypher/functions/`

**Implementation Classes**:
- `CypherFunctionRegistry` - Function lookup and dispatch
- `ScalarFunctions` - nodes(), relationships(), properties(), type(), etc.
- `AggregateFunctions` - count(), sum(), avg(), max(), min(), collect()
- `ListFunctions` - head(), tail(), range(), size(), reverse()
- `StringFunctions` - substring(), trim(), toLower(), toUpper()
- `MathFunctions` - abs(), ceil(), floor(), round(), sqrt()

### 2.8 Result Transformation

**File**: `/opencypher/src/main/java/com/arcadedb/opencypher/result/CypherResultTransformer.java`

**Transformations**:
- `Vertex` → `Result` with @rid, @type, and all properties
- `Edge` → `Result` with @rid, @type, @in, @out, and properties
- `Path` → `Result` with nodes, relationships, length
- Primitives, lists, maps → native JSON representation

**Leverage ArcadeDB's serialization**:
- Use `vertex.toJSON()` for vertex serialization
- Use `edge.toJSON()` for edge serialization
- Custom serialization for Cypher-specific types (paths)

---

## 3. Database API Integration

### 3.1 Schema Operations

```java
// Type (Label) access
DocumentType personType = database.getSchema().getOrCreateVertexType("Person");
DocumentType knowsType = database.getSchema().getOrCreateEdgeType("KNOWS");

// Property access
Property nameProperty = personType.getProperty("name");
```

**Mapping**: Cypher labels → ArcadeDB types

### 3.2 Vertex/Edge Operations

```java
// Create vertex
MutableVertex vertex = database.newVertex("Person");
vertex.set("name", "John");
vertex.set("age", 30);
vertex.save();

// Create edge
MutableEdge edge = source.newEdge("KNOWS", target, true);
edge.set("since", 2020);
edge.save();

// Query vertices by type
Iterator<Vertex> vertices = database.iterateType("Person", true);

// Get edges from vertex
Iterable<Edge> edges = vertex.getEdges(Vertex.DIRECTION.OUT, "KNOWS");
```

### 3.3 Index Utilization

```java
// Get index
TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Person[name]");

// Exact lookup
IndexCursor cursor = typeIndex.get(new Object[]{"John"});

// Range scan
IndexCursor cursor = typeIndex.range(
    true, new Object[]{18},
    true, new Object[]{65},
    true
);
```

**Optimizer Integration**:
- `IndexMatchOptimizer` detects indexed predicates in WHERE
- Generates `FetchFromIndexStep` instead of full type scan
- Applies remaining filters as post-processing

### 3.4 Transaction Handling

- Queries run in existing transaction context
- Read queries: can use read-only transactions
- Write queries: require read-write transactions
- ArcadeDB handles ACID guarantees

---

## 4. Implementation Phases

### Phase 1: Foundation & Basic Queries (MVP)

**Goal**: Basic read-only MATCH queries working

**Tasks**:
1. Create module structure and Maven pom.xml
2. Download and integrate ANTLR4 grammars from Neo4j
3. Implement AntlrCypherParser (basic parsing only)
4. Implement minimal AST nodes (MatchClause, ReturnClause, NodePattern)
5. Implement OpenCypherQueryEngine and Factory
6. Implement basic execution steps:
   - MatchNodeStep (type scan)
   - FilterPropertiesStep (simple WHERE)
   - ProjectReturnStep (simple RETURN)
7. Write basic tests

**Validation Queries**:
```cypher
MATCH (n:Person) RETURN n
MATCH (n:Person) WHERE n.age > 25 RETURN n.name
MATCH (n:Person {name: 'John'}) RETURN n
```

**Critical Files**:
1. `/opencypher/pom.xml` - Module configuration
2. `/opencypher/src/main/java/com/arcadedb/opencypher/query/OpenCypherQueryEngine.java`
3. `/opencypher/src/main/java/com/arcadedb/opencypher/parser/AntlrCypherParser.java`
4. `/opencypher/src/main/java/com/arcadedb/opencypher/planner/CypherExecutionPlanner.java`
5. `/opencypher/src/main/java/com/arcadedb/opencypher/executor/steps/MatchNodeStep.java`

**Verification**:
- Compile: `mvn clean install -DskipTests`
- Run tests: `mvn test`
- Test via HTTP: `POST /api/v1/query/{db}` with `language=opencypher`

### Phase 2: Relationship Patterns & Graph Traversal

**Goal**: Multi-node patterns and graph navigation

**Tasks**:
1. Implement RelationshipPattern AST
2. Implement MatchRelationshipStep (edge expansion)
3. Implement ExpandPathStep (variable-length paths)
4. Implement GraphTraverser hierarchy (BFS, DFS)
5. Support multiple patterns in single MATCH
6. Index optimization for pattern starting points
7. Comprehensive pattern matching tests

**Validation Queries**:
```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b
MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b
MATCH (a:Person {name: 'John'})-[:WORKS_FOR]->(c:Company) RETURN a, c
MATCH (a:Person), (b:Person) WHERE a.employer = b.employer RETURN a, b
```

### Phase 3: Advanced Read Operations

**Goal**: Complete read query support

**Tasks**:
1. Implement all aggregate functions (COUNT, SUM, AVG, etc.)
2. Implement GROUP BY logic in AggregationStep
3. Implement ORDER BY, SKIP, LIMIT
4. Implement WITH clause for query composition
5. Implement OPTIONAL MATCH (null propagation)
6. Implement UNWIND (list expansion)
7. Implement list comprehensions
8. Implement path functions (nodes, relationships, length)
9. Implement full function library (string, math, list)
10. Comprehensive test coverage

**Validation Queries**:
```cypher
MATCH (n:Person) RETURN count(*), avg(n.age)
MATCH (n:Person) RETURN n.country, count(*) GROUP BY n.country
MATCH (n:Person) WITH n ORDER BY n.age SKIP 10 LIMIT 10 RETURN n
MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a, b
UNWIND [1,2,3] AS x RETURN x * 2
MATCH p = (a)-[:KNOWS*1..3]->(b) RETURN nodes(p), length(p)
```

### Phase 4: Write Operations

**Goal**: CREATE, MERGE, SET, DELETE support

**Tasks**:
1. Implement CreateNodeStep and CreateRelationshipStep
2. Implement MergeNodeStep (MATCH or CREATE logic)
3. Implement SET clause (property updates)
4. Implement DELETE and DETACH DELETE
5. Implement REMOVE clause (property removal)
6. Implement ON CREATE and ON MATCH for MERGE
7. Transaction integration and validation
8. Write operation tests

**Validation Queries**:
```cypher
CREATE (n:Person {name: 'John', age: 30}) RETURN n
MATCH (a:Person {name: 'John'}), (b:Person {name: 'Jane'})
  CREATE (a)-[:KNOWS {since: 2020}]->(b)
MERGE (n:Person {email: 'john@example.com'})
  ON CREATE SET n.created = timestamp()
  ON MATCH SET n.accessed = timestamp()
MATCH (n:Person {name: 'John'}) SET n.age = 31
MATCH (n:Person {name: 'Old'}) DELETE n
```

### Phase 5: Optimization & Performance

**Goal**: Query optimization and performance tuning

**Tasks**:
1. Implement execution plan caching
2. Implement statement caching
3. Optimize index selection logic
4. Implement filter pushdown optimization
5. Optimize join order for multiple patterns
6. Memory usage optimization (minimize allocations)
7. Implement query profiling/EXPLAIN support
8. Performance benchmarking suite
9. Compare with current Cypher→Gremlin implementation

**Performance Metrics**:
- Query execution time
- Memory usage
- Index hit rate
- Records scanned vs returned
- Comparison with old Cypher implementation

### Phase 6: Polish & Production Readiness

**Goal**: Documentation, testing, release preparation

**Tasks**:
1. Comprehensive documentation (README, EXAMPLES, MIGRATION)
2. Improve error messages
3. Edge case handling
4. Neo4j compatibility testing
5. Integration test suite
6. Code review and cleanup
7. Final performance validation
8. Release preparation

**Deliverables**:
- Complete implementation
- Test coverage > 80%
- Documentation (user and developer)
- Performance report
- Migration guide from `cypher` to `opencypher`

---

## 5. Maven Configuration

### 5.1 Module pom.xml

**File**: `/opencypher/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.arcadedb</groupId>
        <artifactId>arcadedb-parent</artifactId>
        <version>26.1.1-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>arcadedb-opencypher</artifactId>
    <packaging>jar</packaging>
    <name>ArcadeDB Native OpenCypher</name>

    <properties>
        <antlr4.version>4.13.2</antlr4.version>
    </properties>

    <dependencies>
        <!-- Core ArcadeDB - MUST be provided scope per CLAUDE.md -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- ANTLR4 Runtime -->
        <dependency>
            <groupId>org.antlr</groupId>
            <artifactId>antlr4-runtime</artifactId>
            <version>${antlr4.version}</version>
        </dependency>

        <!-- Test dependencies -->
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-test-utils</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- ANTLR4 Maven Plugin -->
            <plugin>
                <groupId>org.antlr</groupId>
                <artifactId>antlr4-maven-plugin</artifactId>
                <version>${antlr4.version}</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>antlr4</goal>
                        </goals>
                        <configuration>
                            <sourceDirectory>${basedir}/src/main/antlr4</sourceDirectory>
                            <outputDirectory>${basedir}/target/generated-sources/antlr4</outputDirectory>
                            <visitor>true</visitor>
                            <listener>true</listener>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

            <!-- Test JAR for cross-module testing -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <executions>
                    <execution>
                        <goals>
                            <goal>test-jar</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>

            <!-- Shade Plugin -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 5.2 Parent pom.xml Integration

**Edit**: `/Users/luca/Documents/GitHub/arcadedb/pom.xml`

Add to `<modules>` section (after gremlin, before package):
```xml
<module>opencypher</module>
```

### 5.3 Grammar Files Source

Download from Neo4j's cypher-language-support repository:
- https://raw.githubusercontent.com/neo4j/cypher-language-support/refs/heads/main/packages/language-support/src/antlr-grammar/Cypher25Lexer.g4
- https://raw.githubusercontent.com/neo4j/cypher-language-support/refs/heads/main/packages/language-support/src/antlr-grammar/Cypher25Parser.g4

Place in: `/opencypher/src/main/antlr4/com/arcadedb/opencypher/grammar/`

---

## 6. Testing Strategy

### 6.1 Test Structure

**Basic Tests**:
- `OpenCypherBasicTest.java` - Simple MATCH, WHERE, RETURN
- `OpenCypherParameterTest.java` - Parameter binding

**Pattern Tests**:
- `OpenCypherMatchTest.java` - All MATCH patterns
- `OpenCypherOptionalMatchTest.java` - OPTIONAL MATCH
- `OpenCypherPathTest.java` - Variable-length paths

**Write Tests**:
- `OpenCypherCreateTest.java` - CREATE operations
- `OpenCypherMergeTest.java` - MERGE operations
- `OpenCypherUpdateTest.java` - SET, DELETE, REMOVE

**Complex Tests**:
- `OpenCypherAggregationTest.java` - Aggregates and GROUP BY
- `OpenCypherComplexQueryTest.java` - Multi-clause queries
- `OpenCypherWithClauseTest.java` - WITH clause composition

**Performance Tests**:
- `OpenCypherPerformanceTest.java` - Benchmark suite
- Compare with current cypher implementation
- Measure index utilization

**Compatibility Tests**:
- `Neo4jCompatibilityTest.java` - Compare with Neo4j behavior

### 6.2 Test Pattern

```java
@Test
void testSimpleMatch() {
    final Database database = DatabaseFactory.create(...);
    try {
        database.getSchema().createVertexType("Person");

        database.transaction(() -> {
            for (int i = 0; i < 10; i++) {
                MutableVertex v = database.newVertex("Person");
                v.set("name", "Person" + i);
                v.set("age", 20 + i);
                v.save();
            }
        });

        ResultSet result = database.query("opencypher",
            "MATCH (n:Person) WHERE n.age > $minAge RETURN n.name, n.age ORDER BY n.age",
            "minAge", 25
        );

        assertThat(result).isNotNull();
        // Assertions...

    } finally {
        database.drop();
    }
}
```

### 6.3 Testing Checklist

**Phase 1 (Foundation)**:
- [ ] Simple node match by label
- [ ] Property filtering with WHERE
- [ ] Basic RETURN projection
- [ ] Parameter binding
- [ ] Compilation passes without errors

**Phase 2 (Traversal)**:
- [ ] Single relationship pattern
- [ ] Multiple relationship patterns
- [ ] Variable-length paths
- [ ] Bidirectional relationships
- [ ] Multi-pattern queries

**Phase 3 (Advanced Read)**:
- [ ] All aggregate functions
- [ ] GROUP BY
- [ ] ORDER BY / SKIP / LIMIT
- [ ] WITH clause
- [ ] OPTIONAL MATCH
- [ ] UNWIND
- [ ] All built-in functions

**Phase 4 (Write)**:
- [ ] CREATE nodes
- [ ] CREATE relationships
- [ ] MERGE nodes with ON CREATE/MATCH
- [ ] SET properties
- [ ] DELETE nodes/relationships
- [ ] REMOVE properties

**Phase 5 (Performance)**:
- [ ] Index utilization validated
- [ ] Performance vs old Cypher benchmarked
- [ ] Memory usage acceptable
- [ ] Large result sets handled

**Phase 6 (Polish)**:
- [ ] Test coverage > 80%
- [ ] Documentation complete
- [ ] Error messages clear
- [ ] Neo4j compatibility validated

---

## 7. Performance Optimization Strategies

### 7.1 Caching

**Statement Cache**:
```java
// Cache parsed AST by query string
Map<String, CypherStatement> statementCache;
// Configuration: OPENCYPHER_STATEMENT_CACHE_SIZE (default 1000)
```

**Execution Plan Cache**:
```java
// Cache compiled execution plans
Map<String, CypherExecutionPlan> planCache;
// Configuration: OPENCYPHER_PLAN_CACHE_SIZE (default 500)
```

### 7.2 Index Optimization

- Analyze WHERE clause for indexed properties
- Choose most selective index
- Use index range scans when applicable
- Combine index results for AND conditions
- Fallback to type scan if no useful indexes

### 7.3 Query Optimization

- **Filter pushdown**: Apply WHERE as early as possible
- **Join order**: Start from smallest cardinality
- **Lazy evaluation**: Pull-based execution (iterator pattern)
- **Early termination**: Stop at LIMIT if possible

### 7.4 Memory Management

- Stream results (don't materialize full result set)
- Respect result set size limits
- Minimize object allocation in hot paths
- Reuse objects where possible
- Follow ArcadeDB's "Low Level Java" principles

---

## 8. Integration Points

### 8.1 Automatic Registration

**Service Provider Interface**:

File: `/opencypher/src/main/resources/META-INF/services/com.arcadedb.query.QueryEngine$QueryEngineFactory`

Content:
```
com.arcadedb.opencypher.query.OpenCypherQueryEngineFactory
```

**QueryEngineManager** will automatically discover and register on classpath.

### 8.2 HTTP API

**Automatic via existing endpoints**:
```bash
POST /api/v1/query/{database}
Content-Type: application/json

{
  "language": "opencypher",
  "command": "MATCH (n:Person) RETURN n",
  "params": {}
}
```

No additional HTTP handlers needed!

### 8.3 Java API

```java
// Via Database interface
ResultSet result = database.query("opencypher",
    "MATCH (n:Person) WHERE n.age > $age RETURN n",
    "age", 25
);

// Via command method
ResultSet result = database.command("opencypher",
    "CREATE (n:Person {name: $name}) RETURN n",
    config, Map.of("name", "John")
);
```

---

## 9. Key Differences from Current Cypher

### Current Implementation (`cypher`)
- Translates Cypher → Gremlin via opencypher-gremlin library
- Uses TinkerPop API through ArcadeGraph
- Translation overhead
- Limited by Gremlin translation capabilities
- Located in gremlin module

### New Implementation (`opencypher`)
- Direct ANTLR parsing of Cypher
- Native ArcadeDB API usage
- No translation overhead
- Full Cypher 2.5 specification support
- Standalone module
- Better performance potential

**Migration Path**: Both implementations coexist, allowing gradual migration and A/B testing.

---

## 10. Documentation Requirements

### User Documentation
- `/opencypher/README.md` - Overview and getting started
- `/opencypher/CYPHER_SUPPORT.md` - Supported features matrix
- `/opencypher/EXAMPLES.md` - Query examples and patterns
- `/opencypher/MIGRATION.md` - Migration from `cypher` to `opencypher`

### Developer Documentation
- `/opencypher/ARCHITECTURE.md` - Technical architecture details
- `/opencypher/CONTRIBUTING.md` - Development guidelines
- `/opencypher/TESTING.md` - Testing approach and patterns
- JavaDoc for all public classes and methods

---

## 11. Potential Challenges & Mitigations

### Challenge: ANTLR Grammar Complexity
**Mitigation**: Start with core features, incremental addition, extensive testing

### Challenge: Semantic Differences from Neo4j
**Mitigation**: Document differences, focus on Cypher 2.5 standard, compatibility tests

### Challenge: Performance vs Current Implementation
**Mitigation**: Direct DB access (no TinkerPop), index optimization, benchmarking from Phase 1

### Challenge: Variable Scoping (WITH clauses)
**Mitigation**: Clear scope management in execution context, test shadowing scenarios

### Challenge: Transaction Semantics
**Mitigation**: Leverage ArcadeDB's transaction management, clear documentation

---

## 12. Success Criteria

### Functional Requirements
- [ ] All Cypher 2.5 core features implemented
- [ ] Read queries: MATCH, WHERE, RETURN, WITH, OPTIONAL MATCH
- [ ] Write queries: CREATE, MERGE, SET, DELETE, REMOVE
- [ ] Aggregations: COUNT, SUM, AVG, MAX, MIN, COLLECT
- [ ] Functions: String, Math, List, Path functions
- [ ] Variable-length paths and graph traversal
- [ ] Parameter binding and injection protection

### Performance Requirements
- [ ] At least as fast as current Cypher→Gremlin implementation
- [ ] Index utilization for WHERE clauses
- [ ] Handles large result sets efficiently
- [ ] Memory usage reasonable for graph traversals

### Quality Requirements
- [ ] Test coverage > 80%
- [ ] All tests pass consistently
- [ ] Compiles without warnings
- [ ] No security vulnerabilities
- [ ] Clear error messages

### Documentation Requirements
- [ ] User documentation complete
- [ ] Developer documentation complete
- [ ] API documentation (JavaDoc)
- [ ] Migration guide available

---

## 13. Next Steps After Plan Approval

1. **Exit plan mode** and copy this plan to `NATIVE_CYPHER.md`
2. **Create module structure** and pom.xml
3. **Download ANTLR grammars** from Neo4j repository
4. **Implement Phase 1** (Foundation & Basic Queries)
5. **Compile and test** after each component
6. **Iterate through phases** with continuous testing
7. **Performance benchmark** after Phase 5
8. **Final polish** and documentation in Phase 6

---

## 14. References

### ArcadeDB Files Referenced
- `/gremlin/pom.xml` - Module structure pattern
- `/engine/src/main/java/com/arcadedb/query/QueryEngine.java` - Interface
- `/engine/src/main/java/com/arcadedb/query/QueryEngineManager.java` - Registration
- `/engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java` - Planner pattern
- `/engine/src/main/java/com/arcadedb/query/sql/executor/AbstractExecutionStep.java` - Step pattern
- `/engine/src/main/java/com/arcadedb/query/sql/executor/FetchFromTypeExecutionStep.java` - Step implementation
- `/gremlin/src/main/java/com/arcadedb/cypher/query/CypherQueryEngine.java` - Current implementation

### External Resources
- Neo4j Cypher Grammars: https://github.com/neo4j/cypher-language-support/tree/main/packages/language-support/src/antlr-grammar
- ANTLR4 Documentation: https://www.antlr.org/
- openCypher Specification: https://opencypher.org/

---

**End of Implementation Plan**
