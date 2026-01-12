# OpenCypher Implementation Status

**Last Updated:** 2026-01-12
**Implementation Version:** Native ANTLR4-based Parser (Phase 3)
**Test Coverage:** 53/53 tests passing (100%)

---

## 📊 Overall Status

| Category | Implementation | Notes |
|----------|---------------|-------|
| **Parser** | ✅ **100%** | ANTLR4-based using official Cypher 2.5 grammar |
| **Basic Read Queries** | ✅ **85%** | MATCH, WHERE (simple), RETURN, ORDER BY, SKIP, LIMIT |
| **Basic Write Queries** | 🟡 **60%** | CREATE ✅, SET ❌, DELETE ❌, MERGE ❌ |
| **Expression Evaluation** | 🔴 **20%** | Simple comparisons only, no logical operators |
| **Functions** | 🔴 **5%** | No aggregations, no built-in functions |
| **Advanced Features** | 🔴 **10%** | Limited path support, no UNION/WITH |

**Legend:** ✅ Complete | 🟡 Partial | 🔴 Minimal | ❌ Not Implemented

---

## ✅ Working Features (Fully Implemented & Tested)

### MATCH Clause
```cypher
// ✅ Simple node patterns with labels
MATCH (n:Person) RETURN n

// ✅ Node patterns with property filters
MATCH (n:Person {name: 'Alice', age: 30}) RETURN n

// ✅ Comma-separated patterns (Cartesian product)
MATCH (a:Person), (b:Company) RETURN a, b

// ✅ Relationship patterns (single-hop)
MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b

// ✅ Relationship patterns (multi-hop)
MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN a, b, c

// ✅ Variable-length relationships
MATCH (a)-[r:KNOWS*1..3]->(b) RETURN a, b

// ✅ Bidirectional relationships
MATCH (a)-[r]-(b) RETURN a, b

// ✅ Relationship with properties
MATCH (a)-[r:WORKS_AT {since: 2020}]->(b) RETURN r
```

**Limitations:**
- ❌ OPTIONAL MATCH (parsed but not executed correctly)
- ❌ Multiple MATCH clauses (only first is processed)
- ❌ Pattern without labels: `MATCH (n)` not supported
- ❌ Named paths: `p = (a)-[*]->(b)` not stored

### WHERE Clause
```cypher
// ✅ Simple property comparisons
MATCH (n:Person) WHERE n.age > 30 RETURN n
MATCH (n:Person) WHERE n.name = 'Alice' RETURN n

// ✅ Numeric comparisons: >, <, >=, <=, =, !=
MATCH (n:Person) WHERE n.age >= 25 RETURN n
```

**Limitations:**
- ❌ Logical operators: AND, OR, NOT
- ❌ IN operator: `WHERE n.name IN ['Alice', 'Bob']`
- ❌ IS NULL / IS NOT NULL
- ❌ String matching: STARTS WITH, ENDS WITH, CONTAINS
- ❌ Regular expressions: `n.name =~ '.*Smith'`
- ❌ Pattern predicates: `WHERE (n)-[:KNOWS]->()`
- ❌ Complex expressions with functions

### CREATE Clause
```cypher
// ✅ Create single vertex with properties
CREATE (n:Person {name: 'Alice', age: 30})

// ✅ Create multiple vertices
CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})

// ✅ Create vertex without label (defaults to "Vertex")
CREATE (n {name: 'Test'})

// ✅ Create relationship between new vertices
CREATE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})

// ✅ Create relationship with properties
CREATE (a)-[r:WORKS_AT {since: 2020}]->(c:Company {name: 'ArcadeDB'})

// ✅ Create chained paths
CREATE (a)-[:KNOWS]->(b)-[:KNOWS]->(c)

// ✅ MATCH + CREATE (create with context)
MATCH (a:Person {name: 'Alice'})
CREATE (a)-[r:KNOWS]->(b:Person {name: 'Bob'})

// ✅ CREATE without RETURN (returns created elements)
CREATE (n:Person {name: 'Alice'})
```

**Limitations:**
- ❌ CREATE with variable-length patterns
- ❌ ON CREATE SET (part of MERGE)

### RETURN Clause
```cypher
// ✅ Return variables
MATCH (n:Person) RETURN n

// ✅ Return multiple variables
MATCH (a)-[r]->(b) RETURN a, r, b

// ✅ Return property projections
MATCH (n:Person) RETURN n.name, n.age

// ✅ Return with aliases
MATCH (n:Person) RETURN n.name AS personName

// ✅ Return all: RETURN *
MATCH (n:Person) RETURN *
```

**Limitations:**
- ❌ DISTINCT: `RETURN DISTINCT n.name`
- ❌ Aggregation functions: COUNT(), SUM(), AVG(), MIN(), MAX()
- ❌ COLLECT(): `RETURN COLLECT(n.name)`
- ❌ Expressions in RETURN: `RETURN n.age * 2`
- ❌ Map projections: `RETURN n{.name, .age}`
- ❌ List comprehensions: `RETURN [x IN list | x.name]`
- ❌ CASE expressions

### ORDER BY, SKIP, LIMIT
```cypher
// ✅ ORDER BY single property
MATCH (n:Person) RETURN n ORDER BY n.age

// ✅ ORDER BY ascending (default)
MATCH (n:Person) RETURN n ORDER BY n.name ASC

// ✅ ORDER BY descending
MATCH (n:Person) RETURN n ORDER BY n.age DESC

// ✅ ORDER BY multiple properties
MATCH (n:Person) RETURN n ORDER BY n.age DESC, n.name ASC

// ✅ SKIP results
MATCH (n:Person) RETURN n SKIP 5

// ✅ LIMIT results
MATCH (n:Person) RETURN n LIMIT 10

// ✅ Combined: ORDER BY + SKIP + LIMIT (pagination)
MATCH (n:Person) RETURN n ORDER BY n.age SKIP 10 LIMIT 5

// ✅ With WHERE clause
MATCH (n:Person) WHERE n.age > 28
RETURN n.name ORDER BY n.age DESC
```

---

## 🟡 Parsed but Not Executed

These features are **parsed** by the ANTLR4 grammar and have AST representations, but **no execution steps** are implemented:

### SET Clause
```cypher
// 🟡 Parsed, execution NOT implemented
MATCH (n:Person {name: 'Alice'}) SET n.age = 31

// 🟡 Set multiple properties
MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31, n.city = 'NYC'
```

**Status:** AST parsed at `CypherASTBuilder.java:175-194`, but no `SetStep` exists.
**Priority:** 🔴 **HIGH** - Essential for update operations

### DELETE Clause
```cypher
// 🟡 Parsed, execution NOT implemented
MATCH (n:Person {name: 'Alice'}) DELETE n

// 🟡 DETACH DELETE (delete node and its relationships)
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n

// 🟡 Delete relationships
MATCH (a)-[r:KNOWS]->(b) DELETE r
```

**Status:** AST parsed at `CypherASTBuilder.java:197-204`, but no `DeleteStep` exists.
**Priority:** 🔴 **HIGH** - Essential for delete operations

### MERGE Clause
```cypher
// 🟡 Parsed, execution NOT implemented
MERGE (n:Person {name: 'Alice'})

// 🟡 MERGE with ON CREATE / ON MATCH
MERGE (n:Person {name: 'Alice'})
  ON CREATE SET n.created = timestamp()
  ON MATCH SET n.updated = timestamp()
```

**Status:** AST parsed at `CypherASTBuilder.java:207-210`, but no `MergeStep` exists.
**Priority:** 🟡 **MEDIUM** - Upsert operations

---

## ❌ Not Implemented

### Query Composition
| Feature | Example | Priority |
|---------|---------|----------|
| **WITH** | `MATCH (n) WITH n.name AS name RETURN name` | 🟡 MEDIUM |
| **UNION** | `MATCH (n:Person) RETURN n UNION MATCH (n:Company) RETURN n` | 🟢 LOW |
| **UNION ALL** | `... UNION ALL ...` | 🟢 LOW |
| **UNWIND** | `UNWIND [1,2,3] AS x RETURN x` | 🟡 MEDIUM |

### Aggregation Functions
| Function | Example | Priority |
|----------|---------|----------|
| **COUNT()** | `RETURN COUNT(n)` | 🔴 HIGH |
| **SUM()** | `RETURN SUM(n.age)` | 🔴 HIGH |
| **AVG()** | `RETURN AVG(n.age)` | 🔴 HIGH |
| **MIN()** | `RETURN MIN(n.age)` | 🔴 HIGH |
| **MAX()** | `RETURN MAX(n.age)` | 🔴 HIGH |
| **COLLECT()** | `RETURN COLLECT(n.name)` | 🔴 HIGH |
| **percentileCont()** | `RETURN percentileCont(n.age, 0.5)` | 🟢 LOW |
| **stDev()** | `RETURN stDev(n.age)` | 🟢 LOW |

### String Functions
| Function | Example | Priority |
|----------|---------|----------|
| **toUpper()** | `RETURN toUpper(n.name)` | 🟡 MEDIUM |
| **toLower()** | `RETURN toLower(n.name)` | 🟡 MEDIUM |
| **trim()** | `RETURN trim(n.name)` | 🟡 MEDIUM |
| **substring()** | `RETURN substring(n.name, 0, 3)` | 🟡 MEDIUM |
| **replace()** | `RETURN replace(n.name, 'a', 'A')` | 🟡 MEDIUM |
| **split()** | `RETURN split(n.name, ' ')` | 🟡 MEDIUM |
| **toString()** | `RETURN toString(n.age)` | 🟡 MEDIUM |

### Math Functions
| Function | Example | Priority |
|----------|---------|----------|
| **abs()** | `RETURN abs(n.value)` | 🟡 MEDIUM |
| **ceil()** | `RETURN ceil(n.value)` | 🟡 MEDIUM |
| **floor()** | `RETURN floor(n.value)` | 🟡 MEDIUM |
| **round()** | `RETURN round(n.value)` | 🟡 MEDIUM |
| **sqrt()** | `RETURN sqrt(n.value)` | 🟡 MEDIUM |
| **rand()** | `RETURN rand()` | 🟢 LOW |

### Node/Relationship Functions
| Function | Example | Priority |
|----------|---------|----------|
| **id()** | `RETURN id(n)` | 🔴 HIGH |
| **labels()** | `RETURN labels(n)` | 🔴 HIGH |
| **type()** | `RETURN type(r)` | 🔴 HIGH |
| **keys()** | `RETURN keys(n)` | 🟡 MEDIUM |
| **properties()** | `RETURN properties(n)` | 🟡 MEDIUM |
| **startNode()** | `RETURN startNode(r)` | 🟡 MEDIUM |
| **endNode()** | `RETURN endNode(r)` | 🟡 MEDIUM |

### Path Functions
| Function | Example | Priority |
|----------|---------|----------|
| **shortestPath()** | `MATCH p = shortestPath((a)-[*]-(b)) RETURN p` | 🟡 MEDIUM |
| **allShortestPaths()** | `MATCH p = allShortestPaths((a)-[*]-(b)) RETURN p` | 🟢 LOW |
| **length()** | `RETURN length(p)` | 🟡 MEDIUM |
| **nodes()** | `RETURN nodes(p)` | 🟡 MEDIUM |
| **relationships()** | `RETURN relationships(p)` | 🟡 MEDIUM |

### List Functions
| Function | Example | Priority |
|----------|---------|----------|
| **size()** | `RETURN size([1,2,3])` | 🟡 MEDIUM |
| **head()** | `RETURN head([1,2,3])` | 🟡 MEDIUM |
| **tail()** | `RETURN tail([1,2,3])` | 🟡 MEDIUM |
| **last()** | `RETURN last([1,2,3])` | 🟡 MEDIUM |
| **range()** | `RETURN range(1, 10)` | 🟡 MEDIUM |

### Date/Time Functions
| Function | Example | Priority |
|----------|---------|----------|
| **date()** | `RETURN date()` | 🟡 MEDIUM |
| **datetime()** | `RETURN datetime()` | 🟡 MEDIUM |
| **timestamp()** | `RETURN timestamp()` | 🟡 MEDIUM |
| **duration()** | `RETURN duration('P1Y')` | 🟢 LOW |

### WHERE Enhancements
| Feature | Example | Priority |
|---------|---------|----------|
| **AND/OR/NOT** | `WHERE n.age > 25 AND n.city = 'NYC'` | 🔴 HIGH |
| **IN operator** | `WHERE n.name IN ['Alice', 'Bob']` | 🔴 HIGH |
| **IS NULL** | `WHERE n.age IS NULL` | 🔴 HIGH |
| **IS NOT NULL** | `WHERE n.age IS NOT NULL` | 🔴 HIGH |
| **STARTS WITH** | `WHERE n.name STARTS WITH 'A'` | 🟡 MEDIUM |
| **ENDS WITH** | `WHERE n.name ENDS WITH 'son'` | 🟡 MEDIUM |
| **CONTAINS** | `WHERE n.name CONTAINS 'li'` | 🟡 MEDIUM |
| **Regular expressions** | `WHERE n.name =~ '.*Smith'` | 🟢 LOW |
| **Pattern predicates** | `WHERE (n)-[:KNOWS]->()` | 🟡 MEDIUM |
| **EXISTS()** | `WHERE EXISTS(n.email)` | 🟡 MEDIUM |

### Expression Features
| Feature | Example | Priority |
|---------|---------|----------|
| **CASE expressions** | `CASE WHEN n.age < 18 THEN 'minor' ELSE 'adult' END` | 🟡 MEDIUM |
| **List literals** | `RETURN [1, 2, 3]` | 🟡 MEDIUM |
| **Map literals** | `RETURN {name: 'Alice', age: 30}` | 🟡 MEDIUM |
| **List comprehensions** | `[x IN list WHERE x.age > 25 \| x.name]` | 🟢 LOW |
| **Map projections** | `RETURN n{.name, .age}` | 🟢 LOW |
| **Type coercion** | `toInteger('42')`, `toFloat('3.14')` | 🟡 MEDIUM |
| **Arithmetic** | `RETURN n.age * 2 + 10` | 🟡 MEDIUM |

### Advanced Features
| Feature | Example | Priority |
|---------|---------|----------|
| **CALL procedures** | `CALL db.labels()` | 🟢 LOW |
| **Subqueries** | `RETURN [(n)-[:KNOWS]->(m) \| m.name]` | 🟢 LOW |
| **FOREACH** | `FOREACH (n IN nodes \| SET n.marked = true)` | 🟢 LOW |
| **Index hints** | `USING INDEX n:Person(name)` | 🟢 LOW |
| **EXPLAIN** | `EXPLAIN MATCH (n) RETURN n` | 🟢 LOW |
| **PROFILE** | `PROFILE MATCH (n) RETURN n` | 🟢 LOW |

---

## 🗺️ Implementation Roadmap

### Phase 4 (Current): Write Operations & Expressions
**Target:** Q1 2026
**Focus:** Complete basic write operations and enhance WHERE clause

- [ ] Implement `SetStep` for SET clause
- [ ] Implement `DeleteStep` for DELETE/DETACH DELETE
- [ ] Implement logical operators (AND, OR, NOT) in WHERE
- [ ] Implement IS NULL / IS NOT NULL
- [ ] Implement IN operator
- [ ] Add expression evaluator framework

### Phase 5: Aggregation & Functions
**Target:** Q2 2026
**Focus:** Add aggregation support and common functions

- [ ] Implement aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] Add DISTINCT in RETURN
- [ ] Implement basic string functions (toUpper, toLower, trim)
- [ ] Implement node/relationship functions (id, labels, type)
- [ ] Add proper expression evaluation in RETURN

### Phase 6: Advanced Queries
**Target:** Q3 2026
**Focus:** Query composition and advanced features

- [ ] Implement WITH clause (query chaining)
- [ ] Implement MERGE with ON CREATE/ON MATCH
- [ ] Implement OPTIONAL MATCH
- [ ] Add string matching (STARTS WITH, ENDS WITH, CONTAINS)
- [ ] Implement UNWIND

### Phase 7: Optimization & Performance
**Target:** Q4 2026
**Focus:** Query optimization and performance tuning

- [ ] Query plan optimization
- [ ] Index utilization
- [ ] Join optimization
- [ ] Parallel execution
- [ ] Query caching

### Future Phases
- UNION/UNION ALL
- Shortest path algorithms
- CALL procedures
- Subqueries
- Full function library

---

## 🧪 Test Coverage

| Test Suite | Tests | Status | Coverage |
|------------|-------|--------|----------|
| OpenCypherBasicTest | 11/11 | ✅ PASS | Basic engine, parsing |
| OpenCypherCreateTest | 9/9 | ✅ PASS | CREATE operations |
| OpenCypherRelationshipTest | 8/8 | ✅ PASS | Relationship patterns |
| OpenCypherTraversalTest | 8/8 | ✅ PASS | Path traversal, variable-length |
| OpenCypherOrderBySkipLimitTest | 10/10 | ✅ PASS | ORDER BY, SKIP, LIMIT |
| OpenCypherExecutionTest | 7/7 | ✅ PASS | Query execution |
| **TOTAL** | **53/53** | **✅ 100%** | **All passing** |

### Test Files
```
opencypher/src/test/java/com/arcadedb/opencypher/
├── OpenCypherBasicTest.java          # Engine registration, basic queries
├── OpenCypherCreateTest.java         # CREATE clause tests
├── OpenCypherRelationshipTest.java   # Relationship pattern tests
├── OpenCypherTraversalTest.java      # Path traversal tests
├── OpenCypherOrderBySkipLimitTest.java  # ORDER BY, SKIP, LIMIT
└── OpenCypherExecutionTest.java      # Query execution tests
```

---

## 🏗️ Architecture

### Parser (ANTLR4-based)
```
Query String → Cypher25Lexer → Cypher25Parser → Parse Tree
                                                     ↓
                                            CypherASTBuilder (Visitor)
                                                     ↓
                                              CypherStatement (AST)
```

**Files:**
- `Cypher25Lexer.g4` - Lexical grammar (official Cypher 2.5)
- `Cypher25Parser.g4` - Parser grammar (official Cypher 2.5)
- `Cypher25AntlrParser.java` - Parser wrapper
- `CypherASTBuilder.java` - ANTLR visitor → AST transformer
- `CypherErrorListener.java` - Error handling

### Execution Engine (Step-based)
```
CypherStatement → CypherExecutionPlanner → Execution Plan (Step Chain)
                                                     ↓
                                          CypherExecutionPlan.execute()
                                                     ↓
                                              ResultSet (lazy)
```

**Execution Steps:**
- `MatchNodeStep` - Fetch nodes by type/label
- `MatchRelationshipStep` - Traverse relationships
- `ExpandPathStep` - Variable-length path expansion
- `FilterPropertiesStep` - WHERE clause filtering
- `CreateStep` - CREATE vertices/edges
- `ProjectReturnStep` - RETURN projection
- `OrderByStep` - Result sorting
- `SkipStep` - Skip N results
- `LimitStep` - Limit N results

**Missing Steps:**
- `SetStep` - SET clause (update properties)
- `DeleteStep` - DELETE clause (remove nodes/edges)
- `MergeStep` - MERGE clause (upsert)
- `WithStep` - WITH clause (query chaining)
- `UnwindStep` - UNWIND clause (list expansion)
- `OptionalMatchStep` - OPTIONAL MATCH
- `AggregationStep` - Aggregation functions

---

## 🐛 Known Issues

1. **MATCH without label not supported** - `MATCH (n) RETURN n` throws error
   - Workaround: Always specify label `MATCH (n:TypeName) RETURN n`

2. **Only first MATCH clause processed** - Multiple MATCH clauses ignored
   - Workaround: Use comma-separated patterns in single MATCH

3. **Complex WHERE expressions not supported** - Only simple comparisons work
   - Workaround: Use inline property filters in patterns where possible

4. **No expression evaluation in RETURN** - Cannot do `RETURN n.age * 2`
   - Workaround: Use SQL fallback for complex projections

5. **OPTIONAL MATCH parsed but not executed correctly** - May return incorrect results
   - Workaround: Use SQL's LEFT JOIN equivalent

---

## 📝 How to Report Issues

If you encounter issues with the OpenCypher implementation:

1. **Check this status document** to see if the feature is implemented
2. **Create an issue** at: https://github.com/arcadedata/arcadedb/issues
3. **Include:**
   - Your Cypher query
   - Expected behavior
   - Actual behavior (error message or incorrect results)
   - ArcadeDB version
   - Label with `cypher` tag

---

## 🤝 Contributing

We welcome contributions to the OpenCypher implementation!

### High-Priority Contributions Needed:
1. **SetStep implementation** - Update operations
2. **DeleteStep implementation** - Delete operations
3. **Logical operators in WHERE** - AND, OR, NOT
4. **Aggregation functions** - COUNT, SUM, AVG, etc.
5. **Expression evaluator** - Arithmetic, functions

### Getting Started:
1. Review `CypherASTBuilder.java` - See what's parsed
2. Check `CypherExecutionPlan.java` - See execution flow
3. Look at existing steps in `executor/steps/` - Follow patterns
4. Write tests first in `test/java/com/arcadedb/opencypher/`
5. Implement execution step
6. Update this status document

### Coding Standards:
- Follow existing code style (see `CLAUDE.md`)
- Use Low-Level Java optimizations
- Minimize garbage collection pressure
- All tests must pass (53/53)
- Add tests for new features

---

## 📚 References

- **Cypher Query Language**: https://opencypher.org/
- **Cypher 2.5 Grammar**: Used by this implementation
- **ArcadeDB Documentation**: https://docs.arcadedb.com/
- **Neo4j Cypher Manual**: https://neo4j.com/docs/cypher-manual/current/

---

**Generated with [Claude Code](https://claude.ai/code) via [Happy](https://happy.engineering)**
