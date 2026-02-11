# GraphBenchmark Design — LDBC-Inspired Benchmark for ArcadeDB

## Summary

A JUnit 5 benchmark class in `engine/src/test/java/performance/GraphBenchmark.java` that generates an LDBC Social Network Benchmark-inspired graph and benchmarks ArcadeDB across creation, lookups, and traversals. Queries run in both SQL and OpenCypher side by side. The database is preserved between runs so only the first execution pays the generation cost.

## Decisions

| Decision | Choice |
|----------|--------|
| Location | `engine/src/test/java/performance/` |
| Query languages | Both SQL and Cypher, side by side |
| Schema fidelity | Full LDBC SNB (8 vertex types, 14 edge types) |
| Execution model | JUnit 5 with `@Tag("benchmark")` |
| Default scale | Medium (~30K Persons, ~150K Posts, ~600K Comments, ~3M edges) |
| Metrics | Micrometer `SimpleMeterRegistry` (test-scoped dependency) |

## File Structure

Single file: `engine/src/test/java/performance/GraphBenchmark.java`

New test-scoped dependency in `engine/pom.xml`:
```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-core</artifactId>
    <version>${micrometer.version}</version>
    <scope>test</scope>
</dependency>
```

## Scale Constants

```java
private static final int NUM_PERSONS        = 30_000;
private static final int NUM_POSTS          = 150_000;
private static final int NUM_COMMENTS       = 600_000;
private static final int NUM_FORUMS         = 5_000;
private static final int NUM_TAGS           = 2_000;
private static final int NUM_TAG_CLASSES    = 100;
private static final int NUM_PLACES         = 1_500;
private static final int NUM_ORGANISATIONS  = 3_000;

private static final int AVG_KNOWS_PER_PERSON    = 40;
private static final int AVG_LIKES_PER_PERSON    = 30;
private static final int AVG_TAGS_PER_POST       = 3;
private static final int AVG_INTERESTS_PER_PERSON = 5;

private static final int PARALLEL    = 4;
private static final int COMMIT_EVERY = 5_000;
private static final String DB_PATH  = "target/databases/graph-benchmark";
```

## Schema

### Vertex Types (8)

| Type | Properties | Index |
|------|-----------|-------|
| Person | id (long), firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed | unique on `id` |
| Post | id (long), imageFile, creationDate, locationIP, browserUsed, language, content, length | unique on `id` |
| Comment | id (long), creationDate, locationIP, browserUsed, content, length | unique on `id` |
| Forum | id (long), title, creationDate | unique on `id` |
| Tag | id (long), name, url | unique on `id` |
| TagClass | id (long), name, url | unique on `id` |
| Place | id (long), name, url, type (City/Country/Continent) | unique on `id`, index on `type` |
| Organisation | id (long), name, url, type (University/Company) | unique on `id`, index on `type` |

All vertex types use `PARALLEL` bucket count for parallel insertion.

### Edge Types (14)

| Edge Type | From | To | Properties |
|-----------|------|-----|------------|
| KNOWS | Person | Person | creationDate |
| HAS_CREATOR | Post, Comment | Person | -- |
| REPLY_OF | Comment | Post or Comment | -- |
| HAS_TAG | Post, Comment, Forum | Tag | -- |
| LIKES | Person | Post or Comment | creationDate |
| CONTAINER_OF | Forum | Post | -- |
| HAS_MEMBER | Forum | Person | joinDate |
| HAS_MODERATOR | Forum | Person | -- |
| WORKS_AT | Person | Organisation | workFrom (int) |
| STUDY_AT | Person | Organisation | classYear (int) |
| IS_LOCATED_IN | Person, Post, Comment, Organisation | Place | -- |
| HAS_INTEREST | Person | Tag | -- |
| IS_PART_OF | Place | Place | -- |
| IS_SUBCLASS_OF | TagClass | TagClass | -- |

KNOWS is bidirectional. All others are unidirectional.

## Data Generation

Generation order (respects dependencies):

1. **TagClass** -- hierarchy with ~5 root classes, rest as children (IS_SUBCLASS_OF)
2. **Tag** -- each assigned to a TagClass (IS_PART_OF)
3. **Place** -- 6 continents, ~50 countries, rest cities. IS_PART_OF links cities->countries->continents
4. **Organisation** -- each IS_LOCATED_IN a random Place (country for University, city for Company)
5. **Person** -- each IS_LOCATED_IN a random city. Random WORKS_AT/STUDY_AT. Random HAS_INTEREST tags
6. **KNOWS** -- power-law distribution via ThreadLocalRandom. Bidirectional. Avg ~40 per Person
7. **Forum** -- each HAS_MODERATOR a random Person. Random HAS_MEMBER edges
8. **Post** -- each in a Forum (CONTAINER_OF), HAS_CREATOR to a Forum member. Random HAS_TAG. IS_LOCATED_IN from creator's location
9. **Comment** -- each REPLY_OF a Post or earlier Comment. HAS_CREATOR, HAS_TAG, IS_LOCATED_IN
10. **LIKES** -- random Persons liking random Posts/Comments

Uses `database.async()` with `PARALLEL` level and `COMMIT_EVERY` batch size. WAL disabled during generation. `ThreadLocalRandom.current()` per thread.

## Class Structure

```java
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("benchmark")
class GraphBenchmark {

  private Database database;
  private MeterRegistry registry;
  private boolean freshlyCreated;

  @BeforeAll
  void setup() {
    registry = new SimpleMeterRegistry();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists()) {
      database = factory.open();
      freshlyCreated = false;
    } else {
      database = factory.create();
      freshlyCreated = true;
      final Timer.Sample sample = Timer.start(registry);
      createSchema();
      populateGraph();
      sample.stop(registry.timer("benchmark.creation"));
    }
  }

  @AfterAll
  void teardownAndReport() {
    printReport();
    if (database != null && database.isOpen())
      database.close();  // close, NOT drop -- preserve for reuse
  }

  private void createSchema() { ... }
  private void populateGraph() { ... }
  private void printReport() { ... }
  private void benchmark(String phase, String name, int iterations,
      String sql, String cypher) { ... }

  @Test @Order(1) void phase2_lookups() { ... }
  @Test @Order(2) void phase3_simpleTraversals() { ... }
  @Test @Order(3) void phase4_complexTraversals() { ... }
}
```

Key: `close()` not `drop()` in teardown enables database reuse across runs.

## Benchmark Phases

### Phase 1: Creation (measured in @BeforeAll)

Total generation time recorded. If database reused, prints vertex/edge counts only.

### Phase 2: Simple Lookups (@Order(1))

| ID | Query | Iterations |
|----|-------|-----------|
| 2a | Person by `id` (indexed) | 1000 |
| 2b | Post by `id` (indexed) | 1000 |
| 2c | Person by `firstName` (non-indexed scan) | 100 |
| 2d | Count vertices per type | 10 |

### Phase 3: Simple Traversals (@Order(2))

| ID | Query | Iterations |
|----|-------|-----------|
| 3a | Direct friends of a Person (1-hop KNOWS) | 500 |
| 3b | Posts created by a Person (HAS_CREATOR reverse) | 500 |
| 3c | Tags of a given Post (HAS_TAG) | 500 |
| 3d | Members of a Forum (HAS_MEMBER) | 500 |

### Phase 4: Complex Traversals (@Order(3))

| ID | Query | Iterations |
|----|-------|-----------|
| 4a | Friends-of-friends (2-hop KNOWS, exclude direct) | 200 |
| 4b | Posts by friends in a city (KNOWS + HAS_CREATOR + IS_LOCATED_IN) | 200 |
| 4c | Common tags between two Persons' posts | 200 |
| 4d | Shortest path between two Persons via KNOWS (Cypher only) | 100 |
| 4e | Forum recommendation (forums with most of Person's friends) | 200 |

All queries run in both SQL and Cypher except 4d (Cypher only, uses shortestPath).

## Representative Queries

### Phase 2 -- Lookups

```sql
-- 2a: Person by ID (indexed)
SQL:    SELECT FROM Person WHERE id = ?
Cypher: MATCH (p:Person {id: $id}) RETURN p

-- 2c: Person by firstName (non-indexed scan)
SQL:    SELECT FROM Person WHERE firstName = ?
Cypher: MATCH (p:Person) WHERE p.firstName = $name RETURN p
```

### Phase 3 -- Simple Traversals

```sql
-- 3a: Direct friends
SQL:    SELECT expand(both('KNOWS')) FROM Person WHERE id = ?
Cypher: MATCH (p:Person {id: $id})-[:KNOWS]-(friend) RETURN friend

-- 3b: Posts by a Person
SQL:    SELECT expand(in('HAS_CREATOR')) FROM Person WHERE id = ?
Cypher: MATCH (p:Person {id: $id})<-[:HAS_CREATOR]-(post:Post) RETURN post
```

### Phase 4 -- Complex Traversals

```sql
-- 4a: Friends-of-friends
SQL:    SELECT expand(both('KNOWS').both('KNOWS')) FROM Person WHERE id = ?
Cypher: MATCH (p:Person {id: $id})-[:KNOWS]-()-[:KNOWS]-(fof)
        WHERE fof <> p AND NOT (p)-[:KNOWS]-(fof)
        RETURN DISTINCT fof

-- 4b: Posts by friends in a city
Cypher: MATCH (p:Person {id: $id})-[:KNOWS]-(friend)-[:IS_LOCATED_IN]->(c:Place {name: $city}),
              (friend)<-[:HAS_CREATOR]-(post:Post)
        RETURN post, friend.firstName

-- 4c: Common tags between two Persons' posts
Cypher: MATCH (a:Person {id: $id1})<-[:HAS_CREATOR]-(p1)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(p2)-[:HAS_CREATOR]->(b:Person {id: $id2})
        RETURN t.name, count(*) AS freq ORDER BY freq DESC

-- 4d: Shortest path via KNOWS
Cypher: MATCH path = shortestPath((a:Person {id: $id1})-[:KNOWS*]-(b:Person {id: $id2}))
        RETURN path, length(path)

-- 4e: Forum recommendation
Cypher: MATCH (p:Person {id: $id})-[:KNOWS]-(friend),
              (forum:Forum)-[:HAS_MEMBER]->(friend)
        RETURN forum.title, count(friend) AS friendCount
        ORDER BY friendCount DESC LIMIT 10
```

## Micrometer Metrics

Uses `SimpleMeterRegistry` (in-process, no external infrastructure).

Metrics per query:
- `Timer`: `benchmark.query.<phase>.<queryName>.<language>` -- with p50, p95, p99 percentile histograms
- `Counter`: `benchmark.query.<phase>.<queryName>.<language>.results` -- total result rows (sanity check)
- `Timer`: `benchmark.creation` -- total graph generation time

## Output Format

Printed to stdout in `@AfterAll`:

```
=== ArcadeDB Graph Benchmark (LDBC-inspired) ===
Database: target/databases/graph-benchmark
Vertices: 789,500 | Edges: 3,240,000

Phase | Query                  | Lang    | Ops |  Avg ms |  p50 ms |  p95 ms |  p99 ms
------|------------------------|---------|-----|---------|---------|---------|--------
2a    | Person by ID           | SQL     | 1000|    0.12 |    0.10 |    0.25 |    0.41
2a    | Person by ID           | Cypher  | 1000|    0.18 |    0.15 |    0.32 |    0.55
...
4d    | Shortest path (KNOWS)  | Cypher  | 100 |   12.40 |   10.20 |   28.50 |   45.00
```
