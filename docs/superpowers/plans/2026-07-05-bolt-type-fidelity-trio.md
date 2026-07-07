# Bolt type-fidelity trio Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit native Bolt `Duration`, `Point`, and `Path` structures and distinguish semantic from syntax errors, closing #4997/#4998/#4999 and flipping conformance cells TYPE-011, TYPE-012, TYPE-003, ERR-002.

**Architecture:** Writer-side additions to `BoltStructureMapper` plus the single inbound param-decode point, mirroring the shipped temporal work (#4907). No query-engine changes except one small exception subclass in `engine`. Native structures reuse the existing `PackStreamStructure`/`BoltPath` machinery.

**Tech Stack:** Java 21, Maven multi-module, JUnit 5 + AssertJ, ArcadeDB `bolt` + `engine` modules.

## Global Constraints

- Java 21+; compile with `mvn -q -pl bolt -am -DskipTests install` before running tests.
- Use `final` on variables/parameters; import classes, no fully-qualified names in code.
- One-child `if` needs no braces; match surrounding style.
- No `System.out`; no Claude attribution in code or commits; comments state behavioral invariants only, no issue numbers in Javadoc.
- License header: copy the Apache-2.0 header block verbatim from any sibling `.java` file for every new source file.
- Bolt `Duration` tag `0x45` fields `[months, days, seconds, nanoseconds]`; `Point2D` tag `0x58` `[srid, x, y]`; `Point3D` tag `0x59` `[srid, x, y, z]`; `Path` tag `0x50`.
- SRID: `cartesian`->7203, `cartesian-3D`->9157, `WGS-84`->4326, `WGS-84-3D`->4979.
- Do not commit on `main`; all work on branch `feat/4890-bolt-type-fidelity` in worktree `.worktrees/feat/4890-bolt-type-fidelity`.

---

## File Structure

- `bolt/.../structure/BoltPointStructure.java` (new) - spatial `PackStreamStructure`, tag 0x58/0x59.
- `bolt/.../structure/BoltStructureMapper.java` (modify) - outbound Duration/Point/Path branches; inbound Duration/Point decode.
- `bolt/.../BoltNetworkExecutor.java` (modify) - `classifyParsingError` helper + use in `handleRun`.
- `engine/.../exception/CommandSemanticException.java` (new) - subclass of `CommandParsingException`.
- `engine/.../query/opencypher/parser/CypherSemanticValidator.java` (modify) - retarget throws.
- `bolt/.../BoltTypeRoundTripTest.java` (modify) - flip TYPE-011, add TYPE-012.
- `bolt/.../Bolt4998PathMappingTest.java` (new) - TYPE-003 embedded-DB mapping test.
- `bolt/.../PackStreamTypeInputTest.java` (new) - inbound Duration/Point round-trip decode.
- `bolt/.../BoltErrorClassificationTest.java` (modify) - `classifyParsingError` unit tests.
- `engine/.../CypherSemanticValidatorExceptionTest.java` (new) - validator throws `CommandSemanticException`.
- `bolt/conformance/spec.yaml` (modify) + the five e2e suites (modify) - flip cells + markers.

---

### Task 1: BoltPointStructure + outbound Point mapping (TYPE-012)

**Files:**
- Create: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltPointStructure.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java`
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java`

**Interfaces:**
- Produces: `BoltPointStructure(int srid, double x, double y)` and `BoltPointStructure(int srid, double x, double y, Double z)`; getters `getSrid()`, `getX()`, `getY()`, `getZ()`, `getSignature()`. `BoltStructureMapper.toPointStructure(Object)` -> `BoltPointStructure` or `null`.

- [ ] **Step 1: Write the failing test** in `BoltTypeRoundTripTest.java` (add imports `com.arcadedb.bolt.structure.BoltPointStructure`, `java.util.LinkedHashMap`, `java.util.Map`):

```java
@Test
@DisplayName("[TYPE-012] cartesian Point serializes as a native Bolt Point2D structure")
void type012_cartesianPointNative() {
  final Map<String, Object> point = new LinkedHashMap<>();
  point.put("x", 12.34);
  point.put("y", 56.78);
  point.put("crs", "cartesian");
  final Object out = BoltStructureMapper.toPackStreamValue(point);
  assertThat(out).isInstanceOf(BoltPointStructure.class);
  final BoltPointStructure p = (BoltPointStructure) out;
  assertThat(p.getSignature()).isEqualTo((byte) 0x58);
  assertThat(p.getSrid()).isEqualTo(7203);
  assertThat(p.getX()).isEqualTo(12.34);
  assertThat(p.getY()).isEqualTo(56.78);
  assertThat(p.getZ()).isNull();
}

@Test
@DisplayName("[TYPE-012] WGS-84 3D Point serializes as a native Bolt Point3D structure")
void type012_wgs84Point3DNative() {
  final Map<String, Object> point = new LinkedHashMap<>();
  point.put("longitude", 12.34);
  point.put("latitude", 56.78);
  point.put("height", 100.0);
  point.put("crs", "WGS-84-3D");
  point.put("srid", 4979);
  final BoltPointStructure p = (BoltPointStructure) BoltStructureMapper.toPackStreamValue(point);
  assertThat(p.getSignature()).isEqualTo((byte) 0x59);
  assertThat(p.getSrid()).isEqualTo(4979);
  assertThat(p.getX()).isEqualTo(12.34);
  assertThat(p.getY()).isEqualTo(56.78);
  assertThat(p.getZ()).isEqualTo(100.0);
}

@Test
@DisplayName("A plain map without crs is not misdetected as a Point")
void plainMapIsNotPoint() {
  final Map<String, Object> m = new LinkedHashMap<>();
  m.put("a", 1);
  m.put("b", 2);
  assertThat(BoltStructureMapper.toPackStreamValue(m)).isInstanceOf(Map.class);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl bolt test -Dtest=BoltTypeRoundTripTest#type012_cartesianPointNative`
Expected: FAIL - `BoltPointStructure` does not exist / value is a Map.

- [ ] **Step 3: Create `BoltPointStructure.java`** (copy the Apache header from `BoltPath.java`):

```java
package com.arcadedb.bolt.structure;

import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;

/**
 * A Bolt spatial Point PackStream structure. Point2D (signature 0x58) carries [srid, x, y];
 * Point3D (signature 0x59) carries [srid, x, y, z]. srid is an integer; coordinates are doubles.
 */
public class BoltPointStructure implements PackStreamStructure {
  public static final byte SIGNATURE_2D = 0x58;
  public static final byte SIGNATURE_3D = 0x59;

  private final int    srid;
  private final double x;
  private final double y;
  private final Double z; // null for 2D

  public BoltPointStructure(final int srid, final double x, final double y) {
    this(srid, x, y, null);
  }

  public BoltPointStructure(final int srid, final double x, final double y, final Double z) {
    this.srid = srid;
    this.x = x;
    this.y = y;
    this.z = z;
  }

  @Override
  public byte getSignature() {
    return z == null ? SIGNATURE_2D : SIGNATURE_3D;
  }

  @Override
  public int getFieldCount() {
    return z == null ? 3 : 4;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(getSignature(), getFieldCount());
    writer.writeValue((long) srid);
    writer.writeValue(x);
    writer.writeValue(y);
    if (z != null)
      writer.writeValue(z);
  }

  public int getSrid() {
    return srid;
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  public Double getZ() {
    return z;
  }
}
```

- [ ] **Step 4: Add Point detection to `BoltStructureMapper.toPackStreamValue`.** Insert this branch immediately **before** the `Map` branch (`if (value instanceof Map<?, ?> map)`, ~line 106):

```java
    // Spatial Point: Cypher point() returns a plain Map (there is no Point class), so detect
    // structurally (a crs key plus numeric x/y or longitude/latitude) before the generic Map branch.
    final BoltPointStructure point = toPointStructure(value);
    if (point != null)
      return point;
```

Add these constants and helpers to `BoltStructureMapper` (near the temporal SIG constants) and the import `import com.arcadedb.bolt.structure.BoltPointStructure;` is not needed (same package); add nothing else:

```java
  private static final byte SIG_POINT_2D = 0x58; // 'X' [srid, x, y]
  private static final byte SIG_POINT_3D = 0x59; // 'Y' [srid, x, y, z]

  /**
   * Recognize a Cypher spatial point (a Map carrying a crs key plus numeric x/y or longitude/latitude)
   * and encode it as a native Bolt Point structure. Returns null when the value is not a point so the
   * caller falls through to generic Map handling.
   */
  static BoltPointStructure toPointStructure(final Object value) {
    if (!(value instanceof Map<?, ?> map) || !map.containsKey("crs"))
      return null;
    final Double x = pointCoord(map, "x", "longitude");
    final Double y = pointCoord(map, "y", "latitude");
    if (x == null || y == null)
      return null;
    final Double z = pointCoord(map, "z", "height");
    final int srid = pointSrid(map, z != null);
    return z == null ? new BoltPointStructure(srid, x, y) : new BoltPointStructure(srid, x, y, z);
  }

  private static Double pointCoord(final Map<?, ?> map, final String primary, final String alt) {
    Object v = map.get(primary);
    if (!(v instanceof Number))
      v = map.get(alt);
    return v instanceof Number n ? n.doubleValue() : null;
  }

  private static int pointSrid(final Map<?, ?> map, final boolean is3d) {
    if (map.get("srid") instanceof Number n)
      return n.intValue();
    final String crs = map.get("crs") != null ? map.get("crs").toString() : "";
    if (crs.startsWith("WGS-84"))
      return is3d ? 4979 : 4326;
    return is3d ? 9157 : 7203; // cartesian
  }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -q -pl bolt test -Dtest=BoltTypeRoundTripTest`
Expected: PASS (TYPE-012 cases and `plainMapIsNotPoint`), existing TYPE-007..010 still pass.

- [ ] **Step 6: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/structure/BoltPointStructure.java \
        bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java
git commit -m "feat(bolt): emit native Point structures (TYPE-012)"
```

---

### Task 2: Outbound Duration mapping (TYPE-011)

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java`
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java`

**Interfaces:**
- Consumes: `BoltTemporalStructure(byte signature, Object... fields)` (existing).
- Produces: `toPackStreamValue(CypherDuration)` -> `BoltTemporalStructure` sig `0x45`, fields `[months, days, seconds, nanos]`.

- [ ] **Step 1: Replace the existing TYPE-011 characterization test** in `BoltTypeRoundTripTest.java` (delete `type011_durationGap`, add the import `com.arcadedb.query.opencypher.temporal.CypherDuration`):

```java
@Test
@DisplayName("[TYPE-011] CypherDuration serializes as a native Bolt Duration structure")
void type011_durationNative() {
  // duration('P1DT2H30M') -> months=0, days=1, seconds=9000, nanos=0
  final CypherDuration d = new CypherDuration(0, 1, 9000, 0);
  final Object out = BoltStructureMapper.toPackStreamValue(d);
  assertThat(out).isInstanceOf(BoltTemporalStructure.class);
  final BoltTemporalStructure s = (BoltTemporalStructure) out;
  assertThat(s.getSignature()).isEqualTo((byte) 0x45);
  assertThat(s.getFieldCount()).isEqualTo(4);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl bolt test -Dtest=BoltTypeRoundTripTest#type011_durationNative`
Expected: FAIL - `CypherDuration` falls through to `value.toString()` (a String, not `BoltTemporalStructure`).

- [ ] **Step 3: Add the Duration branch.** In `BoltStructureMapper`, add the constant and a branch in `toTemporalStructure(...)` (before the final `return null;`), and add the import `com.arcadedb.query.opencypher.temporal.CypherDuration`:

```java
  private static final byte SIG_DURATION = 0x45; // 'E' [months, days, seconds, nanoseconds]
```

```java
    if (value instanceof CypherDuration d)
      return new BoltTemporalStructure(SIG_DURATION, d.getMonths(), d.getDays(), d.getSeconds(), (long) d.getNanosAdjustment());
```

Update the comment in `unwrapCypherTemporal` that currently says `CypherDuration is intentionally not unwrapped` to state that Duration is now emitted as a native Bolt Duration struct via the branch above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl bolt test -Dtest=BoltTypeRoundTripTest`
Expected: PASS. All temporal cases still pass.

- [ ] **Step 5: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java
git commit -m "feat(bolt): emit native Duration structures (TYPE-011)"
```

---

### Task 3: Inbound Duration + Point decode (round-trip)

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java`
- Test: `bolt/src/test/java/com/arcadedb/bolt/PackStreamTypeInputTest.java` (new)

**Interfaces:**
- Consumes: `fromPackStreamValue(Object)` (existing), `PackStreamReader.StructureValue`.
- Produces: inbound `0x45` -> `CypherDuration`; `0x58/0x59` -> `LinkedHashMap` with `x`,`y`(,`z`),`srid`,`crs`.

- [ ] **Step 1: Write the failing test** `PackStreamTypeInputTest.java` (copy header + the `decode`/`readBack` helpers from `PackStreamTemporalInputTest.java`; imports: `com.arcadedb.bolt.packstream.PackStreamReader`, `PackStreamWriter`, `com.arcadedb.bolt.structure.BoltStructureMapper`, `com.arcadedb.query.opencypher.temporal.CypherDuration`, `java.util.Map`, `org.junit.jupiter.api.Test`, `static org.assertj.core.api.Assertions.assertThat`):

```java
private static final byte SIG_DURATION = 0x45;
private static final byte SIG_POINT_2D = 0x58;
private static final byte SIG_POINT_3D = 0x59;

@Test
void decodesDuration() throws Exception {
  final Object out = decode(SIG_DURATION, 0L, 1L, 9000L, 0L);
  assertThat(out).isInstanceOf(CypherDuration.class);
  final CypherDuration d = (CypherDuration) out;
  assertThat(d.getMonths()).isEqualTo(0);
  assertThat(d.getDays()).isEqualTo(1);
  assertThat(d.getSeconds()).isEqualTo(9000);
  assertThat(d.getNanosAdjustment()).isEqualTo(0);
}

@Test
void decodesCartesianPoint2D() throws Exception {
  final Object out = decode(SIG_POINT_2D, 7203L, 12.34, 56.78);
  assertThat(out).isInstanceOf(Map.class);
  final Map<?, ?> m = (Map<?, ?>) out;
  assertThat(((Number) m.get("srid")).intValue()).isEqualTo(7203);
  assertThat(((Number) m.get("x")).doubleValue()).isEqualTo(12.34);
  assertThat(((Number) m.get("y")).doubleValue()).isEqualTo(56.78);
  assertThat(m.get("crs")).isEqualTo("cartesian");
}

@Test
void decodesWgs84Point3D() throws Exception {
  final Object out = decode(SIG_POINT_3D, 4979L, 12.34, 56.78, 100.0);
  final Map<?, ?> m = (Map<?, ?>) out;
  assertThat(m.get("crs")).isEqualTo("WGS-84-3D");
  assertThat(((Number) m.get("z")).doubleValue()).isEqualTo(100.0);
}

@Test
void malformedDurationIsLeftOpaque() throws Exception {
  // Duration expects 4 fields; 2 must degrade to an opaque StructureValue, not crash.
  assertThat(decode(SIG_DURATION, 1L, 2L)).isInstanceOf(PackStreamReader.StructureValue.class);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl bolt test -Dtest=PackStreamTypeInputTest`
Expected: FAIL - structs come back as raw `StructureValue`.

- [ ] **Step 3: Extend the inbound decoder.** In `BoltStructureMapper.fromTemporalStructure(...)`, add cases to the `switch`, extend `hasExpectedArity`, and add helpers. Add import `java.util.LinkedHashMap` (already have `java.util.*`) and `CypherDuration` (added in Task 2):

```java
      case SIG_DURATION:
        return new CypherDuration(asLong(f.get(0)), asLong(f.get(1)), asLong(f.get(2)), (int) asLong(f.get(3)));

      case SIG_POINT_2D:
        return pointMap((int) asLong(f.get(0)), asDouble(f.get(1)), asDouble(f.get(2)), null);

      case SIG_POINT_3D:
        return pointMap((int) asLong(f.get(0)), asDouble(f.get(1)), asDouble(f.get(2)), asDouble(f.get(3)));
```

In `hasExpectedArity`, extend the switch:

```java
      case SIG_DURATION, SIG_POINT_3D -> fieldCount == 4;
      case SIG_POINT_2D -> fieldCount == 3;
```

Add helpers near `asLong`:

```java
  private static double asDouble(final Object value) {
    return ((Number) value).doubleValue();
  }

  private static Map<String, Object> pointMap(final int srid, final double x, final double y, final Double z) {
    final Map<String, Object> m = new LinkedHashMap<>();
    m.put("srid", srid);
    m.put("x", x);
    m.put("y", y);
    if (z != null)
      m.put("z", z);
    m.put("crs", crsForSrid(srid, z != null));
    return m;
  }

  private static String crsForSrid(final int srid, final boolean is3d) {
    return switch (srid) {
      case 4326 -> "WGS-84";
      case 4979 -> "WGS-84-3D";
      case 9157 -> "cartesian-3D";
      default -> is3d ? "cartesian-3D" : "cartesian";
    };
  }
```

Note: `SIG_POINT_2D`/`SIG_POINT_3D` constants already exist from Task 1; `SIG_DURATION` from Task 2. The `asDouble` cast in the point cases can throw `ClassCastException` on a malformed field - the existing `try/catch (RuntimeException)` around the switch already returns the struct opaque, so no extra guard needed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl bolt test -Dtest=PackStreamTypeInputTest,PackStreamTemporalInputTest`
Expected: PASS - both new and existing inbound tests green.

- [ ] **Step 5: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java \
        bolt/src/test/java/com/arcadedb/bolt/PackStreamTypeInputTest.java
git commit -m "feat(bolt): decode inbound Duration and Point parameters (round-trip)"
```

---

### Task 4: Native Path mapping (TYPE-003)

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java`
- Test: `bolt/src/test/java/com/arcadedb/bolt/Bolt4998PathMappingTest.java` (new)

**Interfaces:**
- Consumes: `toNode(Vertex)`, `toUnboundRelationship(Edge)` (existing); `TraversalPath.getVertices()/getEdges()`.
- Produces: `toPackStreamValue(TraversalPath)` -> `BoltPath`; static `BoltStructureMapper.toPath(TraversalPath)`.

- [ ] **Step 1: Write the failing test** `Bolt4998PathMappingTest.java` (Apache header; imports: `com.arcadedb.database.Database`, `com.arcadedb.database.DatabaseFactory`, `com.arcadedb.graph.MutableVertex`, `com.arcadedb.query.opencypher.traversal.TraversalPath`, `com.arcadedb.query.sql.executor.ResultSet`, `com.arcadedb.bolt.structure.BoltPath`, `com.arcadedb.bolt.structure.BoltStructureMapper`, `org.junit.jupiter.api.*`, `static org.assertj.core.api.Assertions.assertThat`):

```java
class Bolt4998PathMappingTest {
  private static final String DB_PATH = "./target/databases/Bolt4998PathMappingTest";
  private Database db;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    db = factory.create();
    db.getSchema().createVertexType("V");
    db.getSchema().createEdgeType("E");
    db.transaction(() -> {
      final MutableVertex a = db.newVertex("V").set("k", "a").save();
      final MutableVertex b = db.newVertex("V").set("k", "b").save();
      final MutableVertex c = db.newVertex("V").set("k", "c").save();
      a.newEdge("E", b).save();
      b.newEdge("E", c).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  @DisplayName("[TYPE-003] a 2-hop forward path maps to a native BoltPath")
  void type003_forwardPath() {
    final ResultSet rs = db.query("opencypher", "MATCH p=(a:V {k:'a'})-[:E*2]->(c) RETURN p");
    final TraversalPath tp = (TraversalPath) rs.next().getProperty("p");
    final BoltPath bp = BoltStructureMapper.toPath(tp);
    assertThat(bp.getNodes()).hasSize(3);
    assertThat(bp.getRelationships()).hasSize(2);
    // [relIdx(1-based, +forward), nodeIdx(0-based), ...] -> rel1 fwd to node1, rel2 fwd to node2
    assertThat(bp.getIndices()).containsExactly(1L, 1L, 2L, 2L);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl bolt test -Dtest=Bolt4998PathMappingTest`
Expected: FAIL - `toPath` does not exist (compile error).

- [ ] **Step 3: Add the Path branch and mapper.** In `toPackStreamValue`, add a branch right after the `Result` branch (~line 77):

```java
    if (value instanceof TraversalPath path)
      return toPath(path);
```

Add the method and helper to `BoltStructureMapper`, plus imports `com.arcadedb.query.opencypher.traversal.TraversalPath`, `com.arcadedb.graph.Vertex` (already imported), `com.arcadedb.graph.Edge` (already imported):

```java
  /**
   * Convert a Cypher path result into a native Bolt Path structure. nodes and relationships are
   * deduplicated (Bolt requires unique lists); indices is a flat [relIndex, nodeIndex, ...] sequence
   * where relIndex is 1-based and signed by traversal direction (positive = the edge's OUT side is the
   * previous path node, negative = traversed backward) and nodeIndex is the 0-based index of the node
   * reached at that hop. The start node is index 0 and implicit.
   */
  static BoltPath toPath(final TraversalPath path) {
    final List<Vertex> vertices = path.getVertices();
    final List<Edge> edges = path.getEdges();

    final List<BoltNode> nodes = new ArrayList<>();
    final Map<RID, Integer> nodeIndex = new HashMap<>();
    final List<BoltUnboundRelationship> rels = new ArrayList<>();
    final Map<RID, Integer> relIndex = new HashMap<>();
    final List<Long> indices = new ArrayList<>(edges.size() * 2);

    addPathNode(vertices.get(0), nodes, nodeIndex);

    for (int i = 0; i < edges.size(); i++) {
      final Edge edge = edges.get(i);
      final Vertex from = vertices.get(i);
      final Vertex to = vertices.get(i + 1);

      final RID relRid = edge.getIdentity();
      Integer ri = relIndex.get(relRid);
      if (ri == null) {
        rels.add(toUnboundRelationship(edge));
        ri = rels.size(); // 1-based
        relIndex.put(relRid, ri);
      }
      final boolean forward = edge.getOut().equals(from.getIdentity());
      indices.add((long) (forward ? ri : -ri));
      indices.add((long) addPathNode(to, nodes, nodeIndex));
    }
    return new BoltPath(nodes, rels, indices);
  }

  private static int addPathNode(final Vertex vertex, final List<BoltNode> nodes, final Map<RID, Integer> nodeIndex) {
    final RID rid = vertex.getIdentity();
    Integer i = nodeIndex.get(rid);
    if (i == null) {
      nodes.add(toNode(vertex));
      i = nodes.size() - 1; // 0-based
      nodeIndex.put(rid, i);
    }
    return i;
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl bolt test -Dtest=Bolt4998PathMappingTest`
Expected: PASS.

- [ ] **Step 5: Add a backward-edge regression test** in the same class (the reverse traversal must produce a negative rel index):

```java
  @Test
  @DisplayName("[TYPE-003] a backward-traversed hop yields a negative rel index")
  void type003_backwardHop() {
    // Path starts at b and walks the incoming edge (a)-[E]->(b) backward to a, so the hop's
    // rel index must be negative: edge.getOut() (a) != the previous path node (b).
    final ResultSet rs = db.query("opencypher", "MATCH p=(b:V {k:'b'})<-[:E]-(a) RETURN p");
    final TraversalPath tp = (TraversalPath) rs.next().getProperty("p");
    final BoltPath bp = BoltStructureMapper.toPath(tp);
    assertThat(bp.getNodes()).hasSize(2);
    assertThat(bp.getRelationships()).hasSize(1);
    assertThat(bp.getIndices().get(0)).isEqualTo(-1L); // backward, negative
    assertThat(bp.getIndices().get(1)).isEqualTo(1L);  // reached node index (a) = 1
  }
```

Run: `mvn -q -pl bolt test -Dtest=Bolt4998PathMappingTest`
Expected: PASS (both tests).

- [ ] **Step 6: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java \
        bolt/src/test/java/com/arcadedb/bolt/Bolt4998PathMappingTest.java
git commit -m "feat(bolt): emit native Path structures (TYPE-003)"
```

---

### Task 5: Semantic vs syntax error classification (ERR-002)

**Files:**
- Create: `engine/src/main/java/com/arcadedb/exception/CommandSemanticException.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidator.java`
- Modify: `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java`
- Test: `engine/src/test/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidatorExceptionTest.java` (new)
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltErrorClassificationTest.java` (modify)

**Interfaces:**
- Produces: `CommandSemanticException extends CommandParsingException`; `BoltNetworkExecutor.classifyParsingError(CommandParsingException)` -> error-code String.

- [ ] **Step 1: Write the failing engine test** `CypherSemanticValidatorExceptionTest.java` (Apache header; imports: `com.arcadedb.exception.CommandParsingException`, `com.arcadedb.exception.CommandSemanticException`, an embedded `Database`/`DatabaseFactory`, `org.junit.jupiter.api.*`, `static org.assertj.core.api.Assertions.*`):

```java
class CypherSemanticValidatorExceptionTest {
  private static final String DB_PATH = "./target/databases/CypherSemanticValidatorExceptionTest";
  private Database db;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    db = factory.create();
    db.getSchema().createVertexType("Beer");
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void undefinedVariableThrowsSemanticException() {
    assertThatThrownBy(() -> db.query("opencypher", "MATCH (n:Beer) RETURN undefinedVariable").close())
        .isInstanceOf(CommandSemanticException.class)
        .isInstanceOf(CommandParsingException.class);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl engine test -Dtest=CypherSemanticValidatorExceptionTest`
Expected: FAIL - thrown exception is `CommandParsingException`, not `CommandSemanticException`.

- [ ] **Step 3: Create `CommandSemanticException.java`** (copy the Apache header and structure from `CommandSQLParsingException.java` in the same package):

```java
package com.arcadedb.exception;

/**
 * Thrown when a query is syntactically valid but violates a semantic rule (for example an undefined
 * variable, a type conflict, or an invalid aggregation). Extends CommandParsingException so existing
 * handlers keep treating it as a parsing error, while callers that care can distinguish it.
 */
public class CommandSemanticException extends CommandParsingException {
  public CommandSemanticException(final String message) {
    super(message);
  }

  public CommandSemanticException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public CommandSemanticException(final Throwable cause) {
    super(cause);
  }
}
```

- [ ] **Step 4: Retarget the validator throws.** In `CypherSemanticValidator.java`, replace every `throw new CommandParsingException(` with `throw new CommandSemanticException(` and update the import from `CommandParsingException` to add `CommandSemanticException` (keep `CommandParsingException` only if still referenced elsewhere in the file). Run:

```bash
sed -i '' 's/throw new CommandParsingException(/throw new CommandSemanticException(/g' \
  engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidator.java
```

Then add `import com.arcadedb.exception.CommandSemanticException;` next to the existing exception import.

- [ ] **Step 5: Run the engine test to verify it passes**

Run: `mvn -q -pl engine test -Dtest=CypherSemanticValidatorExceptionTest`
Expected: PASS.

- [ ] **Step 6: Add the Bolt classification test** to `BoltErrorClassificationTest.java` (add imports `com.arcadedb.exception.CommandParsingException`, `com.arcadedb.exception.CommandSemanticException`):

```java
@Test
void semanticExceptionClassifiesAsSemanticError() {
  assertThat(BoltNetworkExecutor.classifyParsingError(new CommandSemanticException("UndefinedVariable")))
      .isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
}

@Test
void plainParsingExceptionClassifiesAsSyntaxError() {
  assertThat(BoltNetworkExecutor.classifyParsingError(new CommandParsingException("unexpected token")))
      .isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
}
```

- [ ] **Step 7: Run to verify it fails**

Run: `mvn -q -pl bolt test -Dtest=BoltErrorClassificationTest#semanticExceptionClassifiesAsSemanticError`
Expected: FAIL - `classifyParsingError` does not exist.

- [ ] **Step 8: Add `classifyParsingError` and wire it into `handleRun`.** In `BoltNetworkExecutor.java` add the package-visible static helper next to `classifyExecutionError` (add import `com.arcadedb.exception.CommandSemanticException`; `CommandParsingException` is already imported):

```java
  static String classifyParsingError(final CommandParsingException error) {
    return error instanceof CommandSemanticException ? BoltErrorCodes.SEMANTIC_ERROR : BoltErrorCodes.SYNTAX_ERROR;
  }
```

Change the `catch (final CommandParsingException e)` block in `handleRun` (line ~629) from:

```java
      sendFailure(BoltException.SYNTAX_ERROR, parseMsg);
```

to:

```java
      sendFailure(classifyParsingError(e), parseMsg);
```

- [ ] **Step 9: Run the Bolt tests to verify they pass**

Run: `mvn -q -pl bolt test -Dtest=BoltErrorClassificationTest`
Expected: PASS (new + existing classification tests).

- [ ] **Step 10: Commit**

```bash
git add engine/src/main/java/com/arcadedb/exception/CommandSemanticException.java \
        engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidator.java \
        engine/src/test/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidatorExceptionTest.java \
        bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltErrorClassificationTest.java
git commit -m "feat(bolt): distinguish semantic from syntax errors (ERR-002)"
```

---

### Task 6: Flip conformance spec + five e2e suite markers

**Files:**
- Modify: `bolt/conformance/spec.yaml`
- Modify: `e2e-js/src/js-bolt-conformance.test.js`
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`
- Modify: `e2e-python/tests/` conformance suite
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`
- Modify: `e2e-go/` conformance suite

- [ ] **Step 1: Flip the four cells in `spec.yaml`.** For each of TYPE-003, TYPE-011, TYPE-012, ERR-002: set `current_status: passing` and delete the `known_limitation:` block and the `tracking_issue: "#4890"` line.

- [ ] **Step 2: Run the spec validator**

Run: `cd bolt/conformance && python3 validate_spec.py`
Expected: PASS - spec is internally consistent (no dangling tracking issues for passing cells).

- [ ] **Step 3: Flip the JS suite.** In `e2e-js/src/js-bolt-conformance.test.js` change the four `it.failing("[TYPE-003]"...)`, `[TYPE-011]`, `[TYPE-012]`, `[ERR-002]` to `it(...)` and update the preceding "flips red when fixed" comments to describe the passing behavior.

- [ ] **Step 4: Flip the Java suite.** In `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java` replace the `assertExpectedFailure("#4890", () -> {...})` wrappers for `type003_path`, `type011_duration`, `type012_point`, `err002_semantic` with the direct positive assertions (assert the driver receives `Path`/`Duration`/`Point` values and the `Neo.ClientError.Statement.SemanticError` code).

- [ ] **Step 5: Flip the Python suite.** In `e2e-python/tests/`, remove the `@pytest.mark.xfail` markers for the four scenarios and assert native `neo4j.time`/`neo4j.spatial` types and the semantic error code.

- [ ] **Step 6: Flip the C# suite.** In `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`, remove the xfail/skip guard for the four scenarios and assert native `Duration`/`Point`/`Path` types and `SemanticError`.

- [ ] **Step 7: Flip the Go suite.** In `e2e-go/`, remove the skip/expected-fail guard for the four scenarios and assert native `dbtype.Duration`/`dbtype.Point2D`/`dbtype.Path` and the semantic error code.

- [ ] **Step 8: Commit**

```bash
git add bolt/conformance/spec.yaml e2e-js e2e e2e-python e2e-csharp e2e-go
git commit -m "test(bolt): flip TYPE-003/011/012 and ERR-002 to passing across conformance suites"
```

---

### Task 7: Full module verification + open PR

**Files:** none (verification + PR).

- [ ] **Step 1: Compile and run the affected module tests**

Run: `mvn -q -pl bolt -am test`
Expected: BUILD SUCCESS - all bolt tests green (BoltTypeRoundTripTest, PackStreamTypeInputTest, Bolt4998PathMappingTest, BoltErrorClassificationTest, plus existing).

- [ ] **Step 2: Run the touched engine tests**

Run: `mvn -q -pl engine test -Dtest="CypherSemanticValidatorExceptionTest,Cypher*SemanticValidator*"`
Expected: BUILD SUCCESS - no regression in the semantic validator path.

- [ ] **Step 3: Push and open the PR**

```bash
git push -u origin feat/4890-bolt-type-fidelity
```

Open a PR titled `feat(#4890): native Bolt Duration, Point, Path + semantic errors (#4997, #4998, #4999)`. Body: summarize the three components, list the flipped conformance cells (TYPE-003/011/012, ERR-002), state that full e2e verification runs in CI against the branch-built image, and note it partially closes #4890 and closes #4997/#4998/#4999. End with the Claude Code generated-with footer.

- [ ] **Step 4: Update tracking issue** - check the three child boxes and the "closes its scenario in spec.yaml" box on #4890.

---

## Notes for the reviewer / executor

- The hard local gate is the bolt-module + engine unit tests (Tasks 1-5). The e2e suite flips (Task 6) only go green in CI once the Docker image is built from this branch - do not block locally on them.
- Point detection is deliberately structural (Cypher `point()` returns a `Map`, there is no Point class). If a user query returns a map that happens to carry a `crs` key plus numeric `x`/`y`, it will be encoded as a Point - this matches how `CypherPointDistanceFunction` already recognizes points and is the accepted trade-off.
- The semantic-exception change is additive: `CommandSemanticException extends CommandParsingException`, so HTTP/Postgres/Gremlin/GraphQL/Redis handlers that catch `CommandParsingException` are unaffected. Only the Bolt handler distinguishes it.
