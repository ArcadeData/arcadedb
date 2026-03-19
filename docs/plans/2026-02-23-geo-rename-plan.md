# Geo Function Rename Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename all 21 `ST_*` SQL geo functions to the `geo.*` dot-namespace convention used by ArcadeDB's vector functions.

**Architecture:** Pure rename — change `NAME` constants, class names, file names, and all usages in the factory and tests. No logic changes. The Java package `com.arcadedb.function.sql.geo` stays the same. The base predicate class `SQLFunctionST_Predicate` is renamed `SQLFunctionGeoPredicate` first so subsequent predicate subclasses can reference it.

**Tech Stack:** Java 21, Maven, JUnit 5 / AssertJ

---

### Task 1: Rename base class `SQLFunctionST_Predicate` → `SQLFunctionGeoPredicate`

**Files:**
- Rename: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionST_Predicate.java`
  → `SQLFunctionGeoPredicate.java`

**Step 1: Rename the file**

```bash
cd engine/src/main/java/com/arcadedb/function/sql/geo
git mv SQLFunctionST_Predicate.java SQLFunctionGeoPredicate.java
```

**Step 2: Update class declaration**

In `SQLFunctionGeoPredicate.java`, change:
```java
public abstract class SQLFunctionST_Predicate extends SQLFunctionAbstract implements IndexableSQLFunction {
```
to:
```java
public abstract class SQLFunctionGeoPredicate extends SQLFunctionAbstract implements IndexableSQLFunction {
```

**Step 3: Compile to verify**

```bash
cd /path/to/repo
mvn compile -pl engine -q
```
Expected: BUILD SUCCESS (predicate subclasses will fail — fix in Task 3)

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoPredicate.java
git commit -m "refactor(geo): rename SQLFunctionST_Predicate to SQLFunctionGeoPredicate"
```

---

### Task 2: Rename the 12 constructor/accessor function classes

These 12 classes extend `SQLFunctionAbstract` directly (not the predicate base).

Rename mapping:

| Old file | New file | Old NAME | New NAME | Old getSyntax prefix |
|---|---|---|---|---|
| `SQLFunctionST_GeomFromText.java` | `SQLFunctionGeoGeomFromText.java` | `ST_GeomFromText` | `geo.geomFromText` | `ST_GeomFromText(` |
| `SQLFunctionST_Point.java` | `SQLFunctionGeoPoint.java` | `ST_Point` | `geo.point` | `ST_Point(` |
| `SQLFunctionST_LineString.java` | `SQLFunctionGeoLineString.java` | `ST_LineString` | `geo.lineString` | `ST_LineString(` |
| `SQLFunctionST_Polygon.java` | `SQLFunctionGeoPolygon.java` | `ST_Polygon` | `geo.polygon` | `ST_Polygon(` |
| `SQLFunctionST_Buffer.java` | `SQLFunctionGeoBuffer.java` | `ST_Buffer` | `geo.buffer` | `ST_Buffer(` |
| `SQLFunctionST_Envelope.java` | `SQLFunctionGeoEnvelope.java` | `ST_Envelope` | `geo.envelope` | `ST_Envelope(` |
| `SQLFunctionST_Distance.java` | `SQLFunctionGeoDistance.java` | `ST_Distance` | `geo.distance` | `ST_Distance(` |
| `SQLFunctionST_Area.java` | `SQLFunctionGeoArea.java` | `ST_Area` | `geo.area` | `ST_Area(` |
| `SQLFunctionST_AsText.java` | `SQLFunctionGeoAsText.java` | `ST_AsText` | `geo.asText` | `ST_AsText(` |
| `SQLFunctionST_AsGeoJson.java` | `SQLFunctionGeoAsGeoJson.java` | `ST_AsGeoJson` | `geo.asGeoJson` | `ST_AsGeoJson(` |
| `SQLFunctionST_X.java` | `SQLFunctionGeoX.java` | `ST_X` | `geo.x` | `ST_X(` |
| `SQLFunctionST_Y.java` | `SQLFunctionGeoY.java` | `ST_Y` | `geo.y` | `ST_Y(` |

**Step 1: Rename all 12 files**

```bash
cd engine/src/main/java/com/arcadedb/function/sql/geo
git mv SQLFunctionST_GeomFromText.java SQLFunctionGeoGeomFromText.java
git mv SQLFunctionST_Point.java         SQLFunctionGeoPoint.java
git mv SQLFunctionST_LineString.java    SQLFunctionGeoLineString.java
git mv SQLFunctionST_Polygon.java       SQLFunctionGeoPolygon.java
git mv SQLFunctionST_Buffer.java        SQLFunctionGeoBuffer.java
git mv SQLFunctionST_Envelope.java      SQLFunctionGeoEnvelope.java
git mv SQLFunctionST_Distance.java      SQLFunctionGeoDistance.java
git mv SQLFunctionST_Area.java          SQLFunctionGeoArea.java
git mv SQLFunctionST_AsText.java        SQLFunctionGeoAsText.java
git mv SQLFunctionST_AsGeoJson.java     SQLFunctionGeoAsGeoJson.java
git mv SQLFunctionST_X.java             SQLFunctionGeoX.java
git mv SQLFunctionST_Y.java             SQLFunctionGeoY.java
```

**Step 2: In each renamed file, apply three changes**

For every file, the pattern is identical — shown here for `SQLFunctionGeoPoint.java` as the example:

1. Class declaration: `SQLFunctionST_Point` → `SQLFunctionGeoPoint`
2. NAME constant: `"ST_Point"` → `"geo.point"`
3. getSyntax return: `"ST_Point(<x>, <y>)"` → `"geo.point(<x>, <y>)"`

Apply the same three-change pattern to all 12 files per the mapping table above.

> Note: `SQLFunctionGeoDistance.java` — also update `getSyntax()` and any Javadoc references to the old name.
> Note: `SQLFunctionGeoAsGeoJson.java` — the `NAME` constant is also referenced in `getSyntax()` only; no cross-references to other NAME constants.

**Step 3: Compile**

```bash
mvn compile -pl engine -q
```
Expected: BUILD SUCCESS (factory and tests will fail at test-compile; that's fine)

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/function/sql/geo/
git commit -m "refactor(geo): rename ST_* constructor/accessor classes to geo.* naming"
```

---

### Task 3: Rename the 9 predicate function classes

These extend `SQLFunctionST_Predicate` (now `SQLFunctionGeoPredicate`).

Rename mapping:

| Old file | New file | Old NAME | New NAME |
|---|---|---|---|
| `SQLFunctionST_Within.java` | `SQLFunctionGeoWithin.java` | `ST_Within` | `geo.within` |
| `SQLFunctionST_Intersects.java` | `SQLFunctionGeoIntersects.java` | `ST_Intersects` | `geo.intersects` |
| `SQLFunctionST_Contains.java` | `SQLFunctionGeoContains.java` | `ST_Contains` | `geo.contains` |
| `SQLFunctionST_DWithin.java` | `SQLFunctionGeoDWithin.java` | `ST_DWithin` | `geo.dWithin` |
| `SQLFunctionST_Disjoint.java` | `SQLFunctionGeoDisjoint.java` | `ST_Disjoint` | `geo.disjoint` |
| `SQLFunctionST_Equals.java` | `SQLFunctionGeoEquals.java` | `ST_Equals` | `geo.equals` |
| `SQLFunctionST_Crosses.java` | `SQLFunctionGeoCrosses.java` | `ST_Crosses` | `geo.crosses` |
| `SQLFunctionST_Overlaps.java` | `SQLFunctionGeoOverlaps.java` | `ST_Overlaps` | `geo.overlaps` |
| `SQLFunctionST_Touches.java` | `SQLFunctionGeoTouches.java` | `ST_Touches` | `geo.touches` |

**Step 1: Rename all 9 files**

```bash
cd engine/src/main/java/com/arcadedb/function/sql/geo
git mv SQLFunctionST_Within.java     SQLFunctionGeoWithin.java
git mv SQLFunctionST_Intersects.java SQLFunctionGeoIntersects.java
git mv SQLFunctionST_Contains.java   SQLFunctionGeoContains.java
git mv SQLFunctionST_DWithin.java    SQLFunctionGeoDWithin.java
git mv SQLFunctionST_Disjoint.java   SQLFunctionGeoDisjoint.java
git mv SQLFunctionST_Equals.java     SQLFunctionGeoEquals.java
git mv SQLFunctionST_Crosses.java    SQLFunctionGeoCrosses.java
git mv SQLFunctionST_Overlaps.java   SQLFunctionGeoOverlaps.java
git mv SQLFunctionST_Touches.java    SQLFunctionGeoTouches.java
```

**Step 2: In each renamed file, apply three changes**

For every file, same pattern — shown for `SQLFunctionGeoWithin.java`:

1. Class declaration: `SQLFunctionST_Within extends SQLFunctionST_Predicate`
   → `SQLFunctionGeoWithin extends SQLFunctionGeoPredicate`
2. Constructor call: `super(NAME)` stays as-is (no change needed here)
3. NAME constant: `"ST_Within"` → `"geo.within"`
4. getSyntax return: `"ST_Within(<geometry>, <shape>)"` → `"geo.within(<geometry>, <shape>)"`

Apply the same pattern to all 9 files per the mapping table above.

**Step 3: Compile**

```bash
mvn compile -pl engine -q
```
Expected: BUILD SUCCESS for main sources. Test compile will fail until Task 6.

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/function/sql/geo/
git commit -m "refactor(geo): rename ST_* predicate classes to geo.* naming"
```

---

### Task 4: Update `DefaultSQLFunctionFactory.java`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java`

**Step 1: Replace all 21 imports**

Find the block from line 32 to line 52 (all `import com.arcadedb.function.sql.geo.SQLFunctionST_*` lines).
Replace with:

```java
import com.arcadedb.function.sql.geo.SQLFunctionGeoArea;
import com.arcadedb.function.sql.geo.SQLFunctionGeoAsGeoJson;
import com.arcadedb.function.sql.geo.SQLFunctionGeoAsText;
import com.arcadedb.function.sql.geo.SQLFunctionGeoBuffer;
import com.arcadedb.function.sql.geo.SQLFunctionGeoContains;
import com.arcadedb.function.sql.geo.SQLFunctionGeoCrosses;
import com.arcadedb.function.sql.geo.SQLFunctionGeoDisjoint;
import com.arcadedb.function.sql.geo.SQLFunctionGeoDistance;
import com.arcadedb.function.sql.geo.SQLFunctionGeoDWithin;
import com.arcadedb.function.sql.geo.SQLFunctionGeoEnvelope;
import com.arcadedb.function.sql.geo.SQLFunctionGeoEquals;
import com.arcadedb.function.sql.geo.SQLFunctionGeoGeomFromText;
import com.arcadedb.function.sql.geo.SQLFunctionGeoIntersects;
import com.arcadedb.function.sql.geo.SQLFunctionGeoLineString;
import com.arcadedb.function.sql.geo.SQLFunctionGeoOverlaps;
import com.arcadedb.function.sql.geo.SQLFunctionGeoPoint;
import com.arcadedb.function.sql.geo.SQLFunctionGeoPolygon;
import com.arcadedb.function.sql.geo.SQLFunctionGeoTouches;
import com.arcadedb.function.sql.geo.SQLFunctionGeoWithin;
import com.arcadedb.function.sql.geo.SQLFunctionGeoX;
import com.arcadedb.function.sql.geo.SQLFunctionGeoY;
```

**Step 2: Replace all 21 `register()` calls in the constructor**

Find the "Geo" section (lines ~169–192) and replace entirely with:

```java
    // Geo — geo.* standard functions
    register(SQLFunctionGeoGeomFromText.NAME, new SQLFunctionGeoGeomFromText());
    register(SQLFunctionGeoPoint.NAME, new SQLFunctionGeoPoint());
    register(SQLFunctionGeoLineString.NAME, new SQLFunctionGeoLineString());
    register(SQLFunctionGeoPolygon.NAME, new SQLFunctionGeoPolygon());
    register(SQLFunctionGeoBuffer.NAME, new SQLFunctionGeoBuffer());
    register(SQLFunctionGeoEnvelope.NAME, new SQLFunctionGeoEnvelope());
    register(SQLFunctionGeoDistance.NAME, new SQLFunctionGeoDistance());
    register(SQLFunctionGeoArea.NAME, new SQLFunctionGeoArea());
    register(SQLFunctionGeoAsText.NAME, new SQLFunctionGeoAsText());
    register(SQLFunctionGeoAsGeoJson.NAME, new SQLFunctionGeoAsGeoJson());
    register(SQLFunctionGeoX.NAME, new SQLFunctionGeoX());
    register(SQLFunctionGeoY.NAME, new SQLFunctionGeoY());

    // Geo — geo.* spatial predicate functions (IndexableSQLFunction)
    register(SQLFunctionGeoWithin.NAME, new SQLFunctionGeoWithin());
    register(SQLFunctionGeoIntersects.NAME, new SQLFunctionGeoIntersects());
    register(SQLFunctionGeoContains.NAME, new SQLFunctionGeoContains());
    register(SQLFunctionGeoDWithin.NAME, new SQLFunctionGeoDWithin());
    register(SQLFunctionGeoDisjoint.NAME, new SQLFunctionGeoDisjoint());
    register(SQLFunctionGeoEquals.NAME, new SQLFunctionGeoEquals());
    register(SQLFunctionGeoCrosses.NAME, new SQLFunctionGeoCrosses());
    register(SQLFunctionGeoOverlaps.NAME, new SQLFunctionGeoOverlaps());
    register(SQLFunctionGeoTouches.NAME, new SQLFunctionGeoTouches());
```

**Step 3: Compile**

```bash
mvn compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java
git commit -m "refactor(geo): update DefaultSQLFunctionFactory for geo.* function names"
```

---

### Task 5: Update `CypherFunctionFactory.java`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherFunctionFactory.java`

**Step 1: Update the import**

Find:
```java
import com.arcadedb.function.sql.geo.SQLFunctionST_Distance;
```
Replace with:
```java
import com.arcadedb.function.sql.geo.SQLFunctionGeoDistance;
```

**Step 2: Update the bridge reference**

Find (around line 408):
```java
case "distance" -> new SQLFunctionBridge(sqlFunctionFactory.getFunctionInstance(SQLFunctionST_Distance.NAME), "distance");
```
Replace with:
```java
case "distance" -> new SQLFunctionBridge(sqlFunctionFactory.getFunctionInstance(SQLFunctionGeoDistance.NAME), "distance");
```

**Step 3: Compile**

```bash
mvn compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherFunctionFactory.java
git commit -m "refactor(geo): update CypherFunctionFactory bridge to geo.distance"
```

---

### Task 6: Update test SQL strings in both test files

**Files:**
- Modify: `engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoFunctionsTest.java`
- Modify: `engine/src/test/java/com/arcadedb/index/geospatial/SQLGeoIndexedQueryTest.java`

**Step 1: Update `SQLGeoFunctionsTest.java`**

This file has ~72 occurrences. Use a bulk find-and-replace for each SQL function name.
The rename pairs (old → new) to apply in the SQL strings:

```
"ST_GeomFromText(" → "geo.geomFromText("
"ST_Point("        → "geo.point("
"ST_LineString("   → "geo.lineString("
"ST_Polygon("      → "geo.polygon("
"ST_Buffer("       → "geo.buffer("
"ST_Envelope("     → "geo.envelope("
"ST_Distance("     → "geo.distance("
"ST_Area("         → "geo.area("
"ST_AsText("       → "geo.asText("
"ST_AsGeoJson("    → "geo.asGeoJson("
"ST_X("            → "geo.x("
"ST_Y("            → "geo.y("
"ST_Within("       → "geo.within("
"ST_Intersects("   → "geo.intersects("
"ST_Contains("     → "geo.contains("
"ST_DWithin("      → "geo.dWithin("
"ST_Disjoint("     → "geo.disjoint("
"ST_Equals("       → "geo.equals("
"ST_Crosses("      → "geo.crosses("
"ST_Overlaps("     → "geo.overlaps("
"ST_Touches("      → "geo.touches("
```

> Important: only replace inside SQL string literals (inside `"..."` passed to `db.query` / `db.command`). Do NOT rename Java class references or test method names — those classes were already renamed in Tasks 1–3.

**Step 2: Update `SQLGeoIndexedQueryTest.java`**

Same substitution list (~26 occurrences, same SQL-string-only rule).

**Step 3: Run the test suite**

```bash
mvn test -pl engine -Dtest="SQLGeoFunctionsTest,SQLGeoIndexedQueryTest,LSMTreeGeoIndexTest,LSMTreeGeoIndexSchemaTest,GeoIndexMetadataTest" 2>&1 | tail -30
```
Expected: all tests GREEN

**Step 4: Compile full engine**

```bash
mvn clean install -DskipTests -pl engine -q
```
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoFunctionsTest.java
git add engine/src/test/java/com/arcadedb/index/geospatial/SQLGeoIndexedQueryTest.java
git commit -m "test(geo): update SQL strings from ST_* to geo.* naming"
```

---

### Task 7: Update docs and final verification

**Files:**
- Modify: `docs/plans/2026-02-22-geospatial-design.md`
- Modify: `docs/plans/2026-02-22-geospatial-implementation.md`

**Step 1: Update doc references**

In both doc files, apply the same SQL function rename pairs from Task 6 (just text substitution in docs — no code).

**Step 2: Run full geo test suite one more time**

```bash
mvn test -pl engine -Dtest="SQLGeoFunctionsTest,SQLGeoIndexedQueryTest,LSMTreeGeoIndexTest,LSMTreeGeoIndexSchemaTest,GeoIndexMetadataTest" 2>&1 | tail -20
```
Expected: all tests GREEN, no `ST_` references in any error output

**Step 3: Verify no leftover ST_ references in production code**

```bash
grep -r "ST_" engine/src/main/java/com/arcadedb/function/sql/geo/ \
           engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java \
           engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherFunctionFactory.java
```
Expected: no output

**Step 4: Commit**

```bash
git add docs/plans/2026-02-22-geospatial-design.md docs/plans/2026-02-22-geospatial-implementation.md
git commit -m "docs: update geo function references from ST_* to geo.* naming"
```
