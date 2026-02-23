# Geospatial Indexing Implementation Plan

**Goal:** Port OrientDB-style geospatial indexing to ArcadeDB with `geo.*` SQL functions and automatic query optimizer integration.

**Architecture:** `LSMTreeGeoIndex` wraps `LSMTreeIndex` (same pattern as `LSMTreeFullTextIndex`). `lucene-spatial-extras` `GeohashPrefixTree` decomposes WKT geometries into GeoHash cell tokens stored in LSM-Tree. `geo.*` predicate functions implement `IndexableSQLFunction` so the query optimizer uses the geo index automatically when `WHERE geo.within(field, shape) = true` is detected.

**Tech Stack:** Java 21, `lucene-spatial-extras` 10.3.2, `spatial4j` 0.8, `jts-core` 1.20.0, JUnit 5 + AssertJ, Maven.

**Design document:** `docs/plans/2026-02-22-geospatial-design.md` — read it first.

**Reference implementations to study before starting:**
- `engine/src/main/java/com/arcadedb/index/fulltext/LSMTreeFullTextIndex.java` — the index wrapper pattern to mirror exactly
- `engine/src/main/java/com/arcadedb/schema/FullTextIndexMetadata.java` — the metadata pattern to mirror
- `engine/src/main/java/com/arcadedb/query/sql/executor/IndexableSQLFunction.java` — the interface to implement
- `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionDistance.java` — existing geo function style
- `engine/src/test/java/com/arcadedb/index/fulltext/LSMTreeFullTextIndexTest.java` — test style to follow

---

## Task 1: Add lucene-spatial-extras Dependency

**Files:**
- Modify: `engine/pom.xml`

**Step 1: Add the dependency**

In `engine/pom.xml`, find the `lucene-analysis-common` dependency block and add `lucene-spatial-extras` immediately after it:

```xml
<dependency>
    <groupId>org.apache.lucene</groupId>
    <artifactId>lucene-spatial-extras</artifactId>
    <version>${lucene.version}</version>
</dependency>
```

The `lucene.version` property is already defined as `10.3.2` in the parent pom.

**Step 2: Verify compilation**

```bash
cd engine && mvn compile -q
```

Expected: `BUILD SUCCESS` with no errors.

**Step 3: Commit**

```bash
git add engine/pom.xml
git commit -m "feat(geo): add lucene-spatial-extras dependency for geospatial indexing"
```

---

## Task 2: Create GeoIndexMetadata

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/GeoIndexMetadata.java`

Pattern: mirror `FullTextIndexMetadata.java` exactly, but storing `precision` (int) instead of analyzer config.

**Step 1: Write the failing test**

Create `engine/src/test/java/com/arcadedb/index/geospatial/GeoIndexMetadataTest.java`:

```java
package com.arcadedb.index.geospatial;

import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeoIndexMetadataTest {

  @Test
  void defaultPrecision() {
    final GeoIndexMetadata meta = new GeoIndexMetadata("Location", new String[]{"coords"}, 0);
    assertThat(meta.getPrecision()).isEqualTo(GeoIndexMetadata.DEFAULT_PRECISION);
  }

  @Test
  void customPrecisionRoundtrip() {
    final GeoIndexMetadata meta = new GeoIndexMetadata("Location", new String[]{"coords"}, 0);
    meta.setPrecision(7);
    final JSONObject json = new JSONObject();
    meta.toJSON(json);
    assertThat(json.getInt("precision", -1)).isEqualTo(7);

    final GeoIndexMetadata loaded = new GeoIndexMetadata("Location", new String[]{"coords"}, 0);
    loaded.fromJSON(json);
    assertThat(loaded.getPrecision()).isEqualTo(7);
  }
}
```

**Step 2: Run test to verify it fails**

```bash
cd engine && mvn test -Dtest=GeoIndexMetadataTest -q 2>&1 | tail -5
```

Expected: FAIL — `GeoIndexMetadata` does not exist yet.

**Step 3: Create GeoIndexMetadata**

```java
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;

public class GeoIndexMetadata extends IndexMetadata {

  public static final int DEFAULT_PRECISION = 11; // ~2.4m cells

  private int precision = DEFAULT_PRECISION;

  public GeoIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  @Override
  public void fromJSON(final JSONObject metadata) {
    if (metadata.has("typeName"))
      super.fromJSON(metadata);
    this.precision = metadata.getInt("precision", DEFAULT_PRECISION);
  }

  public void toJSON(final JSONObject json) {
    json.put("precision", precision);
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(final int precision) {
    this.precision = precision;
  }
}
```

**Step 4: Run test to verify it passes**

```bash
cd engine && mvn test -Dtest=GeoIndexMetadataTest -q 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/schema/GeoIndexMetadata.java \
        engine/src/test/java/com/arcadedb/index/geospatial/GeoIndexMetadataTest.java
git commit -m "feat(geo): add GeoIndexMetadata for geospatial index configuration"
```

---

## Task 3: Create LSMTreeGeoIndex

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/geospatial/LSMTreeGeoIndex.java`
- Create: `engine/src/test/java/com/arcadedb/index/geospatial/LSMTreeGeoIndexTest.java`

This is the core component. Study `LSMTreeFullTextIndex.java` in full before writing this — `LSMTreeGeoIndex` mirrors its structure exactly. The difference: instead of Lucene `Analyzer` → tokens, we use `GeohashPrefixTree` + `RecursivePrefixTreeStrategy` → GeoHash cell tokens.

**Step 1: Write the failing test**

```java
package com.arcadedb.index.geospatial;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LSMTreeGeoIndexTest extends TestHelper {

  @Test
  void indexAndQueryPoint() {
    database.command("sql", "CREATE DOCUMENT TYPE Location");
    database.command("sql", "CREATE PROPERTY Location.coords STRING");
    database.command("sql", "CREATE INDEX ON Location (coords) GEOSPATIAL");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Location");
      doc.set("coords", "POINT (10.0 45.0)");
      doc.save();
    });

    // Direct index lookup via a WKT polygon covering the point
    final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Location[coords]");
    final LSMTreeGeoIndex geoIdx = (LSMTreeGeoIndex) idx.getIndexesOnBuckets()[0];

    // Parse the search shape and query directly
    final org.locationtech.spatial4j.shape.Shape searchShape =
        com.arcadedb.function.sql.geo.GeoUtils.getSpatialContext()
            .getShapeFactory().rect(5.0, 15.0, 40.0, 50.0);

    final IndexCursor cursor = geoIdx.get(new Object[]{ searchShape });
    assertThat(cursor.hasNext()).isTrue();
  }

  @Test
  void pointOutsideQueryReturnsNoResults() {
    database.command("sql", "CREATE DOCUMENT TYPE Location2");
    database.command("sql", "CREATE PROPERTY Location2.coords STRING");
    database.command("sql", "CREATE INDEX ON Location2 (coords) GEOSPATIAL");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Location2");
      doc.set("coords", "POINT (100.0 45.0)"); // Tokyo area, far from Europe
      doc.save();
    });

    final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Location2[coords]");
    final LSMTreeGeoIndex geoIdx = (LSMTreeGeoIndex) idx.getIndexesOnBuckets()[0];

    // Search shape is Europe — point is in Pacific
    final org.locationtech.spatial4j.shape.Shape searchShape =
        com.arcadedb.function.sql.geo.GeoUtils.getSpatialContext()
            .getShapeFactory().rect(5.0, 15.0, 40.0, 50.0);

    final IndexCursor cursor = geoIdx.get(new Object[]{ searchShape });
    assertThat(cursor.hasNext()).isFalse();
  }

  @Test
  void nullWktIsSkipped() {
    database.command("sql", "CREATE DOCUMENT TYPE Location3");
    database.command("sql", "CREATE PROPERTY Location3.coords STRING");
    database.command("sql", "CREATE INDEX ON Location3 (coords) GEOSPATIAL");

    // Should not throw — null geometry is silently skipped
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Location3");
      doc.set("coords", (Object) null);
      doc.save();
    });

    final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Location3[coords]");
    final LSMTreeGeoIndex geoIdx = (LSMTreeGeoIndex) idx.getIndexesOnBuckets()[0];

    final org.locationtech.spatial4j.shape.Shape searchShape =
        com.arcadedb.function.sql.geo.GeoUtils.getSpatialContext()
            .getShapeFactory().rect(-180, 180, -90, 90);

    final IndexCursor cursor = geoIdx.get(new Object[]{ searchShape });
    assertThat(cursor.hasNext()).isFalse();
  }
}
```

**Step 2: Run to verify it fails**

```bash
cd engine && mvn test -Dtest=LSMTreeGeoIndexTest -q 2>&1 | tail -5
```

Expected: FAIL — `GEOSPATIAL` index type not registered yet.

**Step 3: Create LSMTreeGeoIndex**

Key design notes:
- The underlying `LSMTreeIndex` stores `String` keys (same as full-text)
- `put()`: parse WKT → `Shape`, use `RecursivePrefixTreeStrategy.createIndexableFields()` to get Lucene fields, extract geohash token strings from the tokenized field's `TokenStream`, store each token as a key in the underlying index
- `get()`: the input key is a `Shape` object; use `strategy.makeQuery(SpatialArgs)` then visit the query with `QueryVisitor` to extract the covering cell token strings, look each up in the underlying index, union RIDs into a `TempIndexCursor`
- All other methods (remove, isEmpty, getAssociatedIndex, getPropertyNames, etc.) delegate to `underlyingIndex` — copy this pattern from `LSMTreeFullTextIndex`

```java
package com.arcadedb.index.geospatial;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.*;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

// All remaining Index/IndexInternal methods MUST be delegated to underlyingIndex.
// Copy them from LSMTreeFullTextIndex — do not omit any method.
public class LSMTreeGeoIndex implements Index, IndexInternal {

  public static final int DEFAULT_PRECISION = GeoIndexMetadata.DEFAULT_PRECISION;

  private final LSMTreeIndex               underlyingIndex;
  private final int                        precision;
  private final SpatialPrefixTree          grid;
  private final RecursivePrefixTreeStrategy strategy;
  private       TypeIndex                  typeIndex;

  public static class GeoIndexFactoryHandler implements IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      if (builder.isUnique())
        throw new IllegalArgumentException("Geospatial index cannot be unique");

      for (final Type keyType : builder.getKeyTypes()) {
        if (keyType != Type.STRING)
          throw new IllegalArgumentException(
              "Geospatial index can only be defined on STRING properties (WKT format), found: " + keyType);
      }

      int precision = DEFAULT_PRECISION;
      if (builder.getMetadata() instanceof GeoIndexMetadata geoMeta)
        precision = geoMeta.getPrecision();

      return new LSMTreeGeoIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getPageSize(), builder.getNullStrategy(), precision);
    }
  }

  /** Called at load time. */
  public LSMTreeGeoIndex(final LSMTreeIndex index) {
    this(index, DEFAULT_PRECISION);
  }

  public LSMTreeGeoIndex(final LSMTreeIndex index, final int precision) {
    this.underlyingIndex = index;
    this.precision = precision;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
  }

  /** Creation time. */
  public LSMTreeGeoIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final int precision) {
    this.precision = precision;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
    underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode, new Type[]{ Type.STRING }, pageSize, nullStrategy);
  }

  /** Loading time from file. */
  public LSMTreeGeoIndex(final DatabaseInternal database, final String name, final String filePath,
      final int fileId, final ComponentFile.MODE mode, final int pageSize, final int version) {
    this.precision = DEFAULT_PRECISION;
    this.grid = new GeohashPrefixTree(GeoUtils.getSpatialContext(), precision);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "geo");
    try {
      underlyingIndex = new LSMTreeIndex(database, name, false, filePath, fileId, mode, pageSize, version);
    } catch (final IOException e) {
      throw new IndexException("Cannot load geospatial index (error=" + e + ")", e);
    }
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return new EmptyIndexCursor();

    final Shape searchShape = toShape(keys[0]);
    if (searchShape == null)
      return new EmptyIndexCursor();

    // Generate covering GeoHash tokens for the search shape
    final SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, searchShape);
    final Query query = strategy.makeQuery(args);

    final Set<String> tokens = new LinkedHashSet<>();
    query.visit(new QueryVisitor() {
      @Override
      public void consumeTerms(final Query q, final Term... terms) {
        for (final Term t : terms)
          tokens.add(t.text());
      }

      @Override
      public QueryVisitor getSubVisitor(final BooleanClause.Occur occur, final Query parent) {
        return this;
      }
    });

    // Collect all matching RIDs from the LSM index
    final Map<RID, Integer> seen = new LinkedHashMap<>();
    for (final String token : tokens) {
      final IndexCursor cursor = underlyingIndex.get(new Object[]{ token });
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        seen.put(rid, 1);
      }
    }

    final List<IndexCursorEntry> entries = new ArrayList<>(seen.size());
    for (final RID rid : seen.keySet())
      entries.add(new IndexCursorEntry(keys, rid, 1));

    return new TempIndexCursor(entries);
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;

    final String wkt = keys[0].toString();
    final Shape shape;
    try {
      shape = GeoUtils.getSpatialContext().getFormats().getWktReader().read(wkt);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Geospatial index: skipping invalid WKT value '%s': %s", wkt, e.getMessage());
      return;
    }

    final Field[] fields = strategy.createIndexableFields(shape);
    for (final Field field : fields) {
      try {
        final TokenStream ts = field.tokenStream(null, null);
        if (ts == null)
          continue;
        final CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
          final String token = termAttr.toString();
          underlyingIndex.put(new Object[]{ token }, rids);
        }
        ts.end();
        ts.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Geospatial index: error extracting tokens for '%s': %s", wkt, e.getMessage());
      }
    }
  }

  @Override
  public void remove(final Object[] keys) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;
    final Shape shape = toShape(keys[0].toString());
    if (shape == null)
      return;
    for (final String token : extractTokens(shape))
      underlyingIndex.remove(new Object[]{ token });
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    if (keys == null || keys.length == 0 || keys[0] == null)
      return;
    final Shape shape = toShape(keys[0].toString());
    if (shape == null)
      return;
    for (final String token : extractTokens(shape))
      underlyingIndex.remove(new Object[]{ token }, rid);
  }

  // --- Delegate everything else to underlyingIndex ---
  // Copy all remaining Index/IndexInternal method implementations from
  // LSMTreeFullTextIndex — they all delegate to underlyingIndex.
  // These include: getType, getTypeName, getPropertyNames, getAssociatedIndex,
  // setAssociatedIndex, getUnderlyingIndex, isEmpty, countEntries, build,
  // setMetadata, getMetadata, getPageSize, getNullStrategy, isUnique,
  // getFileId, onAfterSchemaLoadIndex, getPaginatedComponent, dropIndex, etc.

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.GEOSPATIAL;
  }

  // ... all other delegating methods

  // --- Private helpers ---

  private Shape toShape(final Object obj) {
    if (obj instanceof Shape s)
      return s;
    try {
      return GeoUtils.getSpatialContext().getFormats().getWktReader().read(obj.toString());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Geospatial index: cannot parse shape '%s': %s", obj, e.getMessage());
      return null;
    }
  }

  private List<String> extractTokens(final Shape shape) {
    final List<String> tokens = new ArrayList<>();
    final Field[] fields = strategy.createIndexableFields(shape);
    for (final Field field : fields) {
      try {
        final TokenStream ts = field.tokenStream(null, null);
        if (ts == null)
          continue;
        final CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        while (ts.incrementToken())
          tokens.add(termAttr.toString());
        ts.end();
        ts.close();
      } catch (final IOException e) {
        // skip
      }
    }
    return tokens;
  }
}
```

> **Implementation note:** After writing the skeleton above, you MUST complete all the delegating methods. Open `LSMTreeFullTextIndex.java` and copy every method that delegates to `underlyingIndex`, adapting them to delegate to `this.underlyingIndex` in `LSMTreeGeoIndex`. There are ~25 methods. Do not omit any — the compiler will catch missing ones from the interfaces.

> **If `CharTermAttribute` doesn't extract tokens:** GeoHash tokens are ASCII. If `CharTermAttribute` produces empty strings, check if the field uses `BytesRefTermAttribute` instead. Add `BytesRefTermAttribute bAttr = ts.addAttribute(BytesRefTermAttribute.class)` and use `bAttr.getBytesRef().utf8ToString()` instead.

**Step 4: Run tests**

```bash
cd engine && mvn test -Dtest=LSMTreeGeoIndexTest -q 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`.

**Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/index/geospatial/ \
        engine/src/test/java/com/arcadedb/index/geospatial/LSMTreeGeoIndexTest.java
git commit -m "feat(geo): add LSMTreeGeoIndex wrapping LSMTreeIndex with GeohashPrefixTree decomposition"
```

---

## Task 4: Register GEOSPATIAL Index Type in Schema

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/schema/Schema.java` (enum)
- Modify: `engine/src/main/java/com/arcadedb/schema/LocalSchema.java` (factory registration)

**Step 1: Write the failing test**

Add to `LSMTreeGeoIndexTest`:

```java
@Test
void createGeoIndexViaSQL() {
  database.command("sql", "CREATE DOCUMENT TYPE Place");
  database.command("sql", "CREATE PROPERTY Place.location STRING");
  database.command("sql", "CREATE INDEX ON Place (location) GEOSPATIAL");

  final com.arcadedb.index.TypeIndex idx =
      (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("Place[location]");
  assertThat(idx).isNotNull();
  assertThat(idx.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
}
```

**Step 2: Run to verify it fails**

```bash
cd engine && mvn test -Dtest="LSMTreeGeoIndexTest#createGeoIndexViaSQL" -q 2>&1 | tail -5
```

Expected: FAIL — `GEOSPATIAL` not a valid index type.

**Step 3: Add GEOSPATIAL to the enum**

In `Schema.java`, find:

```java
enum INDEX_TYPE {
  LSM_TREE, FULL_TEXT, LSM_VECTOR
}
```

Change to:

```java
enum INDEX_TYPE {
  LSM_TREE, FULL_TEXT, LSM_VECTOR, GEOSPATIAL
}
```

**Step 4: Register the factory handler**

In `LocalSchema.java`, find:

```java
indexFactory.register(INDEX_TYPE.LSM_VECTOR.name(), new LSMVectorIndex.LSMVectorIndexFactoryHandler());
```

Add after it:

```java
indexFactory.register(INDEX_TYPE.GEOSPATIAL.name(), new LSMTreeGeoIndex.GeoIndexFactoryHandler());
```

Also handle loading from file: in `LocalSchema.java`, search for the block that handles `FULL_TEXT` when loading existing indexes from disk (around line 1380). Add an equivalent branch for `GEOSPATIAL`:

```java
} else if (configuredIndexType.equalsIgnoreCase(Schema.INDEX_TYPE.GEOSPATIAL.toString())) {
  index = new LSMTreeGeoIndex(database, indexName, indexFilePath, fileId, mode, pageSize, version);
}
```

**Step 5: Run tests**

```bash
cd engine && mvn test -Dtest=LSMTreeGeoIndexTest -q 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

**Step 6: Compile entire engine to catch any issues**

```bash
cd engine && mvn compile -q
```

Expected: `BUILD SUCCESS`.

**Step 7: Commit**

```bash
git add engine/src/main/java/com/arcadedb/schema/Schema.java \
        engine/src/main/java/com/arcadedb/schema/LocalSchema.java
git commit -m "feat(geo): register GEOSPATIAL index type in Schema and LocalSchema"
```

---

## Task 5: Create geo.* Constructor and Accessor Functions

**Files:**
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoGeomFromText.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoPoint.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoLineString.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoPolygon.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoBuffer.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoEnvelope.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoDistance.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoArea.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoAsText.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoAsGeoJson.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoX.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoY.java`
- Modify: `engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java`

**Step 1: Write the failing tests**

Update `engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoFunctionsTest.java`. The existing `point()`, `distance()` etc. tests will become regression tests that the OLD names are gone. Add new `geo.*` tests:

```java
@Test
void geoPoint() throws Exception {
  TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
    final ResultSet result = db.query("sql", "select geo.point(11, 11) as pt");
    assertThat(result.hasNext()).isTrue();
    final Object pt = result.next().getProperty("pt");
    assertThat(pt).isNotNull();
    // Should be a Spatial4j Point or WKT string
  });
}

@Test
void geoGeomFromText() throws Exception {
  TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
    final ResultSet result = db.query("sql", "select geo.geomFromText('POINT (10.0 45.0)') as geom");
    assertThat(result.hasNext()).isTrue();
    final Object geom = result.next().getProperty("geom");
    assertThat(geom).isNotNull();
  });
}

@Test
void geoAsText() throws Exception {
  TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
    final ResultSet result = db.query("sql", "select geo.asText(geo.point(10.0, 45.0)) as wkt");
    assertThat(result.hasNext()).isTrue();
    final String wkt = result.next().getProperty("wkt");
    assertThat(wkt).contains("10").contains("45");
  });
}

@Test
void geoXgeoY() throws Exception {
  TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
    final ResultSet result = db.query("sql", "select geo.x(geo.point(10.0, 45.0)) as x, geo.y(geo.point(10.0, 45.0)) as y");
    assertThat(result.hasNext()).isTrue();
    final com.arcadedb.query.sql.executor.Result row = result.next();
    assertThat(((Number) row.getProperty("x")).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) row.getProperty("y")).doubleValue()).isEqualTo(45.0);
  });
}

@Test
void geoDistance() throws Exception {
  TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
    final ResultSet result = db.query("sql",
        "select geo.distance(geo.point(0.0, 0.0), geo.point(1.0, 0.0), 'km') as dist");
    assertThat(result.hasNext()).isTrue();
    final Number dist = result.next().getProperty("dist");
    assertThat(dist.doubleValue()).isGreaterThan(100.0).isLessThan(120.0); // ~111km per degree
  });
}

@Test
void oldFunctionNamesGone() throws Exception {
  TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
    assertThatThrownBy(() -> db.query("sql", "select point(11,11)").close())
        .isInstanceOf(Exception.class); // unknown function
  });
}
```

**Step 2: Run to verify tests fail**

```bash
cd engine && mvn test -Dtest=SQLGeoFunctionsTest -q 2>&1 | tail -10
```

Expected: FAIL — `geo.*` functions not registered.

**Step 3: Create the function classes**

Each function follows the exact same pattern as existing geo functions. Study `SQLFunctionPoint.java` and `SQLFunctionDistance.java` before writing. Key patterns:

- Extend `SQLFunctionAbstract`
- Constructor: `super("geo.functionName")`
- `execute()` validates params, calls `GeoUtils.getSpatialContext()` for shape creation
- `getSyntax()` returns a docs string
- `getMinArgs()` / `getMaxArgs()` for validation

`SQLFunctionGeoGeomFromText.java`:
```java
public class SQLFunctionGeoGeomFromText extends SQLFunctionAbstract {
  public static final String NAME = "geo.geomFromText";

  public SQLFunctionGeoGeomFromText() { super(NAME); }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord,
      final Object currentResult, final Object[] params, final CommandContext ctx) {
    if (params == null || params.length < 1 || params[0] == null)
      return null;
    try {
      return GeoUtils.getSpatialContext().getFormats().getWktReader().read(params[0].toString());
    } catch (final Exception e) {
      throw new IllegalArgumentException("geo.geomFromText: invalid WKT: " + params[0], e);
    }
  }

  @Override public String getSyntax() { return "geo.geomFromText(<wkt>)"; }
  @Override public int getMinArgs() { return 1; }
  @Override public int getMaxArgs() { return 1; }
}
```

`SQLFunctionGeoAsText.java`:
```java
public class SQLFunctionGeoAsText extends SQLFunctionAbstract {
  public static final String NAME = "geo.asText";

  public SQLFunctionGeoAsText() { super(NAME); }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord,
      final Object currentResult, final Object[] params, final CommandContext ctx) {
    if (params == null || params.length < 1 || params[0] == null)
      return null;
    final Shape shape = (params[0] instanceof Shape s) ? s
        : GeoUtils.getSpatialContext().getShapeFactory()
            .makePoint(0, 0); // will be overridden by parse below
    // If it's already a string, return as-is; if Shape, convert
    if (params[0] instanceof Shape s)
      return GeoUtils.getSpatialContext().getFormats().getWktWriter().toString(s);
    return params[0].toString();
  }

  @Override public String getSyntax() { return "geo.asText(<geometry>)"; }
  @Override public int getMinArgs() { return 1; }
  @Override public int getMaxArgs() { return 1; }
}
```

`SQLFunctionGeoX.java`:
```java
public class SQLFunctionGeoX extends SQLFunctionAbstract {
  public static final String NAME = "geo.x";

  public SQLFunctionGeoX() { super(NAME); }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord,
      final Object currentResult, final Object[] params, final CommandContext ctx) {
    if (params == null || params.length < 1 || params[0] == null)
      return null;
    if (params[0] instanceof org.locationtech.spatial4j.shape.Point p)
      return p.getX();
    throw new IllegalArgumentException("geo.x: argument must be a Point");
  }

  @Override public String getSyntax() { return "geo.x(<point>)"; }
  @Override public int getMinArgs() { return 1; }
  @Override public int getMaxArgs() { return 1; }
}
```

`SQLFunctionGeoY.java` — same as `SQLFunctionGeoX` but returns `p.getY()`.

`SQLFunctionGeoPoint.java` — same logic as existing `SQLFunctionPoint.java` but named `geo.point`.

`SQLFunctionGeoDistance.java` — same logic as existing `SQLFunctionDistance.java` but named `geo.distance`.

`SQLFunctionGeoLineString.java` — same as existing `SQLFunctionLineString.java` but named `geo.lineString`.

`SQLFunctionGeoPolygon.java` — same as existing `SQLFunctionPolygon.java` but named `geo.polygon`.

`SQLFunctionGeoBuffer.java` — same as `SQLFunctionCircle.java` (circle = point + buffer radius) but named `geo.buffer`.

`SQLFunctionGeoEnvelope.java` — same as `SQLFunctionRectangle.java` but named `geo.envelope`.

`SQLFunctionGeoArea.java`:
```java
public class SQLFunctionGeoArea extends SQLFunctionAbstract {
  public static final String NAME = "geo.area";

  public SQLFunctionGeoArea() { super(NAME); }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord,
      final Object currentResult, final Object[] params, final CommandContext ctx) {
    if (params == null || params.length < 1 || params[0] == null)
      return null;
    final Shape shape = (params[0] instanceof Shape s) ? s
        : GeoUtils.getSpatialContext().getShapeFactory().makePoint(0, 0); // placeholder
    if (params[0] instanceof Shape s)
      return s.getArea(GeoUtils.getSpatialContext());
    throw new IllegalArgumentException("geo.area: argument must be a Shape");
  }

  @Override public String getSyntax() { return "geo.area(<geometry>)"; }
  @Override public int getMinArgs() { return 1; }
  @Override public int getMaxArgs() { return 1; }
}
```

`SQLFunctionGeoAsGeoJson.java` — use JTS `GeoJsonWriter` (from `org.locationtech.jts.io.geojson`):
```java
// Convert Spatial4j Shape → JTS Geometry → GeoJSON string
// GeoUtils.SPATIAL_CONTEXT has getGeometryFrom(Shape) if using JtsSpatialContext
final org.locationtech.jts.geom.Geometry jtsGeom =
    GeoUtils.SPATIAL_CONTEXT.getGeometryFrom(shape);
return new org.locationtech.jts.io.geojson.GeoJsonWriter().write(jtsGeom);
```

**Step 4: Update DefaultSQLFunctionFactory**

In `DefaultSQLFunctionFactory.java`:

1. Find and **remove** the old registrations:
   ```java
   register(SQLFunctionCircle.NAME, new SQLFunctionCircle());
   register(SQLFunctionDistance.NAME, new SQLFunctionDistance());
   register(SQLFunctionLineString.NAME, new SQLFunctionLineString());
   register(SQLFunctionPoint.NAME, new SQLFunctionPoint());
   register(SQLFunctionPolygon.NAME, new SQLFunctionPolygon());
   register(SQLFunctionRectangle.NAME, new SQLFunctionRectangle());
   ```

2. **Add** the new `geo.*` registrations in their place:
   ```java
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
   ```

**Step 5: Compile to check all references to old classes**

```bash
cd engine && mvn compile -q 2>&1 | grep -i error | head -20
```

Fix any compilation errors (likely import cleanup in `DefaultSQLFunctionFactory`).

**Step 6: Run tests**

```bash
cd engine && mvn test -Dtest=SQLGeoFunctionsTest -q 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`.

**Step 7: Commit**

```bash
git add engine/src/main/java/com/arcadedb/function/sql/geo/ \
        engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java \
        engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoFunctionsTest.java
git commit -m "feat(geo): add geo.* constructor and accessor functions, remove old geo function names"
```

---

## Task 6: Create Spatial Predicate Functions with IndexableSQLFunction

**Files:**
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoPredicate.java` (abstract base)
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoWithin.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoIntersects.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoContains.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoDWithin.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoDisjoint.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoEquals.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoCrosses.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoOverlaps.java`
- Create: `engine/src/main/java/com/arcadedb/function/sql/geo/SQLFunctionGeoTouches.java`
- Modify: `engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java`
- Create: `engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoIndexedQueryTest.java`

**Step 1: Write the failing tests (both non-indexed and indexed)**

```java
package com.arcadedb.function.sql.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLGeoIndexedQueryTest extends TestHelper {

  // ---- Non-indexed (full-scan) predicate evaluation ----

  @Test
  void geoWithinNoIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Place");
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Place");
      d.set("coords", "POINT (10.0 45.0)");
      d.save();
    });
    // Point (10,45) is inside POLYGON 5-15, 40-50
    final ResultSet rs = database.query("sql",
        "SELECT FROM Place WHERE geo.within(geo.geomFromText(coords), " +
        "geo.geomFromText('POLYGON ((5 40, 15 40, 15 50, 5 50, 5 40))')) = true");
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  @Test
  void geoWithinOutsideNoIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Place2");
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Place2");
      d.set("coords", "POINT (100.0 45.0)"); // Pacific, not in Europe box
      d.save();
    });
    final ResultSet rs = database.query("sql",
        "SELECT FROM Place2 WHERE geo.within(geo.geomFromText(coords), " +
        "geo.geomFromText('POLYGON ((5 40, 15 40, 15 50, 5 50, 5 40))')) = true");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void geoIntersectsNoIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Place3");
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Place3");
      d.set("coords", "POINT (10.0 45.0)");
      d.save();
    });
    final ResultSet rs = database.query("sql",
        "SELECT FROM Place3 WHERE geo.intersects(geo.geomFromText(coords), " +
        "geo.geomFromText('POLYGON ((5 40, 15 40, 15 50, 5 50, 5 40))')) = true");
    assertThat(rs.hasNext()).isTrue();
    rs.close();
  }

  // ---- Indexed predicate evaluation ----

  @Test
  void geoWithinWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE IndexedPlace");
    database.command("sql", "CREATE PROPERTY IndexedPlace.coords STRING");
    database.command("sql", "CREATE INDEX ON IndexedPlace (coords) GEOSPATIAL");

    database.transaction(() -> {
      // Inside Europe box
      database.newDocument("IndexedPlace").set("coords", "POINT (10.0 45.0)").save();
      // Outside Europe box
      database.newDocument("IndexedPlace").set("coords", "POINT (100.0 45.0)").save();
    });

    final ResultSet rs = database.query("sql",
        "SELECT FROM IndexedPlace WHERE geo.within(coords, " +
        "geo.geomFromText('POLYGON ((5 40, 15 40, 15 50, 5 50, 5 40))')) = true");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    rs.close();
    assertThat(count).isEqualTo(1);
  }

  @Test
  void geoIntersectsWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE IndexedPlace2");
    database.command("sql", "CREATE PROPERTY IndexedPlace2.coords STRING");
    database.command("sql", "CREATE INDEX ON IndexedPlace2 (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.newDocument("IndexedPlace2").set("coords", "POINT (10.0 45.0)").save();
      database.newDocument("IndexedPlace2").set("coords", "POINT (100.0 45.0)").save();
    });

    final ResultSet rs = database.query("sql",
        "SELECT FROM IndexedPlace2 WHERE geo.intersects(coords, " +
        "geo.geomFromText('POLYGON ((5 40, 15 40, 15 50, 5 50, 5 40))')) = true");

    int count = 0;
    while (rs.hasNext()) { rs.next(); count++; }
    rs.close();
    assertThat(count).isEqualTo(1);
  }

  @Test
  void geoContainsWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Region");
    database.command("sql", "CREATE PROPERTY Region.bounds STRING");
    database.command("sql", "CREATE INDEX ON Region (bounds) GEOSPATIAL");

    database.transaction(() -> {
      // A large polygon that contains the query point
      database.newDocument("Region")
          .set("bounds", "POLYGON ((0 40, 20 40, 20 50, 0 50, 0 40))").save();
      // A small polygon that does not contain the query point
      database.newDocument("Region")
          .set("bounds", "POLYGON ((50 60, 70 60, 70 70, 50 70, 50 60))").save();
    });

    final ResultSet rs = database.query("sql",
        "SELECT FROM Region WHERE geo.contains(bounds, " +
        "geo.geomFromText('POINT (10.0 45.0)')) = true");

    int count = 0;
    while (rs.hasNext()) { rs.next(); count++; }
    rs.close();
    assertThat(count).isEqualTo(1);
  }

  @Test
  void geoNullReturnsNull() {
    final ResultSet rs = database.query("sql",
        "SELECT geo.within(null, geo.geomFromText('POINT (0 0)')) as result");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.getProperty("result")).isNull();
    rs.close();
  }
}
```

**Step 2: Run to verify it fails**

```bash
cd engine && mvn test -Dtest=SQLGeoIndexedQueryTest -q 2>&1 | tail -10
```

Expected: FAIL — `geo.within` etc. not registered.

**Step 3: Create the abstract base class**

```java
package com.arcadedb.function.sql.geo;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import java.util.ArrayList;
import java.util.List;

public abstract class SQLFunctionGeoPredicate extends SQLFunctionAbstract
    implements IndexableSQLFunction {

  protected SQLFunctionGeoPredicate(final String name) {
    super(name);
  }

  /** The Spatial4j SpatialRelation this predicate checks. */
  protected abstract SpatialRelation getExpectedRelation();

  /**
   * Non-indexed execution: evaluates the predicate directly using Spatial4j.
   * params[0] = geometry of the field (Shape or WKT string)
   * params[1] = search shape (Shape or WKT string)
   */
  @Override
  public Object execute(final Object self, final Identifiable currentRecord,
      final Object currentResult, final Object[] params, final CommandContext ctx) {
    if (params == null || params.length < 2)
      throw new IllegalArgumentException(getName() + "() requires 2 arguments");
    if (params[0] == null || params[1] == null)
      return null;

    final Shape g1 = toShape(params[0]);
    final Shape g2 = toShape(params[1]);
    if (g1 == null || g2 == null)
      return null;

    final SpatialRelation relation = g1.relate(g2);
    return relation == getExpectedRelation() || relation == SpatialRelation.CONTAINS
        && getExpectedRelation() == SpatialRelation.WITHIN
        ? checkRelation(g1, g2)
        : checkRelation(g1, g2);
  }

  /** Override for predicates needing JTS topology (Crosses, Touches, Overlaps). */
  protected Boolean checkRelation(final Shape g1, final Shape g2) {
    return g1.relate(g2) == getExpectedRelation();
  }

  // --- IndexableSQLFunction ---

  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator,
      final Object right, final CommandContext ctx, final Expression[] params) {
    if (params == null || params.length < 2)
      return false;
    // First arg must reference a field name (bare identifier)
    final String fieldName = extractFieldName(params[0]);
    if (fieldName == null)
      return false;
    // That field must have a GEOSPATIAL index on the target type
    return findGeoIndex(target, fieldName, ctx) != null;
  }

  @Override
  public boolean canExecuteInline(final FromClause target, final BinaryCompareOperator operator,
      final Object right, final CommandContext ctx, final Expression[] params) {
    return true; // always falls back to full-scan evaluation
  }

  @Override
  public boolean shouldExecuteAfterSearch(final FromClause target, final BinaryCompareOperator operator,
      final Object right, final CommandContext ctx, final Expression[] params) {
    return true; // index returns superset; exact predicate must post-filter
  }

  @Override
  public long estimate(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext ctx, final Expression[] params) {
    return -1; // no estimation
  }

  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext ctx, final Expression[] params) {
    if (params == null || params.length < 2)
      return List.of();

    final String fieldName = extractFieldName(params[0]);
    if (fieldName == null)
      return List.of();

    // Evaluate the search shape argument
    final Object shapeArg = params[1].execute((com.arcadedb.query.sql.executor.Result) null, ctx);
    final Shape searchShape = toShape(shapeArg);
    if (searchShape == null)
      return List.of();

    final TypeIndex typeIdx = findGeoIndex(target, fieldName, ctx);
    if (typeIdx == null)
      return List.of();

    final List<Record> results = new ArrayList<>();
    for (final Index bucketIdx : typeIdx.getIndexesOnBuckets()) {
      if (bucketIdx instanceof LSMTreeGeoIndex geoIdx) {
        final IndexCursor cursor = geoIdx.get(new Object[]{ searchShape });
        while (cursor.hasNext()) {
          final com.arcadedb.database.RID rid = cursor.next().getIdentity();
          final Record record = ctx.getDatabase().lookupByRID(rid, true);
          if (record != null)
            results.add(record);
        }
      }
    }
    return results;
  }

  // --- Helpers ---

  protected Shape toShape(final Object obj) {
    if (obj == null)
      return null;
    if (obj instanceof Shape s)
      return s;
    try {
      return GeoUtils.getSpatialContext().getFormats().getWktReader().read(obj.toString());
    } catch (final Exception e) {
      return null;
    }
  }

  private String extractFieldName(final Expression expr) {
    if (expr == null)
      return null;
    final String text = expr.toString().trim();
    // A bare field name has no spaces or function call syntax
    if (!text.contains("(") && !text.contains(" "))
      return text;
    return null;
  }

  private TypeIndex findGeoIndex(final FromClause target, final String fieldName,
      final CommandContext ctx) {
    if (target == null || target.getItem() == null)
      return null;
    final String typeName = target.getItem().toString();
    final Database db = ctx.getDatabase();
    if (!db.getSchema().existsType(typeName))
      return null;
    final DocumentType docType = db.getSchema().getType(typeName);
    for (final com.arcadedb.index.TypeIndex idx : docType.getAllIndexes(true)) {
      if (idx.getType() == Schema.INDEX_TYPE.GEOSPATIAL
          && idx.getPropertyNames().contains(fieldName))
        return idx;
    }
    return null;
  }
}
```

**Step 4: Create the 9 predicate subclasses**

Each is ~20 lines. Example for `geo.within`:

```java
package com.arcadedb.function.sql.geo;

import org.locationtech.spatial4j.shape.SpatialRelation;

public class SQLFunctionGeoWithin extends SQLFunctionGeoPredicate {
  public static final String NAME = "geo.within";

  public SQLFunctionGeoWithin() { super(NAME); }

  @Override
  protected SpatialRelation getExpectedRelation() { return SpatialRelation.WITHIN; }

  @Override
  public String getSyntax() { return "geo.within(<geometry>, <shape>)"; }

  @Override
  public int getMinArgs() { return 2; }

  @Override
  public int getMaxArgs() { return 2; }
}
```

Spatial4j `SpatialRelation` values:
- `geo.within` → `SpatialRelation.WITHIN`
- `geo.intersects` → `SpatialRelation.INTERSECTS`
- `geo.contains` → `SpatialRelation.CONTAINS`
- `geo.disjoint` → `SpatialRelation.DISJOINT`
- `geo.equals` → override `checkRelation` to use JTS `equals()`

For `geo.crosses`, `geo.overlaps`, `geo.touches` — Spatial4j doesn't have these as `SpatialRelation` values. Override `checkRelation` to use JTS topology:

```java
// geo.crosses example — needs JTS conversion
@Override
protected Boolean checkRelation(final Shape g1, final Shape g2) {
  final org.locationtech.jts.geom.Geometry jg1 = GeoUtils.SPATIAL_CONTEXT.getGeometryFrom(g1);
  final org.locationtech.jts.geom.Geometry jg2 = GeoUtils.SPATIAL_CONTEXT.getGeometryFrom(g2);
  return jg1.crosses(jg2);
}
```

`geo.dWithin` has a different signature `(g1, g2, distance)`, so override `execute()` directly:

```java
// geo.dWithin: returns true if g1 is within 'distance' of g2
// Use Spatial4j's distance calculation
@Override
public Object execute(..., Object[] params, ...) {
  if (params.length < 3) throw new IllegalArgumentException("geo.dWithin requires 3 args");
  if (params[0] == null || params[1] == null || params[2] == null) return null;
  final Shape g1 = toShape(params[0]);
  final Shape g2 = toShape(params[1]);
  final double distDeg = ((Number) params[2]).doubleValue(); // distance in degrees
  return GeoUtils.getSpatialContext().calcDistance(
      g1.getCenter(), g2.getCenter()) <= distDeg;
}
```

**Step 5: Register predicates in DefaultSQLFunctionFactory**

Add after the `geo.asGeoJson` registration:

```java
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

**Step 6: Compile**

```bash
cd engine && mvn compile -q
```

Fix any errors before running tests.

**Step 7: Run all geo tests**

```bash
cd engine && mvn test -Dtest="SQLGeoIndexedQueryTest,SQLGeoFunctionsTest,LSMTreeGeoIndexTest" -q 2>&1 | tail -15
```

Expected: `BUILD SUCCESS`. If any test fails, check the Spatial4j `SpatialRelation` mapping — `relate()` can return `WITHIN`, `CONTAINS`, `INTERSECTS`, `DISJOINT`. Adjust `checkRelation()` accordingly.

**Step 8: Run all engine tests to catch regressions**

```bash
cd engine && mvn test -q 2>&1 | tail -20
```

Fix any failures before committing.

**Step 9: Commit**

```bash
git add engine/src/main/java/com/arcadedb/function/sql/geo/ \
        engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java \
        engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoIndexedQueryTest.java
git commit -m "feat(geo): add geo.* spatial predicate functions with IndexableSQLFunction for automatic index usage"
```

---

## Task 7: Final Validation and Cleanup

**Step 1: Run all engine tests**

```bash
cd engine && mvn test -q 2>&1 | tail -30
```

Expected: `BUILD SUCCESS`.

**Step 2: Remove any debug System.out calls**

```bash
grep -r "System.out" engine/src/main/java/com/arcadedb/index/geospatial/ \
                     engine/src/main/java/com/arcadedb/function/sql/geo/
```

Expected: no output. Remove any found.

**Step 3: Compile the full project**

```bash
mvn compile -q
```

Expected: `BUILD SUCCESS`.

**Step 4: Final commit**

```bash
git add -A
git commit -m "feat(geo): complete geospatial indexing implementation with geo.* functions and LSMTreeGeoIndex"
```

---

## Known Gotchas

**Token extraction:** `RecursivePrefixTreeStrategy.createIndexableFields()` returns a `Field[]` where spatial fields have an embedded `TokenStream`. Call `field.tokenStream(null, null)` — passing `null` for the analyzer is valid when the field owns its token stream. If `CharTermAttribute` returns empty strings, also try `BytesRefTermAttribute` and call `.getBytesRef().utf8ToString()`.

**Query term extraction:** `strategy.makeQuery(SpatialArgs)` returns a `BooleanQuery` or `ConstantScoreQuery`. Use `query.visit(QueryVisitor)` with a recursive `getSubVisitor()` to collect all `Term` objects from nested queries.

**SpatialRelation mapping:** Spatial4j's `Shape.relate()` returns `WITHIN`, `CONTAINS`, `INTERSECTS`, or `DISJOINT`. There is no `CROSSES`, `OVERLAPS`, or `TOUCHES` — use JTS geometry operations for these via `GeoUtils.SPATIAL_CONTEXT.getGeometryFrom(shape)`.

**WKT format:** Spatial4j's WKT reader accepts `POINT (x y)` with a space before the parenthesis. JTS requires `POINT(x y)` without space. The `GeoUtils.getSpatialContext().getFormats().getWktReader()` handles both.

**geo.dWithin distance units:** The base implementation uses degrees. For user-facing meter/km input, add a conversion using `DistanceUtils.dist2Degrees(distKm, DistanceUtils.EARTH_MEAN_RADIUS_KM)` from Spatial4j.

**Index loading:** After adding `GEOSPATIAL` to `LocalSchema`'s load path, verify that opening a database with an existing geo index (from disk) correctly instantiates `LSMTreeGeoIndex`. Test by creating a database, inserting data, closing and re-opening it, then querying.
