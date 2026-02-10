# Geospatial Indexing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a native geospatial index type (`SPATIAL`) to ArcadeDB, storing geohash-encoded keys in the existing LSM-Tree engine. Provide OGC-standard `ST_*` SQL functions and a `SEARCH_SPATIAL()` index-aware function. Port the concept from OrientDB's geospatial module but use ArcadeDB's LSM-Tree instead of Lucene Spatial.

**Architecture:** Wrap `LSMTreeIndex` with `LSMTreeSpatialIndex` (same pattern as `LSMTreeFullTextIndex`). Encode geometries as geohash strings on insertion, perform prefix range scans + JTS exact filtering on query. Leverage existing Spatial4j 0.8 + JTS 1.20.0 dependencies (already in `engine/pom.xml`).

**Tech Stack:** Java 21, Spatial4j 0.8, JTS 1.20.0 (both already present), ArcadeDB SQL engine

**Design Document:** `docs/plans/2026-02-10-geospatial-indexing-design.md`

---

## Phase 1: Geohash Utility

### Task 1.1: Create GeohashHelper

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/geo/GeohashHelper.java`
- Test: `engine/src/test/java/com/arcadedb/index/geo/GeohashHelperTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index.geo;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GeohashHelperTest {

  @Test
  void encodePoint() {
    // Rome: lat=41.9028, lon=12.4964
    final String hash = GeohashHelper.encode(41.9028, 12.4964, 8);
    assertThat(hash).hasSize(8);
    assertThat(hash).startsWith("sr2yk");
  }

  @Test
  void encodeDecode() {
    final String hash = GeohashHelper.encode(41.9028, 12.4964, 12);
    final double[] center = GeohashHelper.decode(hash);
    assertThat(center[0]).isCloseTo(41.9028, org.assertj.core.data.Offset.offset(0.0001));
    assertThat(center[1]).isCloseTo(12.4964, org.assertj.core.data.Offset.offset(0.0001));
  }

  @Test
  void decodeBbox() {
    final String hash = GeohashHelper.encode(41.9028, 12.4964, 6);
    final double[] bbox = GeohashHelper.decodeBbox(hash);
    // bbox: [minLat, minLon, maxLat, maxLon]
    assertThat(bbox).hasSize(4);
    assertThat(bbox[0]).isLessThan(41.9028);
    assertThat(bbox[2]).isGreaterThan(41.9028);
    assertThat(bbox[1]).isLessThan(12.4964);
    assertThat(bbox[3]).isGreaterThan(12.4964);
  }

  @Test
  void coverBoundingBox() {
    // Small area around Rome
    final List<String> cells = GeohashHelper.coverBoundingBox(41.89, 12.48, 41.91, 12.51, 6);
    assertThat(cells).isNotEmpty();
    // All cells should have the same precision
    for (final String cell : cells)
      assertThat(cell).hasSize(6);
  }

  @Test
  void coverBoundingBoxDoesNotExceedMaxCells() {
    // Large area - should be capped
    final List<String> cells = GeohashHelper.coverBoundingBox(40.0, 10.0, 43.0, 15.0, 6, 16);
    assertThat(cells.size()).isLessThanOrEqualTo(16);
  }

  @Test
  void prefixRange() {
    final String[] range = GeohashHelper.prefixRange("sr2yk");
    assertThat(range).hasSize(2);
    assertThat(range[0]).isEqualTo("sr2yk");
    // range[1] is the exclusive upper bound (next sibling)
    assertThat(range[1]).isGreaterThan("sr2yk");
  }

  @Test
  void neighbors() {
    final String[] nbrs = GeohashHelper.neighbors("sr2yk");
    assertThat(nbrs).hasSize(8);
    for (final String nbr : nbrs)
      assertThat(nbr).hasSize(5);
  }

  @Test
  void optimalPrecision() {
    // Very small area → high precision
    final int small = GeohashHelper.optimalPrecision(41.900, 12.496, 41.901, 12.497, 16);
    assertThat(small).isGreaterThanOrEqualTo(7);

    // Large area → low precision
    final int large = GeohashHelper.optimalPrecision(40.0, 10.0, 45.0, 15.0, 16);
    assertThat(large).isLessThanOrEqualTo(4);
  }

  @Test
  void coverGeometry() {
    // Use Spatial4j to create a rectangle, then cover it
    final org.locationtech.spatial4j.shape.Shape rect =
        com.arcadedb.query.sql.function.geo.GeoUtils.getSpatialContext()
            .getShapeFactory().rect(12.48, 12.51, 41.89, 41.91);
    final List<String> cells = GeohashHelper.coverGeometry(rect, 2, 12, 16);
    assertThat(cells).isNotEmpty();
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=GeohashHelperTest -pl engine`
Expected: FAIL with "cannot find symbol: class GeohashHelper"

**Step 3: Write implementation**

```java
package com.arcadedb.index.geo;

import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for geohash encoding, decoding, and spatial cell decomposition.
 * Wraps Spatial4j's GeohashUtils and adds bounding-box coverage logic.
 */
public class GeohashHelper {

  private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

  public static String encode(final double lat, final double lon, final int precision) {
    return GeohashUtils.encodeLatLon(lat, lon, precision);
  }

  public static double[] decode(final String geohash) {
    final Point point = GeohashUtils.decode(geohash,
        com.arcadedb.query.sql.function.geo.GeoUtils.getSpatialContext());
    return new double[] { point.getY(), point.getX() }; // [lat, lon]
  }

  public static double[] decodeBbox(final String geohash) {
    final Rectangle rect = GeohashUtils.decodeBoundary(geohash,
        com.arcadedb.query.sql.function.geo.GeoUtils.getSpatialContext());
    return new double[] { rect.getMinY(), rect.getMinX(), rect.getMaxY(), rect.getMaxX() };
  }

  public static List<String> coverBoundingBox(final double minLat, final double minLon,
      final double maxLat, final double maxLon, final int precision) {
    return coverBoundingBox(minLat, minLon, maxLat, maxLon, precision, Integer.MAX_VALUE);
  }

  public static List<String> coverBoundingBox(final double minLat, final double minLon,
      final double maxLat, final double maxLon, final int precision, final int maxCells) {
    final List<String> cells = new ArrayList<>();
    // Encode corner points and iterate through the grid
    final String swHash = encode(minLat, minLon, precision);
    final String neHash = encode(maxLat, maxLon, precision);

    // Get bbox dimensions of a single cell at this precision
    final double[] cellBbox = decodeBbox(swHash);
    final double cellLatHeight = cellBbox[2] - cellBbox[0];
    final double cellLonWidth = cellBbox[3] - cellBbox[1];

    if (cellLatHeight <= 0 || cellLonWidth <= 0)
      return List.of(swHash);

    // Iterate through the grid
    for (double lat = minLat; lat <= maxLat + cellLatHeight; lat += cellLatHeight) {
      for (double lon = minLon; lon <= maxLon + cellLonWidth; lon += cellLonWidth) {
        final double clampedLat = Math.min(lat, maxLat);
        final double clampedLon = Math.min(lon, maxLon);
        final String hash = encode(clampedLat, clampedLon, precision);
        if (!cells.contains(hash))
          cells.add(hash);
        if (cells.size() >= maxCells)
          return cells;
      }
    }
    return cells;
  }

  public static String[] prefixRange(final String prefix) {
    // Compute the exclusive upper bound for a geohash prefix range scan
    if (prefix.isEmpty())
      return new String[] { "", "~" }; // full range
    final char lastChar = prefix.charAt(prefix.length() - 1);
    final int idx = BASE32.indexOf(lastChar);
    if (idx < BASE32.length() - 1) {
      final String upper = prefix.substring(0, prefix.length() - 1) + BASE32.charAt(idx + 1);
      return new String[] { prefix, upper };
    }
    // Last char in BASE32 - need to carry over
    return new String[] { prefix, prefix + "~" };
  }

  public static String[] neighbors(final String geohash) {
    // 8 directional neighbors: N, NE, E, SE, S, SW, W, NW
    final double[] center = decode(geohash);
    final double[] bbox = decodeBbox(geohash);
    final double latStep = bbox[2] - bbox[0];
    final double lonStep = bbox[3] - bbox[1];
    final int precision = geohash.length();
    final double[][] offsets = {
        { latStep, 0 },          // N
        { latStep, lonStep },    // NE
        { 0, lonStep },          // E
        { -latStep, lonStep },   // SE
        { -latStep, 0 },         // S
        { -latStep, -lonStep },  // SW
        { 0, -lonStep },         // W
        { latStep, -lonStep }    // NW
    };
    final String[] result = new String[8];
    for (int i = 0; i < 8; i++) {
      double lat = center[0] + offsets[i][0];
      double lon = center[1] + offsets[i][1];
      // Clamp to valid range
      lat = Math.max(-90, Math.min(90, lat));
      lon = Math.max(-180, Math.min(180, lon));
      result[i] = encode(lat, lon, precision);
    }
    return result;
  }

  public static int optimalPrecision(final double minLat, final double minLon,
      final double maxLat, final double maxLon, final int maxCells) {
    for (int precision = 1; precision <= 12; precision++) {
      final List<String> cells = coverBoundingBox(minLat, minLon, maxLat, maxLon, precision, maxCells + 1);
      if (cells.size() > maxCells)
        return Math.max(1, precision - 1);
    }
    return 12;
  }

  public static List<String> coverGeometry(final Shape shape, final int minPrecision,
      final int maxPrecision, final int maxCells) {
    if (shape instanceof Point point) {
      // Point → single geohash at max precision
      return List.of(encode(point.getY(), point.getX(), maxPrecision));
    }
    // Non-point → decompose bounding box
    final Rectangle bbox = shape.getBoundingBox();
    final int precision = Math.max(minPrecision,
        optimalPrecision(bbox.getMinY(), bbox.getMinX(), bbox.getMaxY(), bbox.getMaxX(), maxCells));
    return coverBoundingBox(bbox.getMinY(), bbox.getMinX(), bbox.getMaxY(), bbox.getMaxX(),
        Math.min(precision, maxPrecision), maxCells);
  }
}
```

**Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=GeohashHelperTest -pl engine`
Expected: PASS

**Step 5: Compile**

Run: `mvn compile -pl engine`

---

## Phase 2: Index Infrastructure

### Task 2.1: Create SpatialIndexMetadata

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/SpatialIndexMetadata.java`
- Test: `engine/src/test/java/com/arcadedb/schema/SpatialIndexMetadataTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SpatialIndexMetadataTest {

  @Test
  void defaultValues() {
    final SpatialIndexMetadata meta = new SpatialIndexMetadata("Place", new String[] { "location" }, 0);
    assertThat(meta.getMaxPrecision()).isEqualTo(12);
    assertThat(meta.getMinPrecision()).isEqualTo(2);
    assertThat(meta.getMaxCellsPerGeom()).isEqualTo(16);
  }

  @Test
  void fromJSON() {
    final SpatialIndexMetadata meta = new SpatialIndexMetadata("Place", new String[] { "location" }, 0);
    final JSONObject json = new JSONObject();
    json.put("maxPrecision", 10);
    json.put("minPrecision", 3);
    json.put("maxCells", 32);
    meta.fromJSON(json);

    assertThat(meta.getMaxPrecision()).isEqualTo(10);
    assertThat(meta.getMinPrecision()).isEqualTo(3);
    assertThat(meta.getMaxCellsPerGeom()).isEqualTo(32);
  }

  @Test
  void toJSON() {
    final SpatialIndexMetadata meta = new SpatialIndexMetadata("Place", new String[] { "location" }, 0);
    final JSONObject json = meta.toJSON();
    assertThat(json.getInt("maxPrecision")).isEqualTo(12);
    assertThat(json.getInt("minPrecision")).isEqualTo(2);
    assertThat(json.getInt("maxCells")).isEqualTo(16);
  }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=SpatialIndexMetadataTest -pl engine`
Expected: FAIL with "cannot find symbol"

**Step 3: Write implementation**

```java
package com.arcadedb.schema;

import com.arcadedb.serializer.json.JSONObject;

/**
 * Metadata for spatial indexes, storing geohash encoding configuration.
 */
public class SpatialIndexMetadata extends IndexMetadata {
  private int maxPrecision    = 12;
  private int minPrecision    = 2;
  private int maxCellsPerGeom = 16;

  public SpatialIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  @Override
  public void fromJSON(final JSONObject metadata) {
    if (metadata.has("typeName"))
      super.fromJSON(metadata);
    maxPrecision = metadata.getInt("maxPrecision", maxPrecision);
    minPrecision = metadata.getInt("minPrecision", minPrecision);
    maxCellsPerGeom = metadata.getInt("maxCells", maxCellsPerGeom);
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("maxPrecision", maxPrecision);
    json.put("minPrecision", minPrecision);
    json.put("maxCells", maxCellsPerGeom);
    return json;
  }

  public int getMaxPrecision() { return maxPrecision; }
  public int getMinPrecision() { return minPrecision; }
  public int getMaxCellsPerGeom() { return maxCellsPerGeom; }

  public void setMaxPrecision(final int v) { maxPrecision = v; }
  public void setMinPrecision(final int v) { minPrecision = v; }
  public void setMaxCellsPerGeom(final int v) { maxCellsPerGeom = v; }
}
```

**Step 4: Run test, compile**

Run: `mvn test -Dtest=SpatialIndexMetadataTest -pl engine`

---

### Task 2.2: Create TypeSpatialIndexBuilder

**Files:**
- Create: `engine/src/main/java/com/arcadedb/schema/TypeSpatialIndexBuilder.java`
- Test: `engine/src/test/java/com/arcadedb/schema/TypeSpatialIndexBuilderTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypeSpatialIndexBuilderTest extends TestHelper {

  @Test
  void builderCreation() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Place");
      database.getSchema().getType("Place").createProperty("location", String.class);
    });
    database.transaction(() -> {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Place", new String[] { "location" });
      final TypeSpatialIndexBuilder spatialBuilder = builder.withType(Schema.INDEX_TYPE.SPATIAL).withSpatialType();

      assertThat(spatialBuilder).isNotNull();
      assertThat(spatialBuilder.metadata).isInstanceOf(SpatialIndexMetadata.class);
    });
  }

  @Test
  void builderWithMetadata() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Place");
      database.getSchema().getType("Place").createProperty("location", String.class);
    });
    database.transaction(() -> {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Place", new String[] { "location" });
      final TypeSpatialIndexBuilder spatialBuilder = builder.withType(Schema.INDEX_TYPE.SPATIAL).withSpatialType();
      spatialBuilder.withMaxPrecision(10).withMinPrecision(3).withMaxCells(32);

      final SpatialIndexMetadata meta = (SpatialIndexMetadata) spatialBuilder.metadata;
      assertThat(meta.getMaxPrecision()).isEqualTo(10);
      assertThat(meta.getMinPrecision()).isEqualTo(3);
      assertThat(meta.getMaxCellsPerGeom()).isEqualTo(32);
    });
  }
}
```

**Step 2: Write implementation**

Model after `TypeFullTextIndexBuilder`. Key contents:

```java
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Builder class for spatial indexes with geohash configuration.
 */
public class TypeSpatialIndexBuilder extends TypeIndexBuilder {

  protected TypeSpatialIndexBuilder(final TypeIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]));
    this.metadata = new SpatialIndexMetadata(
        copyFrom.metadata.typeName,
        copyFrom.metadata.propertyNames.toArray(new String[0]),
        copyFrom.metadata.associatedBucketId);
    this.indexType = Schema.INDEX_TYPE.SPATIAL;
    this.unique = copyFrom.unique;
    this.pageSize = copyFrom.pageSize;
    this.nullStrategy = copyFrom.nullStrategy;
    this.callback = copyFrom.callback;
    this.ignoreIfExists = copyFrom.ignoreIfExists;
    this.indexName = copyFrom.indexName;
    this.filePath = copyFrom.filePath;
    this.keyTypes = copyFrom.keyTypes;
    this.batchSize = copyFrom.batchSize;
    this.maxAttempts = copyFrom.maxAttempts;
  }

  public TypeSpatialIndexBuilder withMaxPrecision(final int precision) {
    ((SpatialIndexMetadata) metadata).setMaxPrecision(precision);
    return this;
  }

  public TypeSpatialIndexBuilder withMinPrecision(final int precision) {
    ((SpatialIndexMetadata) metadata).setMinPrecision(precision);
    return this;
  }

  public TypeSpatialIndexBuilder withMaxCells(final int maxCells) {
    ((SpatialIndexMetadata) metadata).setMaxCellsPerGeom(maxCells);
    return this;
  }

  public void withMetadata(final JSONObject json) {
    ((SpatialIndexMetadata) metadata).fromJSON(json);
  }
}
```

**Step 3: Wire into TypeIndexBuilder**

Modify `TypeIndexBuilder.withType()` to handle `SPATIAL`:

```java
// In TypeIndexBuilder.java, inside withType():
if (indexType == Schema.INDEX_TYPE.SPATIAL && !(this instanceof TypeSpatialIndexBuilder))
    return new TypeSpatialIndexBuilder(this);
```

Add `withSpatialType()` method:

```java
public TypeSpatialIndexBuilder withSpatialType() {
    if (this instanceof TypeSpatialIndexBuilder)
        return (TypeSpatialIndexBuilder) this;
    if (indexType != Schema.INDEX_TYPE.SPATIAL)
        throw new IllegalStateException("withSpatialType() can only be called after withType(SPATIAL)");
    return new TypeSpatialIndexBuilder(this);
}
```

**Step 4: Add `SPATIAL` to `Schema.INDEX_TYPE` enum**

In `Schema.java`, change:
```java
enum INDEX_TYPE {
    LSM_TREE, FULL_TEXT, LSM_VECTOR, SPATIAL
}
```

**Step 5: Run test, compile**

Run: `mvn test -Dtest=TypeSpatialIndexBuilderTest -pl engine`

---

### Task 2.3: Create LSMTreeSpatialIndex

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/geo/LSMTreeSpatialIndex.java`
- Test: `engine/src/test/java/com/arcadedb/index/geo/LSMTreeSpatialIndexTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LSMTreeSpatialIndexTest extends TestHelper {

  @Test
  void createSpatialIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Restaurant");
      database.command("sql", "CREATE PROPERTY Restaurant.name STRING");
      database.command("sql", "CREATE PROPERTY Restaurant.location STRING");
      database.command("sql", "CREATE INDEX ON Restaurant(location) SPATIAL");
    });

    database.transaction(() -> {
      final var idx = database.getSchema().getIndexByName("Restaurant[location]");
      assertThat(idx).isNotNull();
      assertThat(idx.getType()).isEqualTo(Schema.INDEX_TYPE.SPATIAL);
    });
  }

  @Test
  void insertAndSearchPoints() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Restaurant");
      database.command("sql", "CREATE PROPERTY Restaurant.name STRING");
      database.command("sql", "CREATE PROPERTY Restaurant.location STRING");
      database.command("sql", "CREATE INDEX ON Restaurant(location) SPATIAL");

      // Insert points in Rome
      database.command("sql", "INSERT INTO Restaurant SET name='Trattoria', location='POINT(12.4964 41.9028)'");
      database.command("sql", "INSERT INTO Restaurant SET name='Pizzeria', location='POINT(12.4922 41.8902)'");
      database.command("sql", "INSERT INTO Restaurant SET name='FarAway', location='POINT(2.3522 48.8566)'"); // Paris
    });

    database.transaction(() -> {
      // Search near Rome - should find Trattoria and Pizzeria, not FarAway
      final ResultSet result = database.query("sql",
          "SELECT name FROM Restaurant WHERE SEARCH_SPATIAL('Restaurant[location]', 'NEAR',"
          + " 'POINT(12.49 41.90)', {\"maxDistance\": 5.0})");

      int count = 0;
      while (result.hasNext()) {
        final String name = result.next().getProperty("name");
        assertThat(name).isIn("Trattoria", "Pizzeria");
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void insertAndSearchWithin() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Restaurant");
      database.command("sql", "CREATE PROPERTY Restaurant.name STRING");
      database.command("sql", "CREATE PROPERTY Restaurant.location STRING");
      database.command("sql", "CREATE INDEX ON Restaurant(location) SPATIAL");

      database.command("sql", "INSERT INTO Restaurant SET name='Inside', location='POINT(12.49 41.90)'");
      database.command("sql", "INSERT INTO Restaurant SET name='Outside', location='POINT(2.35 48.85)'");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT name FROM Restaurant WHERE SEARCH_SPATIAL('Restaurant[location]', 'WITHIN',"
          + " 'POLYGON((12.0 41.0, 13.0 41.0, 13.0 42.0, 12.0 42.0, 12.0 41.0))')");

      int count = 0;
      while (result.hasNext()) {
        assertThat(result.next().getProperty("name").toString()).isEqualTo("Inside");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
```

**Step 2: Write implementation**

Follow the `LSMTreeFullTextIndex` pattern exactly. The class wraps `LSMTreeIndex` with STRING keys (geohash values).

Key implementation points:

```java
package com.arcadedb.index.geo;

// imports...

public class LSMTreeSpatialIndex implements Index, IndexInternal {
    private final LSMTreeIndex          underlyingIndex;
    private final SpatialIndexMetadata  spatialMetadata;
    private       TypeIndex             typeIndex;

    public static class LSMTreeSpatialIndexFactoryHandler implements IndexFactoryHandler {
        @Override
        public IndexInternal create(final IndexBuilder builder) {
            if (builder.isUnique())
                throw new IllegalArgumentException("Spatial index cannot be unique");

            SpatialIndexMetadata spatialMeta = null;
            if (builder.getMetadata() instanceof SpatialIndexMetadata)
                spatialMeta = (SpatialIndexMetadata) builder.getMetadata();

            return new LSMTreeSpatialIndex(builder.getDatabase(), builder.getIndexName(),
                builder.getFilePath(), ComponentFile.MODE.READ_WRITE, builder.getPageSize(),
                builder.getNullStrategy(), spatialMeta);
        }
    }

    // Constructor at creation time
    public LSMTreeSpatialIndex(final DatabaseInternal database, final String name,
        final String filePath, final ComponentFile.MODE mode, final int pageSize,
        final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
        final SpatialIndexMetadata metadata) {
        this.spatialMetadata = metadata;
        underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode,
            new Type[] { Type.STRING }, pageSize, nullStrategy);
    }

    // Constructor at load time (wrapping existing LSMTreeIndex)
    public LSMTreeSpatialIndex(final LSMTreeIndex index) {
        this(index, null);
    }

    public LSMTreeSpatialIndex(final LSMTreeIndex index, final SpatialIndexMetadata metadata) {
        this.underlyingIndex = index;
        this.spatialMetadata = metadata;
    }

    // Constructor at file load time
    public LSMTreeSpatialIndex(final DatabaseInternal database, final String name,
        final String filePath, final int fileId, final ComponentFile.MODE mode,
        final int pageSize, final int version) {
        try {
            underlyingIndex = new LSMTreeIndex(database, name, false, filePath, fileId,
                mode, pageSize, version);
        } catch (final IOException e) {
            throw new IndexException("Cannot create spatial index", e);
        }
        this.spatialMetadata = null;
    }

    @Override
    public void put(final Object[] keys, final RID[] rids) {
        // keys[0] = WKT string (e.g., "POINT(12.49 41.90)")
        final Shape shape = GeoUtils.toShape(keys[0]);
        final int maxPrec = spatialMetadata != null ? spatialMetadata.getMaxPrecision() : 12;
        final int minPrec = spatialMetadata != null ? spatialMetadata.getMinPrecision() : 2;
        final int maxCells = spatialMetadata != null ? spatialMetadata.getMaxCellsPerGeom() : 16;

        final List<String> cells = GeohashHelper.coverGeometry(shape, minPrec, maxPrec, maxCells);
        for (final String cell : cells)
            underlyingIndex.put(new Object[] { cell }, rids);
    }

    @Override
    public void remove(final Object[] keys, final Identifiable rid) {
        final Shape shape = GeoUtils.toShape(keys[0]);
        final int maxPrec = spatialMetadata != null ? spatialMetadata.getMaxPrecision() : 12;
        final int minPrec = spatialMetadata != null ? spatialMetadata.getMinPrecision() : 2;
        final int maxCells = spatialMetadata != null ? spatialMetadata.getMaxCellsPerGeom() : 16;

        final List<String> cells = GeohashHelper.coverGeometry(shape, minPrec, maxPrec, maxCells);
        for (final String cell : cells)
            underlyingIndex.remove(new Object[] { cell }, rid);
    }

    @Override
    public Schema.INDEX_TYPE getType() {
        return Schema.INDEX_TYPE.SPATIAL;
    }

    // ... delegate all remaining Index/IndexInternal methods to underlyingIndex
    // (same pattern as LSMTreeFullTextIndex)
}
```

**Step 3: Register in LocalSchema.java**

Add at line 133 (after LSM_VECTOR registration):
```java
indexFactory.register(INDEX_TYPE.SPATIAL.name(),
    new LSMTreeSpatialIndex.LSMTreeSpatialIndexFactoryHandler());
```

**Step 4: Update CreateIndexStatement DDL parsing**

In `CreateIndexStatement.java`:
- Add `"SPATIAL"` to the `validate()` switch
- Add `SPATIAL` to the `executeDDL()` type mapping
- Add metadata handling block (same pattern as FULL_TEXT)

**Step 5: Run test, compile**

Run: `mvn test -Dtest=LSMTreeSpatialIndexTest -pl engine`
Then: `mvn compile -pl engine`

---

### Task 2.4: Create SpatialQueryExecutor

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/geo/SpatialQueryExecutor.java`
- Test: `engine/src/test/java/com/arcadedb/index/geo/SpatialQueryExecutorTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SpatialQueryExecutorTest extends TestHelper {

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Place");
      database.command("sql", "CREATE PROPERTY Place.name STRING");
      database.command("sql", "CREATE PROPERTY Place.location STRING");
      database.command("sql", "CREATE INDEX ON Place(location) SPATIAL");

      database.command("sql", "INSERT INTO Place SET name='Rome', location='POINT(12.4964 41.9028)'");
      database.command("sql", "INSERT INTO Place SET name='Milan', location='POINT(9.1900 45.4642)'");
      database.command("sql", "INSERT INTO Place SET name='Naples', location='POINT(14.2681 40.8518)'");
      database.command("sql", "INSERT INTO Place SET name='Paris', location='POINT(2.3522 48.8566)'");
    });
  }

  @Test
  void searchNear() {
    database.transaction(() -> {
      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Place[location]");
      final LSMTreeSpatialIndex spatialIdx = (LSMTreeSpatialIndex) idx.getIndexesOnBuckets()[0];
      final SpatialQueryExecutor executor = new SpatialQueryExecutor(spatialIdx, database);

      final org.locationtech.spatial4j.shape.Point center =
          com.arcadedb.query.sql.function.geo.GeoUtils.getSpatialContext()
              .getShapeFactory().pointXY(12.49, 41.90);

      // 200km radius from Rome should include Rome and Naples (not Milan/Paris)
      final IndexCursor cursor = executor.searchNear(center, 200.0, -1);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(2); // Rome + Naples
    });
  }

  @Test
  void searchWithin() {
    database.transaction(() -> {
      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Place[location]");
      final LSMTreeSpatialIndex spatialIdx = (LSMTreeSpatialIndex) idx.getIndexesOnBuckets()[0];
      final SpatialQueryExecutor executor = new SpatialQueryExecutor(spatialIdx, database);

      // Bounding box around central Italy
      final org.locationtech.spatial4j.shape.Shape bbox =
          com.arcadedb.query.sql.function.geo.GeoUtils.getSpatialContext()
              .getShapeFactory().rect(10.0, 16.0, 40.0, 43.0);

      final IndexCursor cursor = executor.searchWithin(bbox, -1);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(2); // Rome + Naples
    });
  }
}
```

**Step 2: Write implementation**

```java
package com.arcadedb.index.geo;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.query.sql.function.geo.GeoUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

import java.util.*;

/**
 * Two-phase spatial query executor:
 *   Phase 1: geohash range scan → candidate RIDs (coarse filter)
 *   Phase 2: JTS exact geometry predicate → filtered results (fine filter)
 */
public class SpatialQueryExecutor {
    private final LSMTreeSpatialIndex index;
    private final Database database;

    public SpatialQueryExecutor(final LSMTreeSpatialIndex index, final Database database) {
        this.index = index;
        this.database = database;
    }

    public IndexCursor searchNear(final Point center, final double radiusKm, final int limit) {
        // 1. Compute bounding box from center + radius
        final double latDelta = radiusKm / 111.32; // ~111.32 km per degree of latitude
        final double lonDelta = radiusKm / (111.32 * Math.cos(Math.toRadians(center.getY())));
        final double minLat = center.getY() - latDelta;
        final double maxLat = center.getY() + latDelta;
        final double minLon = center.getX() - lonDelta;
        final double maxLon = center.getX() + lonDelta;

        // 2. Get candidates via geohash range scan
        final Set<RID> candidates = getCandidatesForBbox(minLat, minLon, maxLat, maxLon);

        // 3. Exact filter: compute Haversine distance, filter by radius
        final List<IndexCursorEntry> results = new ArrayList<>();
        final String[] propertyNames = index.getPropertyNames().toArray(new String[0]);

        for (final RID rid : candidates) {
            final Document doc = rid.asDocument(true);
            if (doc == null) continue;

            final Object geomValue = doc.get(propertyNames[0]);
            if (geomValue == null) continue;

            final Shape shape = GeoUtils.toShape(geomValue);
            if (!(shape instanceof Point point)) continue;

            final double dist = haversineKm(center.getY(), center.getX(), point.getY(), point.getX());
            if (dist <= radiusKm) {
                // Score = inverse distance (closer = higher score, multiply by 1000 for integer)
                final int score = (int) (1000.0 / (dist + 0.001));
                results.add(new IndexCursorEntry(new Object[] { geomValue }, rid, score));
            }
        }

        results.sort((a, b) -> Integer.compare(b.score, a.score));
        if (limit > 0 && results.size() > limit)
            return new TempIndexCursor(results.subList(0, limit));
        return new TempIndexCursor(results);
    }

    public IndexCursor searchWithin(final Shape queryShape, final int limit) {
        return searchWithPredicate(queryShape, SpatialRelation.WITHIN, limit);
    }

    public IndexCursor searchIntersects(final Shape queryShape, final int limit) {
        return searchWithPredicate(queryShape, null, limit); // null = not DISJOINT
    }

    private IndexCursor searchWithPredicate(final Shape queryShape, final SpatialRelation relation,
        final int limit) {
        final Rectangle bbox = queryShape.getBoundingBox();
        final Set<RID> candidates = getCandidatesForBbox(
            bbox.getMinY(), bbox.getMinX(), bbox.getMaxY(), bbox.getMaxX());

        final List<IndexCursorEntry> results = new ArrayList<>();
        final String[] propertyNames = index.getPropertyNames().toArray(new String[0]);

        for (final RID rid : candidates) {
            final Document doc = rid.asDocument(true);
            if (doc == null) continue;

            final Object geomValue = doc.get(propertyNames[0]);
            if (geomValue == null) continue;

            final Shape docShape = GeoUtils.toShape(geomValue);
            final SpatialRelation rel = docShape.relate(queryShape);

            boolean matches;
            if (relation == SpatialRelation.WITHIN)
                matches = (rel == SpatialRelation.WITHIN);
            else
                matches = (rel != SpatialRelation.DISJOINT); // INTERSECTS

            if (matches)
                results.add(new IndexCursorEntry(new Object[] { geomValue }, rid, 1));
        }

        if (limit > 0 && results.size() > limit)
            return new TempIndexCursor(results.subList(0, limit));
        return new TempIndexCursor(results);
    }

    private Set<RID> getCandidatesForBbox(final double minLat, final double minLon,
        final double maxLat, final double maxLon) {
        final int precision = GeohashHelper.optimalPrecision(minLat, minLon, maxLat, maxLon, 16);
        final List<String> cells = GeohashHelper.coverBoundingBox(
            minLat, minLon, maxLat, maxLon, precision, 32);

        // Also add neighbors of border cells to handle boundary issues
        final Set<String> allCells = new LinkedHashSet<>(cells);
        for (final String cell : cells) {
            for (final String nbr : GeohashHelper.neighbors(cell))
                allCells.add(nbr);
        }

        final Set<RID> candidates = new HashSet<>();
        final RangeIndex rangeIndex = index.getUnderlyingRangeIndex();

        for (final String cell : allCells) {
            final String[] range = GeohashHelper.prefixRange(cell);
            final IndexCursor cursor = rangeIndex.range(true,
                new Object[] { range[0] }, true, new Object[] { range[1] }, false);
            while (cursor.hasNext())
                candidates.add(cursor.next().getIdentity());
        }
        return candidates;
    }

    private static double haversineKm(final double lat1, final double lon1,
        final double lat2, final double lon2) {
        final double R = 6371.0;
        final double dLat = Math.toRadians(lat2 - lat1);
        final double dLon = Math.toRadians(lon2 - lon1);
        final double a = Math.pow(Math.sin(dLat / 2), 2)
            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.pow(Math.sin(dLon / 2), 2);
        return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
```

**Step 3: Add `getUnderlyingRangeIndex()` method to LSMTreeSpatialIndex**

```java
public RangeIndex getUnderlyingRangeIndex() {
    return underlyingIndex;
}
```

**Step 4: Run test, compile**

Run: `mvn test -Dtest=SpatialQueryExecutorTest -pl engine`

---

## Phase 3: GeoUtils Enhancement & SQL Functions

### Task 3.1: Enhance GeoUtils with Shape Conversion Helpers

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/function/geo/GeoUtils.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/function/geo/GeoUtilsTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.function.geo;

import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import static org.assertj.core.api.Assertions.assertThat;

class GeoUtilsTest {

  @Test
  void toShapeFromWKT() {
    final Shape shape = GeoUtils.toShape("POINT(12.49 41.90)");
    assertThat(shape).isInstanceOf(Point.class);
    final Point p = (Point) shape;
    assertThat(p.getX()).isCloseTo(12.49, org.assertj.core.data.Offset.offset(0.001));
    assertThat(p.getY()).isCloseTo(41.90, org.assertj.core.data.Offset.offset(0.001));
  }

  @Test
  void toShapeFromShape() {
    final Shape original = GeoUtils.getSpatialContext().getShapeFactory().pointXY(12.49, 41.90);
    final Shape result = GeoUtils.toShape(original);
    assertThat(result).isSameAs(original);
  }

  @Test
  void toWKT() {
    final Shape shape = GeoUtils.getSpatialContext().getShapeFactory().pointXY(12.49, 41.90);
    final String wkt = GeoUtils.toWKT(shape);
    assertThat(wkt).contains("POINT");
  }

  @Test
  void parseWKT() {
    final Shape shape = GeoUtils.parseWKT("POLYGON((12 41, 13 41, 13 42, 12 42, 12 41))");
    assertThat(shape).isNotNull();
  }
}
```

**Step 2: Add methods to GeoUtils**

```java
public static Shape toShape(final Object value) {
    if (value instanceof Shape) return (Shape) value;
    if (value instanceof String) return parseWKT((String) value);
    throw new IllegalArgumentException("Cannot convert to Shape: " + value.getClass().getName());
}

public static Shape parseWKT(final String wkt) {
    try {
        return SPATIAL_CONTEXT.getFormats().getWktReader().read(wkt);
    } catch (final Exception e) {
        throw new IllegalArgumentException("Invalid WKT: " + wkt, e);
    }
}

public static String toWKT(final Shape shape) {
    return SPATIAL_CONTEXT.getFormats().getWktWriter().toString(shape);
}
```

**Step 3: Run test**

Run: `mvn test -Dtest=GeoUtilsTest -pl engine`

---

### Task 3.2: Create ST_* Geometry Constructor Functions

**Files to create** (one class each, all follow same pattern):
- `SQLFunctionSTGeomFromText.java` — `ST_GeomFromText(wkt)`
- `SQLFunctionSTPoint.java` — `ST_Point(lon, lat)` (OGC order)
- `SQLFunctionSTMakeEnvelope.java` — `ST_MakeEnvelope(xmin, ymin, xmax, ymax)`
- `SQLFunctionSTBuffer.java` — `ST_Buffer(geom, distance)`

**Test:** `engine/src/test/java/com/arcadedb/query/sql/function/geo/SQLGeoSTConstructorTest.java`

**Pattern** (same for each function):
```java
public class SQLFunctionSTGeomFromText extends SQLFunctionAbstract {
    public static final String NAME = "st_geomfromtext";

    public SQLFunctionSTGeomFromText() { super(NAME); }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord,
        final Object currentResult, final Object[] params, final CommandContext context) {
        if (params.length < 1 || params[0] == null)
            throw new IllegalArgumentException("ST_GeomFromText() requires a WKT string");
        return GeoUtils.parseWKT(params[0].toString());
    }

    @Override
    public String getSyntax() { return "ST_GeomFromText(<wkt>)"; }
}
```

**Register all in `DefaultSQLFunctionFactory`** under a new `// Spatial (ST_*)` section.

---

### Task 3.3: Create ST_* Geometry Accessor Functions

**Files to create:**
- `SQLFunctionSTAsText.java` — `ST_AsText(geom)` → WKT string
- `SQLFunctionSTEnvelope.java` — `ST_Envelope(geom)` → bounding box
- `SQLFunctionSTX.java` — `ST_X(point)` → longitude
- `SQLFunctionSTY.java` — `ST_Y(point)` → latitude

**Test:** `engine/src/test/java/com/arcadedb/query/sql/function/geo/SQLGeoSTAccessorTest.java`

---

### Task 3.4: Create ST_* Spatial Predicate Functions

**Files to create:**
- `SQLFunctionSTContains.java` — `ST_Contains(geomA, geomB)` → boolean
- `SQLFunctionSTWithin.java` — `ST_Within(geomA, geomB)` → boolean
- `SQLFunctionSTIntersects.java` — `ST_Intersects(geomA, geomB)` → boolean
- `SQLFunctionSTDWithin.java` — `ST_DWithin(geomA, geomB, distance)` → boolean
- `SQLFunctionSTDisjoint.java` — `ST_Disjoint(geomA, geomB)` → boolean
- `SQLFunctionSTEquals.java` — `ST_Equals(geomA, geomB)` → boolean

**Test:** `engine/src/test/java/com/arcadedb/query/sql/function/geo/SQLGeoSTPredicateTest.java`

**Pattern** (all predicates follow this):

```java
public class SQLFunctionSTWithin extends SQLFunctionAbstract {
    public static final String NAME = "st_within";

    public SQLFunctionSTWithin() { super(NAME); }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord,
        final Object currentResult, final Object[] params, final CommandContext context) {
        if (params.length < 2)
            throw new IllegalArgumentException("ST_Within() requires two geometry parameters");
        final Shape geomA = GeoUtils.toShape(params[0]);
        final Shape geomB = GeoUtils.toShape(params[1]);
        return geomA.relate(geomB) == SpatialRelation.WITHIN;
    }

    @Override
    public String getSyntax() { return "ST_Within(<geomA>, <geomB>)"; }
}
```

`ST_DWithin` is special — it takes 3 parameters:

```java
public class SQLFunctionSTDWithin extends SQLFunctionAbstract {
    public static final String NAME = "st_dwithin";

    @Override
    public Object execute(final Object self, final Identifiable currentRecord,
        final Object currentResult, final Object[] params, final CommandContext context) {
        if (params.length < 3)
            throw new IllegalArgumentException("ST_DWithin() requires geomA, geomB, distance");
        final Shape geomA = GeoUtils.toShape(params[0]);
        final Shape geomB = GeoUtils.toShape(params[1]);
        final double maxDistance = ((Number) params[2]).doubleValue();

        final double distance = GeoUtils.getSpatialContext().calcDistance(
            ((Point) geomA).getCenter(), ((Point) geomB).getCenter());
        // Spatial4j distance is in degrees; convert to km
        final double distKm = distance * 111.32;
        return distKm <= maxDistance;
    }
}
```

---

### Task 3.5: Create ST_* Measurement Functions

**Files to create:**
- `SQLFunctionSTDistance.java` — `ST_Distance(geomA, geomB)` → double (km)
- `SQLFunctionSTArea.java` — `ST_Area(geom)` → double
- `SQLFunctionSTLength.java` — `ST_Length(geom)` → double

**Test:** `engine/src/test/java/com/arcadedb/query/sql/function/geo/SQLGeoSTMeasurementTest.java`

---

### Task 3.6: Create SEARCH_SPATIAL SQL Function

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/function/geo/SQLFunctionSearchSpatial.java`
- Modify: `engine/src/main/java/com/arcadedb/query/sql/function/DefaultSQLFunctionFactory.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/function/geo/SQLFunctionSearchSpatialTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.query.sql.function.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionSearchSpatialTest extends TestHelper {

  @BeforeEach
  void setup() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Place");
      database.command("sql", "CREATE PROPERTY Place.name STRING");
      database.command("sql", "CREATE PROPERTY Place.location STRING");
      database.command("sql", "CREATE INDEX ON Place(location) SPATIAL");

      database.command("sql", "INSERT INTO Place SET name='Rome', location='POINT(12.4964 41.9028)'");
      database.command("sql", "INSERT INTO Place SET name='Naples', location='POINT(14.2681 40.8518)'");
      database.command("sql", "INSERT INTO Place SET name='Paris', location='POINT(2.3522 48.8566)'");
    });
  }

  @Test
  void searchNear() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT name FROM Place WHERE SEARCH_SPATIAL('Place[location]', 'NEAR',"
          + " 'POINT(12.49 41.90)', {\"maxDistance\": 200.0})");

      int count = 0;
      while (result.hasNext()) {
        final String name = result.next().getProperty("name");
        assertThat(name).isIn("Rome", "Naples");
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void searchWithin() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT name FROM Place WHERE SEARCH_SPATIAL('Place[location]', 'WITHIN',"
          + " 'POLYGON((10 40, 16 40, 16 43, 10 43, 10 40))')");

      int count = 0;
      while (result.hasNext()) {
        final String name = result.next().getProperty("name");
        assertThat(name).isIn("Rome", "Naples");
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void searchIntersects() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT name FROM Place WHERE SEARCH_SPATIAL('Place[location]', 'INTERSECTS',"
          + " 'POLYGON((12 41, 13 41, 13 42, 12 42, 12 41))')");

      int count = 0;
      while (result.hasNext()) {
        assertThat(result.next().getProperty("name").toString()).isEqualTo("Rome");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
```

**Step 2: Write implementation**

Follows the `SQLFunctionSearchIndex` pattern:

```java
package com.arcadedb.query.sql.function.geo;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.geo.LSMTreeSpatialIndex;
import com.arcadedb.index.geo.SpatialQueryExecutor;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import java.util.*;

public class SQLFunctionSearchSpatial extends SQLFunctionAbstract {
    public static final String NAME = "search_spatial";

    public SQLFunctionSearchSpatial() { super(NAME); }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord,
        final Object currentResult, final Object[] params, final CommandContext context) {
        if (params.length < 3)
            throw new CommandExecutionException(
                "SEARCH_SPATIAL() requires: indexName, operation, geometry [, options]");

        final String indexName = params[0].toString();
        final String operation = params[1].toString().toUpperCase();
        final Shape queryShape = GeoUtils.toShape(params[2]);

        final Database database = context.getDatabase();
        final Index index = database.getSchema().getIndexByName(indexName);
        if (!(index instanceof final TypeIndex typeIndex))
            throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

        final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
        if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeSpatialIndex))
            throw new CommandExecutionException("Index '" + indexName + "' is not a spatial index");

        // Cache key for this search
        final String cacheKey = "search_spatial:" + indexName + ":" + operation + ":"
            + GeoUtils.toWKT(queryShape);
        @SuppressWarnings("unchecked")
        Map<RID, Integer> allResults = (Map<RID, Integer>) context.getVariable(cacheKey);

        if (allResults == null) {
            allResults = new HashMap<>();

            for (final Index bucketIndex : bucketIndexes) {
                if (!(bucketIndex instanceof final LSMTreeSpatialIndex spatialIdx))
                    continue;
                final SpatialQueryExecutor executor = new SpatialQueryExecutor(spatialIdx, database);
                final IndexCursor cursor;

                switch (operation) {
                case "NEAR" -> {
                    if (!(queryShape instanceof Point))
                        throw new CommandExecutionException("NEAR requires a POINT geometry");
                    double maxDist = 10.0; // default 10km
                    if (params.length > 3 && params[3] != null) {
                        if (params[3] instanceof Map map)
                            maxDist = ((Number) map.get("maxDistance")).doubleValue();
                        else if (params[3] instanceof String s) {
                            final JSONObject opts = new JSONObject(s);
                            maxDist = opts.getDouble("maxDistance", 10.0);
                        }
                    }
                    cursor = executor.searchNear((Point) queryShape, maxDist, -1);
                }
                case "WITHIN" -> cursor = executor.searchWithin(queryShape, -1);
                case "INTERSECTS" -> cursor = executor.searchIntersects(queryShape, -1);
                default -> throw new CommandExecutionException("Unknown operation: " + operation);
                }

                while (cursor.hasNext()) {
                    final Identifiable match = cursor.next();
                    final int score = cursor.getScore();
                    allResults.merge(match.getIdentity(), score, Integer::sum);
                }
            }
            context.setVariable(cacheKey, allResults);
        }

        // Filter current record
        if (currentRecord != null) {
            final RID rid = currentRecord.getIdentity();
            final boolean matches = allResults.containsKey(rid);
            if (matches)
                context.setVariable("$distance", allResults.get(rid));
            return matches;
        }

        // Return cursor
        final List<IndexCursorEntry> entries = new ArrayList<>();
        for (final Map.Entry<RID, Integer> e : allResults.entrySet())
            entries.add(new IndexCursorEntry(new Object[] {}, e.getKey(), e.getValue()));
        entries.sort((a, b) -> Integer.compare(b.score, a.score));
        return new TempIndexCursor(entries);
    }

    @Override
    public String getSyntax() {
        return "SEARCH_SPATIAL(<index-name>, <operation>, <geometry> [, <options>])";
    }
}
```

**Step 3: Register in `DefaultSQLFunctionFactory`**

Add in the Geo section:
```java
register(SQLFunctionSearchSpatial.NAME, new SQLFunctionSearchSpatial());
```

And register all `ST_*` functions:
```java
// Spatial (ST_* OGC standard)
register(SQLFunctionSTGeomFromText.NAME, new SQLFunctionSTGeomFromText());
register(SQLFunctionSTPoint.NAME, new SQLFunctionSTPoint());
register(SQLFunctionSTMakeEnvelope.NAME, new SQLFunctionSTMakeEnvelope());
register(SQLFunctionSTBuffer.NAME, new SQLFunctionSTBuffer());
register(SQLFunctionSTAsText.NAME, new SQLFunctionSTAsText());
register(SQLFunctionSTEnvelope.NAME, new SQLFunctionSTEnvelope());
register(SQLFunctionSTX.NAME, new SQLFunctionSTX());
register(SQLFunctionSTY.NAME, new SQLFunctionSTY());
register(SQLFunctionSTContains.NAME, new SQLFunctionSTContains());
register(SQLFunctionSTWithin.NAME, new SQLFunctionSTWithin());
register(SQLFunctionSTIntersects.NAME, new SQLFunctionSTIntersects());
register(SQLFunctionSTDWithin.NAME, new SQLFunctionSTDWithin());
register(SQLFunctionSTDisjoint.NAME, new SQLFunctionSTDisjoint());
register(SQLFunctionSTEquals.NAME, new SQLFunctionSTEquals());
register(SQLFunctionSTDistance.NAME, new SQLFunctionSTDistance());
register(SQLFunctionSTArea.NAME, new SQLFunctionSTArea());
register(SQLFunctionSTLength.NAME, new SQLFunctionSTLength());
register(SQLFunctionSearchSpatial.NAME, new SQLFunctionSearchSpatial());
```

**Step 4: Run tests**

Run: `mvn test -Dtest=SQLFunctionSearchSpatialTest -pl engine`

---

## Phase 4: Non-Point Geometry Indexing

### Task 4.1: Multi-Cell Indexing for Polygons and LineStrings

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/index/geo/LSMTreeSpatialIndex.java` (already handles via `GeohashHelper.coverGeometry`)
- Test: `engine/src/test/java/com/arcadedb/index/geo/SpatialPolygonIndexTest.java`

**Step 1: Write the failing test**

```java
package com.arcadedb.index.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SpatialPolygonIndexTest extends TestHelper {

  @Test
  void indexAndSearchPolygon() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Region");
      database.command("sql", "CREATE PROPERTY Region.name STRING");
      database.command("sql", "CREATE PROPERTY Region.boundary STRING");
      database.command("sql", "CREATE INDEX ON Region(boundary) SPATIAL");

      // Small polygon around central Rome
      database.command("sql",
          "INSERT INTO Region SET name='CentroStorico', "
          + "boundary='POLYGON((12.46 41.89, 12.52 41.89, 12.52 41.91, 12.46 41.91, 12.46 41.89))'");

      // Polygon around Milan
      database.command("sql",
          "INSERT INTO Region SET name='Milano', "
          + "boundary='POLYGON((9.15 45.44, 9.22 45.44, 9.22 45.49, 9.15 45.49, 9.15 45.44))'");
    });

    database.transaction(() -> {
      // Point inside CentroStorico, outside Milano
      final ResultSet result = database.query("sql",
          "SELECT name FROM Region WHERE SEARCH_SPATIAL('Region[boundary]', 'INTERSECTS',"
          + " 'POINT(12.49 41.90)')");

      int count = 0;
      while (result.hasNext()) {
        assertThat(result.next().getProperty("name").toString()).isEqualTo("CentroStorico");
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
```

**Step 2: Verify test passes** (the `put()` and `SpatialQueryExecutor` already handle polygons)

Run: `mvn test -Dtest=SpatialPolygonIndexTest -pl engine`

---

## Phase 5: Persistence & Integration Testing

### Task 5.1: Index Persistence After Reopen

**Files:**
- Test: `engine/src/test/java/com/arcadedb/index/geo/SpatialIndexPersistenceTest.java`

```java
@Test
void indexSurvivesReopen() {
    database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Place");
        database.command("sql", "CREATE PROPERTY Place.name STRING");
        database.command("sql", "CREATE PROPERTY Place.location STRING");
        database.command("sql", "CREATE INDEX ON Place(location) SPATIAL");
        database.command("sql", "INSERT INTO Place SET name='Rome', location='POINT(12.49 41.90)'");
    });

    reopenDatabase();

    database.transaction(() -> {
        assertThat(database.getSchema().existsIndex("Place[location]")).isTrue();

        final ResultSet result = database.query("sql",
            "SELECT name FROM Place WHERE SEARCH_SPATIAL('Place[location]', 'NEAR',"
            + " 'POINT(12.49 41.90)', {\"maxDistance\": 1.0})");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("name").toString()).isEqualTo("Rome");
    });
}
```

### Task 5.2: Lat/Lon Dual-Property Convenience

**Files:**
- Modify: `LSMTreeSpatialIndex.put()` to detect two DOUBLE properties
- Test: `engine/src/test/java/com/arcadedb/index/geo/SpatialLatLonTest.java`

When two DOUBLE properties are indexed (e.g., `latitude`, `longitude`), `put()` automatically:
1. Reads both values as doubles
2. Encodes them as a point geohash: `GeohashHelper.encode(keys[0], keys[1], maxPrecision)`
3. Stores in underlying index

```sql
CREATE INDEX ON Restaurant(latitude, longitude) SPATIAL
INSERT INTO Restaurant SET name='Trattoria', latitude=41.9028, longitude=12.4964
-- Internally: geohash = GeohashHelper.encode(41.9028, 12.4964, 12)
```

### Task 5.3: Backward Compatibility

**Test:** Ensure existing geo tests still pass:

Run: `mvn test -Dtest=SQLGeoFunctionsTest -pl engine`

The existing `point()`, `circle()`, `rectangle()`, `polygon()`, `linestring()`, `distance()`, `.isWithin()`, `.intersectsWith()` must all remain unchanged.

---

## Phase 6: Edge Cases & Performance

### Task 6.1: Edge Case Tests

**File:** `engine/src/test/java/com/arcadedb/index/geo/SpatialEdgeCaseTest.java`

| Test | Description |
|------|-------------|
| `nullLocationSkipped` | NULL location value does not crash index |
| `emptyWKTThrows` | Empty string WKT gives clear error |
| `invalidWKTThrows` | Malformed WKT gives clear error |
| `antimeridianQuery` | Query crossing 180/-180 longitude |
| `duplicatePointsSameCell` | Two identical points in same geohash cell |
| `veryLargePolygon` | Polygon spanning entire hemisphere caps at maxCells |
| `spatialIndexOnNonStringPropertyThrows` | SPATIAL on INTEGER property is rejected |

### Task 6.2: Performance Test

**File:** `engine/src/test/java/performance/SpatialIndexPerformanceTest.java`

```java
@Test
void nearbySearchPerformance() {
    final int TOTAL = 100_000;
    // Insert 100K random points
    // Time a SEARCH_SPATIAL NEAR query
    // Assert < 100ms for 10km radius returning ~100 results
}
```

---

## Summary

| Phase | Tasks | Key Deliverables |
|-------|-------|-----------------|
| 1. Geohash Utility | 1 task | `GeohashHelper` with encode/decode/cover |
| 2. Index Infrastructure | 4 tasks | `SpatialIndexMetadata`, `TypeSpatialIndexBuilder`, `LSMTreeSpatialIndex`, `SpatialQueryExecutor` |
| 3. SQL Functions | 6 tasks | `GeoUtils` enhancement, 17 `ST_*` functions, `SEARCH_SPATIAL()` |
| 4. Non-Point Geometry | 1 task | Polygon/LineString indexing via bbox decomposition |
| 5. Persistence & Integration | 3 tasks | Reopen survival, lat/lon convenience, backward compat |
| 6. Edge Cases & Performance | 2 tasks | Error handling, 100K point benchmark |

**Total:** ~17 tasks, ~40 new files, ~35 test methods

Each task follows TDD: write failing test → implement → verify → compile.

**No new dependencies.** All work uses existing Spatial4j 0.8 + JTS 1.20.0.

**Key files modified in existing codebase:**
- `Schema.java` — add `SPATIAL` to `INDEX_TYPE` enum
- `LocalSchema.java` — register spatial factory handler
- `TypeIndexBuilder.java` — add `withSpatialType()`, update `withType()`
- `CreateIndexStatement.java` — add `SPATIAL` to DDL parser
- `DefaultSQLFunctionFactory.java` — register 18 new functions
- `GeoUtils.java` — add `toShape()`, `parseWKT()`, `toWKT()` helpers
