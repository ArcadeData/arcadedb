# Geospatial Indexing for ArcadeDB — Design Document

## 1. Executive Summary

This document designs the porting of OrientDB's geospatial indexing and SQL functions to
ArcadeDB, storing spatial data natively in ArcadeDB's LSM-Tree index engine. The design
leverages the existing full-text index pattern (`LSMTreeFullTextIndex`) as an architectural
template, the already-present Spatial4j + JTS dependencies, and a **geohash-based encoding**
strategy that maps 2D spatial coordinates to 1D sortable strings — a natural fit for
LSM-Tree range queries.

---

## 2. Current State of ArcadeDB Geospatial Support

### 2.1 What Already Exists

ArcadeDB already has foundational geospatial pieces:

| Component | Location | Description |
|-----------|----------|-------------|
| **Shape functions** | `query/sql/function/geo/` | `point()`, `circle()`, `rectangle()`, `polygon()`, `linestring()`, `distance()` |
| **Spatial methods** | `query/sql/method/geo/` | `.isWithin()`, `.intersectsWith()` |
| **GeoUtils** | `query/sql/function/geo/GeoUtils.java` | Shared `JtsSpatialContext` factory |
| **Shape serialization** | `serializer/BinarySerializer.java:358` | Shapes auto-serialized as WKT strings |
| **GeohashUtils test** | `SQLGeoFunctionsTest.java:181` | Manual geohash + LSM_TREE range query demo |
| **Dependencies** | `engine/pom.xml` | `spatial4j:0.8`, `jts-core:1.20.0` (both Apache-compatible) |

### 2.2 What is Missing

- **No dedicated spatial index type** — users must manually compute geohashes
- **No `ST_*` SQL functions** — the OGC-standard functions from OrientDB
- **No automatic index integration** — no `SEARCH_SPATIAL()` function
- **No geohash encoding/decoding in the index layer**
- **No multi-resolution spatial queries** (nearby, within polygon, etc.)

---

## 3. OrientDB Geospatial Approach (Reference)

OrientDB's spatial module:
- Uses **Lucene Spatial** with a `RecursivePrefixTreeStrategy` (quad-tree or geohash-based)
- Stores geometries via JTS/Spatial4j shapes
- Provides `ST_*` SQL functions following OGC SQL-MM spec
- Index type: `SPATIAL ENGINE LUCENE`
- Two-phase query: **bounding-box coarse filter** (from Lucene spatial prefix tree) + **exact JTS geometry check**

**Key OrientDB Functions**: `ST_GeomFromText`, `ST_AsText`, `ST_Contains`, `ST_Within`,
`ST_Intersects`, `ST_DWithin`, `ST_Distance`, `ST_Buffer`, `ST_Envelope`, `ST_Area`,
`ST_Length`, `ST_Equals`, `ST_Disjoint`.

---

## 4. Design: Geohash-Based LSM Spatial Index

### 4.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Layer                                  │
│  ST_* Functions  │  SEARCH_SPATIAL()  │  isWithin/intersects  │
└────────┬─────────┴──────────┬─────────┴──────────────────────┘
         │                    │
         ▼                    ▼
┌────────────────┐  ┌───────────────────────────────┐
│ Geometry Ops   │  │   LSMTreeSpatialIndex          │
│ (JTS/Spatial4j)│  │   wraps LSMTreeIndex            │
│ exact predicate│  │   key = geohash (STRING)       │
│ check          │  │   val = RID[]                   │
└────────────────┘  │                                 │
                    │   put(): encode geometry bbox   │
                    │         → multi-resolution      │
                    │           geohash cells          │
                    │                                 │
                    │   get(): encode query shape      │
                    │         → geohash range scan    │
                    │         → candidate RIDs         │
                    │         → JTS exact filter        │
                    └───────────────┬──────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │      LSMTreeIndex (existing)    │
                    │   sorted STRING keys            │
                    │   range queries ✓               │
                    │   ACID, WAL, compaction ✓        │
                    │   replication, HA ✓              │
                    └─────────────────────────────────┘
```

### 4.2 Why Geohash + LSM-Tree

**Geohashes** encode (lat, lon) pairs into Base32 strings that are **lexicographically
sortable** and **prefix-hierarchical** — neighboring points share common prefixes. This is
a perfect fit for LSM-Tree indexes because:

1. **Range queries are native** — `WHERE geohash >= 'u33d' AND geohash < 'u33e'` already
   works with the existing `RangeIndex` interface
2. **Prefix = precision** — shorter geohash = larger cell, longer = smaller cell
3. **No new dependencies** — `org.locationtech.spatial4j.io.GeohashUtils` is already
   available (used in test at line 34 of `SQLGeoFunctionsTest.java`)
4. **Multi-resolution indexing** — one geometry indexed at multiple precision levels for
   different query scales

**Comparison with alternatives**:
| Approach | LSM fit | Complexity | Polygon support | Dependencies |
|----------|---------|------------|----------------|--------------|
| **Geohash cells** | Excellent (sorted strings) | Low | Via decomposition | None new |
| Lucene Spatial | Poor (separate index) | High | Native | lucene-spatial |
| R-Tree | Poor (not 1D sortable) | High | Native | New lib |
| S2/H3 cells | Good (cell IDs sortable) | Medium | Via decomposition | New lib |

### 4.3 Geohash Encoding Strategy

#### 4.3.1 Point Geometries

For a point `(lat, lon)`, we encode at multiple precision levels:

```
Precision  Cell size (approx)    Geohash example
12         ~3.7cm × 1.9cm        "u33dc0cpke67"   (maximum precision)
8          ~38m × 19m            "u33dc0cp"
6          ~1.2km × 610m         "u33dc0"
4          ~39km × 20km          "u33d"
```

**Index storage**: A single point is stored in the LSM-Tree at its **maximum precision
geohash** (e.g., precision 12). During spatial queries, we perform prefix-range scans at
the appropriate precision level.

```
LSM Key (STRING)              Value (RID[])
"u33dc0cpke67"                [#12:0, #12:5]    ← two points in same cell
"u33dc0cpke68"                [#12:1]
"u33dc1a0bc12"                [#12:2]
```

#### 4.3.2 Non-Point Geometries (Polygons, LineStrings, etc.)

For non-point geometries, we compute the geometry's **bounding box** and then
**decompose** it into covering geohash cells:

1. Compute bounding box of the geometry
2. Find the geohash precision level where cells roughly match the bbox size
3. Enumerate all geohash cells that intersect the bounding box
4. Store the RID under **each covering cell** in the LSM index

```
Example: A polygon covering central Rome
  → bbox: (41.826, 12.314) to (41.963, 12.661)
  → at precision 5: covers cells ["sr2y0", "sr2y1", "sr2y4", "sr2y5", ...]
  → each cell gets the polygon's RID in the index
```

This is analogous to how the full-text index stores one RID under multiple token keys.

#### 4.3.3 Geohash Cell Decomposition Algorithm

```java
List<String> coveringCells(Geometry geom, int maxPrecision, int maxCells) {
    Rectangle bbox = geom.getBoundingBox();
    // Find appropriate precision: start coarse, refine until cell count < maxCells
    for (int precision = 1; precision <= maxPrecision; precision++) {
        List<String> cells = enumerateCells(bbox, precision);
        if (cells.size() > maxCells)
            return enumerateCells(bbox, precision - 1);
    }
    return enumerateCells(bbox, maxPrecision);
}
```

**`maxCells`** (configurable, default 16) caps the number of index entries per geometry
to avoid index bloat for very large geometries.

---

## 5. Index Implementation

### 5.1 New Index Type: `SPATIAL`

```java
// In Schema.java
enum INDEX_TYPE {
    LSM_TREE, FULL_TEXT, LSM_VECTOR, SPATIAL  // ← new
}
```

### 5.2 Core Class: `LSMTreeSpatialIndex`

**Location**: `com.arcadedb.index.geo.LSMTreeSpatialIndex`

Follows the exact same pattern as `LSMTreeFullTextIndex`:

```java
public class LSMTreeSpatialIndex implements Index, IndexInternal {
    private final LSMTreeIndex          underlyingIndex;  // STRING keys (geohashes)
    private final SpatialIndexMetadata  metadata;
    private       TypeIndex             typeIndex;

    // --- Factory Handler (inner static class) ---
    public static class LSMTreeSpatialIndexFactoryHandler implements IndexFactoryHandler {
        @Override
        public IndexInternal create(final IndexBuilder builder) {
            // Validate: at least one STRING or DOUBLE property
            // Create underlying LSMTreeIndex with STRING key type
            // Return new LSMTreeSpatialIndex(...)
        }
    }

    // --- Index operations ---

    @Override
    public void put(Object[] keys, RID[] rids) {
        // keys[0] = geometry WKT string or Shape object
        // 1. Parse geometry from key
        // 2. Compute covering geohash cells
        // 3. For each cell: underlyingIndex.put([cellHash], rids)
    }

    @Override
    public IndexCursor get(Object[] keys, int limit) {
        // keys[0] = query shape (WKT or Shape)
        // 1. Parse query geometry
        // 2. Compute covering geohash cells for query bbox
        // 3. For each cell: range-scan underlyingIndex
        // 4. Collect candidate RIDs (deduplicate)
        // 5. For each candidate: exact JTS geometry check
        // 6. Return filtered results as IndexCursor
    }

    @Override
    public void remove(Object[] keys, Identifiable rid) {
        // 1. Parse geometry from key
        // 2. Compute covering geohash cells (same as put)
        // 3. For each cell: underlyingIndex.remove([cellHash], rid)
    }

    // Spatial-specific search methods:

    public IndexCursor searchWithin(Shape queryShape, int limit) { ... }
    public IndexCursor searchIntersects(Shape queryShape, int limit) { ... }
    public IndexCursor searchNear(Point center, double radiusKm, int limit) { ... }
    public IndexCursor searchDWithin(Shape shape, double distance, int limit) { ... }
}
```

### 5.3 Index Metadata: `SpatialIndexMetadata`

**Location**: `com.arcadedb.schema.SpatialIndexMetadata`

```java
public class SpatialIndexMetadata extends IndexMetadata {
    private int    maxPrecision    = 12;     // max geohash precision (1-12)
    private int    minPrecision    = 2;      // min geohash precision for decomposition
    private int    maxCellsPerGeom = 16;     // max index entries per non-point geometry
    private String geometryField;            // which property holds the geometry
    private String coordFormat = "WKT";      // WKT | GEOJSON | LATLON

    // Serialization to/from JSON for schema persistence
    public JSONObject toJSON() { ... }
    public void fromJSON(JSONObject json) { ... }
}
```

### 5.4 Builder: `TypeSpatialIndexBuilder`

**Location**: `com.arcadedb.schema.TypeSpatialIndexBuilder`

```java
public class TypeSpatialIndexBuilder extends TypeIndexBuilder {
    public TypeSpatialIndexBuilder withMaxPrecision(int precision) { ... }
    public TypeSpatialIndexBuilder withMinPrecision(int precision) { ... }
    public TypeSpatialIndexBuilder withMaxCells(int maxCells) { ... }
    public TypeSpatialIndexBuilder withCoordFormat(String format) { ... }
}
```

### 5.5 Registration

In `LocalSchema.java`, add alongside existing registrations:

```java
indexFactory.register(INDEX_TYPE.SPATIAL.name(),
    new LSMTreeSpatialIndex.LSMTreeSpatialIndexFactoryHandler());
```

### 5.6 DDL Support

```sql
-- Create spatial index on a WKT geometry property
CREATE INDEX Restaurant_location ON Restaurant(location) SPATIAL

-- With metadata
CREATE INDEX Restaurant_location ON Restaurant(location) SPATIAL
  {"maxPrecision": 10, "maxCells": 32}

-- On separate lat/lon properties (convenience)
CREATE INDEX Restaurant_coords ON Restaurant(latitude, longitude) SPATIAL
```

When two DOUBLE properties are specified (lat, lon), the index automatically encodes
them as a point geohash during put/get.

---

## 6. Geohash Utility Class

**Location**: `com.arcadedb.index.geo.GeohashHelper`

```java
public class GeohashHelper {
    // Encode a point to geohash at given precision
    public static String encode(double lat, double lon, int precision);

    // Decode geohash back to lat/lon center point
    public static double[] decode(String geohash);

    // Get bounding box of a geohash cell
    public static double[] decodeBbox(String geohash);  // [minLat, minLon, maxLat, maxLon]

    // Get all geohash cells covering a bounding box at given precision
    public static List<String> coverBoundingBox(
        double minLat, double minLon, double maxLat, double maxLon, int precision);

    // Get neighboring geohash cells (8 neighbors)
    public static String[] neighbors(String geohash);

    // Get the geohash range [start, end) that covers a prefix
    public static String[] prefixRange(String prefix);

    // Find optimal precision for a given bounding box
    public static int optimalPrecision(
        double minLat, double minLon, double maxLat, double maxLon, int maxCells);

    // Decompose a geometry into covering geohash cells
    public static List<String> coverGeometry(
        Shape shape, int minPrecision, int maxPrecision, int maxCells);
}
```

**Implementation note**: Internally delegates to `org.locationtech.spatial4j.io.GeohashUtils`
for basic encoding/decoding (already available), supplemented with custom cell enumeration
logic for bounding box coverage.

---

## 7. Spatial Query Executor

**Location**: `com.arcadedb.index.geo.SpatialQueryExecutor`

Analogous to `FullTextQueryExecutor`, this handles the two-phase spatial query pipeline:

```java
public class SpatialQueryExecutor {
    private final LSMTreeSpatialIndex spatialIndex;

    // Phase 1: Geohash prefix range scan → candidate RIDs
    // Phase 2: JTS exact geometry predicate → filtered results

    public IndexCursor searchWithin(Shape queryShape, int limit) {
        // 1. Compute covering cells for queryShape
        // 2. For each cell prefix, do LSM range scan to get candidate RIDs
        // 3. Deduplicate candidates
        // 4. For each candidate, load document, parse stored geometry
        // 5. Apply JTS: candidate.relate(queryShape) == WITHIN
        // 6. Build scored IndexCursor
    }

    public IndexCursor searchNear(Point center, double radiusKm, int limit) {
        // 1. Compute bounding box from center + radius
        // 2. Convert to covering geohash cells
        // 3. LSM range scan for candidates
        // 4. For each candidate: compute Haversine distance
        // 5. Filter by radius, sort by distance ascending
        // 6. Score = inverse distance (closer = higher score)
    }

    public IndexCursor searchIntersects(Shape queryShape, int limit) {
        // Similar to searchWithin but uses INTERSECTS predicate
    }

    public IndexCursor searchDWithin(Shape shape, double distance, int limit) {
        // 1. Buffer the shape by distance
        // 2. Compute covering cells for buffered shape
        // 3. LSM range scan + JTS distance check
    }
}
```

---

## 8. SQL Functions (ST_* OGC Functions)

### 8.1 Geometry Constructors

| Function | Signature | Description |
|----------|-----------|-------------|
| `ST_GeomFromText` | `ST_GeomFromText(wkt)` | Parse WKT to Shape |
| `ST_GeomFromGeoJSON` | `ST_GeomFromGeoJSON(json)` | Parse GeoJSON to Shape |
| `ST_Point` | `ST_Point(lon, lat)` | Create point (note: lon, lat order per OGC) |
| `ST_MakeEnvelope` | `ST_MakeEnvelope(xmin, ymin, xmax, ymax)` | Create rectangle |
| `ST_Buffer` | `ST_Buffer(geom, distance)` | Buffer geometry by distance |

### 8.2 Geometry Accessors / Output

| Function | Signature | Description |
|----------|-----------|-------------|
| `ST_AsText` | `ST_AsText(geom)` | Convert to WKT string |
| `ST_AsGeoJSON` | `ST_AsGeoJSON(geom)` | Convert to GeoJSON string |
| `ST_Envelope` | `ST_Envelope(geom)` | Get bounding box as polygon |
| `ST_X` | `ST_X(point)` | Get X (longitude) of point |
| `ST_Y` | `ST_Y(point)` | Get Y (latitude) of point |

### 8.3 Spatial Predicates (index-optimizable)

| Function | Signature | Index-optimized |
|----------|-----------|:---:|
| `ST_Contains` | `ST_Contains(geomA, geomB)` | Yes |
| `ST_Within` | `ST_Within(geomA, geomB)` | Yes |
| `ST_Intersects` | `ST_Intersects(geomA, geomB)` | Yes |
| `ST_DWithin` | `ST_DWithin(geomA, geomB, dist)` | Yes |
| `ST_Disjoint` | `ST_Disjoint(geomA, geomB)` | No (negate intersects) |
| `ST_Equals` | `ST_Equals(geomA, geomB)` | No |

### 8.4 Spatial Measurements

| Function | Signature | Description |
|----------|-----------|-------------|
| `ST_Distance` | `ST_Distance(geomA, geomB)` | Distance between geometries |
| `ST_Area` | `ST_Area(geom)` | Area of polygon |
| `ST_Length` | `ST_Length(geom)` | Length of linestring |

### 8.5 Spatial Search Function (index-aware)

```sql
-- Search using spatial index directly (like SEARCH_INDEX for full-text)
SELECT FROM Restaurant WHERE SEARCH_SPATIAL('Restaurant[location]', 'WITHIN',
    'POLYGON((12.31 41.82, 12.31 41.96, 12.66 41.96, 12.66 41.82, 12.31 41.82))')

-- Near query with distance
SELECT *, $distance FROM Restaurant WHERE SEARCH_SPATIAL('Restaurant[location]', 'NEAR',
    'POINT(12.49 41.89)', {"maxDistance": 5.0})
```

### 8.6 Implementation Pattern

Each `ST_*` function extends `SQLFunctionAbstract`, registered in
`DefaultSQLFunctionFactory`. Example:

```java
public class SQLFunctionSTWithin extends SQLFunctionAbstract {
    public static final String NAME = "st_within";

    @Override
    public Object execute(Object self, Identifiable record, Object result,
                          Object[] params, CommandContext context) {
        final Shape geomA = GeoUtils.toShape(params[0]);
        final Shape geomB = GeoUtils.toShape(params[1]);
        return geomA.relate(geomB) == SpatialRelation.WITHIN;
    }
}
```

---

## 9. GeoUtils Enhancement

The existing `GeoUtils` class is extended with conversion helpers:

```java
public class GeoUtils {
    // Existing
    static final JtsSpatialContextFactory FACTORY = ...;
    static final JtsSpatialContext SPATIAL_CONTEXT = ...;

    // New helpers
    public static Shape toShape(Object value) {
        if (value instanceof Shape) return (Shape) value;
        if (value instanceof String) return parseWKT((String) value);
        throw new IllegalArgumentException("Cannot convert to Shape: " + value);
    }

    public static Shape parseWKT(String wkt) {
        return SPATIAL_CONTEXT.getFormats().getWktReader().read(wkt);
    }

    public static Shape parseGeoJSON(String json) {
        return SPATIAL_CONTEXT.getFormats().getReader("GeoJSON").read(json);
    }

    public static String toWKT(Shape shape) {
        return SPATIAL_CONTEXT.getFormats().getWktWriter().toString(shape);
    }

    public static String toGeoJSON(Shape shape) {
        return SPATIAL_CONTEXT.getFormats().getWriter("GeoJSON").toString(shape);
    }

    public static Rectangle boundingBox(Shape shape) {
        return shape.getBoundingBox();
    }
}
```

---

## 10. Query Flow Example

### 10.1 Index Creation

```sql
CREATE DOCUMENT TYPE Restaurant
CREATE PROPERTY Restaurant.name STRING
CREATE PROPERTY Restaurant.location STRING  -- stores WKT
CREATE INDEX Restaurant_location ON Restaurant(location) SPATIAL
```

### 10.2 Data Insertion

```sql
INSERT INTO Restaurant SET name = 'Trattoria Roma', location = 'POINT(12.4964 41.9028)'
```

**What happens internally**:
1. The `SPATIAL` index intercepts the `put()` for the `location` property
2. Parses `POINT(12.4964 41.9028)` → Spatial4j Point
3. Encodes to geohash: `GeohashUtils.encodeLatLon(41.9028, 12.4964)` → `"sr2ykk5t6"`
4. Stores in underlying LSM index: `put(["sr2ykk5t6"], [#12:0])`

### 10.3 Spatial Query

```sql
SELECT name, ST_Distance(location, 'POINT(12.49 41.89)') AS dist
FROM Restaurant
WHERE ST_DWithin(location, 'POINT(12.49 41.89)', 5.0)
ORDER BY dist
```

**What happens internally**:
1. Query optimizer detects `ST_DWithin` on indexed property `location`
2. Delegates to `SEARCH_SPATIAL` logic
3. `SpatialQueryExecutor.searchDWithin(point, 5.0km, -1)`:
   a. Computes bbox: (41.845, 12.445) to (41.935, 12.535) (5km around center)
   b. Finds covering cells at precision 5: `["sr2yh", "sr2yk", "sr2ym", "sr2yj"]`
   c. For each cell prefix, scans LSM range: `["sr2yh", "sr2yi")`, `["sr2yk", "sr2yl")`, ...
   d. Collects candidate RIDs
   e. Loads each candidate document, parses `location` WKT
   f. Computes Haversine distance, filters `<= 5.0km`
4. Returns results with `$distance` context variable

---

## 11. File Structure

```
engine/src/main/java/com/arcadedb/
├── index/geo/
│   ├── LSMTreeSpatialIndex.java          // Main index implementation
│   ├── SpatialQueryExecutor.java         // Two-phase query engine
│   └── GeohashHelper.java               // Geohash encoding/decomposition
├── schema/
│   ├── SpatialIndexMetadata.java         // Index configuration
│   └── TypeSpatialIndexBuilder.java      // Builder with fluent API
├── query/sql/function/geo/
│   ├── GeoUtils.java                     // Enhanced (WKT/GeoJSON parse)
│   ├── SQLFunctionSTGeomFromText.java    // ST_GeomFromText
│   ├── SQLFunctionSTGeomFromGeoJSON.java // ST_GeomFromGeoJSON
│   ├── SQLFunctionSTAsText.java          // ST_AsText
│   ├── SQLFunctionSTAsGeoJSON.java       // ST_AsGeoJSON
│   ├── SQLFunctionSTPoint.java           // ST_Point (OGC lon,lat order)
│   ├── SQLFunctionSTMakeEnvelope.java    // ST_MakeEnvelope
│   ├── SQLFunctionSTBuffer.java          // ST_Buffer
│   ├── SQLFunctionSTEnvelope.java        // ST_Envelope
│   ├── SQLFunctionSTX.java              // ST_X
│   ├── SQLFunctionSTY.java              // ST_Y
│   ├── SQLFunctionSTContains.java        // ST_Contains
│   ├── SQLFunctionSTWithin.java          // ST_Within
│   ├── SQLFunctionSTIntersects.java      // ST_Intersects
│   ├── SQLFunctionSTDWithin.java         // ST_DWithin
│   ├── SQLFunctionSTDisjoint.java        // ST_Disjoint
│   ├── SQLFunctionSTEquals.java          // ST_Equals
│   ├── SQLFunctionSTDistance.java        // ST_Distance (geodesic)
│   ├── SQLFunctionSTArea.java            // ST_Area
│   ├── SQLFunctionSTLength.java          // ST_Length
│   ├── SQLFunctionSearchSpatial.java     // SEARCH_SPATIAL (index-aware)
│   └── (existing) SQLFunctionPoint.java, etc.

engine/src/test/java/com/arcadedb/index/geo/
│   ├── LSMTreeSpatialIndexTest.java      // Unit tests for index
│   ├── GeohashHelperTest.java            // Unit tests for geohash utils
│   └── SpatialQueryExecutorTest.java     // Integration tests
engine/src/test/java/com/arcadedb/query/sql/function/geo/
│   └── SQLGeoSTFunctionsTest.java        // ST_* function tests
```

---

## 12. Dependencies

**No new dependencies required.** Everything uses already-present libraries:

| Library | Version | License | Already in pom.xml | Purpose |
|---------|---------|---------|:--:|---------|
| `spatial4j` | 0.8 | Apache 2.0 | Yes | Shape model, GeohashUtils, spatial relations |
| `jts-core` | 1.20.0 | EPL 2.0 / EDL 1.0 | Yes | Complex geometry ops (buffer, area, intersection) |
| `lucene-*` | 10.3.2 | Apache 2.0 | Yes | Not needed for spatial (only for full-text) |

---

## 13. Implementation Phases

### Phase 1: Core Index + Point Queries
1. `GeohashHelper` — encoding, decoding, cell enumeration
2. `LSMTreeSpatialIndex` — put/get/remove for point geometries
3. `SpatialIndexMetadata` + `TypeSpatialIndexBuilder`
4. Registration in `Schema.INDEX_TYPE` and `LocalSchema`
5. DDL: `CREATE INDEX ... SPATIAL`
6. `SEARCH_SPATIAL()` function for explicit index queries
7. Tests for point insertion and nearby/within-bbox queries

### Phase 2: ST_* SQL Functions
1. Geometry constructors: `ST_GeomFromText`, `ST_Point`, `ST_MakeEnvelope`
2. Geometry output: `ST_AsText`, `ST_AsGeoJSON`
3. Predicates: `ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_DWithin`
4. Measurements: `ST_Distance`, `ST_Area`, `ST_Length`
5. Accessors: `ST_X`, `ST_Y`, `ST_Envelope`, `ST_Buffer`
6. Register all in `DefaultSQLFunctionFactory`

### Phase 3: Non-Point Geometry Indexing
1. Bounding-box decomposition into geohash cells
2. Multi-cell indexing for polygons and linestrings
3. `SpatialQueryExecutor` with full JTS exact-filter pipeline
4. Tests for polygon-within-polygon, intersection, etc.

### Phase 4: Query Optimizer Integration
1. Detect `ST_Within`, `ST_DWithin`, etc. in WHERE clauses
2. Automatically use spatial index when available (like range index optimization)
3. `$distance` context variable support

---

## 14. Trade-offs and Limitations

### 14.1 Geohash Edge Cases
- **Boundary problem**: Points near geohash cell boundaries may be missed in naive
  prefix queries. Mitigation: always query neighboring cells (8 neighbors) for
  near/dwithin queries.
- **Pole/anti-meridian**: Geohash has known issues near poles and the ±180° meridian.
  For typical use cases (cities, buildings) this is not a concern.

### 14.2 Non-Point Geometry Scaling
- Large polygons (countries, oceans) generate many covering cells. The `maxCells`
  limit caps this, but at the cost of precision (larger cells → more false positives
  in Phase 1, more work in Phase 2 exact filter).
- Very thin/elongated geometries may have poor geohash coverage efficiency.

### 14.3 Comparison with Lucene Spatial
- **Lucene Spatial** uses a `RecursivePrefixTreeStrategy` with adaptive-precision cells,
  which is more sophisticated for complex geometries
- **Our approach** trades some sophistication for simplicity and full integration with
  ArcadeDB's ACID/WAL/replication/HA stack via the existing LSM-Tree
- For most real-world use cases (restaurant finders, asset trackers, POI queries), the
  geohash approach performs excellently

### 14.4 Future Enhancements
- **S2 cell IDs**: Could be added as an alternative encoding (uint64 keys instead of
  strings) for better pole/meridian handling
- **R-Tree overlay**: For heavy polygon-vs-polygon workloads, an in-memory R-Tree
  could serve as an additional filter layer
- **Spatial aggregation**: `ST_Union`, `ST_Collect` for result set geometry merging
