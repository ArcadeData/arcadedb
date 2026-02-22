# Geospatial Indexing Design

**Date:** 2026-02-22
**Branch:** lsmtree-geospatial
**Status:** Approved

## Overview

Port OrientDB-style geospatial indexing to ArcadeDB, using the native LSM-Tree engine as storage (following the same pattern as `LSMTreeFullTextIndex`) and the OGC/PostGIS `ST_*` SQL function naming convention.

## Goals

- Support all OGC spatial predicate functions OrientDB supported: `ST_Within`, `ST_Intersects`, `ST_Contains`, `ST_DWithin`, `ST_Disjoint`, `ST_Equals`, `ST_Crosses`, `ST_Overlaps`, `ST_Touches`
- Replace existing non-standard geo functions (`point()`, `distance()`, `circle()`, etc.) with `ST_*` equivalents
- Automatic query optimizer integration — no explicit `search_index()` call needed
- WKT as the geometry storage format (consistent with existing partial support)
- LSM-Tree as storage backend (ACID, WAL, HA, compaction all inherited for free)

## Non-Goals

- GeoJSON storage format
- New native schema `Type` entries for geometry (WKT strings in existing STRING properties)
- 3D geometry support
- Raster data

## Architecture

### Layers

```
SQL Query:  WHERE ST_Within(location, ST_GeomFromText('POLYGON(...)')) = true
                │
                ▼
    SelectExecutionPlanner
    detects IndexableSQLFunction on ST_Within
    calls allowsIndexedExecution()
                │
                ▼
    LSMTreeGeoIndex.get(shape)
    decomposes shape → GeoHash tokens via lucene-spatial-extras
    looks up each token in underlying LSMTreeIndex
    returns candidate RIDs
                │
                ▼
    ST_Within.shouldExecuteAfterSearch() = true
    → exact Spatial4j predicate post-filters candidates
```

### Dependencies

- `lucene-spatial-extras` (version 10.3.2, Apache 2.0) — adds `GeohashPrefixTree` and `RecursivePrefixTreeStrategy` for geometry decomposition into GeoHash tokens. Lucene core is already a dependency; this is a sibling module.
- `spatial4j` 0.8 — already present
- `jts-core` 1.20.0 — already present

## Component 1: LSMTreeGeoIndex

**Package:** `com.arcadedb.index.geospatial`
**File:** `LSMTreeGeoIndex.java`

Wraps `LSMTreeIndex` (identical to how `LSMTreeFullTextIndex` wraps it).

### Indexing (`put(keys, rid)`)

1. Parse the WKT string value using Spatial4j/JTS → `Shape`
2. Call `RecursivePrefixTreeStrategy.createIndexableFields(shape)` → Lucene `Field[]`
3. Extract string token values from the `TextField` among those fields
4. Store each token → RID in the underlying `LSMTreeIndex` (non-unique)

### Querying (`get(keys)`)

1. The key is a `Shape` (passed from the ST_* function)
2. Generate covering GeoHash cells via `SpatialArgs` + `RecursivePrefixTreeStrategy`
3. Extract cell token strings from the Lucene query
4. Look up each token in the LSM-Tree, union all matching RIDs
5. Return `TempIndexCursor` (candidates; exact post-filter happens in the ST_* function)

### Configuration

- **Precision level:** configurable at index creation (default: 11, ~2.4m resolution). Stored in index metadata JSON.
- **Metadata class:** `GeoIndexMetadata` (analogous to `FullTextIndexMetadata`)

### Schema Registration

- Add `GEOSPATIAL` to `Schema.INDEX_TYPE` enum
- Register `LSMTreeGeoIndex.GeoIndexFactoryHandler` in `LocalSchema` alongside `LSM_TREE`, `FULL_TEXT`, `LSM_VECTOR`

## Component 2: ST_* SQL Functions

**Package:** `com.arcadedb.function.sql.geo`
**Registered in:** `DefaultSQLFunctionFactory`

### Constructor / Accessor Functions (pure compute, no index)

| Function | Replaces | Notes |
|---|---|---|
| `ST_GeomFromText(wkt)` | — | Parse any WKT string → Spatial4j `Shape` |
| `ST_Point(x, y)` | `point(x,y)` | Returns Spatial4j `Point` as WKT |
| `ST_LineString(pts)` | `lineString(pts)` | |
| `ST_Polygon(pts)` | `polygon(pts)` | |
| `ST_Buffer(geom, dist)` | `circle(c,r)` | OGC buffer around any geometry |
| `ST_Envelope(geom)` | `rectangle(pts)` | Bounding rectangle as WKT |
| `ST_Distance(g1, g2 [,unit])` | `distance(...)` | Haversine; keeps SQL and Cypher-style params |
| `ST_Area(geom)` | — | Area in square degrees via Spatial4j |
| `ST_AsText(geom)` | — | Spatial4j `Shape` → WKT string |
| `ST_AsGeoJson(geom)` | — | Shape → GeoJSON string via JTS |
| `ST_X(point)` | — | Extract X coordinate |
| `ST_Y(point)` | — | Extract Y coordinate |

### Spatial Predicate Functions (implement `SQLFunction` + `IndexableSQLFunction`)

| Function | Semantics | Post-filter |
|---|---|---|
| `ST_Within(g, shape)` | g is fully within shape | yes |
| `ST_Intersects(g, shape)` | g and shape share any point | yes |
| `ST_Contains(g, shape)` | g fully contains shape | yes |
| `ST_DWithin(g, shape, dist)` | g is within dist of shape | yes |
| `ST_Disjoint(g, shape)` | g and shape share no points | yes |
| `ST_Equals(g, shape)` | geometrically equal | yes |
| `ST_Crosses(g, shape)` | g crosses shape | yes |
| `ST_Overlaps(g, shape)` | g overlaps shape | yes |
| `ST_Touches(g, shape)` | g touches shape boundary | yes |

All predicates return `null` when either argument is null (SQL three-valued logic).

**Implementation notes on `allowsIndexedExecution()`:**

- `ST_Disjoint` — returns `false`. The GeoHash index stores records whose geometry intersects
  the indexed cells. Disjoint records are precisely those *not* present in the intersection
  result, so the index cannot produce a valid candidate superset. The predicate always falls
  back to a full scan with inline evaluation.
- `ST_DWithin` — returns `false`. The current implementation evaluates proximity as a
  straight-line distance between geometry centers. The GeoHash index returns cells that
  intersect the query shape, which does not correspond to a distance radius. Correct indexed
  proximity would require first expanding the search geometry into a bounding circle before
  GeoHash querying; this is a planned future enhancement. The predicate always falls back to
  full scan.

Each predicate's `IndexableSQLFunction` implementation:
- `allowsIndexedExecution()` — returns `true` when first argument is a bare field reference AND a `GEOSPATIAL` index exists on that field in the target type
- `canExecuteInline()` — always `true` (falls back to full-scan with exact Spatial4j predicate if no index)
- `shouldExecuteAfterSearch()` — always `true` (index returns superset; exact predicate post-filters)
- `searchFromTarget()` — resolves the field's `LSMTreeGeoIndex`, evaluates the shape argument, calls `index.get(shape)`, returns `Iterable<Record>`

## Component 3: Query Optimizer Integration

No changes to `SelectExecutionPlanner` required. The existing `indexedFunctionConditions` path fully supports this pattern:

1. `block.getIndexedFunctionConditions(typez, context)` collects conditions where the left `Expression` is a function call implementing `IndexableSQLFunction`
2. `ST_Within.allowsIndexedExecution()` checks for a `GEOSPATIAL` index on the referenced field
3. `BinaryCondition.executeIndexedFunction()` → `ST_Within.searchFromTarget()` executes the indexed search
4. `shouldExecuteAfterSearch() = true` → exact post-filter applied to all returned candidates

**Multi-bucket:** `searchFromTarget()` iterates all per-bucket `LSMTreeGeoIndex` instances via `TypeIndex.getIndexesOnBuckets()` and unions results, matching the full-text search pattern.

## Error Handling

| Scenario | Behavior |
|---|---|
| Invalid WKT in `ST_GeomFromText()` | `IllegalArgumentException` with clear message |
| Null geometry argument in predicate | returns `null` (three-valued SQL logic) |
| No geospatial index on field | falls back to full-scan; no error |
| Non-WKT value in indexed property | `put()` skips record, logs warning |
| Antimeridian / polar shapes | handled correctly by `GeohashPrefixTree` |
| Precision change after indexing | must rebuild index (same as full-text analyzer change) |

## Testing

All tests in `engine/src/test/java/com/arcadedb/`:

### `index/geospatial/LSMTreeGeoIndexTest`
- Index and query a point; verify RID returned
- Index and query a circle; verify candidates include nearby points
- Index and query a polygon; verify post-filter removes false positives
- Null / invalid WKT handling
- No-index fallback path

### `function/sql/geo/SQLGeoFunctionsTest` (extend existing)
- All ST_* constructor and accessor functions
- Verify old `point()`, `distance()`, etc. throw "unknown function"

### `function/sql/geo/SQLGeoIndexedQueryTest` (new)
- Create type with `GEOSPATIAL` index on WKT property
- Insert records with point WKT values at known coordinates
- `SELECT ... WHERE ST_Within(...) = true` — verify correct results
- `SELECT ... WHERE ST_Intersects(...) = true` — verify
- `SELECT ... WHERE ST_DWithin(..., dist) = true` — proximity radius query
- All nine predicate functions covered
- Query with no index (fallback) produces same results

All assertions use `assertThat(...).isTrue()` / `isFalse()` / `isEqualTo()` per project conventions.

## File Layout

```
engine/src/main/java/com/arcadedb/
  index/geospatial/
    LSMTreeGeoIndex.java
    GeoIndexMetadata.java
  function/sql/geo/
    SQLFunctionST_GeomFromText.java
    SQLFunctionST_Point.java
    SQLFunctionST_LineString.java
    SQLFunctionST_Polygon.java
    SQLFunctionST_Buffer.java
    SQLFunctionST_Envelope.java
    SQLFunctionST_Distance.java
    SQLFunctionST_Area.java
    SQLFunctionST_AsText.java
    SQLFunctionST_AsGeoJson.java
    SQLFunctionST_X.java
    SQLFunctionST_Y.java
    SQLFunctionST_Within.java        ← implements IndexableSQLFunction
    SQLFunctionST_Intersects.java    ← implements IndexableSQLFunction
    SQLFunctionST_Contains.java      ← implements IndexableSQLFunction
    SQLFunctionST_DWithin.java       ← implements IndexableSQLFunction
    SQLFunctionST_Disjoint.java      ← implements IndexableSQLFunction
    SQLFunctionST_Equals.java        ← implements IndexableSQLFunction
    SQLFunctionST_Crosses.java       ← implements IndexableSQLFunction
    SQLFunctionST_Overlaps.java      ← implements IndexableSQLFunction
    SQLFunctionST_Touches.java       ← implements IndexableSQLFunction
    GeoUtils.java                    ← extend existing
    LightweightPoint.java            ← keep existing

engine/src/test/java/com/arcadedb/
  index/geospatial/
    LSMTreeGeoIndexTest.java
  function/sql/geo/
    SQLGeoFunctionsTest.java         ← extend existing
    SQLGeoIndexedQueryTest.java      ← new

engine/pom.xml                       ← add lucene-spatial-extras dependency
```

## Open Questions

- Should `ST_Distance` return meters by default (Neo4j/Cypher compat) or kilometers (current `distance()` SQL default)? Current implementation keeps both styles based on argument count — recommend preserving this.
- Should `ST_Buffer` accept distance in meters, kilometers, or degrees? Spatial4j works in degrees; conversion at the function boundary needed for user-facing meter/km inputs.
