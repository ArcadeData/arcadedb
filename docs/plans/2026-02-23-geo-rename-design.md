# Design: Rename ST_* Geo Functions to geo.* Namespace

**Date:** 2026-02-23
**Branch:** lsmtree-geospatial
**Scope:** Pure rename — no behavior changes

## Background

The geospatial feature introduced 21 SQL functions using the PostGIS/OGC `ST_*` prefix convention.
The project maintainer requested alignment with ArcadeDB's established dot-namespace pattern
(e.g., `vector.neighbors`, `vector.cosineSimilarity`) by moving to a `geo.*` prefix instead.
No users are expected to be migrating PostGIS queries to ArcadeDB, so backward compatibility
aliases are not needed.

## Function Name Mapping

| Old SQL name       | New SQL name        | Old Java class               | New Java class              |
|--------------------|---------------------|------------------------------|-----------------------------|
| `ST_GeomFromText`  | `geo.geomFromText`  | `SQLFunctionST_GeomFromText` | `SQLFunctionGeoGeomFromText`|
| `ST_Point`         | `geo.point`         | `SQLFunctionST_Point`        | `SQLFunctionGeoPoint`       |
| `ST_LineString`    | `geo.lineString`    | `SQLFunctionST_LineString`   | `SQLFunctionGeoLineString`  |
| `ST_Polygon`       | `geo.polygon`       | `SQLFunctionST_Polygon`      | `SQLFunctionGeoPolygon`     |
| `ST_Buffer`        | `geo.buffer`        | `SQLFunctionST_Buffer`       | `SQLFunctionGeoBuffer`      |
| `ST_Envelope`      | `geo.envelope`      | `SQLFunctionST_Envelope`     | `SQLFunctionGeoEnvelope`    |
| `ST_Distance`      | `geo.distance`      | `SQLFunctionST_Distance`     | `SQLFunctionGeoDistance`    |
| `ST_Area`          | `geo.area`          | `SQLFunctionST_Area`         | `SQLFunctionGeoArea`        |
| `ST_AsText`        | `geo.asText`        | `SQLFunctionST_AsText`       | `SQLFunctionGeoAsText`      |
| `ST_AsGeoJson`     | `geo.asGeoJson`     | `SQLFunctionST_AsGeoJson`    | `SQLFunctionGeoAsGeoJson`   |
| `ST_X`             | `geo.x`             | `SQLFunctionST_X`            | `SQLFunctionGeoX`           |
| `ST_Y`             | `geo.y`             | `SQLFunctionST_Y`            | `SQLFunctionGeoY`           |
| `ST_Within`        | `geo.within`        | `SQLFunctionST_Within`       | `SQLFunctionGeoWithin`      |
| `ST_Intersects`    | `geo.intersects`    | `SQLFunctionST_Intersects`   | `SQLFunctionGeoIntersects`  |
| `ST_Contains`      | `geo.contains`      | `SQLFunctionST_Contains`     | `SQLFunctionGeoContains`    |
| `ST_DWithin`       | `geo.dWithin`       | `SQLFunctionST_DWithin`      | `SQLFunctionGeoDWithin`     |
| `ST_Disjoint`      | `geo.disjoint`      | `SQLFunctionST_Disjoint`     | `SQLFunctionGeoDisjoint`    |
| `ST_Equals`        | `geo.equals`        | `SQLFunctionST_Equals`       | `SQLFunctionGeoEquals`      |
| `ST_Crosses`       | `geo.crosses`       | `SQLFunctionST_Crosses`      | `SQLFunctionGeoCrosses`     |
| `ST_Overlaps`      | `geo.overlaps`      | `SQLFunctionST_Overlaps`     | `SQLFunctionGeoOverlaps`    |
| `ST_Touches`       | `geo.touches`       | `SQLFunctionST_Touches`      | `SQLFunctionGeoTouches`     |

Base class: `SQLFunctionST_Predicate` → `SQLFunctionGeoPredicate`

## Affected Files

### Production code
- **21 files** in `engine/src/main/java/com/arcadedb/function/sql/geo/`:
  - Rename each `.java` file
  - Update `class` declaration and `NAME` constant
  - Update any cross-references to other `NAME` constants (e.g., predicate subclasses)
- **`DefaultSQLFunctionFactory.java`**: update all imports and `register()` calls
- **`CypherFunctionFactory.java`**: update import from `SQLFunctionST_Distance` → `SQLFunctionGeoDistance`;
  the `distance` bridge uses `SQLFunctionGeoDistance.NAME` so it auto-resolves to `"geo.distance"`

### Test code
- **`SQLGeoFunctionsTest.java`**: ~72 occurrences of `ST_*` in SQL query strings → `geo.*`
- **`SQLGeoIndexedQueryTest.java`**: ~26 occurrences of `ST_*` in SQL query strings → `geo.*`

### Docs
- **`docs/plans/2026-02-22-geospatial-design.md`**: update function name references
- **`docs/plans/2026-02-22-geospatial-implementation.md`**: update function name references

## What Does NOT Change

- Java package paths (`com.arcadedb.function.sql.geo`) — no package move
- Index type name (`GEOSPATIAL`)
- `GeoUtils`, `LightweightPoint`, `CypherPointFunction` — no rename needed
- All logic, behavior, and index integration

## Verification

Run after each file change:
```
mvn test -pl engine -Dtest="SQLGeoFunctionsTest,SQLGeoIndexedQueryTest,LSMTreeGeoIndexTest,LSMTreeGeoIndexSchemaTest,GeoIndexMetadataTest"
```

Final full compile:
```
mvn clean install -DskipTests -pl engine
```
