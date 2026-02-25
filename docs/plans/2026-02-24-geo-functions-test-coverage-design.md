# Geo Functions Test Coverage — Design

**Date:** 2026-02-24
**Branch:** lsmtree-geospatial

## Problem

`SQLGeoFunctionsTest.java` is one large file covering all 20 geo SQL functions with SQL-only
happy-path and null-first-arg tests. It lacks:

- Direct Java `execute()` tests (no `new SQLFunctionGeoXxx().execute(...)` calls)
- Error/wrong-path coverage (invalid WKT, wrong arg count, invalid unit, bad list elements)
- Clear per-function isolation

## Decision

**File split:** Group by logical category (4 new files). Rename the existing file to keep only
its two geohash index tests.

**Within-file structure:** `@Nested` inner class per function, each containing SQL and Java
execute() tests plus error paths.

## File Structure

```
engine/src/test/java/com/arcadedb/function/sql/geo/
  GeoHashIndexTest.java              # renamed: keeps geoManualIndexPoints + geoManualIndexBoundingBoxes only
  GeoConstructionFunctionsTest.java  # NEW: geomFromText, point, lineString, polygon
  GeoMeasurementFunctionsTest.java   # NEW: buffer, distance, area, envelope
  GeoConversionFunctionsTest.java    # NEW: asText, asGeoJson, x, y
  GeoPredicateFunctionsTest.java     # NEW: within, intersects, contains, dWithin,
                                     #       disjoint, equals, crosses, overlaps, touches
```

## Test Method Pattern (per @Nested class)

| Method | Approach |
|---|---|
| `sqlHappyPath()` | SQL via `TestHelper.executeInNewDatabase` + `db.query(...)` |
| `javaExecuteHappyPath()` | `new SQLFunctionGeoXxx().execute(null, null, null, params, null)` |
| `nullFirstArg_returnsNull()` | SQL with null arg |
| `nullFirstArg_execute_returnsNull()` | Java execute with null arg |
| function-specific error methods | See per-group error paths below |

SQL tests require a database context (`TestHelper.executeInNewDatabase`).
Java `execute()` tests do **not** require a database — all functions use only `iParams`.

## Error Path Coverage

### GeoConstructionFunctionsTest
- `GeomFromText`: invalid WKT → `IllegalArgumentException`; empty string → `IllegalArgumentException`
- `Point`: one arg only → returns null; both args null → returns null
- `LineString`: list with invalid element → `IllegalArgumentException`; single-point list (degenerate valid case)
- `Polygon`: open ring → auto-closed (already covered in old file, keep it); invalid element → `IllegalArgumentException`

### GeoMeasurementFunctionsTest
- `Buffer`: null distance arg → returns null; invalid geometry string → `IllegalArgumentException`
- `Distance`: invalid unit `"lightyear"` → `IllegalArgumentException`; second arg null → returns null
- `Area`: point geometry → returns 0.0 (point has zero area)
- `Envelope`: invalid WKT → `IllegalArgumentException`

### GeoConversionFunctionsTest
- `AsText`: Shape object input (not raw string) → returns WKT; null → null
- `AsGeoJson`: LineString → `"LineString"` GeoJSON type; Polygon → `"Polygon"` GeoJSON type
- `X` / `Y`: polygon input → returns null (silently); invalid WKT string → returns null (silently)

### GeoPredicateFunctionsTest
- All predicates: second arg null → returns null
- `DWithin`: third arg null → returns null; negative distance → false
- `Crosses`: polygon vs polygon (same type, never crosses) → false

## Assertions Style

Use AssertJ throughout:
```java
assertThat(result).isNotNull();
assertThat(result).isInstanceOf(Shape.class);
assertThat(result).isEqualTo("POINT (10 20)");
assertThatThrownBy(() -> fn.execute(null, null, null, params, null))
    .isInstanceOf(IllegalArgumentException.class);
```

## Non-Goals

- No performance tests
- No index interaction tests (those belong in `GeoHashIndexTest`)
- No changes to production code
