# Fix #4692: gRPC ExecuteQuery drops null-valued projected columns

## Issue

`ArcadeDbGrpcService.convertResultToGrpcRecord` skips any property whose value is `null` when building the `GrpcRecord.properties` map. This means a query like `SELECT sqrt(-4) AS r` returns `{"r": null}` over HTTP but returns a record with no `r` key at all over gRPC. Clients cannot distinguish "column not projected" from "column projected but null".

The same `if (value != null)` guard exists in `convertPropToGrpcValue`, and that method also calls `propValue.getClass()` in the log statement, which throws NPE if the value is null - meaning the streaming paths would crash rather than silently drop null columns.

## Root Cause

`convertResultToGrpcRecord` (line 3212):
```java
if (value != null) {
    // ... only non-null values reach putProperties
    builder.putProperties(propertyName, gv);
}
```

`convertPropToGrpcValue` both overloads call `propValue.getClass()` in the log line without a null check.

`toGrpcValue(null)` already returns a valid default `GrpcValue` (no `kind` set - the protobuf "unset" sentinel), so the fix is simply to remove the guard and fix the log statements.

## Fix

1. `convertResultToGrpcRecord`: Remove the `if (value != null)` guard; always call `toGrpcValue(value)` and put the result in the map.
2. Both `convertPropToGrpcValue` overloads: guard the `propValue.getClass()` log call against null.

## Affected Files

- `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`
- `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

## Tests

Regression test `executeQueryKeepsNullValuedProjectedColumn` added to `GrpcServerIT`:
- Queries `SELECT sqrt(-4) AS r` via gRPC `ExecuteQuery`
- Asserts the key `r` is present in `getPropertiesMap()`
- Asserts the value has no `kind` set (unset GrpcValue - the null representation)

Additional test `executeQueryKeepsMultipleNullProjectedColumns` covers multi-column projections where some are null and some are not.

## Test Results

- `GrpcServerIT#executeQueryKeepsNullValuedProjectedColumn` - PASS
- `GrpcServerIT#executeQueryKeepsMultipleNullAndNonNullProjectedColumns` - PASS
- Full `GrpcServerIT` suite (30 tests) - all PASS, no regressions

## Changes Made

### `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`

1. `convertResultToGrpcRecord` (line ~3209): Removed `if (value != null)` guard. All projected columns now reach `putProperties`, with null values producing an unset `GrpcValue` (the existing null handling in `toGrpcValue`).
2. Both `convertPropToGrpcValue` overloads: Fixed NPE in log call where `propValue.getClass()` would throw when `propValue` is null. Changed to conditional `propValue == null ? "null" : propValue.getClass()`.

### `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcServerIT.java`

Added two regression tests:
- `executeQueryKeepsNullValuedProjectedColumn` - single null-valued column via `SELECT sqrt(-4) AS r`
- `executeQueryKeepsMultipleNullAndNonNullProjectedColumns` - mix of null and non-null columns
