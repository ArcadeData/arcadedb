# Issue #4805 - gRPC write path truncates DATETIME_MICROS/DATETIME_NANOS to milliseconds

- Issue: https://github.com/ArcadeData/arcadedb/issues/4805
- Type: bug
- Branch: fix/4805-grpc-datetime-micros-nanos-truncation

## Summary

The gRPC record write path (`createRecord` / `updateRecord`) routes property
conversion through `ArcadeDbGrpcService.convertWithSchemaType`. For columns typed
`DATETIME_SECOND` / `DATETIME_MICROS` / `DATETIME_NANOS`, the `TIMESTAMP_VALUE`
branch converted the inbound proto `Timestamp` via
`new Date(GrpcTypeConverter.tsToMillis(...))`, collapsing the value to
millisecond precision before the engine ever saw it. Sub-millisecond precision
(micros / nanos) that the column is declared to keep was silently discarded.

The parameter-binding path (`fromGrpcValue`, fixed in #4149) already keeps full
precision by returning `Instant.ofEpochSecond(sec, nanos)` and letting
`Type.convert()` truncate to the column's declared precision via
`DateUtils.getPrecisionFromType(...)`. The write path disagreed with the
parameter path.

## Root cause

`ArcadeDbGrpcService.convertWithSchemaType`, `DATETIME_SECOND/MICROS/NANOS`
branch, `TIMESTAMP_VALUE` case:

```java
case TIMESTAMP_VALUE -> new Date(GrpcTypeConverter.tsToMillis(v.getTimestampValue()));
```

`java.util.Date` is millisecond-resolution, so micro/nano digits are lost up
front. The fix is to hand the engine an `Instant` carrying the full nanosecond
precision and let `Type.convert()` truncate to the declared column precision -
exactly mirroring `fromGrpcValue`.

## Expected vs actual

- Write a `DATETIME_NANOS` column with proto Timestamp nanos=123_456_789.
- Actual (before fix): stored value truncated to .123 (milliseconds).
- Expected: stored value keeps .123456789 (nanoseconds).

## Fix

1. Add a static helper `GrpcTypeConverter.tsToInstant(Timestamp)` mirroring the
   existing `tsToMillis`, returning `Instant.ofEpochSecond(sec, nanos)`.
2. In `ArcadeDbGrpcService.convertWithSchemaType`, the
   `DATETIME_SECOND/MICROS/NANOS` `TIMESTAMP_VALUE` case now returns
   `GrpcTypeConverter.tsToInstant(...)` instead of `new Date(tsToMillis(...))`.

The millisecond-precision `DATE`/`DATETIME` branch is intentionally left
unchanged: `DATETIME` is millisecond precision and `DATE` is date precision, so
`new Date(tsToMillis(...))` loses nothing there.

## Tests

- New unit test in `GrpcTypeConverterTest` for `tsToInstant` precision.
- New IT `Issue4805GrpcDatetimePrecisionIT` exercising the `createRecord` write
  path end-to-end for DATETIME_NANOS and DATETIME_MICROS columns, reading the
  value back and asserting the preserved precision.

## Verification

- `mvn -pl grpcw -am test -Dtest=...` (scoped to grpcw; ha-raft has an unrelated
  pre-existing test-compile error on main HEAD).
