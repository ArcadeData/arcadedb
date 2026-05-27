# Issue #4358 - DateTime format exception when using gRPC

## Summary

Setting a `LocalDateTime` property on a vertex via `RemoteGrpcDatabase.newVertex().set(...)` fails with
`NumberFormatException: For input string: "2026-05-26T14:27:38.470"` in
`ArcadeDbGrpcService.convertWithSchemaType`.

## Root Cause

Two-part problem:

**Client (`ProtoUtils.toGrpcValue`):** No `LocalDateTime`, `LocalDate`, `Instant`, or `ZonedDateTime`
branch exists. These types fall through to the catch-all `setStringValue(String.valueOf(value))` at line
321, producing an ISO 8601 string like `"2026-05-26T14:27:38.470"` instead of a `TIMESTAMP_VALUE`.

The server-side `GrpcTypeConverter.toGrpcValue()` already has these branches (fixed in issue #4149),
but that fix was never ported to the client-side `ProtoUtils.toGrpcValue()`.

**Server (`ArcadeDbGrpcService.convertWithSchemaType`):** The `STRING_VALUE` case for `DATE`/`DATETIME`
does `Long.parseLong(v.getStringValue())` unconditionally. This works for epoch-ms strings but throws
`NumberFormatException` for ISO 8601 strings.

## Affected Components

- `grpc-client/src/main/java/com/arcadedb/remote/grpc/utils/ProtoUtils.java`
- `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`

## Fix

1. **`ProtoUtils.toGrpcValue()`**: Add `LocalDateTime`, `LocalDate`, `Instant`, `ZonedDateTime` branches
   that emit `TIMESTAMP_VALUE` - mirroring the implementation already in `GrpcTypeConverter.toGrpcValue()`.

2. **`ArcadeDbGrpcService.convertWithSchemaType()`**: Change the `STRING_VALUE` case for `DATE`/`DATETIME`
   to try `Long.parseLong` first (backward compat), then fall back to ISO parsing via
   `DateUtils.dateTimeToTimestamp(db, s, ChronoUnit.MILLIS)`.

## Tests

- Unit: `ProtoUtilsTest.toGrpcValueLocalDateTime` - verifies client emits `TIMESTAMP_VALUE`
- Unit: `ProtoUtilsTest.toGrpcValueLocalDate` - verifies client emits `TIMESTAMP_VALUE`
- Integration: `Issue4358GrpcDateTimeIT` - end-to-end vertex creation with `LocalDateTime` property via gRPC

## Test Results

- `ProtoUtilsTest`: 39/39 pass (includes 4 new unit tests for LocalDateTime, LocalDate, Instant, ZonedDateTime)
- `Issue4358GrpcDateTimeIT`: 2/2 pass (end-to-end LocalDateTime and LocalDate round-trip via gRPC)
- `GrpcTypeConverterTest`, `Issue4149GrpcTypeConverterTest`, `Issue4181GrpcDateCorruptionIT`, `ArcadeDbGrpcServiceExtendedTest`: 71/71 pass (no regressions)
- `Issue4260ReloadInsideTransactionIT`, `RemoteGrpcDatabaseCoverageIT`: 33/33 pass

## PR

https://github.com/ArcadeData/arcadedb/pull/4362

## Review Cycles

**Cycle 1** - HEAD `d7c8c429`:
- gemini-code-assist (COMMENTED): One inline suggestion on `ProtoUtils.java` line 287 - use `ld.toEpochDay() * 86400L` instead of `ld.atStartOfDay(ZoneOffset.UTC).toEpochSecond()` to avoid object allocations. Applied.
- claude bot: not present on this repo, did not respond.

**Cycle 2** - HEAD `788cc3c9`:
- gemini-code-assist: no new actionable comments (stale cycle-1 comment only, already addressed).
- claude bot: not present on this repo.

## Final State

`timeout` (claude bot does not exist on ArcadeData/arcadedb; gemini reviewed cycle 1 and the cycle 1 feedback was fully addressed).
