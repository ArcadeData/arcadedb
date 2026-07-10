# Issue #5045 - gRPC temporal (DATE/DATETIME) type fidelity

## Symptom
Temporal values do not round-trip faithfully over gRPC:
- **COR-5**: the client (`ProtoUtils.fromGrpcValue`) maps every `TIMESTAMP_VALUE` to `tsToMillis(...)`, returning a bare `Long` epoch-millis. Sub-millisecond precision (DATETIME_MICROS/NANOS) is lost and the temporal type identity is gone.
- **COR-6**: DATE/DATETIME encode is UTC-anchored but the client decode returned a raw epoch-millis Long, so consumers had to re-apply a timezone to reconstruct a date, risking an off-by-one day.
- **COR-12**: the server `DATETIME_SECOND/MICROS/NANOS` STRING branch in `convertWithSchemaType` did `new Date(Long.parseLong(s))`, so an ISO-8601 string throws `NumberFormatException` and `java.util.Date` caps sub-millisecond precision.

## Root cause
- Client `fromGrpcValue` ignored the `logical_type` tag the server always sets (`"date"` / `"datetime"`) and collapsed the proto Timestamp to epoch-millis.
- The high-precision STRING branch on the server only accepted numeric epoch-millis strings and used `java.util.Date`.

## Fix
- `grpc-client` `ProtoUtils.fromGrpcValue` TIMESTAMP_VALUE: reconstruct the temporal type from `logical_type` symmetrically with the encode side (UTC-anchored):
  - `"date"` -> `LocalDate.ofEpochDay(floorDiv(seconds, 86400))`
  - `"datetime"` -> `LocalDateTime.ofEpochSecond(seconds, nanos, UTC)` (preserves micros/nanos)
  - no `logical_type` -> unchanged legacy `Long` epoch-millis (keeps `ProtoUtilsTest.fromGrpcValueTimestamp` green and backward-compatible).
- `grpcw` `ArcadeDbGrpcService.convertWithSchemaType` DATETIME_SECOND/MICROS/NANOS STRING branch: numeric string -> `Instant.ofEpochMilli(...)`; otherwise `DateUtils.dateTimeToTimestamp(db, s, NANOS)` -> `Instant.ofEpochSecond(0, nanos)` so ISO-8601 is accepted and sub-millisecond precision survives (engine truncates to the column precision).

## Tests
- `ProtoUtilsTemporalDecodeTest` (grpc-client unit): date/datetime/micros/nanos decode to the right temporal type and value; bare timestamp still decodes to Long.
- `Issue5045GrpcTemporalFidelityIT` (grpc-client IT): full round-trip through `RemoteGrpcDatabase` for DATE, DATETIME, DATETIME_MICROS, DATETIME_NANOS asserting temporal type + exact value on the client.
- `Issue5045GrpcDatetimeStringPrecisionIT` (grpcw IT): sending an ISO-8601 string to a DATETIME_NANOS column no longer throws and preserves nanosecond precision.

## Impact
Silent precision loss and type-identity loss on temporal reads over gRPC are fixed. Backward compatible: timestamps without a `logical_type` still decode to `Long`. Out of scope (noted for follow-up): explicit proto representations for `LocalTime`/TIME, zoned datetime offset retention, and `Duration`.
