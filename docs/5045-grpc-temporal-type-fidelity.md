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
Silent precision loss and type-identity loss on temporal reads over gRPC are fixed. Out of scope (noted for follow-up): explicit proto representations for `LocalTime`/TIME, zoned datetime offset retention, and `Duration`.

## PR
https://github.com/ArcadeData/arcadedb/pull/5196

## Review cycles
- **c599135** (initial): gemini-code-assist COMMENTED (LGTM, no feedback); claude[bot] non-blocking ("core fix is sound"). Suggested a pre-epoch date test and a release-note callout.
- **e37d876** (cycle 2): added `preEpochDateRoundTripsAsLocalDate` IT + compatibility note. claude[bot] re-reviewed, non-blocking; only substantive ask was deciding/documenting malformed-string behavior.
- **2844277** (cycle 3): added `malformedStringSurfacesAnErrorInsteadOfSilentlyStoringNull` IT proving a malformed datetime string surfaces a loud gRPC error (not a silent null). claude[bot] re-reviewed: **LGTM pending release-note callout**; remaining points out-of-scope / pre-existing / non-blocking.
- **a200031** (cycle 4): added cross-referencing comments tying the shared UTC-anchored temporal encode/decode contract across `ProtoUtils` (grpc-client) and `GrpcTypeConverter` (grpcw) so they cannot silently diverge (claude point 3, non-blocking).

Final state: max review cycles (4) reached. Both bots LGTM. The only open item is a maintainer-owned release-note/changelog callout for the wire-decode behavior change (temporals now decode to `LocalDate`/`LocalDateTime` instead of `Long`); it is documented below and is not a code change. Deferred/skipped review items are tracked locally in `docs/review-deferred-c599135.md` (not committed, per repo convention).

## Compatibility note (surface in release notes)
Because the server always sets a `logical_type` on temporal values, this is a wire-decode behavior change for real server responses, not only the untagged-timestamp edge case: `grpc-client` reads of DATE/DATETIME columns that previously returned a bare `Long` epoch-millis now return `LocalDate` / `LocalDateTime`. Any downstream consumer that read a temporal column and expected a `Long` (or did epoch-millis arithmetic on it) must be updated. Timestamps with no `logical_type` still decode to `Long` (unchanged). On the encode side `java.util.Date`, `Instant`, and `ZonedDateTime` all carry `logical_type "datetime"`, so all three now round-trip to a `LocalDateTime` at UTC; instant/zone identity is not preserved (tracked as the out-of-scope proto-gap follow-up above).
