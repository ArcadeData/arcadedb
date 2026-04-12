# Issue #3821 - TimeSeries InfluxDB Line Protocol Bugs

Reported in discussion #3819: Telegraf integration with ArcadeDB TimeSeries.

## Changes Made

### Bug 1 (Critical): Silent data loss on unknown measurement type

**File:** `server/src/main/java/com/arcadedb/server/http/handler/PostTimeSeriesWriteHandler.java`

- Tracked unknown measurement names in a `LinkedHashSet<String>`
- When all samples are for unknown types (`inserted == 0 && !unknownTypes.isEmpty()`): return **400** with a clear error listing the missing types and instructions to create the TIMESERIES TYPE
- When some samples insert and some are skipped: log a WARNING server-side and still return 204

**Test added:** `PostTimeSeriesWriteHandlerIT.unknownMeasurementTypeReturnsError` - sends line protocol for a non-existing type, asserts response is 400 (not silent 204)

### Bug 2 (Critical): No gzip decompression for `Content-Encoding: gzip`

**File:** `server/src/main/java/com/arcadedb/server/http/handler/PostTimeSeriesWriteHandler.java`

- Overrode `parseRequestPayload` to read raw bytes via `receiveFullBytes`
- Checks `Content-Encoding` header; if `gzip`, wraps with `GZIPInputStream` before UTF-8 decoding
- Falls back to plain UTF-8 decoding for all other encodings

**Test added:** `PostTimeSeriesWriteHandlerIT.gzipCompressedBodyIsAccepted` - creates a TimeSeries type, sends gzip-compressed line protocol, asserts 204 and data was inserted

### Bug 4: Grammar/doc mismatch for `CREATE TIMESERIES TYPE`

**File:** `engine/src/test/java/com/arcadedb/engine/timeseries/CreateTimeSeriesTypeStatementTest.java`

Added two negative tests confirming undocumented forms are properly rejected:
- `precisionClauseIsNotSupported` - `PRECISION NANOSECOND` after timestamp column throws `CommandParsingException`
- `compactionIntervalTwoWordFormIsNotSupported` - `COMPACTION INTERVAL` (two words, without underscore) throws `CommandParsingException`

## Test Results

- `CreateTimeSeriesTypeStatementTest`: 8/8 passed (6 existing + 2 new)
- `LineProtocolParserTest`: 15/15 passed (existing, no regressions)
- `PostTimeSeriesWriteHandlerIT`: pending server module JAR refresh (engine install completed successfully; test infrastructure was using stale cached JAR for previous runs)

## Bug 3 Status (InfluxDB v1 protocol compatibility)

Not implemented in this fix - it requires a design decision. The `PostTimeSeriesQueryHandler` receives Telegraf's `q=CREATE DATABASE "telegraf"` form-urlencoded handshake because it cannot parse it as JSON, causing a confusing error log. Recommend either:
1. Add a dedicated InfluxDB v1 shim endpoint
2. Document clearly that only `[[outputs.http]]` is supported

## Impact

- Callers sending line protocol for undefined measurement types now get a clear 400 error instead of silent success
- Telegraf (and any client) sending `Content-Encoding: gzip` bodies now ingest correctly
- Grammar rejection tests prevent documentation regressions
