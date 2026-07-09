# Issue #5036 - Batch and time-series write endpoints: partial-commit atomicity and lossy error reporting

## Symptom
- `POST /api/v1/batch`: a mid-stream failure (malformed line, unknown temp id, dropped connection)
  returns 4xx/5xx but the error body does not report how many records were already committed. Because
  `GraphBatch` commits every `commitEvery` records, earlier chunks stay persisted, so a naive retry of
  the whole file duplicates data with no way for the client to know what survived.
- `POST /api/v1/ts/{db}/write` (InfluxDB line protocol): samples for unknown / non-timeseries
  measurements are silently dropped with only a server-side WARNING. If at least one sample is inserted
  the response is `204`, so a partial write looks like a full success to the client.

## Root cause
- `PostBatchHandler.execute` lets exceptions propagate to `AbstractServerHttpHandler`, which builds a
  generic error body with no `verticesCreated` / `edgesCreated` count.
- `PostTimeSeriesWriteHandler.execute` only returned `400` when `inserted == 0`; any partial insert
  (`inserted > 0` with a non-empty drop set) fell through to the `204` success path.

## Fix
- `PostBatchHandler`: wrap the streaming ingest in a try/catch. On failure return an explicit
  `ExecutionResponse` (400 for `IllegalArgumentException` client-input errors, 500 otherwise) whose JSON
  body carries `verticesCreated`, `edgesCreated`, `partialCommit: true`, and the original `error`
  message. Class Javadoc documents the non-atomic contract.
- `PostTimeSeriesWriteHandler`: return a `400` partial-write payload whenever any sample was dropped
  (regardless of how many were inserted), naming the dropped measurements and reporting `written` /
  `dropped` counts. A clean ingest still returns `204`.

## Tests
- `PostBatchHandlerIT.partialCommitErrorReportsPersistedCounts` - mid-stream edge failure returns 400
  with `verticesCreated=2`, `partialCommit=true`, and the offending reference in the message.
- `PostTimeSeriesWriteHandlerIT.partialWriteReportsDroppedMeasurements` - mixed known/unknown ingest
  returns 400 naming the dropped measurement while the valid sample is persisted.

## Impact
- Batch and line-protocol clients can now detect partial writes and reconcile instead of blindly
  retrying. Existing full-success paths and all-dropped error paths keep their previous status codes.
- Note for operators: the time-series line-protocol endpoint now returns `400` (InfluxDB partial-write
  semantics) instead of `204` when some samples are dropped; Telegraf and similar clients treat `400`
  as non-retryable. Worth a CHANGELOG/release-note entry.

## PR
- https://github.com/ArcadeData/arcadedb/pull/5167
