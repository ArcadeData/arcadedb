# Issue #5047 - gRPC streaming & pagination correctness

## Symptom
Several correctness/efficiency defects in the gRPC streaming and pagination paths (audit
`docs/2026-07-06-grpc-modules-audit.md`): CON-3, STR-1, STR-2, STR-3, STR-4, PERF-2, PERF-3.

## Root cause & fix (per defect)

- **PERF-2 (fixed)** - `queryStreamBatched` / `queryStreamBatchesIterator` never set `.setLanguage(...)`
  on the `StreamQueryRequest`, so Cypher/Gremlin batched-stream queries silently ran as SQL. Added
  `.setLanguage(langOrDefault(language))` to both builders.
- **CON-3 (fixed)** - `StreamingResultSet.close()` drained the entire remaining stream (transferring and
  discarding every remaining row) and had no cross-thread guard. Now it cancels the underlying
  `BlockingClientCall` and calls `checkCrossThreadUse` like `hasNext`/`next`.
- **STR-1 (fixed)** - `InsertError.row_index` is documented as "index within the chunk" but the server
  reported the cumulative stream-wide `ctx.received - 1`. `insertRows` per-row errors now use the
  chunk-relative `c.received - 1`; the bidi whole-chunk-failure path now reports `-1` (not row-attributable),
  matching the non-bidi `insertStream` path.
- **STR-4 (fixed)** - `is_last_batch` was never true on exact-multiple totals. `streamCursor` and
  `streamPaged` now defer the final full batch/page by one step so the terminal batch always carries
  `is_last_batch=true` (fixes `BatchedStreamingResultSet.isLastBatch()` metadata too and removes the extra
  client-visible empty-batch round trip; PAGED still runs the server-side empty SKIP/LIMIT probe query).
- **PERF-3 (fixed)** - `streamPaged` rebuilt `convertParameters` + a wrapping map every page. The base
  parameter map is now converted once and copied per page.
- **STR-3 (partially fixed)** - `streamPaged` now rejects a query whose parameters collide with the reserved
  `_skip`/`_limit` names (`INVALID_ARGUMENT`) instead of silently overwriting them. The deeper snapshot-isolation
  and projection-only `@rid`-wrapper fragility are deferred (see below).

## Deferred (documented, not fixed here)

- **STR-2** - bidi replay duplication under `PER_ROW`/`PER_BATCH`: on chunk failure the watermark is
  deliberately not advanced, but rows already committed mid-chunk get re-inserted on replay. A correct fix
  requires a semantic decision (advance the watermark for already-committed rows vs. make chunk commit atomic)
  and touches transaction lifecycle; deferred to avoid an untested transaction-semantics change.
- **STR-3 snapshot isolation** - PAGED mode re-executes `SELECT FROM (<q>) ORDER BY @rid SKIP/LIMIT` per page,
  so concurrent inserts/deletes shift offsets. True snapshot consistency needs CURSOR-style single execution;
  ArcadeDB's MVCC read semantics across a spanning transaction do not guarantee it, so this is left as a
  documented limitation (use CURSOR mode for snapshot-consistent large reads).
- **STR-3 `@rid` wrapper on projection-only results** - detecting projection-only queries generically requires
  query analysis; deferred.

## Tests
- `grpc-client` `Issue5047StreamingCorrectnessIT` - PERF-2 (Cypher batched stream), STR-4 (exact-multiple
  CURSOR + PAGED), CON-3 (close after partial read releases and stream stays usable).
- `grpcw` `Issue5047InsertErrorRowIndexIT` - STR-1 (chunk-relative row_index) and STR-3 reserved-name rejection.

## Impact
Client-side streaming/pagination correctness in the gRPC wire protocol. No change to non-streaming paths.

## PR & review history
- PR: https://github.com/ArcadeData/arcadedb/pull/5199
- Cycle 1 (`8fd2a8008`): initial fixes. Gemini review COMMENTED (no actionable comments); Claude review
  non-blocking with 5 polish suggestions.
- Cycle 2 (`ef5adedae`): applied review items - removed stray doc marker, softened the CON-3 comment,
  aligned the two ITs on the `GrpcServer:` plugin name, tightened the reserved-name assertion. Claude
  re-review: "No blocking issues found" (4 non-blocking observations).
- Cycle 3 (`bc0e14ba2`): applied 2 more non-blocking items - `streamExhausted` made `volatile`, doc wording
  clarified (PAGED still runs the server-side empty probe). Claude re-review: "Nothing here blocks merge."
- Final state: clean-approval (both bots non-blocking; merge left to the developer).

## Recommended follow-ups (from review, out of scope here)
- Add a `chunk_seq` field to `InsertError` in `arcadedb-server.proto` so aggregated (multi-chunk) non-bidi
  insert errors remain attributable to a chunk now that `row_index` is chunk-relative.
- Optional: a bidi test asserting `row_index=-1` on a whole-chunk failure, and `@Tag("slow")` on the CON-3
  IT if its 1000 sequential single-row commands prove slow in CI.
