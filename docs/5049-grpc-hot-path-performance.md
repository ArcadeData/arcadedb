# Issue #5049 - gRPC hot-path performance

## Symptom
Server-side gRPC read/write hot paths do unguarded work at the default (non-debug) log
level:

- **PERF-1**: `ArcadeDbGrpcService.dbgEnc`/`dbgDec` and several `Level.FINE` call sites
  (`applyGrpcRecord`, `convertToGrpcRecord`, `convertResultToGrpcRecord`,
  `convertPropToGrpcValue`, and the core `toGrpcValue` converter) build debug summaries
  (`summarizeJava`/`summarizeGrpc`, plus `value.getClass()` / `String.valueOf(...)`) as
  *eager* arguments to `LogManager.log`. Java evaluates the arguments before `log()` can
  drop the message, so the summary strings are built **per property, per row** (recursively
  for collections/maps), on both the read and write paths - even when FINE logging is off.
  The client's `ProtoUtils.dbgEnc`/`dbgDec` already guard with `isDebugEnabled()`; the
  server did not. (Note: `LogManager` provides fixed-arity `log` overloads up to 7 args, so
  these 3-6 arg call sites do not allocate a varargs `Object[]`; the eliminated cost is the
  summary-string building, not an array.)
- **PERF-L (bytesOf)**: `GrpcTypeConverter.bytesOf` allocated a throwaway `byte[]`
  (`s.getBytes(UTF_8)`) purely to measure a string's UTF-8 length, on the projection
  budgeting path (once per projected field, plus `@rid`/`@type`).

## Root cause
Debug-logging arguments were passed eagerly with no level guard, and UTF-8 length was
measured by fully encoding the string. Both allocate on hot paths independent of the
logging outcome.

## Fix
- Guard every eager `summarizeJava`/`summarizeGrpc` server call site behind
  `LogManager.instance().isDebugEnabled()`, mirroring the client. When debug is off, no
  summary strings or varargs arrays are built.
- Also guard the per-value FINE logs inside `toGrpcValue(Object, ProjectionConfig)` - the
  core converter every property value flows through, recursively for collections/maps -
  including its entry log and every projection-encoding branch. A single `debug` boolean is
  hoisted once per call and reused, and the `debug` flag is likewise hoisted above the
  per-property loops in `convertToGrpcRecord` / `convertResultToGrpcRecord`.
- Reimplement `bytesOf` as an allocation-free UTF-8 length counter that reproduces
  `String.getBytes(UTF_8)` semantics exactly, including 1-byte replacement for unpaired
  surrogates.

Both changes are behavior-preserving: identical wire output, and identical debug log
content when debug output is driven by the `debug` flag (`LogManager.isDebugEnabled()`),
which is how the rest of the engine gates FINE logging. Caveat: these particular FINE lines
were previously also emittable purely via JUL level configuration
(`DefaultLogger.log`/`isLoggable`) independent of the `debug` flag; after this change they
follow the `debug` flag, consistent with the already-guarded client path and the rest of
the engine. The WARNING-level fallback log in the JSON encoder is intentionally left
unguarded.

## Scope note (deferred)
The audit issue bundles several larger architectural items - PERF-4 (stream `query()`
instead of full materialization), PERF-5 (default MAP projection encoding vs double JSON
serialization), PERF-6 (thread-per-transaction redesign), PERF-7/8 (ingest rebuild/double
materialization), PERF-9 (forced gzip). These change wire behavior and/or result-set
semantics and carry regression risk unsuitable for an unattended fix; they are left as
follow-ups. This PR takes the highest-leverage, zero-behavior-change items (PERF-1 and the
`bytesOf` part of PERF-L).

## Tests
- `Issue5049HotPathPerfTest` (grpcw): `bytesOf` matches `String.getBytes(UTF_8).length`
  across ASCII, multi-byte, 4-byte (surrogate-pair) and unpaired-surrogate inputs, plus an
  exhaustive sweep over the full BMP + supplementary code-point range; and a converter-level
  sanity check that `GrpcTypeConverter` conversion output is independent of the debug flag.
- The guarded per-value logging added here lives in the private
  `ArcadeDbGrpcService.toGrpcValue`/`convert*` methods, exercised end-to-end at the default
  (debug-off) level by the existing `ArcadeDbGrpcServiceExtendedTest` /
  `ArcadeDbGrpcServiceCoverageIT`. The guards are behavior-neutral by construction (each only
  wraps a `Level.FINE` log call, never a value-producing statement), so no new server IT is
  added for the debug-on path.

## Impact
Removes per-property/per-row summary-string allocation on the server gRPC read and write
paths at the default log level, and one byte[] allocation per projected field. No change to
results, and no change to debug log content when debug output is driven by the `debug` flag.

## PR
https://github.com/ArcadeData/arcadedb/pull/5195

## Review cycles
- **Cycle 1** - `8b07055` (initial): Gemini COMMENTED, no feedback. Claude flagged that the
  core per-value converter `toGrpcValue(Object, ProjectionConfig)` still logged eagerly at
  its entry and in every projection branch (same read hot path) - the summarize sites were
  guarded but the hottest method's own logs were not. Applied: guard the entry log and all
  projection-branch FINE logs behind a single hoisted `debug` boolean; hoist `debug` above
  the per-property loops in `convertToGrpcRecord`/`convertResultToGrpcRecord`; document the
  JUL-vs-debug-flag gating caveat.
- **Cycle 2** - `c948901`: Claude LGTM. Non-blocking doc nuance: `LogManager` has fixed-arity
  `log` overloads up to 7 args, so the 3-6 arg call sites never built a varargs `Object[]` -
  the eliminated cost is the summary-string building, not an array. Applied: corrected the
  doc wording.
- **Cycle 3** - `0f7e019`: Claude LGTM. Non-blocking: `conversionResultIsIndependentOfDebugFlag`
  exercises `GrpcTypeConverter` (which has no logging), not the guarded `ArcadeDbGrpcService`
  paths. Applied: reframed the test javadoc + doc to describe it accurately as a
  converter-level sanity check, and documented that the guarded private paths are covered
  end-to-end (debug-off) by the existing service tests; the guards are behavior-neutral by
  construction, so no disproportionate new server IT was added.
- **Cycle 4** - `504a76f`: Claude LGTM, "pending a reviewer's nod on the intentional
  JUL-vs-`debug`-flag logging change." Remaining suggestions (a server-side debug-on test;
  the pre-existing non-volatile `LogManager.debug` field) are explicitly non-blocking
  follow-ups. No code changes applied.

## Deferred / follow-ups (non-blocking, for the maintainer)
- Larger audit items PERF-4 (stream `query()`), PERF-5 (default MAP projection encoding),
  PERF-6 (thread-per-transaction), PERF-7/8 (ingest rebuild / double materialization),
  PERF-9 (forced gzip) - each changes wire behavior and/or result-set semantics; deferred.
- Optional: a server-side test that runs a query with `setDebugEnabled(true)` to directly
  execute the guarded-on branches in `ArcadeDbGrpcService`.
- Reviewer sign-off requested on the intentional behavior change: these gRPC `Level.FINE`
  lines now follow the ArcadeDB `debug` flag rather than raw JUL level configuration.

## Final state
`max-cycles-reached` (4 of 4). Substantively converged: Claude LGTM on the final revision
with only non-blocking follow-ups; Gemini reviewed the initial revision with no feedback and
did not re-review later revisions. Merge remains the developer's decision.
