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
  across ASCII, multi-byte, 4-byte (surrogate-pair) and unpaired-surrogate inputs; and a
  regression check that gRPC value conversion round-trips identically with debug on and
  off.

## Impact
Removes per-property/per-row string + varargs allocation on the server gRPC read and write
paths at the default log level, and one byte[] allocation per projected field. No change to
results or to debug-level log content.
