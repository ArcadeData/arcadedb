# Issue #5032 - ServerQueryProfiler: racy counter, array publication, ring-buffer overflow, per-call regex

## Symptom
`ServerQueryProfiler` (server module) records queries on an unsynchronized hot path from Undertow workers but has three defects:
1. `totalRecorded++` on a plain `int` loses updates under concurrency; plain array stores lack happens-before to the `synchronized` readers.
2. `writeIndex.getAndIncrement() % MAX_ENTRIES` on an `AtomicInteger` goes negative once the counter overflows `Integer.MAX_VALUE`, throwing `ArrayIndexOutOfBoundsException` and killing recording.
3. `normalizeQuery` compiles the `\s+` regex on every call during aggregation.

## Root cause
- Non-atomic counter and non-published array in a concurrent producer / synchronized consumer design.
- Signed 32-bit modulo of an overflowing counter.
- Regex not hoisted to a constant.

## Fix
- Replace the separate `totalRecorded` int with a derived count from an `AtomicLong writeIndex` (one increment per record => exact count, no lost updates).
- Store entries in an `AtomicReferenceArray` so each element write is published to the synchronized readers.
- Compute the slot with `Math.floorMod(seq, MAX_ENTRIES)` on the `long` sequence so it is always non-negative.
- Hoist `static final Pattern WHITESPACE = Pattern.compile("\\s+")` and reuse it in `normalizeQuery`.

## Tests (added to ServerQueryProfilerTest)
- `concurrentRecordingCountsExactly` - N threads x M records, asserts exact `totalQueries` (fails before fix due to lost updates).
- `ringBufferIndexDoesNotOverflow` - seeds `writeIndex` near `Integer.MAX_VALUE` via reflection (type-agnostic), asserts recording does not throw AIOOBE (fails before fix).

## Impact
Server monitor profiler only. No behavior change for correct single-threaded use; fixes accuracy under concurrency, long-running robustness, and removes a hot-path allocation.
