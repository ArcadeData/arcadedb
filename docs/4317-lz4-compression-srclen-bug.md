# Fix #4317: LZ4Compression.compress passes total size() where srcLen is expected

## Summary

`LZ4Compression.compress(Binary)` was passing `data.size()` (total backing array length)
as the third argument to `LZ4Compressor.compress()`, which expects the number of bytes to
compress (`srcLen`). The correct value is `data.size() - data.position()`, already computed
as `decompressedLength` two lines earlier.

When `data.position() > 0`, the compressor read past the intended slice into the unrelated
tail of the buffer. The destination was sized for the correct (smaller) length, so the call
could throw `ArrayIndexOutOfBoundsException` or silently corrupt the output.

The `decompress(Binary, …)` overload immediately above already subtracted position correctly,
so round-trips with a non-zero position either threw or returned garbage.

## Root Cause

Single off-by-one in `LZ4Compression.java` line 88:

```java
// Before (wrong)
compressor.compress(data.getContent(), data.position(), data.size(), …)
// After (correct)
compressor.compress(data.getContent(), data.position(), decompressedLength, …)
```

`decompressedLength` is already assigned to `data.size() - data.position()` two lines above.

## Files Changed

- `engine/src/main/java/com/arcadedb/compression/LZ4Compression.java` — one-line fix
- `engine/src/test/java/com/arcadedb/compression/CompressionTest.java` — regression test

## Test Results

```
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0  (CompressionTest)
```

The new test `compressionWithNonZeroPosition` reproduces the bug (throws
`ArrayIndexOutOfBoundsException` at line 88 before fix) and passes cleanly after.

## Impact Analysis

Any caller that passes a `Binary` with `position() > 0` or a slice `Binary` (non-zero
`getContentBeginOffset()`) would get corrupted output or an exception. Page-level compression
and snapshot compression are the primary risk paths.

---

## PR

https://github.com/ArcadeData/arcadedb/pull/4322

## Review Cycles

### Cycle 1 — head SHA 54a0ce60a

Gemini reviewed with COMMENTED state. Two comments:

1. **[High] compress(Binary): add getContentBeginOffset() to srcOff** — for slice Binaries,
   `position()` is relative to the slice start but the backing array has a non-zero
   `arrayOffset()`. Fix: `getContentBeginOffset() + position()` as srcOff. Applied.

2. **[Medium] Same issue in decompress(Binary, int)** — same structural bug in the decompress
   path. Fixed together with compress for consistency. Test updated to use `Binary.slice()`.

### Cycle 1 resolution — head SHA 463507bc0

Applied both comments:
- `compress(Binary)`: `data.position()` → `data.getContentBeginOffset() + data.position()`
- `decompress(Binary, int)`: same fix
- Added `compressionOfSlicedBinary` test covering the slice/offset case
- All 4 CompressionTest tests pass

## Final State

`timeout` — only `gemini-code-assist` is active in this repo; `claude` bot is not installed.
The skill requires both bot logins before declaring a clean approval. Gemini's single review
was fully addressed in cycle 1. PR is ready for developer review and merge.
