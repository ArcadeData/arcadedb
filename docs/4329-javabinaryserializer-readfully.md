# Fix #4329 — JavaBinarySerializer.readExternal partial read corruption

## Issue

`JavaBinarySerializer.readExternal` calls `in.read(array)` to read serialized property bytes.
`ObjectInput.read(byte[])` is permitted by contract to return fewer bytes than the array length (short read).
On compressed or networked `ObjectInput` implementations this can happen; the remaining bytes are silently lost,
corrupting every subsequent property in the record.

## Root cause

`engine/src/main/java/com/arcadedb/serializer/JavaBinarySerializer.java` line 129:

```java
in.read(array);   // may short-read
```

The correct API that guarantees the buffer is fully populated is `DataInput.readFully(byte[])`.

## Verification plan

1. Write a regression test `readExternalSurvivesShortReadInput` that wraps the deserialization
   stream in a `ThrottledObjectInput` that returns at most 1 byte per `read(byte[], off, len)` call.
2. Run the test — it must FAIL before the fix (corrupt values or stream format exception).
3. Apply the one-line fix: `in.readFully(array)`.
4. Re-run — test must PASS.
5. Run the full `JavaBinarySerializerTest` suite to confirm no regressions.

## Changes

- `engine/src/main/java/com/arcadedb/serializer/JavaBinarySerializer.java` — `in.read(array)` → `in.readFully(array)` (line 129).
- `engine/src/test/java/com/arcadedb/serializer/JavaBinarySerializerTest.java` — added `readExternalSurvivesShortReadInput` regression test.

## Test results

- `readExternalSurvivesShortReadInput` — FAILED before fix (UTFDataFormatException: stream misaligned after 1-byte read)
- `readExternalSurvivesShortReadInput` — PASSED after fix
- Full `JavaBinarySerializerTest` suite: 6/6 passed
- All serializer tests (`*Serializer*`): 30/30 passed, zero regressions

## PR

https://github.com/ArcadeData/arcadedb/pull/4348

## Review cycles

**Cycle 1** — HEAD `b038b84d`
- gemini-code-assist: COMMENTED - two medium suggestions: add bounds checks to `ThrottledObjectInput.read(byte[],int,int)` and `readFully(byte[],int,int)`
- Applied: added `if (off < 0 || len < 0 || len > b.length - off) throw new IndexOutOfBoundsException()` to both methods
- Commit: `4f40314c5`

**Cycle 2** — HEAD `4f40314c5`
- gemini-code-assist: COMMENTED - same suggestions repeated but changes are already applied (stale feedback)
- No actionable items; working tree clean
- Early-exit: clean-approval

## Final state

`clean-approval` - all review items addressed, no new actionable feedback on cycle 2

## Status

Done
