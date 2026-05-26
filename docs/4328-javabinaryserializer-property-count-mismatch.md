# Fix #4328: JavaBinarySerializer property count mismatch

## Issue

`JavaBinarySerializer.writeExternal` writes `properties.size()` as the property count, then skips
writing size/payload bytes for null-valued properties and properties whose type cannot be inferred
(`type == -1`). The reader iterates `propertyCount` times and always reads UTF + int + bytes for
each entry, so a single skipped property desyncs every subsequent read.

## Root Cause

In `writeExternal` (lines 57-87):
- Line 57: `out.writeInt(properties.size())` - count includes ALL entries, including nulls and unknowns
- Lines 63+: property name (UTF) is always written
- Null values: only name written; no size/payload written
- `type == -1`: name written, then `continue` skips size/payload

In `readExternal` (lines 123-136):
- Always reads: `readUTF()` + `readInt()` (size) + `read(array)` (payload) per iteration
- When writer skipped size/payload, reader misreads the next name bytes as the size int, desyncing

## Fix

Pre-filter `properties` to only entries where `propValue != null` AND `type != -1`. Write the
filtered count and iterate only the filtered set.

## Test Plan

1. `documentWithNullProperty` - document with a null property round-trips correctly; null property absent
2. `documentNullPropertyDoesNotCorruptFollowingProperties` - null property in the middle does not
   corrupt subsequently named properties (regression for the desync scenario)
3. `documentAllNullProperties` - document with only null properties serializes to empty map

## Changes

- `engine/src/main/java/com/arcadedb/serializer/JavaBinarySerializer.java` - pre-serialize all
  properties into `(names, payloads)` parallel lists in one pass, write count from `names.size()`.
  Null values and unsupported types are excluded before the count is written.
- `engine/src/test/java/com/arcadedb/serializer/JavaBinarySerializerTest.java` - two new regression
  tests: `documentWithNullPropertyDoesNotDesync` and `documentAllNullPropertiesDeserializesEmpty`

## Test Results

All 7 tests in `JavaBinarySerializerTest` pass. 73 serializer/document tests pass with no regressions.

New tests confirmed FAIL before fix (EOFException) and PASS after fix.
