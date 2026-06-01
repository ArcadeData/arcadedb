# Fix: REBUILD INDEX fails for BY ITEM indexes (Issue #4448)

## Summary

`REBUILD INDEX *` (and any bucket-level index rebuild) fails with
`SchemaException: Cannot create the index on type 'Doc.lst by item' because the property does not exist`
when any index was created with the `BY ITEM` modifier.

## Root Cause

When `REBUILD INDEX *` runs:
1. It iterates all automatic indexes, skipping `TypeIndex` wrappers.
2. Each underlying bucket-level index is rebuilt via `BucketIndexBuilder.create()`.
3. `BucketIndexBuilder` receives property names as stored - e.g., `"lst by item"`.
4. It calls `type.getPolymorphicPropertyIfExists("lst by item")` without stripping
   the `" by item"` suffix, finds no property, and throws `SchemaException`.

`TypeIndexBuilder.create()` already handles this correctly (strips `" by item"` before
property lookup at lines 180-183), but `BucketIndexBuilder.create()` did not.

## Affected Files

- `engine/src/main/java/com/arcadedb/schema/BucketIndexBuilder.java`
  - `create()`: property name lookup loop (lines 101-112) did not strip `" by item"`.

## Fix

Apply the same `" by item"` stripping pattern from `TypeIndexBuilder` to
`BucketIndexBuilder.create()`: strip suffix before `getPolymorphicPropertyIfExists()`
and use `Type.STRING` as the key type for BY ITEM properties (consistent with TypeIndexBuilder).

## Tests

- New regression test in `ListIndexByItemTest`: `rebuildIndexByItemAllWildcard` and
  `rebuildIndexByItemByName` - reproduces the reported scenario exactly.

## Verification

```
cd engine && mvn test -Dtest=ListIndexByItemTest
```
