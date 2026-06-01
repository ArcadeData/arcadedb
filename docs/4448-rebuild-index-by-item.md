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

## PR

https://github.com/ArcadeData/arcadedb/pull/4449

## Review cycles

### Cycle 1 - SHA c81cf97f6

- **gemini-code-assist** (COMMENTED, 2026-06-01T13:44:25Z): Flagged that the fix handles
  `" by item"` stripping but does not handle nested property paths (e.g., `nums.a BY ITEM`).
  When the property name contains `.`, `getPolymorphicPropertyIfExists("nums.a")` returns null
  and throws `SchemaException`. Suggested replicating the nested-path resolution block from
  `TypeIndexBuilder.create()` (root property lookup + LIST type validation + `continue` branch).
- **claude**: Did not review within the 15-minute window (PR open since 13:43 UTC; timeout at 13:58 UTC).

## Unaddressed feedback from cycle 1 (gemini)

Gemini's nested-path handling suggestion was technically sound and actionable, but was not applied
because the `claude` bot did not complete its review within the 15-minute per-iteration timeout.

The suggested change to `BucketIndexBuilder.create()`:
- After stripping `" by item"`, if `getPolymorphicPropertyIfExists(actualPropertyName)` returns null
  AND `actualPropertyName` contains `.`, split on the first `.` and look up the root property.
- If root property found: validate it's a LIST (for BY ITEM), use `Type.STRING`, and `continue`.
- If root property not found: throw the existing `SchemaException`.

This is the same logic already present in `TypeIndexBuilder.create()` lines 188-211.

## Final state

`timeout` - claude did not review within the 15-minute window in cycle 1. Gemini's nested-path
feedback remains unaddressed. Developer should evaluate and apply before merging.
