# Issue #4398: ResultInternal.getProperty falls through to element when content becomes empty

## Problem

`ResultInternal.getProperty(String name)` uses `!content.isEmpty()` as the guard, but the overload at line 207 uses `content.containsKey(name)`. The single-arg variant mixes two semantics:

- If `content` is non-empty (any key present), it reads from `content` - even if `name` is not there
- If `content` is empty, it falls through to `element`

This means:
1. After `removeProperty(name)`, if other keys still remain in `content`, a removed property still falls through to `element` and re-surfaces with its original value.
2. There's a subtle inconsistency: two callers can get different behavior depending on which overload they call.

## Root Cause

Line 183:
```java
if (content != null && !content.isEmpty())   // ← should be content.containsKey(name)
```

The correct guard is `content.containsKey(name)`, which is what the two-arg overload already uses.

## Fix Applied

Added a lazy-initialised `Set<String> tombstones` field. When `removeProperty(name)` is called on a result that has a backing element, the name is added to tombstones. All read paths check tombstones first:

- `getProperty(name)` - if tombstoned, return null before any content/element check
- `getProperty(name, defaultValue)` - if tombstoned, return defaultValue
- `getElementProperty(name)` - if tombstoned, return null
- `hasProperty(name)` - if tombstoned, return false
- `getPropertyNames()` - element property names filtered to exclude tombstoned keys
- `toMap()` - when tombstones are present, builds a merged view: element.toMap() minus tombstoned keys, overlaid with content entries

`setProperty(name, value)` lifts the tombstone when a key is re-set.

The `!content.isEmpty()` guard in the single-arg `getProperty` is intentionally preserved (not replaced with `containsKey`) because projection mode populates content with all non-excluded fields - replacing it breaks `SELECT *, !field` exclusion queries that rely on the projection-mode semantics.

## Tests Added

New test methods added to `ResultInternalTest`:
- `getPropertyAfterRemoveDoesNotFallThroughToElement` - regression for the core bug: after removing a property from a result that has an element backing, the property should be null, not the element's value.
- `getPropertyUsesContentWhenKeyPresent` - content key shadows element value when explicitly set.
- `getPropertyFallsThroughToElementWhenContentDoesNotContainKey` - normal fall-through still works for keys not in content.
- `getPropertyDoesNotFallThroughWhenContentHasOtherKeys` - with other keys present in content, accessing a removed key no longer falls through.

## Verification

- All new tests passed
- Existing tests in `ResultInternalTest` and `ResultInternalScoreTest` passed

## Impact

Removal of a property from a `ResultInternal` backed by an element is now permanent within that result's lifetime. Previously, the element value would re-appear if any other property remained in `content`.
