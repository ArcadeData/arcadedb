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

Changed the guard in `getProperty(String name)` from `!content.isEmpty()` to `content.containsKey(name)`. This aligns both overloads and closes the fall-through loophole after `removeProperty`.

The semantics are now: if content holds the key (even as a null value), use content; otherwise fall through to element.

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
