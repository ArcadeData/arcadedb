# Issue #6464: Empty typed primitive-array property round-trips to an ArrayList, not an array

## Summary

`BinaryTypes.getTypeFromValue()` decided the binary type of a Java array from its **first element**:
for a primitive array (`int[]`, `short[]`, `long[]`, `float[]`, `double[]`), the first element was
sniffed with a pattern-match `switch`, and an empty array's "first element" is `null`, which fell
through to `TYPE_LIST`. So the same property's runtime type after a reload depended on whether it
happened to be empty: `new int[0]` round-tripped as an `ArrayList`, `new int[]{1,2}` round-tripped
as `int[]`. `(int[]) doc.get("arr")` threw `ClassCastException` only in the empty case.

For a boxed wrapper array (`Integer[]`, `Long[]`, `Short[]`, `Float[]`, `Double[]`) the code took the
non-primitive-component-type branch unconditionally and always returned `TYPE_LIST`, regardless of
content or of any declared `ARRAY_OF_*` schema property.

## Fix

`BinaryTypes.getTypeFromValue()` (`engine/src/main/java/com/arcadedb/serializer/BinaryTypes.java`)
now decides the `TYPE_ARRAY_OF_*` binary type from the array's **declared component type**
(`value.getClass().getComponentType()`), matched against both the primitive class (`short.class`,
`int.class`, ...) and its wrapper (`Short.class`, `Integer.class`, ...), instead of from the first
element:

- An empty primitive array now keeps its `TYPE_ARRAY_OF_*` type - the component type is known
  regardless of length, so emptiness no longer collapses it to `TYPE_LIST`.
- A boxed wrapper array (`Integer[]`, ...) is now also classified `TYPE_ARRAY_OF_*`, matching what
  `Type.ARRAY_OF_INTEGERS` et al. already accept as an alternate input class
  (`new Class<?>[] { int[].class, Integer[].class }` in `Type.java`) and already declare as their
  canonical Java class (`int[].class`, not `Integer[].class`).
- A heterogeneous `Object[]` (e.g. `list.toArray()`, whose component type is `Object`, not a numeric
  wrapper) is unaffected and still classifies as `TYPE_LIST` - it only takes the new branches when the
  array's *declared* component type is one of the five numeric primitives or their wrappers.

`BinarySerializer.serializeValue()`'s five `TYPE_ARRAY_OF_*` cases used
`java.lang.reflect.Array.getShort/getInt/getLong/getFloat/getDouble()`, which **reject** a boxed
wrapper array with `IllegalArgumentException: Argument is not an array of primitive type` (verified
with a standalone repro). Since a wrapper array can now reach these cases, each one branches on
`instanceof <primitive>[]` and keeps the direct primitive read on that path (no behavior or
performance change for the existing primitive-array path), falling back to an explicit
`(Wrapper[]) value` unboxing loop only for the wrapper-array path. Deserialization is unchanged: all
five `TYPE_ARRAY_OF_*` cases already produced a primitive array, so a boxed `Integer[]` property now
round-trips as `int[]`, consistent with `Type.ARRAY_OF_INTEGERS.getJavaClass() == int[].class`.

The now-unused `java.lang.reflect.Array` import was removed from `BinaryTypes.java` (still used
elsewhere in `BinarySerializer.java`).

## Why existing `Object[]` tests were unaffected

`BinarySerializerTest.listPropertiesInDocument` sets `"arrayOfIntegers"` (etc.) to
`listOfIntegers.toArray()`. `List.toArray()` returns `Object[]` regardless of the list's generic
type - the array's actual `getComponentType()` is `Object.class`, not `Integer.class` - so that test
continues to hit the `TYPE_LIST` branch exactly as before. Only an array whose *declared* component
type is a genuine `Integer[]`/`Short[]`/etc. (e.g. `new Integer[]{1,2,3}` or
`list.toArray(new Integer[0])`) is affected by the fix.

## Tests added (`engine/src/test/java/com/arcadedb/serializer/BinarySerializerTest.java`)

- `emptyPrimitiveArrayKeepsItsArrayType` - asserts `BinaryTypes.getTypeFromValue()` classifies
  `new short[0]`/`int[0]`/`long[0]`/`float[0]`/`double[0]` as the matching `TYPE_ARRAY_OF_*`, then a
  full serialize/deserialize round trip confirms each reloads as the same empty primitive array type
  (not `ArrayList`).
- `boxedPrimitiveArraysSerializeAsTypedArrays` - same classifier assertion for empty
  `Short[]`/`Integer[]`/`Long[]`/`Float[]`/`Double[]`, then a round trip with populated boxed arrays
  confirms they reload as the corresponding primitive array with the same values.

Both were confirmed **red** before the fix (`expected: 23 but was: 16`, i.e. `TYPE_LIST` instead of
the declared `TYPE_ARRAY_OF_*`) and green after.

## Verification run

- `BinarySerializerTest`: 19/19 passed (2 new + all pre-existing, including
  `arraysOfPrimitive` and `listPropertiesInDocument` which exercise the paths this fix must not
  regress).
- `com.arcadedb.serializer.**` + `com.arcadedb.schema.**`: 620/620 passed.
- `com.arcadedb.index.vector.**` + `com.arcadedb.index.sparsevector.**` (excluding
  `benchmark,vector,slow` lanes): 410/410 passed - these exercise `float[]`/`double[]` embeddings
  heavily and confirm the primitive fast path is untouched.
- `JavaBinarySerializerTest` + `com.arcadedb.graph.GraphBatch*Test`: 26/26 passed - the other caller
  of `BinaryTypes.getTypeFromValue`/`BinarySerializer.serializeValue`.
- `com.arcadedb.database.**` (excluding `benchmark,vector,slow`): 282/282 passed, 1 pre-existing
  skip unrelated to this change.

## Scope not addressed

The issue's repro and suggested fix are both scoped to the five numeric `ARRAY_OF_*` types
(`SHORT`/`INTEGER`/`LONG`/`FLOAT`/`DOUBLE`). `byte[]` is unaffected by the original bug (handled
earlier in `getTypeFromValue` as `TYPE_BINARY`, unconditionally, regardless of emptiness) and is out
of scope here.

## PR

https://github.com/ArcadeData/arcadedb/pull/6649

## Review cycles

- **Cycle 1** - head `f8bb9109` (PR open). `claude` bot review (posted as a PR issue comment, no
  commit SHA - see note below) found one real bug: `getTypeFromValue()` classified any
  `Short[]/Integer[]/Long[]/Float[]/Double[]` as `TYPE_ARRAY_OF_*` purely from component type,
  regardless of content, but `BinarySerializer.serializeValue()`'s new `TYPE_ARRAY_OF_*` branches
  unbox each wrapper element with an enhanced for-loop (`for (final Integer v : ints)
  content.putNumber(v)`), which auto-unboxes and throws an uncaught `NullPointerException` on a
  `null` element. Reachable via the ordinary public API for a schemaless property
  (`doc.set("someIntArrayProp", new Integer[]{1, null, 3})`). **Verified** by reading
  `BinarySerializer.java:663-720` - confirmed the auto-unboxing NPE is real for all five wrapper
  types. **Applied**: added `BinaryTypes.arrayHasNullElement()` and fell back to `TYPE_LIST` in
  `getTypeFromValue()` whenever a wrapper array contains a `null` element (the reviewer's first
  suggested option - mirrors the pre-existing `Object[]` fallback and keeps the pre-#6464 behavior
  for this case unchanged). Added regression test
  `boxedPrimitiveArrayWithNullElementFallsBackToListInsteadOfNPE` covering the classifier and a full
  serialize/deserialize round trip. Verification: `BinarySerializerTest` 20/20,
  `com.arcadedb.serializer.**` + `com.arcadedb.schema.**` 621/621, `BUILD SUCCESS`. Pushed as
  `ce4942b`.
- **Cycle 2** - head `ce4942b`. `claude` bot review (PR issue comment) confirmed the
  `arrayHasNullElement` fallback closes the NPE gap and the new regression test pins it; traced the
  schema-declared-property path (`Type.convert()`'s `requireNonNullNumber`/`narrowToIntegral`) and
  confirmed the null-fallback in this PR matters for schemaless properties and other
  direct-serialization callers, which is the scope the added test exercises. **No blocking issues
  found.** Three items raised, all explicitly non-blocking and left as-is (see below) - no code
  change made this cycle, working tree stayed clean.

### Nitpicks raised in cycle 2, not actioned (reviewer marked all three non-blocking)

- `Type.TYPES_BY_USERTYPE` has no entries for the boxed wrapper array classes
  (`Short[].class`/`Integer[].class`/...), only their primitive counterparts. Reviewer: "not touched
  by this PR and not a regression... possibly worth a follow-up issue if that path exists." No such
  call site was identified as in-scope for this issue; left as a possible future follow-up, not filed
  separately since it is speculative ("if that path exists").
- The five-way `componentType == X.class ? arrayHasNullElement(...) : ...` chain in
  `getTypeFromValue()` repeats the same ternary shape and could be a small helper. Reviewer's own
  words: "a nitpick, not a blocker... matches the existing style of the surrounding if/else chain."
  Left as-is - the current form stays consistent with the chain's existing if/else style, which the
  reviewer confirmed is more readable than the alternative here.
- `Byte[]` (boxed) still falls through to `TYPE_LIST`, unlike the other four wrapper types. Reviewer:
  "consistent with the stated scoping, just flagging it's not 'fixed' by symmetry." `byte[]`/`Byte[]`
  were explicitly out of scope for issue #6464 from the start (see "Scope not addressed" above,
  `byte[]` is handled earlier as `TYPE_BINARY` and was never affected by the original bug) - no change
  needed.

### Note on the review-polling bug this run started from

Cycle 1's review (`f8bb9109`, 2026-08-23T20:28:27Z) was originally missed by a version of this skill's
Phase 3a that only polled `reviews[]` and inline PR comments (both keyed by commit SHA) - the `claude`
bot on this repo posts its review as a plain PR **issue comment** with no SHA at all. That polling gap
is fixed in the current skill (adds a third surface: `gh pr view --json comments`, filtered to
`claude`-authored comments newer than the push timestamp). Both cycles in this run were detected
correctly by the fixed polling on the first or near-first poll.

## Final state

`clean-approval` - cycle 2 review found no blocking issues and required no further changes. Ready
for developer review and merge (merge intentionally not performed by this skill).
