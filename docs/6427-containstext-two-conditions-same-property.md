# Issue #6427: CONTAINSTEXT - two conditions on the same property, only one reaches the full-text index

## Root cause

Two independent single-condition-per-property limits combined to create the bug:

1. `SelectExecutionPlanner.buildIndexSearchDescriptorForFulltext` matched at most ONE `CONTAINSTEXT`
   condition per indexed property (`break` right after the first match). A second condition on an
   already-claimed property was left in the residual filter, where `ContainsTextCondition.evaluate`
   answers it with a case-sensitive `String.contains` instead of the index's analyzer-token semantics.
2. Even if the planner claimed both, `FetchFromIndexStep.processFullTextBlock` built a POSITIONAL key
   with exactly one slot per indexed property (`Object[] keys`, sized `properties.size()`), so a second
   condition on the same property had nowhere to go (`keys[position] != null` bailed to the generic,
   non-full-text path).

`LSMTreeFullTextIndex.intersectPerProperty` already ANDs an arbitrary list of per-property lookups; it
was driven by a key with one slot per property rather than one per condition.

## Fix

1. `buildIndexSearchDescriptorForFulltext`: removed the `break`, so every `CONTAINSTEXT` condition on an
   indexed property is claimed, not just the first.
2. `FetchFromIndexStep.processFullTextBlock`: when a property's slot is already filled, accumulate the
   values into a `List` instead of bailing out.
3. `LSMTreeFullTextIndex.splitPositionalKey`: now also triggers the positional/intersecting path for a
   SINGLE-property index when a slot holds a `List` (previously it only triggered for indexes with 2+
   properties), and expands a `List` slot into one entry per element. A single-property index does not
   store field-qualified tokens (`put()`), so entries built from a single-property key stay unqualified.

The conjunction now comes from the number of `CONTAINSTEXT` conditions in the query, not from how the
index happened to be declared: a third condition on the same property costs one more lookup, intersected
with the rest via the existing `intersectPerProperty`.

## Tests

- `engine/src/test/java/com/arcadedb/index/fulltext/Issue6427ContainsTextSamePropertyTwiceTest.java` (new):
  covers single- and multi-property indexes, BM25 similarity, 3+ conditions on the same property, a
  condition alongside a non-fulltext filter, and a mix of same-property + different-property conditions.
- `Issue6414ContainsTextMultiPropertyIndexTest.aSecondConditionOnTheSamePropertyIsStillAnsweredByTheRowFilter`
  (existing, pre-existing test explicitly documents in its own Javadoc/comments that its third assertion
  is "the one #6427 will flip" - updated to assert the new, corrected behavior now that the fix lands. No
  other assertion in that test changed.

## Verification

- `mvn -o -pl engine -am test -Dtest=Issue6427ContainsTextSamePropertyTwiceTest,Issue6414ContainsTextMultiPropertyIndexTest,Issue6382ContainsTextColonTest,Issue6438ContainsTextNonStringArgumentTest -DexcludedGroups=benchmark,vector`

## PR

https://github.com/ArcadeData/arcadedb/pull/6523

## Review cycles

Ran the full `--max-cycles=4` budget; every cycle's `claude` review carried genuine, minor
feedback that was addressed with a follow-up commit (no cycle reached a clean approval).

- **Cycle 1** (head `1c33ce82`): review flagged one doc-precision nit - a `{@link}` in
  `FetchFromIndexStep.processFullTextBlock`'s Javadoc pointed at `LSMTreeFullTextIndex#get(Object[],
  int)` for the list-slot expansion, when the expansion actually happens in `splitPositionalKey`.
  Fixed in `d1902efe`.
- **Cycle 2** (head `d1902efe`): review raised an edge case - a property indexed under two modifiers
  sharing one base name (e.g. `m by key`, `m by value`) with two repeated `CONTAINSTEXT` conditions on
  that base name resolves both conditions to the FIRST matching index property only; the other modifier
  is never queried. Confirmed (via a throwaway experiment, not committed) that this pre-dates #6427 -
  even a single such condition already only ever queried the first modifier - and that two repeated
  conditions used to bail to the generic path entirely pre-fix, so this is a net improvement, not a
  regression. Documented the limitation in a Javadoc/comment (the reviewer's "either a test or a doc
  note" option) rather than a test, since correctly pinning that interaction needs deeper investigation
  into MAP `BY KEY`/`BY VALUE` full-text indexing that is out of scope for this bug fix. Also applied
  the review's mechanical nit, `@SuppressWarnings("unchecked")` on the accumulator cast. Fixed in `0550c622`.
- **Cycle 3** (head `0550c622`): review confirmed the dual-modifier analysis independently (same
  conclusion: net improvement, not a regression) and had one style nit - scope
  `@SuppressWarnings("unchecked")` to the single cast rather than the whole method. Fixed in `1177c751`.
- **Cycle 4** (head `1177c751`): review was positive overall, with one optional, non-blocking test-
  coverage suggestion - no test combined a repeated same-property condition with a null-valued repeated
  condition on that same property. Added
  `Issue6427ContainsTextSamePropertyTwiceTest.aNullValuedRepeatedConditionOnTheSamePropertyMatchesNothing`.
  Fixed in `0ab777b3`.

Max-cycles (4) reached after the cycle-4 push; the loop stops there per the skill's constraints rather
than polling a 5th time. `0ab777b3` (the final head at the time this doc was written) had not yet been
reviewed when the loop ended.

## Deferred / left to the developer

- Cycle 2's review suggested (optionally) filing a tracking GitHub issue for the dual-modifier
  (`BY KEY`/`BY VALUE` sharing a base property name) position-resolution limitation, "if you want it
  discoverable later." Not filed by this automated run - filing new issues is a developer judgment call,
  and the limitation is already recorded in `FetchFromIndexStep.processFullTextBlock`'s Javadoc. Left for
  the developer to file if they want a tracking issue.

## Final state

`max-cycles-reached` - every review cycle's feedback was addressed with a commit; none were a clean
approval outright, and the 4-cycle budget was exhausted before one arrived. Merge remains the developer's
call.
