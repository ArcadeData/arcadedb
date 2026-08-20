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
