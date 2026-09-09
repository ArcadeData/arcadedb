# #7285 - A stringified JSON array aborts the graph import for vector/list on a JSONL source

Branch: `fix/7285-jsonl-stringified-array-vector-list`

## Problem

`JsonlRowSource.JsonlRecordReader` overrides `GraphImporter.RecordReader.getFloatArray` and
`getList` and goes straight to `JSONObject.getJSONArray`, which throws on a JSON *string*. The
interface defaults - which `CsvRowSource` and `XmlRowSource` inherit - parse the textual form
`"[0.1,0.2]"` deliberately, and say so in their javadoc.

A JSONL file produced by stringifying a CSV export therefore imports its `int`, `long` and
`double` columns (Gson parses a quoted number lazily) and then aborts the whole import on its
vector or list column. That asymmetry is the defect: the same file, the same converter, one
column type that works and one that ends the load.

Follow-up to #7269 (empty value, same drift, same file), itself a follow-up to #7265.

## Analysis

`readProperty` (`GraphImporter.java:1471`) dispatches `FLOAT_ARRAY` to `record.getFloatArray` and
`LIST` to `record.getList`, wrapping `IllegalArgumentException | JSONException` as `badValue`.
`badValue` is thrown from inside the row loop, so it ends the import rather than skipping a row.

The interface defaults (`GraphImporter.java:815`, `:828`) read through `get()`, which every
source implements, and parse the text with `VectorUtils.toFloatArray` / `new JSONArray(v)`.
They also call `checkNotSplit`, whose diagnostic is about a *delimited* source cutting an array
in half. `RecordSource.fieldSeparator()` returns `null` for JSONL, so that check is not wanted
on this path - the fix must not delegate to the default wholesale, or a malformed JSONL vector
would be reported as a delimiter-choice problem it cannot be.

## Completeness

### Invariant

**On every `GraphImporter.RecordSource`, a `FLOAT_ARRAY` or `LIST` property whose attribute holds
the textual array form (`"[0.1,0.2]"`, `"[\"x\"]"`) yields the same value it yields on a CSV or
XML source, instead of aborting the import.**

### Enumeration

```
$ grep -rn "implements GraphImporter.RecordReader" --include='*.java' . | grep -v /target/
integration/src/main/java/com/arcadedb/integration/importer/graph/XmlRowSource.java:138:  private static class AttrRecordReader implements GraphImporter.RecordReader {
integration/src/main/java/com/arcadedb/integration/importer/graph/XmlRowSource.java:154:  private static class MapRecordReader implements GraphImporter.RecordReader {
integration/src/main/java/com/arcadedb/integration/importer/graph/JsonlRowSource.java:65:  private static class JsonlRecordReader implements GraphImporter.RecordReader {
integration/src/main/java/com/arcadedb/integration/importer/graph/CsvRowSource.java:132:  private static class CsvRecordReader implements GraphImporter.RecordReader {
```

Four readers, three sources. Which of them override the two accessors at issue:

```
$ grep -rn "float\[\] getFloatArray\|List<Object> getList" --include='*.java' . | grep -v /target/
integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:815:    default float[] getFloatArray(final String attribute) {
integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:828:    default List<Object> getList(final String attribute) {
integration/src/main/java/com/arcadedb/integration/importer/graph/JsonlRowSource.java:136:    public float[] getFloatArray(final String attribute) {
integration/src/main/java/com/arcadedb/integration/importer/graph/JsonlRowSource.java:148:    public List<Object> getList(final String attribute) {
```

Only `JsonlRecordReader` overrides them. CSV and XML use the defaults, which already parse text.

Call sites that reach the two accessors:

```
$ grep -rn "record.getFloatArray\|record.getList\|readProperty(record" --include='*.java' integration/src/main/java
integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:1129:          final Object val = readProperty(record, pd);
integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:1384:            ec.objProps.computeIfAbsent(pd.name, k -> new ArrayList<>(BUFFER_INITIAL_CAPACITY)).add(readProperty(record, pd));
integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:1484:        return record.getFloatArray(pd.attribute);
integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:1490:        return record.getList(pd.attribute);
```

Two dispatch sites into `readProperty`: `:1129` is the vertex pass, `:1384` is the edge-source
pass (`FLOAT_ARRAY` and `LIST` fall into `processEdgeSource`'s default branch, since only
INTEGER/LONG/DOUBLE have primitive buffers). Both reach the same two accessors, so both are
entry points and both get a test.

Sibling shape - `getJSONArray` on a value a stringified export could have quoted, importer-wide:

```
$ grep -rn "getJSONArray(" --include='*.java' integration/src/main/java
GraphImporter.java:172,179,189,213,290,297,346          <- import-config JSON ("vertices", "edges", "edgeSources", "postImportCommands")
JsonlRowSource.java:139,149                             <- THE DEFECT
format/JsonlImporterFormat.java:289,308,361,396,429,461,497,505,516  <- schema/type/timeseries config in the dump header
format/JSONImporterFormat.java:323,596                  <- attribute-mapping config
Neo4jImporter.java:787                                  <- "labels" in a Neo4j dump record
```

18 hits, 2 of them the defect. The other 16 read *structural* JSON - an import-config file, a
dump's schema header, a Neo4j exporter's own `labels` array - not a user data column that a
CSV-to-JSONL converter could have quoted. None of them has an interface default that parses text,
so there is no drift to close there. Argued, not fixed.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| JSONL vertex pass -> `readProperty:1484` -> `getFloatArray`, stringified array | yes | yes - `aStringifiedVectorImportsOnTheVertexPass` |
| JSONL vertex pass -> `readProperty:1490` -> `getList`, stringified array | yes | yes - `aStringifiedListImportsOnTheVertexPass` |
| JSONL edge-source pass -> `:1384` -> `readProperty` -> both accessors, stringified array | yes | yes - `aStringifiedVectorAndListImportOnTheEdgeSourcePass` |
| JSONL, native JSON array (must not regress) | unchanged | yes - both vertex cases and the edge case assert the native row alongside the quoted one |
| JSONL, quoted `"[]"` / native `[]` parity | yes | yes - `aQuotedEmptyArrayMeansAnEmptyArrayJustLikeTheNativeOne` |
| `GraphImporter.fromJSON` config -> `parsePropertySpec` `"vector:"` / `"list:"` -> same accessors | yes | yes - `aStringifiedArrayImportsThroughTheJsonConfigEntryPoint` |
| JSONL, whitespace-only value now reads as CSV and XML read it | yes (behaviour change) | yes - `aWhitespaceOnlyValueReadsTheSameOnAllThreeSources` |
| JSONL, whitespace-only value for a LIST property still aborts, as on CSV and XML | unchanged | yes - `aWhitespaceOnlyListStillAbortsOnEveryThreeSourcesAlike` (added in review cycle 2) |
| JSONL, empty `""` still "not set" (#7269 must not regress) | unchanged | yes - `anEmptyStringIsStillNotSetForBothArrayAccessors`, plus the whole of `GraphImporterEmptyValueTest` |
| JSONL, malformed text (`"abc"`, `"[0.1"`) still reported, naming property + attribute | unchanged | yes - `aMalformedStringifiedArrayIsStillReported` |
| JSONL, a non-array non-string value (a JSON number under a vector property) still reported | unchanged | yes - `aScalarUnderAnArrayPropertyIsStillReported` |
| CSV source -> interface defaults | n/a - never overridden (grep above); already parses text | yes - `aStringifiedArrayReadsTheSameOnCsvAndXmlAsOnJsonl` pins CSV against the JSONL result |
| XML source -> interface defaults | n/a - same argument | yes - same test pins XML |
| JSONL numeric accessors on a quoted number | untouched by this change - `getInt`/`getLong`/`getDouble` are not edited | argued: pinned already by `GraphImporterEmptyValueTest.aQuotedNumberIsStillImported` |
| `get()` (plain STRING property) over a stringified array | untouched - already returns the raw text (#7185) | argued: `get()` is not edited |
| 16 other `getJSONArray` hits in the importer | no - structural config, not a data column | argued above |

### Reachability

`JsonlRowSource` is constructed by the import-config parser (`GraphImporter.java:172-297`, which
builds a source per `vertices`/`edgeSources` entry) and directly by callers of
`GraphImporter.builder(...).vertex(type, new JsonlRowSource(path), ...)`. `JsonlRecordReader` is
instantiated once per `forEach` (`JsonlRowSource.java:53`) and visited for every line. No flag
gates either accessor: `readProperty` dispatches on the declared property type alone. The changed
code runs on every JSONL row that declares a vector or list property.

### Residual risk

The fix changes only the two array accessors on the JSONL source. It does not touch `get()`, the
numeric accessors, the interface defaults, or any other importer format. A JSONL row whose vector
column holds neither a JSON array nor a parseable textual array still aborts the import with the
same `badValue` message it did before - that is a data error, not the drift this issue is about,
and `aMalformedStringifiedArrayIsStillReported` pins it.

`checkNotSplit` is deliberately not applied on the JSONL text path: JSONL has no field separator
(`RecordSource.fieldSeparator()` returns `null` for it), so its "use a delimiter such as ';'"
advice would point an operator at a setting that does not exist for the format. A truncated
textual array on JSONL therefore surfaces as the underlying parse failure instead, which is the
accurate diagnosis for a source nothing split.

## Fix

`JsonlRowSource.JsonlRecordReader`, two accessors:

- `getFloatArray` and `getList` hoist the `json.opt(attribute)` lookup they were already making
  through `notSet(attribute)`, then branch on the value: a `String` takes the textual parse the
  interface default uses (`VectorUtils.toFloatArray(text)` / `new JSONArray(text).toList()`), and
  anything else keeps the native `getJSONArray` path.
- `notSet` gains an overload over an already-looked-up value, so the two accessors branch without
  a second map lookup; `notSet(String)` delegates to it. One predicate, as #7269 left it.
- Lookup count on the row loop is unchanged: the old code did `opt` (inside `notSet`) plus
  `getJSONArray`; the new code does `opt` plus `getJSONArray` on the native path, and `opt` alone
  on the new textual path.
- `checkNotSplit` is deliberately not applied - see Residual risk above.

## Tests

New: `integration/src/test/java/com/arcadedb/integration/importer/GraphImporterStringifiedArrayTest.java`
(11 cases - 8 written before the fix, 2 added by the adversarial pass below, 1 by the review). No existing test was
modified or deleted.

Before the fix, 6 of the 8 failed with exactly the reported error, and the 2 "must not change"
cases passed:

```
[ERROR] Tests run: 8, Failures: 0, Errors: 6, Skipped: 0
  aStringifiedVectorImportsOnTheVertexPass » IllegalArgument Property 'embedding' is declared as a
    vector but attribute 'embedding' does not hold a numeric array
    (JSONObject[embedding] is not a JSON array ("[0.1,0.2,0.3]"))
  aStringifiedListImportsOnTheVertexPass » ... (JSONObject[tags] is not a JSON array ("[\"java\", \"sql\"]"))
  aStringifiedVectorAndListImportOnTheEdgeSourcePass » ... ("[0.5,0.6]")
  aStringifiedArrayReadsTheSameOnCsvAndXmlAsOnJsonl » ... ("[0.1,0.2]")
  aQuotedEmptyArrayMeansAnEmptyArrayJustLikeTheNativeOne » ... ("[]")
  anEmptyStringIsStillNotSetForBothArrayAccessors » ... ("[0.1]")
```

After the fix:

```
[INFO] Tests run: 10, Failures: 0, Errors: 0, Skipped: 0 -- GraphImporterStringifiedArrayTest
                                                             (11 after the review fixes below)
```

Whole `integration` module, which is where every `GraphImporter` caller lives
(`grep -rln 'JsonlRowSource|GraphImporter\b' --include='*.java' .` returns only `integration/`):

```
$ mvn -o test -pl integration -DexcludedGroups=benchmark,slow,vector
[INFO] Tests run: 309, Failures: 0, Errors: 0, Skipped: 9
[INFO] BUILD SUCCESS
```

`GraphImporterEmptyValueTest` (#7269's suite) and `GraphImporterEmptyDatetimeTest` (#7265's) are
in that run and stayed green.

## Impact

A JSONL file whose vector or list columns arrive quoted now imports instead of aborting. Nothing
else changes: the native JSON array path, `get()`, the numeric accessors, the interface defaults
and every other source are untouched, and a genuinely malformed value still ends the import with
the same message naming the property and the source attribute.

## Adversarial pass

The orchestrator's Phase 1.5 spawns an independent subagent that has not seen the author's
reasoning. **The `Task` tool is disabled in this session**, so no independent agent could be
spawned and the pass was run by the author against the tree instead. That is weaker by
construction - it is the same reader who wrote the patch - and is recorded as such rather than
presented as an independent review.

| # | Finding | Disposition |
|---|---|---|
| 1 | A whitespace-only value (`"embedding": "  "`) changes behaviour: it used to abort the import on JSONL and now yields an empty `float[]`, because `VectorUtils.toFloatArray("  ")` trims to nothing and returns `new float[0]`. Nothing pinned it. | **Real, in scope - fixed here.** It is the intended outcome (CSV and XML have always read it that way, so the three now agree), but it is a behaviour change and had no test. Added `aWhitespaceOnlyValueReadsTheSameOnAllThreeSources`, which asserts all three sources return `new float[0]`. It passed on the first run, so the reasoning above is now evidence rather than argument. |
| 2 | The new javadoc asserted a *mechanism* - "Gson parses a quoted number lazily" - inherited from #7269's comment in the same file and not proved in this session. | **Real, in scope - fixed here.** Reworded to the observable fact ("a quoted number in that same file already imported"), which `GraphImporterEmptyValueTest.aQuotedNumberIsStillImported` does prove. |
| 3 | `GraphImporter.fromJSON` is a public entry point that reaches the same accessors through `parsePropertySpec` (`"vector:"` / `"list:"`, `GraphImporter.java:416-421`) and `createRecordSource` (`:474-475`), and the coverage table did not name it. | **Real, in scope - fixed here.** It builds an identical `PropDef`, so no production change was needed, but it is the route an operator uses and it is now tested: `aStringifiedArrayImportsThroughTheJsonConfigEntryPoint`. |
| 4 | Skipping `checkNotSplit` on the JSONL text path could hide a genuinely truncated array. | **Not real.** `checkNotSplit`'s only action is to throw with advice to change the field delimiter; `fieldSeparator()` is overridden solely by `CsvRowSource` (`grep -rn fieldSeparator` returns `GraphImporter.java:771` default, `:969` caller, `CsvRowSource.java:62` override - JSONL inherits `null`), so that advice names a setting the format does not have. The truncated value still fails, via the parse itself, and `aMalformedStringifiedArrayIsStillReported` asserts both that it fails and that the message does not mention a delimiter. |
| 5 | The rewritten accessors might cost an extra map lookup per row. | **Not real.** The old code called `json.opt` (inside `notSet(attribute)`) and then `json.getJSONArray`; the new code calls `json.opt` and then `json.getJSONArray` on the native path, and `json.opt` alone on the new textual path. Same on the native path, one fewer on the textual one. |

No finding was out of scope, so no follow-up issue was filed.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7317

## Review cycles

| Cycle | Head SHA | Change | Bot review outcome |
|---|---|---|---|
| 1 | `c67cbf9215` | Initial push (14:46:19Z): the fix, 10 tests, this tracking doc. | Reviewed by `claude` at 14:50:12Z. No blocking objection - the fix, the deliberate `checkNotSplit` skip and the `notSet(Object)` overload were each traced and confirmed - plus two minor nits, both actionable and both applied in cycle 2. |
| 2 | `2e059ac349` | Both cycle-1 nits addressed. No production code changed. | Two further `claude` reviews landed (14:54:21Z, 14:55:50Z). The first repeated the cycle-1 duplicate-assertion nit against a pre-fix snapshot (it describes a 10-case suite, so it had not seen `2e059ac349`) - already fixed, nothing to do. The second raised one real coverage gap and one non-blocking performance note, both addressed in cycle 3. |
| 3 | `e89ea7f345` | Truncated-text case for `getList`; native-array path reads the `JSONArray` `opt()` already built. | Pushed for review. |

### A correction about cycle 1

An earlier revision of this document recorded cycle 1 as a **timeout**, and claimed the reviewer
had "posted nothing on any of the three surfaces" across "more than 80 minutes". Both halves were
wrong, and the way they were wrong is worth recording rather than quietly overwriting.

The facts, from the timestamps rather than from impression:

- the branch was pushed at **14:46:19Z**;
- the `claude` review was posted at **14:50:12Z**, three and a half minutes later;
- the revision of this file that declared the timeout was committed at **14:50:25Z** - *thirteen
  seconds after the review it said had never arrived*.

So the reviewer was not slow and nothing was missing from the API. The mistake was in the waiting:
each wait between polls was a `sleep` started as a **background** task, which returns control
immediately instead of blocking. Every "wait, then poll again" cycle therefore consumed no real
time at all, and what was believed to be eighty minutes of patient polling was roughly four minutes
of rapid polling against a review that had not been written yet. The `claude-review` workflow run
still showing `in_progress` was read as corroboration, when it was only the same few minutes seen
from a second angle.

Two lessons for the loop, one of them the opposite of what the first correction claimed:

1. A poll loop is only as slow as its waits actually are. A backgrounded `sleep` does not pace an
   agent, so a loop built from them can burn its whole retry budget inside a minute and report a
   timeout that describes nothing but its own haste. Pace with a blocking wait, or with a monitor
   that sleeps in the same process as the poll.
2. Elapsed time has to be read off timestamps, not accumulated in the head. Both the false claim
   here and its first correction were confident about a duration that a single `date -u` against
   the comment's `created_at` would have refuted.

### Cycle 1 feedback and its disposition

| Nit | Assessment | Action |
|---|---|---|
| Duplicate `assertThat(database.isTransactionActive()).isFalse();` in `aMalformedStringifiedArrayIsStillReported`. | **Real.** Verified: `grep -n isTransactionActive` showed the assertion at both line 424 and line 426, a copy/paste leftover introduced when the truncated-array case was added during the adversarial pass. | Removed the duplicate. |
| `aWhitespaceOnlyValueReadsTheSameOnAllThreeSources` pins the whitespace-only case for the vector accessor only. The reviewer traced that `new JSONArray("  ")` throws where `VectorUtils.toFloatArray("  ")` returns an empty vector, so the list side behaves differently and nothing pinned it. | **Real, and the derivation was correct** - confirmed by running it rather than by agreeing with it. The asymmetry is inherited from the interface defaults and predates this change, so it is pinned, not fixed. | Added `aWhitespaceOnlyListStillAbortsOnEveryThreeSourcesAlike`, asserting all three sources reject a whitespace-only list value. It passed on the first run. |

### Cycle 2 feedback and its disposition

| Item | Assessment | Action |
|---|---|---|
| `aMalformedStringifiedArrayIsStillReported` pins the truncated case (`"[0.1"`) for `getFloatArray` only; there is no equivalent for `getList` (`"[\"x\""`). The reviewer expected it to be fine but noted it was not directly pinned. | **Real.** It is a different parser (`new JSONArray(text)`, not `VectorUtils`) reached by a different branch, so "very likely fine" was worth converting into a test rather than accepting. | Added a truncated-list case to the same test, asserting it aborts naming the property and, like the vector case, without blaming a delimiter. It passed on the first run, so the expectation was correct - and is now pinned. |
| The native-array branch called `json.opt(attribute)` and then `json.getJSONArray(attribute)`, so it paid for a second lookup and a second `elementToObject` conversion whose first result was used only for the `instanceof String` test. Flagged as pre-existing and explicitly non-blocking. | **Real, and worth taking.** It is on the row loop, and `opt()` already returns the constructed `JSONArray` (`JSONObject.elementToObject` builds one for a JSON array), so the value can simply be used. | Both accessors now branch on `value instanceof JSONArray` and read it directly; the `getJSONArray` call remains only on the cold error path, where it still raises the identical `JSONException`. The `float[]` loop moved into a small `toFloatArray(JSONArray)` helper so the error path needs no unreachable branch. The native path is now one lookup, down from two. |

Nothing was deferred and nothing was skipped across the three reviews, so no `review-deferred-*.md`
notes file was produced.

## Tests, final

`GraphImporterStringifiedArrayTest` is now 11 cases, all green, and the whole `integration` module
is green after the review fixes:

```
[INFO] Tests run: 11, Failures: 0, Errors: 0, Skipped: 0 -- GraphImporterStringifiedArrayTest
[INFO] Tests run: 310, Failures: 0, Errors: 0, Skipped: 9   (whole module)
[INFO] BUILD SUCCESS
```

The full reactor also compiles clean with the change in place:

```
$ mvn -o compile -DskipTests
EXIT=0
```

## Final state

`clean-approval` on the substance: none of the three reviews received raised a correctness
objection - each traced the fix, the deliberate `checkNotSplit` skip and the `notSet(Object)`
overload independently and confirmed them - and every actionable item any of them raised is
applied. The developer owns the merge - this workflow does not merge PRs.
