# Issue #7266 - three small defects with the same shape

<https://github.com/ArcadeData/arcadedb/issues/7266>

Three unrelated defects, filed together. The shape they share: a guard or a figure added or
corrected recently, whose new form does not do what its own comment or contract says.

## Finding ledger

- [x] 1. JSON importer config crashes with `ArrayIndexOutOfBoundsException` before the new
      `checkEdgeSourceEndpoint` validation can report the mistake - **fixed**, on both endpoints and
      on the `"filter"` sibling of the same shape in the same parser
- [x] 2. Studio's "Databases Disk" card counts the filesystem's root-reserved blocks as used -
      **fixed**, the server now reports `diskUsedSpace` and the card renders it
- [x] 3. The second-pass full-rebuild guard in `LocalSchema.loadIncremental` is unreachable -
      **fixed for the bloom filter** (the half that was actually broken), made explicit rather than
      accidental for the compacted index, and deliberately left off for the dictionary with the
      reason stated in both places the contract is written down

## Analysis

### 1. `GraphImporter` JSON edge endpoint parse

`integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:352-355`
splits `"from"` / `"to"` on `:` and indexes `[1]` unconditionally. A value that forgot the
`:VertexType` half - the exact mistake `checkEdgeSourceEndpoint` (`:892-906`, added by
`2a3cb60ef`) was written to describe - throws
`ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1` inside the
`b.edgeSource(...)` consumer, which `Builder.edgeSource` runs eagerly (`:459-465`), i.e. before
`build()` ever reaches the validation.

Two further mis-shapes of the same value are silently accepted rather than reported:
`"a:b:c"` drops `:c`, and an empty half yields an empty attribute or vertex type.

**Sibling of the same shape, same file, same parser:** `:306`
`vj.getString("filter").split("=", 2)` then `parts[1]`. A filter written without `=` throws the
same `ArrayIndexOutOfBoundsException` from the same JSON entry point.

### 2. Studio "Databases Disk" used figure

`#7223` moved the profiler's free-space reading to `File.getUsableSpace()`, which excludes the
blocks the filesystem reserves for root. `total` stayed `File.getTotalSpace()`, which counts
them. Studio derives `used = total - free`
(`studio/src/main/resources/static/js/studio-server.js:153-155`), so the reservation - 5% of an
ext4 by default - is charged to the databases. On a 100 GB volume holding 1 GB of databases the
card reports roughly 6 GB used, a constant offset an operator watching for growth reads as data.

`java.io.File`, precisely:

| reading | counts root-reserved blocks as |
|---|---|
| `getTotalSpace()` | part of the total |
| `getFreeSpace()` | free |
| `getUsableSpace()` | neither free nor available to this JVM |

So `total - usable` overstates used by the reservation, while `total - free` is what `df` calls
Used. The fix reports the allocated figure from the server, derived from the two readings that
describe the same thing, and leaves `diskFreeSpace` (usable) alone - that one is the right
reading for "how much room is left", which is what the low-space warning and the percentage use.

`ServerMonitor.checkDiskSpace()` (`server/.../monitor/ServerMonitor.java:118-142`) reads the same
pair but only ever reports *available* space and a percentage of it. It computes no "used"
figure, so it carries none of this defect.

### 3. `LocalSchema.loadIncremental` second pass

`engine/src/main/java/com/arcadedb/schema/LocalSchema.java:419-436`. The loop reads:

```java
final Component current = getFileByIdIfExists(fileId);
if (current == null || !(current.getMainComponent() instanceof IndexInternal))
  continue;                                     // <- short-circuits here
...
if (NON_INCREMENTAL_COMPONENT_EXTENSIONS.contains(file.getFileExtension()))
  return false;                                 // <- never reached for .bfidx
```

What each owner of a non-incremental extension answers from `getMainComponent()`:

| extension | component | `getMainComponent()` | second-pass guard reached? |
|---|---|---|---|
| `dict` | `Dictionary extends PaginatedComponent` | `this` | no |
| `uctidx` / `nuctidx` | `LSMTreeIndexCompacted extends LSMTreeIndexAbstract` | `mainIndex`, wired by `LSMTreeIndexMutable.onAfterLoad` (`:134-137`) | yes, but only because of that wiring |
| `bfidx` | `LSMTreeIndexBloomFilter extends PaginatedComponent` | `this` | **no** |

So the issue's claim is half right, and the half that is right is the one that matters: a touched
**bloom filter** never forces the rebuild, and its in-RAM `directory` (`LSMTreeIndexBloomFilter:133`,
filled once by `loadDirectory()` through `attachBloomFilters()` and re-read by no load hook) is
left describing the file as it was before this entry appended to it. A touched **compacted index**
does fall back today, but only as a side effect of `mainIndex` having been wired by its mutable
index - nothing in the guard says so, and a compacted component built by the factory starts with
`mainIndex == null` (`LSMTreeIndex.java:112,124`).

**The dictionary must NOT be added to the second-pass fallback.** `walTouchedFileIds`
(`ha-raft/.../ArcadeStateMachine.java:2292-2311`) is every file id the entry's WAL wrote a page
into, and a DDL entry that adds a type or property name writes dictionary pages. Making `dict`
force the rebuild from `touchedFileIds` would send every name-adding DDL entry back to the full
`load()` - which is exactly the O(entries x total files) cost issue #6988 removed. The second
pass's own comment already states why a touched dictionary needs nothing here
(`TransactionManager.applyChanges` reloads it). So the two passes need two sets, and the comments
have to say so rather than claiming one set covers both.

Severity, per the issue's own triage note: a stale bloom-filter directory is a *performance*
regression, not a wrong answer. `mightContain` validates each directory entry against the series'
page count and fingerprint before trusting it (`LSMTreeIndexBloomFilter:264-266`) and answers
`true` - "search this series" - for everything it does not recognise, so a stale directory can
only cost skipped optimisations, never hide a row.

## Completeness

### Invariants

1. A malformed `"from"`, `"to"` or `"filter"` value in a JSON importer config raises an
   `IllegalArgumentException` naming the offending value; it can never raise
   `ArrayIndexOutOfBoundsException`.
2. The "used" figure the Studio disk card renders is derived from two readings that count the
   filesystem's root-reserved blocks the same way, so the reservation is never charged to the
   databases.
3. `loadIncremental` refuses - returning `false`, having changed nothing - for a touched
   compacted index or bloom filter, regardless of what that component answers from
   `getMainComponent()`.

### Enumeration

Commands run in the worktree, with their output.

```
$ grep -rn "split(" --include='*.java' integration/src/main/java/com/arcadedb/integration/importer/graph/
GraphImporter.java:306:        final String[] parts = vj.getString("filter").split("=", 2);
GraphImporter.java:352:      final String[] fromParts = ej.getString("from").split(":");
GraphImporter.java:354:      final String[] toParts = ej.getString("to").split(":");
CsvRowSource.java:91:    return line.split(String.valueOf(delimiter), -1);      # split(...,-1), never indexed blind
```

```
$ grep -rn "fromJSON(database\|fromJSON(db\|parseEdgeSource(\|parseVertexSource(" --include='*.java' integration/src server/src console/src package/src
GraphImporter.java:148:      try (final GraphImporter importer = fromJSON(database, config, baseDir)) {    # main()
GraphImporter.java:269:    return fromJSON(database, new JSONObject(json), baseDir);                      # String overload
GraphImporter.java:283:        parseVertexSource(b, vertices.getJSONObject(i), baseDir);
GraphImporter.java:290:        parseEdgeSource(b, edgeSources.getJSONObject(i), baseDir);
(+ 6 test call sites; no other production caller in server/, console/ or package/)
```

Both public `fromJSON` overloads and `main()` funnel through the same two private parsers, so
one fix at the parse site covers every entry point into the JSON config.

```
$ grep -rn "diskFreeSpace\|diskTotalSpace\|diskUsedSpace" --include='*.js' --include='*.html' --include='*.java' --include='*.py' --include='*.ts' . | grep -v test
studio/src/main/resources/static/js/studio-server.js:153,154,273
engine/src/main/java/com/arcadedb/Profiler.java:421,422,423
```

Exactly one producer (`Profiler.toJSON`) and one consumer (`displayServerSummary`, plus the
skip-list at `:273` that keeps the card's own fields out of the details table). No other client -
no Python or JS binding, no HTTP handler - derives a used figure from the pair.

```
$ grep -rn -A3 "public Object getMainComponent" --include='*.java' engine/src/main/java/
HashIndexBucket:213 -> mainIndex          LSMTreeIndexAbstract:289 -> mainIndex
LSMTreeIndexBloomFilter:168 -> this       SparseSegmentComponent:126 -> this
LSMVectorIndexCompacted:107 -> mainIndex  LSMVectorIndexMutable:78 -> mainIndex
LSMVectorIndexGraphFile:99 -> mainIndex   Component:50 -> this
```

The two overrides answering `this` are the bloom filter and the sparse-vector segment. Only the
bloom filter's extension is in `NON_INCREMENTAL_COMPONENT_EXTENSIONS`; `SparseSegmentComponent`
is not, and never was, so it is out of this issue's scope and unchanged by this fix.

```
$ grep -rn "loadIncremental" --include='*.java' .
ha-raft/.../ArcadeStateMachine.java:2352      # the only production caller
engine/.../LocalSchema.java:385               # the definition
engine/src/test/.../Issue6988IncrementalSchemaLoadTest.java  # 9 call sites
```

### Coverage table

| # | Entry point | Covered by fix? | Covered by a test? |
|---|---|---|---|
| 1 | `GraphImporter.fromJSON(db, JSONObject, dir)` -> `parseEdgeSource` -> `"from"` malformed | yes | yes - `Issue7266ImporterConfigValidationTest#aFromEndpointMissingItsVertexTypeIsReported` |
| 1 | ... -> `"to"` malformed | yes | yes - `#aToEndpointMissingItsVertexTypeIsReported` |
| 1 | ... -> `"from"` with more than one colon / an empty half | yes | yes - `#anEndpointWithTheWrongNumberOfPartsIsReported` |
| 1 | `GraphImporter.fromJSON(db, String, dir)` (String overload -> same parser) | yes | yes - `#theStringOverloadReportsItToo` |
| 1 | `parseVertexSource` -> `"filter"` without `=` (sibling) | yes | yes - `#aFilterWithoutAnEqualsIsReported` |
| 1 | `parseVertexSource` -> `"filter"` with an empty VALUE (`"attr="`) | deliberately accepted | yes - `#aFilterWithAnEmptyValueIsAccepted` pins the asymmetry |
| 1 | `parseEdgeSource` -> `"from"` / `"to"` key ABSENT (found by the adversarial pass) | yes | yes - `#anAbsentEndpointKeyIsReportedAsTheMissingEndpointItIs` |
| 1 | `GraphImporter.main()` | yes, via `fromJSON` | argued: `main()` is a two-line wrapper over `createSchemaFromConfig` + `fromJSON`; it adds no parsing of its own (`GraphImporter.java:127-157`) |
| 1 | a well-formed config still parses | n/a | yes - `#aWellFormedConfigStillParses` |
| 2 | `Profiler.toJSON()` -> `diskUsedSpace` | yes | yes - `Issue7266ProfilerDiskUsedSpaceTest` |
| 2 | Studio card -> `displayServerSummary` with a server that reports `diskUsedSpace` | yes | yes - `studio/test/server-disk-used.test.js` |
| 2 | Studio card -> a server that does not report it (older build) | yes, falls back to the old difference | yes - same file |
| 2 | `ServerMonitor.checkDiskSpace()` | not applicable | argued: it reports available space and its percentage only, and computes no used figure (`ServerMonitor.java:126-136`) |
| 3 | `loadIncremental` second pass, touched `.bfidx` | yes | yes - `Issue7266IncrementalSchemaSecondPassTest#aTouchedBloomFilterForcesTheFullRebuild` |
| 3 | `loadIncremental` second pass, touched `.uctidx`/`.nuctidx` | yes (made explicit; previously implicit) | yes - `#aTouchedCompactedIndexForcesTheFullRebuild` |
| 3 | `loadIncremental` second pass, touched `dict` | deliberately NOT changed | argued + pinned: `#aTouchedDictionaryStaysIncremental` proves it stays incremental, because forcing it would undo #6988 (see Analysis) |
| 3 | `loadIncremental` first pass, unregistered non-incremental file | unchanged | pre-existing: `Issue6988IncrementalSchemaLoadTest#anUnregisteredDictionaryCompactedIndexOrBloomFilterForcesTheFullRebuild` |

No row is blank, so no follow-up issue is filed for this change.

### Reachability

- **1.** `GraphImporter.fromJSON` is public API and the entry point `GraphImporter.main()` uses;
  the new checks sit on the only two parsers both overloads and `main()` reach.
- **2.** `Profiler.toJSON()` backs `GET /api/v1/server`, which is what Studio's summary polls -
  the path #7223 named as "the copy that actually reaches an operator". The Studio JS is served
  as-is (no bundler for application JS), so editing the file changes the page.
- **3.** `arcadedb.ha.schemaIncrementalApply` defaults to **true**
  (`GlobalConfiguration.java:1637-1641`, pinned by
  `Issue6988SchemaIncrementalApplySettingTest:44`), so
  `loadIncremental` runs on every cluster on this build, and `ArcadeStateMachine` is its only
  production caller.

### Residual risk

- Finding 3 leaves a touched **dictionary** on the incremental path, on purpose. That is not a
  regression - it is today's behaviour and the second pass's own comment already argues for it -
  but it does mean `NON_INCREMENTAL_COMPONENT_EXTENSIONS` and the new touched-file set differ by
  one entry, which is now stated in both places rather than implied.
- Finding 3 does not change the first pass at all, and does not touch
  `SparseSegmentComponent`, whose extension was never in the set.
- Finding 2 changes what the card renders, not what any alert fires on: the low-space warning and
  `diskFreeSpacePerc` still read usable space, which is the right reading for "room left".
- A Studio talking to a server older than this change keeps the old, overstated figure. There is
  no way to derive the right one from a payload that does not carry it, and the fallback is what
  keeps the card rendering at all.

## Changes

| File | Finding | What changed |
|---|---|---|
| `integration/.../importer/graph/GraphImporter.java` | 1 | new `splitEdgeSourceEndpoint()` raises an `IllegalArgumentException` naming the edge source, the endpoint and the offending value; the `"filter"` parse gets the same guard |
| `engine/src/main/java/com/arcadedb/Profiler.java` | 2 | `toJSON()` adds `diskUsedSpace`, derived as `getTotalSpace() - getFreeSpace()`; `diskFreeSpace` still reports usable space |
| `studio/.../js/studio-server.js` | 2 | the card renders `diskUsedSpace` when the server reports it, falls back to the old difference when it does not, and the new field joins the card's siblings in `skipProfiler` |
| `engine/src/main/java/com/arcadedb/schema/LocalSchema.java` | 3 | new `NON_INCREMENTAL_TOUCHED_COMPONENT_EXTENSIONS`, checked before the `instanceof IndexInternal` narrowing; the class javadoc, the `@param` and the `attachBloomFilters` skip comment now name which set governs which pass |

## Test results

Every run used an isolated local Maven repository (`-Dmaven.repo.local=$WORKTREE/.m2repo`),
because other agents were building overlapping modules at the same time.

| Suite | Result |
|---|---|
| `Issue7266ImporterConfigValidationTest` (new) | 5 of 6 failed with the reported `ArrayIndexOutOfBoundsException` before the fix; 6/6 green after |
| `Issue7266ProfilerDiskUsedSpaceTest` (new) | 3 tests, 1 skipped on APFS - `theReservedBlocksAreNotChargedToTheDatabases` needs a filesystem that reserves blocks, and says so instead of passing for the wrong reason |
| `studio/test/server-disk-used.test.js` (new) | 5/5; reverting the JS line turns 3 of them red, which is the proof they can fail |
| `Issue7266IncrementalSchemaSecondPassTest` (new) | `aTouchedBloomFilterForcesTheFullRebuild` failed before the fix ("Expecting value to be false but was true"); 3/3 green after |
| `integration`: `GraphImporter*`, `StackOverflowImporterConfigTest`, `UberTripsImportTest` | 67 run, 0 failures (6 skipped - the StackOverflow fixture needs a data dump that is not in the repo) |
| `engine`: `Issue7223*`, `Issue6988*`, `ProfilerTest`, `SchemaTest`, `LSMTreeIndexTest`, `Issue5662*`, `Issue5119*`, `Issue5120*` | 94 run, 0 failures |
| `studio`: full `node scripts/run-tests.js` | 76/76 |
| `ha-raft`: `Issue6988SchemaIncrementalApplySettingTest`, `Issue6988FullRebuildFallbackIT`, `Issue6988IncrementalSchemaApplyIT` | 6/6 - the run that proves the second-pass change did not send the common DDL path back to the full rebuild |
| `engine,integration,ha-raft,server -am install -DskipTests` | BUILD SUCCESS |

### A note on what the profiler test can and cannot prove

`theUsedFigureIsDerivedFromTheUnallocatedReadingNotTheUsableOne` pins the formula against the
same directory's own readings, but on a filesystem that reserves nothing - APFS, tmpfs - the two
formulas produce the same number and the assertion cannot separate them. The second test says so
explicitly with an `assumeTrue` rather than passing quietly. The decisive numeric case for the
arithmetic lives in the JS test, where the numbers are synthetic and a 5% reservation is modelled
directly.

## Adversarial pass

The orchestrator asks for a subagent that has not been persuaded by the author's reasoning. No
`Task` tool is available in this harness, so no independent agent could be spawned; the pass was
run by the same agent reading only the diff, which is weaker and is recorded as such. Findings:

1. **Real, in scope, fixed here.** An edge source config with **no** `"from"` key at all - which
   is literally what the message `checkEdgeSourceEndpoint` gives ("declares no 'from' endpoint")
   describes - was still not covered: `JSONObject.getString(name)` throws a `JSONException`
   naming the key and nothing else (`JSONObject.java:234-241`), so the first version of this fix
   only helped a key that was present and malformed. The endpoints are now read with a `null`
   default and an absent one is reported by the same helper. New test:
   `#anAbsentEndpointKeyIsReportedAsTheMissingEndpointItIs`, and the coverage table gained a row.
2. **Real, out of scope, argued rather than filed.** The `existsFile` check in the second pass
   now runs before the `instanceof` narrowing, so a touched file whose component is registered but
   whose file the `FileManager` no longer holds sends the caller to the full rebuild where it used
   to be skipped for a non-index component. Reachable only from an inconsistent state, and the
   full rebuild is the fallback, so it is strictly the safer of the two: a retired file id already
   makes `loadIncremental` refuse at its first guard (`removedFileIds` non-empty), and a file
   retired by an earlier entry leaves neither a component nor a `FileManager` entry, so
   `getFileByIdIfExists` answers null and the loop continues as before.
3. **Not real.** "A `dict` extension could belong to a component that IS an `IndexInternal`, so
   dropping the dictionary from the touched set loses a case the old code caught."
   `Dictionary.DICT_EXT` is `"dict"` and `TimeSeriesTagDictionary.DICT_EXT` is `"tstd"`
   (`Dictionary.java:72`, `TimeSeriesTagDictionary.java:98`), so `"dict"` has exactly one owner,
   and `Dictionary extends PaginatedComponent` inherits `Component.getMainComponent()`, which
   answers `this`. No case is lost.
4. **Not real.** "The extra `getFreeSpace()` call costs a syscall on every Studio poll."
   `toJSON()` already makes two `statfs`-class calls at the same site and is called at the poll
   interval, not per request served. One more is not measurable against the JSON the same method
   builds.
5. **Argued, deliberately not fixed.** `vj.getString("type")` and the other required keys in the
   same parsers throw a bare `JSONException` when absent. That is pre-existing and general to the
   config format, and no recently-added validation is being bypassed by it - which is what makes
   finding 1 a defect rather than a wish. Widening the fix to every key would be a different
   change.

## Review cycles

### Cycle 1 - `5926c3d` (PR #7274)

The `claude` bot reviewed and raised nothing blocking. It independently confirmed three things this
branch relies on: that `split(":")` on `":"` collapses to a zero-length array so `parts.length != 2`
short-circuits before any indexing (the new guard cannot itself throw the exception it exists to
catch), that `getString(name, default)` reaches the default through `isNull()` for an absent key as
well as a null value, and that the new extension set names the right constants. Two points were
actionable:

1. **The `"filter"` guard checks an empty half on the attribute only, while the endpoint guard
   checks both sides** - an asymmetry between two guards added in the same commit "for the same
   shape" of bug. The reviewer asked for either a comment confirming it is deliberate or an
   alignment of the two. It IS deliberate, and the evidence decides it: `"attr="` selects rows whose
   attribute is empty, and two of the three record sources can answer that - `XmlRowSource:142-144`
   returns the raw attribute value, `JsonlRowSource:75-85` returns `""` for an explicit empty
   string - while only `CsvRowSource:98-101` folds empty to null. Rejecting it would refuse a config
   that is meaningful on two of three sources. Added the comment naming those three sites, plus
   `#aFilterWithAnEmptyValueIsAccepted` so the asymmetry stays deliberate rather than drifting.
2. **The `existsFile` reordering changes behaviour for a touched non-index component** whose file
   the `FileManager` has lost. Already argued in the adversarial pass above; the reviewer asked for
   it to be visible in the PR description rather than only here, which is right - it is the kind of
   thing a reader of the PR should not have to find in a doc. Added to the PR body.

Not actioned, with the reason: the reviewer noted the tracking doc "reads more like an internal
working log than user-facing documentation" and then answered itself - `docs/6989-*.md`,
`docs/7122-*.md` and `docs/7225-*.md` are the same shape, so this follows the repo's convention.
No deferred items.

### Cycle 2 - `9d0c308`

Nothing blocking again, and the bot re-verified the cycle-1 answers rather than taking them on
faith - it traced `String.split(":")`'s trailing-empty semantics for every malformed shape the test
enumerates, and checked the filter comparison against `CsvRowSource` itself to confirm `attr=` is
genuinely unmatchable on CSV and meaningful on the other two. Three minor points:

1. **Test weight** - `Issue7266IncrementalSchemaSecondPassTest` inserted 10,000 records and forced a
   compaction per test, and the reviewer asked whether that earned `@Tag("slow")`. Measured rather
   than assumed: the class ran in **1.56 s**. Rather than tag a class that is not slow, the fixture
   was sized down to the smallest that still produces a multi-page compacted series with a bloom
   filter over it - 2,000 records, **1.10 s** for the whole class - and the constant now carries a
   javadoc saying what the number is for. It stays in the default lane, which is the honest answer:
   `@Tag("slow")` is for classes that genuinely cost seconds, and tagging a fast one dilutes the
   lane.
2. **The filter asymmetry was documented where only an internals reader would find it** - an inline
   comment in `parseVertexSource` and a test. `Builder.filter(String, String)` is the method a config
   author actually reads, and it said nothing. Its javadoc now states which sources can match an
   empty value and which cannot, and that an empty attribute is refused.
3. **Whether `docs/<issue>-*.md` is the right long-term home** for a per-issue working log. The
   reviewer noted it matches the existing precedent and framed it as a repo-wide convention question
   rather than something for this PR. Left alone deliberately: changing where these live is not this
   branch's business, and doing it here would bury the fix.

No deferred items in either cycle.

### Cycle 3 - `c91dee2` - clean approval

No actionable items. The bot re-derived the three load-bearing facts from the source rather than
from this branch's claims: that every malformed endpoint shape lands in the `parts.length != 2`
branch before any indexing, so the new guard cannot throw the exception it exists to prevent; that
`JSONObject.getString(name, default)` routes both an absent key and an explicit JSON null through
the guarded path; and that `LSMTreeIndexBloomFilter.getMainComponent()` answers `this` while
`LSMTreeIndexCompacted` answers its `mainIndex` only after `onAfterLoad` wired it - which is exactly
the reading that makes the old ordering let a touched `.bfidx` through. It also confirmed the
"returns false implies nothing modified" contract still holds, since every early return happens
before `toReplace` or `registerLoadedComponent` are touched.

Three observations, none requiring a change:

- the extra `getFreeSpace()` syscall - "negligible at the polling interval this runs at, not worth
  restructuring";
- the `existsFile` reordering - "the reasoning holds up", and the case is only reachable when the
  `FileManager` already disagrees with the schema's registered components. The full `engine` and
  `ha-raft` suites passing is the evidence that nothing depended on the narrower ordering;
- the tracking-doc convention - raised for the third time and each time framed as a repo-wide
  question rather than something this branch should settle.

## Final state

**clean-approval** after 3 review cycles. Every finding fixed, every coverage row accounted for, no
follow-up issue needed and none filed, no deferred items.

| Cycle | Head | Outcome |
|---|---|---|
| 1 | `5926c3d` | 2 actionable: document the deliberate filter asymmetry, surface the `existsFile` behaviour change in the PR body. Both applied. |
| 2 | `9d0c308` | 2 actionable: size the schema fixture down rather than tag it slow (measured 1.56 s -> 1.10 s), document the asymmetry on `Builder.filter`. Both applied. |
| 3 | `c91dee2` | No actionable items. Clean approval. |

PR: <https://github.com/ArcadeData/arcadedb/pull/7274>

Merge is the developer's.
