# #5802 - a mis-ordered index after an in-place upgrade is only discoverable by grepping the startup log

## Issue

A production install running `26.7.2` had its `lib/*.jar` swapped for a build of current `main` and was
restarted. Before any query ran, the log emitted one `WARNING` per affected index:

```
WARNI [LSMTreeIndexCompacted] Index 'Paper_0_84306331895885' is physically ordered differently
than the current key comparator, so lookups can return fewer (or foreign) records than a scan:
run 'REBUILD INDEX Paper_0_84306331895885'. Details: [page 20 entry 8: ...]
```

Both a `FULL_TEXT` index (`Paper[title,abstract]`) and a plain non-unique string index (`Author.name`)
were reported. The reporter rolled back to `26.7.2` and asked two things:

1. Is this an expected manual migration step, and should it be surfaced more prominently than a
   `WARNING` log line?
2. Is there any way to learn *which* indexes are affected without grepping the startup log?

## Root cause of the underlying condition

The report is accurate: the disorder is real, not a false positive from the new detector.

`BinaryComparator.compareBytes(byte[], Binary)` - the routine the LSM binary search uses to place and to
find a `STRING` key - compared bytes as **signed** until `#5321` (`02d5800ea`), while every other string
comparison in the engine is unsigned. A UTF-8 lead byte is `>= 0x80`, so it is negative as a Java byte:
under the old comparison `Á` sorted *before* every ASCII letter, under the new one it sorts after.

That is why the reporter's `Author.name` evidence starts with an accented name and why several of the
reported pairs are pure ASCII. The mutable index pages were physically laid out in signed order. A
compaction then reads those pages with cursors that assume ascending order and merges them with
`LSMTreeIndexAbstract.compareKeys` (object comparison, always unsigned), so once the two orders diverge
at one accented key the merge emits a stretch of keys out of order - ASCII neighbours included - and
writes them to the compacted pages as-is. `#5321` did not create the disorder; it made the reader
disagree with what an older build had already written.

The remedy is therefore correctly `REBUILD INDEX`, and it cannot be applied automatically: rebuilding
every index of a multi-million-record database on the open path would turn a restart into an outage.

## What was actually wrong

The reporting, not the detection. `LSMTreeIndexCompacted.checkKeyOrderOnLoad()` (added in `8489de7fc`)
was a dead end for an operator:

- It named the **physical bucket sub-index** (`Paper_0_84306331895885`), the name a user never sees
  anywhere else. The logical index they would act on is `Paper[title,abstract]`.
- It existed only as a log line. There was no way to ask the database which indexes were affected, so a
  restart whose logs had rotated away left no trace at all.
- `CHECK DATABASE` does report it, but it walks **every page of every index** - not something to run on
  the box the reporter was trying to bring back up.

Meanwhile the engine already had exactly the right channel for "this index should be rebuilt, here is
why": `IndexInternal.getUpgradeWarning()`. `LocalSchema.reportUpgradeWarning` logs it once per database
open per **logical** index with the `REBUILD INDEX` command to run, and it is exposed as `upgradeWarning`
on `schema:indexes` and `schema:index:<name>`, which is what Studio renders. The key-order check simply
was not wired into it.

## Fix

`engine/src/main/java/com/arcadedb/index/lsm/LSMTreeIndexCompacted.java`

- `checkKeyOrderOnLoad()` records its verdict in a `volatile String keyOrderMismatch` instead of only
  logging it, and exposes it through `getKeyOrderMismatch()`. Caching is what lets the reader honour the
  `getUpgradeWarning()` contract of being cheap and side-effect free - it is called per index on every
  schema listing and must not touch a page to answer.
- The remaining log line keeps the physical evidence (page and entry numbers of *this* sub-index) but
  drops to `FINE`. That detail is support material; the operator-facing warning is now the one below,
  which names something they can act on. Emitting both at `WARNING` would restate the same finding under
  two different names, which is the confusion the issue is about.

`engine/src/main/java/com/arcadedb/index/lsm/LSMTreeIndex.java`

- `getUpgradeWarning()` returns the sub-index verdict. Only the compacted sub-index is covered: checking
  the mutable pages means walking all of them, which is `checkIntegrity()`'s job for `CHECK DATABASE` and
  not something the open path can afford.

`engine/src/main/java/com/arcadedb/index/TypeIndex.java`

- `getUpgradeWarning()` scans every bucket sub-index instead of answering from the first one. The
  existing comment - "every bucket sub-index shares one definition, so the first one answers for all of
  them" - holds for a warning derived from the *definition* (the geospatial cell layout of `#5478`), but
  a key-order mismatch is physical state that one bucket can be in and another not. Asking only bucket 0
  would report a type index healthy while one of its buckets needed a rebuild.

`engine/src/main/java/com/arcadedb/index/fulltext/LSMTreeFullTextIndex.java`

- Delegates `getUpgradeWarning()` to the LSM index it stores its terms in. It implements `IndexInternal`
  by composition rather than inheritance, so without this it silently answered the interface default of
  `null` - and a full-text index is the one whose keys are user text, where non-ASCII characters actually
  live. This is the `Paper[title,abstract]` half of the report.

### The channel's premise had to widen

`getUpgradeWarning()` was built for one shape of advice: an index on an older layout that still answers
correctly and is only missing what the new layout buys (the geospatial cell layout of `#5478`). Its
contract said so, `LocalSchema.reportUpgradeWarning` repeated it, and Studio's banner told the reader in
so many words that the flagged indexes "keep working as they are - rebuilding is optional".

A key-order mismatch is the other shape: the index does **not** answer correctly. Reusing the channel
without saying so would have put a reassurance directly above a message explaining that lookups return
fewer records than a scan.

- `engine/src/main/java/com/arcadedb/index/IndexInternal.java` and
  `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`: the contract now names both shapes and
  asks an implementation to make clear which one it is reporting.
- `studio/src/main/resources/static/js/studio-database.js`: the banner header now states neither a
  severity nor a remedy - "Some indexes need attention. Each note below says what is wrong and what to do
  about it." Three conditions can reach it now (missing a newer layout's benefit, confirmed under-return,
  and not verifiable at startup), and a shared header naming a remedy would overstate the mildest and
  understate the worst. The per-message text carries both, and Studio already groups rows by message, so
  each condition renders as its own block with its own affected index names. No other Studio change was
  needed - it already keys those blocks on `typeIndexName` and renders one
  ``REBUILD INDEX `Paper[title,abstract]` `` per logical index.

### Every LSM-backed wrapper reports, invariant or not

`LSMTreeGeoIndex` and `LSMSparseVectorIndex` also wrap an LSM index by composition, and neither should
ever be exposed to this: geohash cells and dimension identifiers are ASCII, so no key either stores can be
ordered differently by the signed/unsigned change.

They delegate anyway. `LSMSparseVectorIndex` delegates outright; `LSMTreeGeoIndex` asks the underlying
index **first** and falls back to its own layout advisory. The argument for exempting them is a claim
about the *keys*, not a property of those classes, and an override that returned early would hide a
mismatch if the claim ever stopped holding - a silent wrong answer being the exact failure mode this
whole issue is about.

The geo ordering is deliberate and was corrected during review: asking the layout advisory first would
have masked a key-order mismatch behind it, which is precisely what the delegation exists to prevent. A
mismatch is a correctness fault (lookups under-return); the legacy cell layout is a cost advisory. The
worse of the two wins.

## What an operator now gets

One `WARNING` per affected logical index at open, naming the command that repairs it:

```
Index 'Paper[title,abstract]' of database 'papers' should be rebuilt: its pages are physically sorted
in a different key order than the one lookups apply, ... Run: REBUILD INDEX `Paper[title,abstract]`
```

and, independent of the log, the affected set as a query:

```sql
SELECT name, typeIndexName, upgradeWarning FROM schema:indexes WHERE upgradeWarning IS NOT NULL
```

`typeIndexName` is the name `REBUILD INDEX` takes. Studio flags the same rows.

**That query is not an exhaustive affected-set, and should not be described as one.** It reports what the
open path can establish cheaply: the compacted sub-index of each index. Two things fall outside it.

- An index small enough never to have been compacted has no compacted sub-index, so it is never checked -
  it can be mis-ordered and still not appear. This is the pre-existing scope of the detection (the old log
  line was compacted-only too), not something this change narrowed, and widening it would mean walking
  every mutable page of every index on the open path.
- An index whose check could not run - a page read that failed - reports a distinct "could not be
  verified" advisory rather than nothing, and logs at `WARNING` rather than `FINE`. Publishing the
  *unknown* verdict through the same field is the point: leaving it null would answer "healthy" for an
  index nobody checked, sending the operator back to the startup log this change exists to replace.
  Rebuilding is a valid response to unverifiable too - it makes the order correct by construction - so
  the remedy the advisory names still applies.

  That verdict is **not latched**. `checkKeyOrderOnLoad()` marks itself done only when the check
  *completed*, so a run that threw is retried on the next load of the component, and a later success
  publishes the clean verdict over the stale one. This matters because the callback runs on every
  component load - including the Raft apply path on a replica and a snapshot resync, where a page read
  can fail transiently - and `onAfterLoad` resolves the sub-index through
  `Schema.getFileById()`, which hands back the *same* instance every time. Latching the first failure
  would therefore pin false "needs rebuild" advice on a healthy replica index for the lifetime of the
  database. The retry costs the bounded root-page read again, only in the already-failing case.

`CHECK DATABASE` remains the exhaustive answer, at the cost of walking every page. The query is the cheap
first pass an operator runs on a restart, not a clearance.

## Verification

`engine/src/test/java/com/arcadedb/index/lsm/Issue5802KeyOrderUpgradeWarningTest.java`.

The disorder is planted by swapping two pointers of a page's entry array - the same physical state an
index written under the old comparator ends up in, reproduced without needing the old comparator, and the
technique the existing `LSMTreeIndexKeyOrderCheckTest` established. Each test compacts, disorders, and
reopens, because the check runs when the sub-index is wired up.

| Test | Pins |
|---|---|
| `aHealthyIndexCarriesNoUpgradeWarning` | No warning on a correctly ordered index, so the others can fail |
| `aDisorderedCompactedIndexIsReportedAsAnUpgradeWarning` | The load-time verdict reaches `getUpgradeWarning()` |
| `theAffectedIndexesAreDiscoverableFromSchemaIndexes` | `schema:indexes` and `schema:index:<name>` expose it, with `typeIndexName` carrying the rebuildable name |
| `aTypeIndexReportsAWarningRaisedByANonFirstBucket` | The `TypeIndex` fix: a warning from a bucket other than the first is not swallowed |
| `aDisorderedFullTextIndexReportsThroughTheCompositionWrapper` | The `LSMTreeFullTextIndex` delegate - the half of the report whose keys are user text |
| `rebuildingTheIndexClearsTheWarning` | `REBUILD INDEX` is a real remedy and the advisory does not persist past it |

The full-text test reaches the compacted component through `getFileIds()` and `Schema.getFileById()`
rather than a typed accessor, so it works against a wrapper that holds its LSM index by composition
without adding production API for the test's benefit.

**Untested:** the "could not be verified" branch. Firing it needs a page read to fail, and the layer
below already converts a key that cannot be read into a reported problem rather than an exception
(`checkKeyOrderInPage` catches its own), so the remaining path is genuine I/O failure. Staging that needs
either mocks or page-internal corruption brittle enough to test the fixture rather than the code.

Before the fix four of the five fail; `aHealthyIndexCarriesNoUpgradeWarning` passes throughout.
`LSMTreeIndexKeyOrderCheckTest` is unchanged and still passes: it asserts on `checkRootPagesKeyOrder`,
`checkIntegrity()` and `CHECK DATABASE`, none of which this touches.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5804

### Review cycles

| Cycle | Head | Outcome |
|---|---|---|
| 1 | `5114ab3` | LGTM, no blockers. Asked that the compacted-only scope be stated (the `schema:indexes` query is not an exhaustive affected-set), that a check which throws not be silently lost, and noted geo/sparse exemption rests entirely on the ASCII-key invariant. All three applied - the third by making both wrappers delegate rather than documenting the invariant harder. |
| 2 | `d039878` | LGTM, no blockers. Caught that cycle 1 fixed only half of its own point: the failed check was raised to `WARNING` but still left the verdict null, so the query kept answering "healthy". Now published as a distinct unverifiable advisory. Also caught that the `LSMTreeFullTextIndex` delegate - described as half the report - had no test; added, and proven to fail with the delegate stubbed back to `null`. PR body corrected where it contradicted the diff. |
| 3 | `2bf7243` | LGTM, no blockers. Found a real defect in cycle 2's change: `keyOrderCheckedOnLoad` was set *before* the check ran, and `onAfterLoad` resolves the sub-index through `Schema.getFileById()`, which returns the same instance on every load - so one transient page-read failure on a replica's Raft apply or snapshot resync would have pinned false "needs rebuild" advice on a healthy index for the lifetime of the database. Now only a *completed* check latches, and a later success clears a stale verdict. Also corrected the lock-free javadoc, which called a plain reference read volatile. Declined ranking UNVERIFIABLE below MISMATCH in `TypeIndex`: both name the same remedy, and `schema:indexes` already shows every distinct message on its own bucket row. |
| 4 | `0d2c395` | LGTM, no blockers. Caught that `LSMTreeGeoIndex` returned its layout advisory *before* falling through, contradicting the comment directly above it - the delegation exists so a mismatch cannot be hidden, and that ordering hid it. Underlying is asked first now. Also flagged the Studio banner collapsing three severities under one "should be rebuilt" header; the header now asserts neither severity nor remedy. Declined adding a severity field to the wire format to reorder information that already renders in full as separate message groups. |

Three of the four cycles found something a previous cycle had introduced or left
half-finished, all in the reporting path rather than the detection: the queryability of the unknown
verdict (cycle 2), the latching of a transient failure (cycle 3), and the geo return ordering (cycle 4).
The detection logic itself was untouched throughout.

### Known gap

The `KEY_ORDER_UNVERIFIABLE_WARNING` branch has no test. `checkKeyOrderInPage` already converts an
unreadable key into a reported problem rather than an exception, so what remains is genuine I/O failure,
and staging it needs either mocks or page corruption brittle enough to test the fixture rather than the
code. Cycle 3 reduced the exposure instead of covering it: the verdict is now self-healing, so the worst
case is a `WARNING` and an advisory that clears on the next successful load.

### Final state

`max-cycles-reached` - four cycles run, every review LGTM with no blocking finding. Merge is the
developer's call.
