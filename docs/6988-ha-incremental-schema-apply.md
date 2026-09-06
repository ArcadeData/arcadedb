# 6988 - HA: every schema entry triggered a full LocalSchema.load() on the follower

## Problem

`ArcadeStateMachine.applySchemaEntry` ended every non-trivial `SCHEMA_ENTRY` with

```java
db.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, true);
```

`LocalSchema.load()` is a from-scratch rebuild: it clears `files`, `types`, `bucketMap` and `indexMap`, drops the
dictionary reference, then re-instantiates a `Component` for **every** file the `FileManager` holds, runs
`onAfterLoad()` on each (page-0 read for an LSM index), re-parses `schema.json`, runs `onAfterSchemaLoad()` over every
component again, and finishes with `attachBloomFilters()`, `sweepOrphanCompactedIndexFiles()` and `updateSecurity()`.

Because a schema build emits one entry per DDL statement, the total cost is O(entries x total files) - quadratic in the
number of types - and all of it runs serially on the single Ratis `StateMachineUpdater` thread. Issue #6982 measured
1209 types with `typeDefaultBuckets=24`: 6128 applied entries, ~58,000 files per reload, ~3.5e8 component
instantiations, 10,406,081 ms wall clock (~2h53m, ~0.6 entries/s).

## Root cause

The entry already names exactly what changed (`filesToAdd`, `filesToRemove`, `schemaJson`), but the refresh threw that
information away and rebuilt everything.

## Fix

### `LocalSchema.loadIncremental(mode, removedFileIds, touchedFileIds)`

A new incremental counterpart of `load()`:

1. **Eligibility is decided in a first pass that modifies nothing**, so a refusal leaves the caller's fallback a
   consistent state.
2. Instantiates a `Component` only for the files the `FileManager` holds that have no component registered yet, plus
   a **replacement** for the already-registered *index* components whose file this entry wrote pages into
   (`touchedFileIds`). That is what re-reads an LSM mutable index' page 0 - its key types, its sub-index pointer and
   its mutable page count - which the full rebuild used to get for free by re-instantiating everything.
3. Runs `onAfterLoad()` on those.
4. Calls the very same `readConfiguration()` the full load calls, so the logical schema (types, properties, index
   links, bucket strategies, triggers, views, functions) is refreshed identically.
5. Runs `onAfterSchemaLoad()` on those.
6. `updateSecurity()`, as `load()` does.

Every other `Component` instance survives, so the per-entry cost drops from O(total files) to O(changed files). The
file walk itself stays O(total files), but it touches no page and allocates no component.

### Why a touched component is REBUILT, not refreshed in place

The first version re-ran `onAfterLoad()`/`onAfterSchemaLoad()` on the live instance. That is unsafe, and the PR review
caught it: those hooks write plain, non-volatile fields - an LSM mutable index' `keyTypes`, `binaryKeyTypes`,
`storageKeyTypes`, `subIndex`, `currentMutablePages` - which `LSMTreeIndex.getKeyTypes()`, `getBinaryKeyTypes()` and
`convertKeys()` read **without** the index lock. Their safety rests entirely on publish-once, mutate-never-after,
which is exactly why a compaction's `splitIndex()` builds a new `LSMTreeIndexMutable` and swaps the `volatile mutable`
reference rather than updating the old one. A follower serves queries concurrently with the Raft apply thread, so
re-running the hooks on a published instance could let a reader observe a torn combination of those fields.

Building a fresh component and handing it the slot in a single synchronized `set` is what `load()` already produces
for that file, so it inherits the concurrency properties the full rebuild has always had, and the instance is fully
constructed before anything can reach it.

Only components whose main component is an `IndexInternal` are replaced: `LSMTreeIndexMutable`, `HashIndexBucket` and
`LSMVectorIndexMutable` are the only classes with non-empty load hooks. A bucket write, or a dictionary write (which
`TransactionManager.applyChanges` reloads on its own), costs nothing here. A touched *compacted* index or bloom filter
forces the full rebuild instead, because a compacted file is claimed by the mutable index holding it as its sub-index
and a replacement would leave that claim pointing at the old instance.

### Why the new components come from the `FileManager`, not from the entry's `filesToAdd`

The first version of this fix used `decoded.filesToAdd()` and **`RaftSchemaWalInstalment3NodesIT` caught it**: a
schema change too large for one Raft entry is split (`RaftTransactionBroker.splitSchemaEntry`), the leading chunks
carry `filesToAdd` and no schema JSON, and their apply deliberately skips the refresh (#5443). Their files therefore
sit in the `FileManager` with no component, and the publishing chunk's own `filesToAdd` does not name them. Trusting
`filesToAdd` left the index unregistered exactly when `readConfiguration()` looked for it - and that is not a
transient miss: the unresolvable index reference is dropped from the in-memory schema **and saved** (the #4083
self-heal path), so the follower loses the index permanently and silently. The failure showed up as
`server 0 must hold the whole index before the rebuild: expected 12000 but was -1`, with the follower's `schema.json`
carrying `"indexes":{}` for the type.

Asking the file manager what has no component yet converges on precisely the set `load()` would have registered,
whatever produced the files.

### What forces the full rebuild (and why)

`loadIncremental` returns `false` - having changed nothing - for:

| Case | Why the incremental path cannot express it |
|---|---|
| the schema was never loaded in this lifecycle | there is no baseline to add to |
| the entry retires a file | `removeFile()` clears only the file-id array, so a stale `bucketMap`/`indexMap` entry would survive; and the mutable index that superseded it must re-read page 0 anyway. This is also what keeps the #4743 / #5443 ordering reasoning on the path it was written for |
| an unregistered file is a compacted index (`uctidx`/`nuctidx`) | a compacted file is *claimed* by the mutable index that names it in page 0, and that claim is what `sweepOrphanCompactedIndexFiles()` distinguishes a live file from an orphan by. The claim can only be re-established when every mutable index is re-instantiated in the same pass |
| an unregistered file is a bloom filter (`bfidx`) | claimed in turn by its compacted index |
| an unregistered file is a dictionary (`dict`) | every component resolves property names through it |

`sweepOrphanCompactedIndexFiles()` is deliberately **not** run on the incremental path: its orphan proof ("no mutable
index claimed this compacted file during the load") only holds when every mutable index was re-instantiated in that
same pass. Running it after a partial refresh would drop live files.

### Safety valve

`arcadedb.ha.schemaIncrementalApply` (`GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY`, `Boolean`, default `true`).
Read per entry rather than cached, so flipping it on a running server sends the very next applied entry back through
the full rebuild without a restart.

## What this does NOT fix

The leader still ships the **whole** schema JSON in every entry, so `new JSONObject(decoded.schemaJson())`,
`getEmbedded().update(json)` (which rewrites `schema.json` in full) and `readConfiguration()` remain O(schema size) per
entry. Removing that term is the companion "delta schema shipping" change the issue names; it is out of scope here.
What this change removes is the O(total files) term, which is the one that dominated the #6982 measurement (~58,000
component instantiations, each with a page-0 read, per entry).

### The one part of that residual cost that is not a cheap JSON walk: TimeSeries types

`readConfiguration()` starts by closing **every** `LocalTimeSeriesType` in the schema and ends by re-creating each of
them with `initEngine()`, whether or not the entry touched them. `TimeSeriesEngine.close()` shuts its shard executor
down with `awaitTermination(30, TimeUnit.SECONDS)` and then closes every shard and the tag dictionary; `initEngine()`
reopens them. On an idle engine the executor drains at once and this is quick, but it is not a JSON walk, and it is
the exact cost `ArcadeStateMachine` names when it justifies skipping the reload for `sealedOnlyEntry` and
`walOnlyEntry`: *"re-instantiates every TimeSeries engine (closing shard executors with a 30s awaitTermination),
stalling replication"*.

So on a schema that mixes many TimeSeries types with many regular types, one unrelated `CREATE DOCUMENT TYPE` still
pays a teardown-and-rebuild for every TimeSeries type. That is **unchanged** by this PR - the full rebuild ran the
same `readConfiguration()` - so nothing regresses, but it does mean a TimeSeries-heavy schema can still show a shape
of the #6982 problem after this fix. Making the logical refresh skip types whose JSON did not change is the natural
follow-up, and it belongs with the delta-schema-shipping work rather than here: the TimeSeries close/reinit is load
bearing for the repair paths of #6356 and #6839, so narrowing it needs its own reasoning and its own tests.

## Tests

### `engine/src/test/java/com/arcadedb/schema/Issue6988IncrementalSchemaLoadTest.java` (9 tests)

Pins the invariant by **identity**, not by elapsed time - a component the refresh did not touch must come back as the
same instance:

- `incrementalRefreshKeepsEveryUntouchedComponentInstance` - a no-change refresh returns `true`, every component is the
  same instance afterwards, and the logical schema (types, properties, indexes) is still intact.
- `fullLoadReplacesEveryComponentInstance` - the contrast that makes the assertion above meaningful: the full rebuild
  replaces *all* of them. This is the cost the issue is about.
- `incrementalRefreshRegistersFilesThatHaveNoComponentYet` - reproduces the follower's state (file registered in the
  `FileManager`, no component registered for it) and checks the file is registered while every other instance is
  untouched.
- `incrementalRefreshRecoversAnIndexFileLeftUnregisteredByAnEarlierEntry` - the engine-level version of the
  `RaftSchemaWalInstalment3NodesIT` failure above: an index file with no component and no `indexMap` entry must be
  picked up by the refresh, and the index must still be attached to its type afterwards rather than dropped from the
  schema and saved away.
- `aTouchedIndexComponentIsRebuiltRatherThanRefreshedInPlace` - the refreshed component is a *different* object and
  every untouched one is not, and the rebuilt instance is the one the type's index resolves to. Pins the
  publish-once invariant by shape rather than by racing the clock.
- `aTouchedNonIndexComponentIsLeftAlone` - naming a bucket in `touchedFileIds` costs nothing.
- `retiredFilesForceTheFullRebuildWithoutTouchingAnything` - a removal returns `false` and leaves the snapshot
  unchanged, so the caller's fallback runs over consistent state.
- `aSchemaThatWasNeverLoadedForcesTheFullRebuild` - the no-baseline refusal.
- `anUnregisteredDictionaryCompactedIndexOrBloomFilterForcesTheFullRebuild` - each of the four extensions in
  `NON_INCREMENTAL_COMPONENT_EXTENSIONS` is planted as a file the `FileManager` knows and nothing is registered for,
  and each must refuse without changing the component snapshot.

### `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue6988IncrementalSchemaApplyIT.java`

Two-node cluster (every DDL lambda is idempotent: a leadership transfer mid-commit makes
`LocalDatabase.transaction()` re-run it, and schema changes are not rolled back with the records):

- `followerKeepsUntouchedComponentsAcrossManySchemaEntries` - creates an anchor type, records the follower's
  `Component` instance for its bucket, then creates 40 more types one transaction (= one entry) at a time, and asserts
  the anchor component is still the same instance while all 40 types, their properties and their indexes replicated.
  Before the fix, every one of the 40 applied entries replaced that instance.
- `replicatedIndexesStillAnswerLookupsAfterAnIncrementalApply` - an index created, then buried under further entries,
  still resolves a lookup on the follower (the #4083 family of failures).
- `droppingATypeStillFallsBackToTheFullRebuild` - a DROP retires files, so the entry takes the fallback; the follower
  ends up with the type gone and the rest intact.

### `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue6988FullRebuildFallbackIT.java`

Two-node cluster with `arcadedb.ha.schemaIncrementalApply=false`: the follower *does* replace the component,
proving the safety valve actually reaches the apply path. Without this the flag could be silently unread and nothing
would notice, since both values produce a correct schema - only the instance identity tells them apart.

**Why no wall-clock assertion.** The acceptance criteria suggested a `StallAwareStopwatch` budget. A timing bound can
only separate linear from quadratic at a type count large enough to take hours to build, and at any smaller count it
asserts nothing while still being at the mercy of whatever else the machine is running. The identity invariant is the
same claim made load-independently: it fails deterministically on the old code and passes deterministically on the new.

## Files changed

- `engine/src/main/java/com/arcadedb/schema/LocalSchema.java` - `loadIncremental`, `registerLoadedComponent` /
  `replaceLoadedComponent` / `registerInLookupMaps` (the first shared with `load()`),
  `NON_INCREMENTAL_COMPONENT_EXTENSIONS`.
- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - `HA_SCHEMA_INCREMENTAL_APPLY`.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` - `applySchemaEntry` collects the WAL's
  touched file ids and calls `loadIncremental` with a `load()` fallback; `keysOrNull`, `incrementalSchemaApplyEnabled`.
- `engine/src/test/java/com/arcadedb/schema/Issue6988IncrementalSchemaLoadTest.java` (new).
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue6988IncrementalSchemaApplyIT.java` (new).
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue6988FullRebuildFallbackIT.java` (new).
