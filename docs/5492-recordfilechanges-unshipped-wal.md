# 5492 - `recordFileChanges` applies buffered WAL on the leader and never ships it

## Scope

Issue #5492 as filed bundled two defects. The second one (materialized view refresh requests
dropped rather than queued) was fixed and merged separately in PR #5502 (`6642e203e`) and has been
removed from the issue description. What remains is the replication half.

The end-to-end materialized-view symptom in the issue - `WALVersionGapException` on both followers
followed by a non-converging snapshot-resync loop - has **not** been reproduced in an in-process
harness, and this branch does not claim to fix it. What it does fix is the one leader-side
"applied locally, never replicated" hole that the issue promotes as its leading candidate, and
which is a correctness defect on the Raft leader path on its own merits.

## Root cause

`RaftReplicatedDatabase` buffers WAL produced by commits that run *inside* a `recordFileChanges()`
DDL callback, rather than shipping each as its own `TX_ENTRY`. The reason is ordering: at the time
those commits run, the files the DDL is creating do not yet exist on followers, so a `TX_ENTRY`
targeting them would be dropped. The buffered entries are meant to be embedded in the
`SCHEMA_ENTRY` sent once the callback returns.

The drain is unconditional, but the send is not:

```java
final List<byte[]> walEntries = new ArrayList<>(schemaWalBuffer.get());
final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>(schemaBucketDeltaBuffer.get());
schemaWalBuffer.get().clear();
schemaBucketDeltaBuffer.get().clear();

if (!addFiles.isEmpty() || !removeFiles.isEmpty() || schemaChanged) {   // walEntries NOT in the guard
  ...replicateSchema(..., walEntries, bucketDeltas);
}
```

and the `finally` block clears both buffers again regardless of the outcome.

So when a callback commits pages but creates no file, removes no file, and leaves the schema
version unchanged, the leader has already applied those pages locally (`commit2ndPhase` runs
before the buffering) and then silently discards the only copy that would have reached followers.

Follower page versions then trail the leader's by exactly the number of dropped page writes. The
next ordinary transaction touching one of those pages ships a `TX_ENTRY` whose version is ahead of
what the follower holds, `TransactionManager.applyChanges` throws `WALVersionGapException`, and the
database is marked diverged and scheduled for a snapshot resync. That is the shape reported in the
issue (leader 1648 vs follower 1646).

The sibling method `runWithCompactionReplication` drains the same two thread-locals and guards them
correctly:

```java
if (addFiles.isEmpty() && removeFiles.isEmpty() && walEntries.isEmpty() && sealedBlobs.isEmpty())
  return result;
```

The asymmetry between the two is the bug.

## Reachability - stated plainly

Every `LocalSchema.recordFileChanges` caller in `engine/` mutates the schema (`createType`, `dropType`,
`addBucket`, index creation, materialized view create/alter/drop), so `schemaChanged` is true and the
old guard already held. No existing caller has been shown to reach the dropped branch, and this branch
does not claim to have found one.

What it does claim: `recordFileChanges` is a published `DatabaseInternal` method whose documented
contract is that WAL committed inside the callback travels with the schema entry, and for one class of
callback it silently did not. The test drives it through that public API rather than through a
contrived internal hook. Treat this as closing a latent hole that `runWithCompactionReplication`
already guards against, not as the proven cause of the materialized-view A/B.

## Follower-side consequence of the fix

Shipping WAL with no files and no schema JSON creates an entry shape that did not previously exist:
the compaction path always sends a non-empty `serializedSchema`, so every `SCHEMA_ENTRY` a follower
had ever seen carried either files or schema.

`ArcadeStateMachine.applySchemaEntry` already tolerates an empty `schemaJson` (the `update()` call
is guarded), but it would still fall through to
`db.getSchema().getEmbedded().load(MODE.READ_WRITE, true)` at the end. That reload is not free: per
the existing `sealedOnlyEntry` comment it re-instantiates every TimeSeries engine on the single Raft
apply thread, closing shard executors with a 30 s `awaitTermination`. For an entry that carries
nothing but page writes there is nothing for `load()` to pick up - `applyChanges` already updates
page counts through `getFileByIdIfExists()` - so the reload is pure cost on the apply path.

The fix therefore also recognizes the WAL-only entry on the follower and skips the schema
update/reload for it, exactly as `sealedOnlyEntry` already does for TimeSeries maintenance entries.

## Verification

`ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue5492SchemaWalNotShippedIT.java`, a 2-node
in-process Raft cluster:

1. Create a type and let it replicate, so the backing files exist on both nodes.
2. On the leader, drive a `recordFileChanges()` callback that commits a record into that existing
   type and touches nothing else - no file created, no file removed, no schema mutation. This is
   the exact shape the guard drops.
3. Assert the record reached the follower.
4. Commit an ordinary transaction into the same bucket and assert the follower observed zero
   `WALVersionGapException` (via the existing `ArcadeStateMachine.TEST_WAL_GAP_COUNTER` hook).

Step 3 fails before the fix because the write never leaves the leader; step 4 fails because the
follower's page version now trails by one. Both are asserted so the test distinguishes "write lost"
from "write lost *and* the cluster subsequently diverges".

Confirmed failing before the fix (`expected: 2L`, follower held 1 record) and passing after. The test
carries `@Tag("slow")`; the ha-raft integration job passes no `-DexcludedGroups`, and the
`integration` profile only flips `skipITs`, so the tag does not hide it from CI.

## PR

https://github.com/ArcadeData/arcadedb/pull/5537 - opened as `Refs #5492`, deliberately not `Closes`.

### Review cycles

| Cycle | Head | Outcome |
|---|---|---|
| 1 | `8c810cb73` | `claude[bot]`: fix endorsed, one actionable item - add `@Tag("slow")` to match the two sibling tests using `TEST_WAL_GAP_COUNTER`. Verified the tag does not exclude the test from CI, then applied. Codacy 0 issues. |
| 2 | `6aad86a66` | `claude[bot]`: nothing blocking. Independently confirmed `walOnlyEntry` and `sealedOnlyEntry` are mutually exclusive by construction and that multi-chunk splits stay covered by `deliveryOnlyEntry`. Two optional remarks, no action. |

Final state: `clean-approval` at cycle 2. `gemini-code-assist` stayed silent, consistent with its
behaviour on the last several PRs in this repo.

One factual slip in the cycle-2 review, recorded so it is not propagated: it attributed the test's
41.7 s -> 18.1 s speedup to the follower `load()`-skip. It is the leader fix - before it, the test
burned the full 30 s convergence deadline waiting for a write that never arrived. The `load()`-skip
has no measured payoff in this test.

## CI verdict

Both tests that mattered passed on CI: `Issue5492SchemaWalNotShippedIT` (14.8 s) and
`Issue5297PolymorphicCountIT` (11.9 s). The latter had failed once in a local full-suite run; that
was local flakiness on a 97%-full disk, not a regression.

The remaining red jobs were each checked against a `main` baseline run rather than assumed benign:

- **`ha-integration-tests`** - fails on `main` too (4 failures there, 5 here). The failing sets are
  largely *disjoint in both directions*: `RaftCommandReadConsistencyIT` and
  `RaftReadConsistencyBookmarkIT` fail on `main` but pass here, while
  `RaftReplicationChangeSchemaIT`, `RemoteStickyConnectionStrategyIT` and
  `RaftHTTP2ServersCreateReplicatedDatabaseIT` do the reverse. All three of the latter pass locally
  on this branch, and their signature is `DatabaseIsClosedException` during teardown - a shutdown
  race, not a divergence. `RaftReplicationChangeSchemaIT` was checked specifically because schema
  replication is this change's own path.
- **`slow-unit-tests`** - `EdgeAppendMergeRaceTest`, an engine-module concurrency race. This PR
  changes zero engine code, so it cannot be implicated.
- **`integration-tests`** - fails on `main` too, and its module list explicitly excludes `ha-raft`.
- **`Meterian client scan`** - fails on `main` on 5 of the last 5 runs; no dependency is added here.
