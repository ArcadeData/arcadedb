# ha-raft CLAUDE.md

Guidance for working under `ha-raft/`. This file records things the code does not tell you, or actively misleads you about. It is not a tour of the module - use `Glob` and the package names for that.

## Replay position comes only from the Ratis snapshot marker

What a restarted node replays is decided **solely** by the Ratis snapshot marker file `snapshot.<term>_<index>` under `.raft-storage/.../sm/`. `ArcadeStateMachine.reinitialize()` seeds `lastAppliedIndex` from `storage.getLatestSnapshot()`; with no marker it seeds -1 and Ratis replays the whole retained log.

The persisted `.raft/applied-index` JSON is read in `reinitialize()` but **never** feeds the replay position. It has exactly three consumers:

1. the snapshot-gap check that decides whether to download from the leader,
2. the per-database bootstrap replay-skip in `applyBootstrapFingerprintEntry`,
3. `hasNeverAppliedApplicationEntry()`, the offline-bootstrap gate.

**Consequence:** to make a committed-but-unapplied entry replayable you must control what `takeSnapshot()` reports. Clamping the applied-index file changes bootstrap behavior and does nothing for durability. This is the single most common wrong turn when chasing a lost-write bug in this module.

Note that `globalAppliedIndex` and `lastAppliedIndex` track the same value on the apply path but are seeded independently, so they can briefly differ right after `reinitialize()`. Do not assert equality across that window.

## `RaftHAServer.getLastAppliedIndex()` does not read ArcadeDB's counter

It reads Ratis: `division.getInfo().getLastAppliedIndex()` -> `StateMachineUpdater.appliedIndex`, which Ratis seeds from `getLatestSnapshot().getIndex()` (in its constructor, and again in `reload()` right *after* `reinitialize()` returns). `ArcadeStateMachine.lastAppliedIndex`, the `AtomicLong`, feeds `takeSnapshot()` and the phase-2 replay floor and is **not** on that path.

Two consequences that have already cost debugging time:

- Changing what `reinitialize()` writes into the `AtomicLong` does **not** change what a `waitForAppliedIndex()` / `waitForLocalApply()` waiter observes. If you are chasing "a read was released too early", the value to reason about is the marker index, not the counter.
- Because the value comes from the marker, a node whose marker runs ahead of the entries it actually applied advertises an applied index covering data it does not hold. That is issue #6111: `reinitialize()`'s snapshot-gap branch now publishes `staleSnapshotAppliedFloor` and the waiters use `getTrustedAppliedIndex()` (the raw value clamped to that floor). **Reporting** paths - cluster status, lag detection - deliberately keep the raw Ratis value; only read guarantees clamp. The floor is cleared where a resync actually restored the state, never where one is merely requested.

### The stale-gap failure mode an operator will meet

A node holding an unfilled gap fails LINEARIZABLE reads (503) and reports not-ready via `isResyncInProgress()`, by design: it is genuinely missing committed entries, and the alternative is serving short data silently. Two things about how it recovers:

- **The lag backstop cannot see it.** `isFollowerLaggingBeyond()` is driven by `commitIndex - appliedIndex`, and the applied index comes from the marker that is ahead - so the node reports *zero lag* while the gap is open. `retryUnfilledSnapshotGap()` (a `HealthMonitor.HealthTarget` hook, throttled to one attempt per snapshot-watchdog timeout) exists specifically because of this; do not try to fold it back into `recoverFromPersistentLag()`.
- **A node that becomes leader while holding a gap wedges, deliberately.** `triggerSnapshotDownload()` refuses to resync when this node is the leader (or when the resolved leader address is its own), because copying its own incomplete databases onto themselves would report success and durably record the marker index as applied - re-opening #6111 permanently. So a single-node cluster, or a node that wins the election because every peer is unreachable, retries and refuses on every tick until a peer that actually holds the entries takes leadership. That is the intended terminal state, not a hang; the refusal is logged at WARNING on every attempt.

## Snapshots are taken far more often than you would guess

Three triggers, not one: the Ratis auto-trigger (`arcadedb.ha.snapshotThreshold`, default 100k entries), ArcadeDB's own wall-clock `RaftLogCompactionScheduler` (`arcadedb.ha.snapshotInterval` 300 s, `arcadedb.ha.snapshotMinEntries` 64), **and shutdown**, via Ratis's `StateMachineUpdater.stop()`.

The shutdown trigger has a counterintuitive consequence when reproducing durability bugs: a graceful `server.stop()` takes a snapshot and makes an apply-ordering gap **permanent**, while a `SIGKILL` takes no snapshot and replays cleanly. If a durability test passes under kill -9 and fails under a clean stop, this is why. Choose the shutdown mode deliberately.

## Peer-list filtering is duplicated across three methods

`RaftHAServer` iterates `raftGroup.getPeers()` and applies its own copy of the leader-exclusion criterion in three places:

- `getStats()` - feeds `ha.network.replicas` in `/api/v1/server?mode=cluster`
- `getReplicaAddresses()` - feeds `ha.replicaAddresses` in the same response
- `getRoutingTable(protocol)` - the client routing table, one resolver shared by Bolt and gRPC

Each has its own local `excludeId` derivation (`leaderId != null ? leaderId : localPeerId`, or the leader directly). They are not factored into a shared helper, so a fix applied to one silently leaves the others wrong and the cluster API response internally inconsistent.

**Any change to what "replica" means must be applied to all three.** Grep `RaftHAServer.java` for `getPeers()` before claiming such a fix is complete. This has already caused one incomplete fix that review caught.

## Anything that commits must hold the WRAPPED database instance, not the inner one

`LocalDatabase.commit()` writes pages locally. `RaftReplicatedDatabase.commit()` proposes them to Raft and *then* writes them. Code holding the wrong one of the two commits successfully, applies its pages on the leader, and replicates nothing. Followers trail by exactly those page versions, and the next replicated entry touching one of them fails its version check - `WALVersionGapException`, database marked diverged, snapshot resync, and the entry after it breaks the same way. Nothing throws at the point of the mistake, so the symptom always surfaces somewhere else.

`getWrappedDatabaseInstance()` returns the Raft wrapper under HA and the instance itself off it, so resolving it is always correct and never costs anything.

This was #5492. `SQLQueryEngine` handed statements the raw instance, so `CommandContext.getDatabase()` was the inner `LocalDatabase`, and every statement that commits mid-execution bypassed replication: `TRUNCATE TYPE`/`BUCKET` batching every `TRUNCATE_BATCH_SIZE` (default **1000**) records, `REBUILD INDEX`, `BatchStep`. `SQLScriptQueryEngine` had always resolved the wrapper before running the *same statement classes*, which is why an identical `TRUNCATE` replicated under `sqlscript` and not under `sql`.

Two things about how it hid, both worth generalizing:

- **A default-sized threshold can look like a heisenbug.** It reproduced only above 1000 records in one statement, so every in-process attempt (180 to 450 records) saw a clean cluster and the failure was written off as needing container-level concurrency. If a replication bug reproduces in containers and not in process, compare the *sizes* before theorizing about timing.
- **A materialized view was the trigger only because it truncates constantly.** `MaterializedViewRefresher` truncates and rebuilds its backing type on every source commit, so a view over 1000+ rows crossed the boundary on every refresh. The view was the amplifier, not the defect - `TRUNCATE TYPE` over 1000 records through `/api/v1/command` with `language=sql` was enough on its own.

Since #6220 `TRUNCATE` only batches when **it** owns the transaction - when none was active as it started. Inside a caller's transaction it commits nothing at all, because committing through a caller silently breaks that caller's `ROLLBACK`. So the shape that reproduces #5492 is now `command("sql", "TRUNCATE ...")` with **no** enclosing transaction; wrapping it in one, which `Issue5492TruncateBatchNotReplicatedIT` used to do, leaves nothing mid-execution to commit and the test passes for the wrong reason. `REBUILD INDEX` and `BatchStep` still commit unconditionally.

When adding any code path that commits, ask which instance it holds. `Issue5492TruncateBatchNotReplicatedIT` guards the statement path; it asserts the WAL gap counter *before* querying the follower, because the resync reinstalls the follower's database and a stale handle then throws `DatabaseIsClosed`, which reads like infrastructure noise rather than divergence.

## A repo-wide pom guard lives in this module's tests

`PomTagFilterContractTest` is not about Raft. It reads every `pom.xml` in the repository and checks how
surefire's and failsafe's `groups`/`excludedGroups` are configured, because those parameters each have a
same-named `-D` user property and getting the precedence wrong is invisible: the build runs the wrong set of
tests and passes.

It enforces three rules, the first and the last of which point in opposite directions:

- a **plugin-wide** default must be a property reference (`${excludedGroups}`, `${failsafe.excludedGroups}`), so
  a CI lane's `-DexcludedGroups=...` still reaches it. A literal there wins over the command line and every lane
  that filters by tag quietly runs whatever the pom named instead. That is issue #5697;
- **failsafe's** plugin-wide `<excludedGroups>` must not read the property surefire's reads. Failsafe's own
  parameter default *is* `${excludedGroups}`, so sharing it applies the unit lane's exclusion to the integration
  lane and drops e.g. `@Tag("benchmark")` ITs from every `-Pintegration` run. That is the second half of #5697;
- a **named execution** that is one half of a tag *partition* must write out both parameters, and neither may
  mention `${groups}` or `${excludedGroups}` anywhere in its value, so no `-Dgroups` or `-DexcludedGroups` aimed
  at another module can reach it. Leaving one out, or interpolating one into a larger expression, is the same as
  handing it to the command line, and the result is the quiet one: measured on this tree with surefire 3.5.6,
  `./mvnw -o -pl ha-raft test -Dgroups=bogus-tag` reports `Tests run: 0` and `BUILD SUCCESS`. That is issue
  #6794 - step 5 of the fork-split re-verification recipe, the one step that had no automated guard.

The partition rule is here rather than in `engine` because the only executions ever meant to be a tag partition are
this module's: the `ha-heavy` split from issue #6343. That split is not on `main` - it was reverted in
f4567f6176 because it surfaced issue #6848 - so the rule currently has nothing in the reactor to judge and is
proven able to fail against the split's own configuration as a fixture. It needs no edit to arm itself when the
split comes back.

It reads the poms as written, not `help:effective-pom`, on purpose: interpolation is the distinction being
tested, and the effective pom resolves `${excludedGroups}` to `benchmark`, which is exactly the literal the
first rule rejects.
