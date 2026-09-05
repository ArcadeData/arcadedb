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

`RaftHAServer` applies its own copy of the leader-exclusion criterion in three places:

- `getStats()` - feeds `ha.network.replicas` in `/api/v1/server?mode=cluster`
- `getReplicaAddresses()` - feeds `ha.replicaAddresses` in the same response
- `getRoutingTable(protocol)` - the client routing table, one resolver shared by Bolt and gRPC

Each has its own local `excludeId` derivation (`leaderId != null ? leaderId : localPeerId`, or the leader directly). They are not factored into a shared helper, so a fix applied to one silently leaves the others wrong and the cluster API response internally inconsistent.

**Any change to what "replica" means must be applied to all three.** This has already caused one incomplete fix that review caught, twice: the second time (#7040) `/api/v1/cluster` learned to tell a declared peer from a member of the live Raft configuration while these three still iterated the static `raftGroup.getPeers()`. Since then the membership itself comes from one place - `ClusterMembership.of(raftGroup.getPeers(), getLivePeers()).configuredPeers()`, read through `RaftHAServer.configuredPeers()` - and `GetClusterHandler` reads the same reconciliation. Grep `RaftHAServer.java` for `raftGroup.getPeers()` before claiming such a fix is complete: the only legitimate direct readers left are the ones that want the *declared* list on purpose (`getConfiguredServers()`, `getDeclaredPeers()`, the presence matrix, the membership reconciliation itself).

`getDeclaredPeers()` is the newest of those (#7136): `RaftClusterStatusExporter` reconciles the declared list against the committed membership itself, to decide which declared peers are still *pending a join* rather than *removed*. It reads the committed side through `getCommittedPeersOrNull()`, not `getLivePeers()` - the latter substitutes the declared list when the division is unreadable, and folding that fallback into the "have I ever seen this peer committed" record would mark every declared peer as once-committed and silence the convergence note for good. A caller that must not mistake the declared list for a committed membership reads `getCommittedPeersOrNull()` and treats `null` as "no information this tick".

## Anything that commits must hold the WRAPPED database instance, not the inner one

`LocalDatabase.commit()` writes pages locally. `RaftReplicatedDatabase.commit()` proposes them to Raft and *then* writes them. Code holding the wrong one of the two commits successfully, applies its pages on the leader, and replicates nothing. Followers trail by exactly those page versions, and the next replicated entry touching one of them fails its version check - `WALVersionGapException`, database marked diverged, snapshot resync, and the entry after it breaks the same way. Nothing throws at the point of the mistake, so the symptom always surfaces somewhere else.

`getWrappedDatabaseInstance()` returns the Raft wrapper under HA and the instance itself off it, so resolving it is always correct and never costs anything.

This was #5492. `SQLQueryEngine` handed statements the raw instance, so `CommandContext.getDatabase()` was the inner `LocalDatabase`, and every statement that commits mid-execution bypassed replication: `TRUNCATE TYPE`/`BUCKET` batching every `TRUNCATE_BATCH_SIZE` (default **1000**) records, `REBUILD INDEX`, `BatchStep`. `SQLScriptQueryEngine` had always resolved the wrapper before running the *same statement classes*, which is why an identical `TRUNCATE` replicated under `sqlscript` and not under `sql`.

Two things about how it hid, both worth generalizing:

- **A default-sized threshold can look like a heisenbug.** It reproduced only above 1000 records in one statement, so every in-process attempt (180 to 450 records) saw a clean cluster and the failure was written off as needing container-level concurrency. If a replication bug reproduces in containers and not in process, compare the *sizes* before theorizing about timing.
- **A materialized view was the trigger only because it truncates constantly.** `MaterializedViewRefresher` truncates and rebuilds its backing type on every source commit, so a view over 1000+ rows crossed the boundary on every refresh. The view was the amplifier, not the defect - `TRUNCATE TYPE` over 1000 records through `/api/v1/command` with `language=sql` was enough on its own.

Since #6220 `TRUNCATE` only batches when **it** owns the transaction - when none was active as it started. Inside a caller's transaction it commits nothing at all, because committing through a caller silently breaks that caller's `ROLLBACK`. So the shape that reproduces #5492 is now `command("sql", "TRUNCATE ...")` with **no** enclosing transaction; wrapping it in one, which `Issue5492TruncateBatchNotReplicatedIT` used to do, leaves nothing mid-execution to commit and the test passes for the wrong reason. `REBUILD INDEX` and `BatchStep` still commit unconditionally.

When adding any code path that commits, ask which instance it holds. `Issue5492TruncateBatchNotReplicatedIT` guards the statement path; it asserts the WAL gap counter *before* querying the follower, because the resync reinstalls the follower's database and a stale handle then throws `DatabaseIsClosed`, which reads like infrastructure noise rather than divergence.

## Adding a field to a Raft log entry: use `writeExtensionSection`, never bare trailing bytes

A Raft log entry has no version field. The type byte is the whole envelope, and the decoder for each type reads exactly the fields it knows about. Anything left over is checked afterwards, and until #7138 that check was flat: for every type except `SCHEMA_ENTRY`, one trailing byte was a fatal `IllegalStateException`.

Fatal in the worst arm, too. `RaftLogEntryCodec.decode` runs *outside* `applyWithRetry`, so the throw was neither a `NeedRetryException` nor a `ReplicationException`, could not be quarantined per database, and landed in `applyTransaction`'s `catch (Throwable)` node-halt - which deliberately does not advance the applied index, so the node halted again on the same entry on every restart. A newer leader adding one field to, say, `DROP_DATABASE_ENTRY` would therefore have permanently halted every not-yet-upgraded peer: on a three-node rolling upgrade, two nodes down.

So: **append a new field with `RaftLogEntryCodec.writeExtensionSection(dos, payload)`**, after the type's own fields, and make its absence mean "the peer that wrote this predates the field". The frame is `[EXTENSION_MAGIC][length][payload]`, repeatable; a decoder skips every section it does not recognise. The magic is what keeps the corruption signal the old check existed for - a truncated entry does not carry it - which is why "just tolerate anything trailing", the thing `SCHEMA_ENTRY` does, was not the answer for the other five.

Two constraints that are easy to miss:

- **A version or length prefix on the envelope cannot fix this**, however much tidier it looks. No already-deployed decoder knows to look for one. The only change that helps an existing peer is one it can act on without being upgraded, which means tolerating what comes *after* what it understands.
- **Tolerating is not the same as emitting.** Peers older than the release carrying this still halt on an extension section. A new field may only be *written* once every peer in the supported upgrade range tolerates them; from that release on, adding one is safe by construction.

`SCHEMA_ENTRY` stays exempt from the framing requirement because its existing optional sections (the #4382 WAL blobs, the #5443 flag, the #4416 slices) are unframed and predate it. New sections on it should use the helper anyway.

The same issue moved decode failures off the node-halt path: a `RaftLogEntryDecodeException` naming a database quarantines that one database and resyncs it, and only a failure with no database name still halts the node. An *unknown type* is a different failure and still halts - skipping a committed mutation nobody can read is a silent divergence (#4798).
