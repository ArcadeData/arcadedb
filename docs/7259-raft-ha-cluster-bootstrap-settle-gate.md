# #7259 - BoltFollowerForwardingIT fails on main with a two-node schema divergence

## Symptom

`BoltFollowerForwardingIT.writeCommandThroughFollowerIsForwardedToLeader` fails on `main` with

```
com.arcadedb.exception.SchemaException: Type with name 'BoltFollowerWrite' was not found
  ... at BoltFollowerForwardingIT.writeCommandThroughFollowerIsForwardedToLeader(BoltFollowerForwardingIT.java:116)
  Suppressed: DatabaseComparator$DatabaseAreNotIdentical: Types: DB1 5 <> DB2 6
```

`DB1` is server 0, `DB2` is server 1 (`BaseGraphServerTest.checkDatabasesAreIdentical` compares
`getServerToCheck()[0]` against each of the others). 5 is the base fixture's type count, 6 is the base plus
`BoltFollowerWrite`, so **server 0 is the node missing the type**.

## Evidence: what the failing CI run actually did

The `integration-test-reports` artifact of run
[34155171656](https://github.com/ArcadeData/arcadedb/actions/runs/34155171656) carries the full per-server log
for the failing test. Reconstructed:

```
19:43:12.020  [BoltFollowerForwardingIT] <ArcadeDB_2> Raft leader elected on server 2   <- waitAllReplicasAreConnected() returned HERE
19:43:12.047  [ArcadeStateMachine] Leader elected: ArcadeDB_2 (term=1)
19:43:12.465  [BootstrapElection]  Bootstrap source for 'graph': peer=localhost_2436, lastTxId=7, fingerprint=f7484120...
19:43:12.675  [ArcadeStateMachine] Database 'graph' bootstrap mismatch (local fp=92f3e044..., baseline fp=f7484120...); reinstalling from leader-shipped full snapshot
19:43:12.679  [ArcadeStateMachine] Database 'graph' bootstrap mismatch (...); reinstalling from leader-shipped full snapshot
19:43:12.680  [ArcadeStateMachine] Database 'graph' bootstrapped locally (fingerprint matches cluster baseline)   <- the leader
19:43:12.695  [SnapshotHttpHandler] Serving database snapshot for 'graph'...   (x2, from the leader)
19:43:12.812  [ArcadeStateMachine] Database 'graph' reinstalled after bootstrap mismatch
19:43:12.824  [ArcadeStateMachine] Database 'graph' reinstalled after bootstrap mismatch
19:43:15.429  [DriverFactory]      Driver instance created for 'bolt://localhost:57687'   <- BASE_BOLT_PORT + 0, so followerIndex = 0
19:43:19.420  END OF THE TEST
```

Two facts follow directly:

1. **The test was released while the cluster was still bootstrapping.** `waitAllReplicasAreConnected()` returned
   the instant a leader existed (12.020). The per-database bootstrap - `BootstrapElection` sampling the peers,
   committing a `BOOTSTRAP_FINGERPRINT_ENTRY`, and each mismatching peer **replacing its entire `graph`
   directory** with a full snapshot pulled from the leader - only started at 12.465 and finished at 12.824. The
   test body (`findLeaderIndex()` -> `createVertexType` -> `waitForAllServers()`) ran inside that window.
2. **Both followers reinstalled; the leader did not.** The reinstall is not an exceptional path here, even
   though `BaseGraphServerTest.prepareDatabase` byte-copies server 0's database directory to every peer before
   startup: by sampling time the fingerprints had diverged anyway (`92f3e044...` against the chosen baseline
   `f7484120...`). The window is therefore opened on runs of `BaseRaftHATest` subclasses generally, and only the
   timing decides whether a test's own writes land inside it.

The node that lost the type (server 0) is also the node the Bolt driver wrote through
(`bolt://localhost:57687` = `BASE_BOLT_PORT + 0`).

## The invariant

**A Raft HA test must not issue its first write until the cluster's first-formation bootstrap pass has finished
on every started peer - including any full-snapshot reinstall that pass decided to perform.**

## Completeness

### Commands run

```
$ grep -rln "extends BaseRaftHATest" --include='*.java' . | wc -l
147

$ grep -rn "waitAllReplicasAreConnected()" --include='*.java' .
ha-raft/.../BaseRaftHATest.java:295:  protected void waitAllReplicasAreConnected() {      <- the override
server/.../BaseGraphServerTest.java:338:    waitAllReplicasAreConnected();                 <- the only call site
server/.../BaseGraphServerTest.java:345:  protected void waitAllReplicasAreConnected() {   <- the non-Raft base

$ grep -n "recordBootstrapBaseline(dbName\|installFromLeaderForBootstrap(dbName)" ha-raft/.../ArcadeStateMachine.java
3021:    recordBootstrapBaseline(dbName, new BootstrapBaseline(chosenFingerprint, chosenLastTxId));
3070:      installFromLeaderForBootstrap(dbName);
3148:      installFromLeaderForBootstrap(dbName);

$ grep -n "applyWithRetry(index" -A 12 ha-raft/.../ArcadeStateMachine.java
930:      });                                               <- applyWithRetry returns (the reinstall is inside it)
933:      updateLastAppliedTermIndex(termIndex.getTerm(), index);   <- only THEN is the applied index published
```

### Why the recorded baseline is not the signal

The obvious gate - "every peer answers a non-null `ArcadeStateMachine.getBootstrapBaseline(db)`" - does not
work, and the greps above are why:

- `recordBootstrapBaseline` runs at line 3021, at the **top** of `applyBootstrapFingerprintEntry`, while both
  `installFromLeaderForBootstrap` calls are at 3070/3148. A peer therefore publishes a non-null baseline while
  the full-database reinstall it triggered is still in flight - which is exactly the state the gate exists to
  keep a test out of.
- On the "superseded" path (`persistedApplied >= 0 && persistedApplied < index`) the method returns before 3021
  and records nothing at all, so a null baseline cannot tell "not yet" from "never will" and the wait would
  burn its whole budget on every such cluster.

The applied index does work: `updateLastAppliedTermIndex` at 933 runs only after `applyWithRetry` at 930 has
returned, and the reinstall is inside it.

### What the wait needed that nothing published

Nothing on the server answered "has the bootstrap pass finished?", only "did it commit a baseline for this
database?", which the two points above disqualify. Hence the one production change in this PR:
`RaftHAServer.getLastBootstrapOutcome()`, recorded by `runBootstrapIfEligible()`.

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `BaseGraphServerTest.startServers()` -> `BaseRaftHATest.waitAllReplicasAreConnected()` -> test body writes (all 147 subclasses, `BoltFollowerForwardingIT` among them) | yes - the new two-part gate | yes - `Issue7259ClusterBootstrapSettledIT`, which fails with the gate removed (see below), plus `BoltFollowerForwardingIT` green |
| `RaftHAServer.getLastBootstrapOutcome()` reachable at runtime (new production code) | yes - written by `runBootstrapIfEligible()`, which `ArcadeStateMachine.notifyLeaderChanged` submits on every election | yes - the IT asserts it is non-null on the leader of a real 3-node cluster |
| `BaseRaftHATest.restartServer(int)` re-forming a cluster mid-test | argued - bootstrap cannot re-engage after first formation (`BootstrapElection.isFirstFormation`, issue #4800), so there is no reinstall window to gate; `RaftBootstrapDoesNotEngageOnRestartIT` pins it and is green in the sweep below | n/a |
| `waitForReplicationIsCompleted()`'s two silent give-ups (no leader within 3 s; 30 s budget expiry) | **no** - filed as #7518 | no |
| A production client writing into the first-formation bootstrap window (no harness involved) | **no** - filed as #7519 | no |

## Fix

**`RaftHAServer`** (production, additive): `runBootstrapIfEligible()` now records the pass's outcome in a
volatile field, readable through `getLastBootstrapOutcome()`. `null` means no pass has finished on this node;
any value means one has, and only `COMMITTED` means it committed a baseline.

**`BaseRaftHATest.waitAllReplicasAreConnected()`**: after the election it calls the new
`waitForClusterBootstrapToSettle()`, which

1. waits for the leader's pass to report a terminal outcome, re-resolving the leader across a `TRANSFERRED`
   (leadership moves to the elected source, whose own pass re-runs the protocol) and returning at once on
   anything that is not `COMMITTED` - so a cluster that disables bootstrap pays none of the budget;
2. on `COMMITTED`, waits until every started peer's published applied index has reached the highest in the
   cluster.

It is not an assertion: exhausting the budget falls through, and `slowWaitReport` turns that into a `GAVE UP`
line. It shares `RESYNC_RETRY_TIMEOUT_MS` as its budget rather than defining its own, because `slowWaitReport`
names that constant in every line it emits.

## Verification

```
$ mvn -o -pl ha-raft -Dit.test='RaftBootstrap*IT,BootstrapElectionIT,Issue7259ClusterBootstrapSettledIT' -DskipITs=false verify
RaftBootstrapFromLocalDatabaseIT ............ Tests run: 1, Failures: 0
Issue7259ClusterBootstrapSettledIT .......... Tests run: 2, Failures: 0
RaftBootstrapTimeoutFallbackIT .............. Tests run: 1, Failures: 0
RaftBootstrapFingerprintMismatchSameLsnIT ... Tests run: 1, Failures: 0
RaftBootstrapLateNewerJoinerIT .............. Tests run: 1, Failures: 0
RaftBootstrapDoesNotEngageOnRestartIT ....... Tests run: 1, Failures: 0
RaftBootstrapPicksHighestLastTxIdIT ......... Tests run: 1, Failures: 0
BootstrapElectionIT ......................... Tests run: 3, Failures: 0
RaftBootstrapLeadershipTransferIT ........... Tests run: 1, Failures: 0
BUILD SUCCESS
```

plus `RaftLeaderFailoverIT` and `RaftQuorumLostIT` (green in an earlier sweep, both left-a-server-down shapes),
and the IT the issue was filed against:

```
$ mvn -o -pl bolt -Dit.test=BoltFollowerForwardingIT -DskipITs=false verify
... WARNI [BoltFollowerForwardingIT] TEST SLOW WAIT: cluster bootstrap settle satisfied after 2594 ms ...
BoltFollowerForwardingIT .................... Tests run: 1, Failures: 0
BUILD SUCCESS
```

The gate satisfies in 2.5-7.7 s across those runs, so what it waits for is real work the tests were previously
racing, not a no-op. No run hit the budget.

**The regression test can fail.** With `waitForClusterBootstrapToSettle()` commented out of
`waitAllReplicasAreConnected()` and nothing else changed:

```
[ERROR] Issue7259ClusterBootstrapSettledIT.bootstrapPassHasReportedAndEveryPeerHasCaughtUpBeforeTheTestBodyRuns -- FAILURE!
[ERROR] Tests run: 2, Failures: 1
```

The second method (`aTypeCreatedRightAfterStartupReachesEveryPeer`) passes either way on a workstation - the
divergence itself has never reproduced locally, which is stated in the issue and is why the first method exists.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen the author's reasoning. **No `Task` tool was
available in this session**, so the pass was run as a self-directed adversarial re-read of the finished diff
instead. That is weaker by construction - it had been persuaded already - and is recorded as such. Two findings
came out of it and both were fixed before the PR opened:

1. **A timed-out gate reported itself as satisfied.** The first draft funnelled "the pass committed nothing"
   and "the pass never reported inside the budget" through one `boolean` and logged
   `reportSlowWait(..., satisfied = true)` for both. A full 15 s burn would therefore have printed the word the
   instrument reserves for a wait that finished - hiding exactly the regression `slowWaitReport` exists to
   catch. `awaitTerminalBootstrapOutcome` now returns the outcome (or `null` on timeout) and the two cases are
   reported differently.
2. **The applied-index check allocated an `int[]` and a `long[]` per poll round.** Replaced with a single
   lowest/highest pass, which is also the honest statement of what it checks.

A third reading was raised and rejected with evidence: that `getLastAppliedTermIndex()` could be seeded from a
Ratis snapshot marker running ahead of what a peer applied (`ha-raft/CLAUDE.md`, "Replay position comes only
from the Ratis snapshot marker"). It cannot bite here - the gate runs at first cluster formation, where the
Raft log is empty and there is no snapshot to seed from - and the existing `waitForReplicationIsCompleted`
reads the same value, so the gate is no weaker than the wait it sits beside.

## Pre-existing red, unrelated

`ArcadeStateMachinePerDatabaseHaltTest` fails both its methods on `main` with the #7259 diff fully reverted.
Filed as **#7520**; not touched here.

## Residual risk

- **The byte-level mechanism of the divergence is not established.** Both orderings that can be reasoned about
  statically preserve the type (schema entry before the bootstrap entry -> the superseded guard skips the
  reinstall; after -> the reinstall runs first and the entry applies on top, both on the single apply thread).
  An earlier reading blamed a torn snapshot served from the leader's live directory; that reading is **wrong**
  and was removed - `SnapshotHttpHandler` serves a point-in-time `PageSnapshot` under `executeInReadLock`, with
  `suspendFlushAndExecute` as the fallback. What remains unexplained is filed as **#7519**.
- This fix removes the overlap the failure needed in the test harness. It does not prove the hazard cannot be
  reached another way, and it gates no production client - also #7519.
- The `waitForReplicationIsCompleted` give-ups (**#7518**) remain: a test can still be told replication
  completed when nothing waited.
