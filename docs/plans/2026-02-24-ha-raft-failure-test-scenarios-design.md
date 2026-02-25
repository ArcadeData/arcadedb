# HA Raft Failure Test Scenarios Design

**Date:** 2026-02-24
**Status:** Approved
**Related plan:** `docs/plans/2026-02-15-ha-raft-redesign.md`

## Overview

Extends the `ha-raft` test infrastructure to cover node crash/recovery and split-brain
scenarios, with full `DatabaseComparator` byte-level consistency verification after
recovery. Two complementary tracks handle different simulation needs.

## Approach Summary

| Track | Mechanism | Scenarios covered |
|---|---|---|
| Extended `BaseRaftHATest` | Real `ArcadeDBServer` + persistent Raft storage | Crash, restart, log replay catchup, snapshot resync |
| New `BaseMiniRaftTest` | `MiniRaftCluster` (ratis-test) + `EmbeddedDatabase` | Split-brain, network partition, partition healing |

## Track 1: Extended `BaseRaftHATest` — Crash / Restart / Recovery

### Storage Directory Strategy

`RaftHAServer` currently creates a per-run temp storage path that is cleaned on startup.
This prevents a restarted peer from finding its Raft log and rejoining as the same peer.

**Fix:** Derive a stable storage root from the server index:

```
target/raft-test-storage/server-{i}/
```

This path is created once in `@BeforeEach` and deleted only in `@AfterEach`.
A boolean flag (`persistentRaftStorage`, default `false`) on `BaseRaftHATest` lets
existing tests keep current behaviour. New crash/restart tests override it to `true`.

### `restartServer(int serverIndex)` Method

Added to `BaseRaftHATest`:

1. Call `getServer(serverIndex).stop()` — does **not** delete the storage directory
2. Construct a new `ArcadeDBServer` instance with the same peer ID and the same stable storage path
3. Call `waitForReplicationIsCompleted(serverIndex)` to block until the restarted peer catches up to the leader's commit index

### New Test Classes

#### `RaftReplicaCrashAndRecoverIT` (2-node, quorum=none)

1. Write 200 records on the leader
2. `assertClusterConsistency()` — both nodes agree
3. Stop the replica
4. Write 200 more records on the leader
5. `restartServer(replicaIndex)` — replica rejoins, replays Raft log
6. `waitForReplicationIsCompleted(replicaIndex)`
7. Assert replica has 400 records
8. `assertClusterConsistency()` — full `DatabaseComparator` comparison (no override)

#### `RaftLeaderCrashAndRecoverIT` (3-node, quorum=majority)

1. Write 200 records on the leader
2. `assertClusterConsistency()`
3. Stop the leader (`leaderIndex`)
4. Wait for new leader election among surviving nodes
5. Write 100 more records on the new leader
6. `restartServer(leaderIndex)` — old leader rejoins as follower, catches up
7. `waitForReplicationIsCompleted(leaderIndex)`
8. Assert all three nodes have 300 records
9. `assertClusterConsistency()` — full `DatabaseComparator` comparison

#### `RaftFullSnapshotResyncIT` (2-node, quorum=none)

1. Configure Ratis snapshot threshold to a small value (e.g. 50 log entries)
2. Write 50 records on the leader (triggers a snapshot on the leader)
3. `assertClusterConsistency()`
4. Stop the replica
5. Write 200 more records on the leader (log entries before the snapshot are purged)
6. `restartServer(replicaIndex)` — replica is too far behind for log replay, receives full snapshot install
7. `waitForReplicationIsCompleted(replicaIndex)`
8. Assert replica has 250 records
9. `assertClusterConsistency()` — full `DatabaseComparator` comparison

### `DatabaseComparator` Use

These three tests do **not** override `checkDatabasesAreIdentical()`. The base class
implementation (inherited from `BaseGraphServerTest`) calls `DatabaseComparator.compare()`
across all running servers. After a full restart + catchup this comparison is expected
to pass cleanly.

## Track 2: `BaseMiniRaftTest` — Split-Brain / Partition Tests

### New Test Dependency

```xml
<dependency>
    <groupId>org.apache.ratis</groupId>
    <artifactId>ratis-test</artifactId>
    <version>${ratis.version}</version>
    <scope>test</scope>
</dependency>
```

Apache 2.0 licensed. No new compile-scope dependency is added.

### `BaseMiniRaftTest` Base Class

- Creates a `MiniRaftClusterWithGrpc` with N peers (configurable per test)
- For each peer, provisions a real on-disk `EmbeddedDatabase` under
  `target/mini-raft-db/{peerId}/` using `DatabaseFactory`
- Wires an `ArcadeStateMachine` instance to each peer (same class used in production)
- Exposes:
  - `partitionLeaderFromFollowers()` — uses `BlockRequestHandlingInjection` to drop messages to/from the leader peer
  - `healPartition()` — removes the injection
  - `assertAllPeersIdentical()` — flushes and closes all peer databases, then calls `DatabaseComparator.compare(db0, dbN)` for each N > 0, then reopens them

### `assertAllPeersIdentical()` Pattern

`DatabaseComparator.compare()` requires the databases to not be concurrently written.
After `healPartition()` + settle wait:

1. Stop writes on all peers
2. Flush + close all `EmbeddedDatabase` instances
3. Reopen each as read-only via `DatabaseFactory.open()`
4. Call `DatabaseComparator.compare(base, peer)` for each peer
5. Close all read-only handles
6. Reopen for continued test use if needed

This 3-line open/compare/close pattern is extracted into `BaseMiniRaftTest.compareClosedDatabases(path1, path2)`, mirroring the existing manual `ClusterDatatbaseChecker` tool.

### New Test Classes

#### `RaftSplitBrain3NodesIT` (replaces @Disabled stub)

3-node cluster, quorum=majority.

1. Submit 50 write entries; verify all 3 state machines apply them
2. `partitionLeaderFromFollowers()` — leader cannot reach the 2 followers
3. Wait for the 2-node majority to elect a new leader (≤ 30 s)
4. Verify: the old leader's role transitions away from LEADER (no majority available)
5. Submit 50 more writes on the new leader
6. Attempt a write on the old leader — must fail or hang (no quorum reachable)
7. `healPartition()`
8. Wait for convergence (old leader discovers new term, steps down, applies majority log)
9. Assert all 3 state machines have applied exactly 100 entries
10. `assertAllPeersIdentical()` — full `DatabaseComparator` comparison

#### `RaftSplitBrain5NodesIT`

5-node cluster, quorum=majority. Partition: 2 nodes isolated, 3-node majority continues.

1. Submit 50 write entries; all 5 nodes confirm
2. Partition: isolate nodes 0 and 1 from nodes 2, 3, 4
3. The 3-node partition elects a leader and accepts 50 more writes
4. The 2-node minority cannot elect a leader (no majority); writes hang/fail
5. `healPartition()`
6. Wait for all 5 nodes to converge on the majority log
7. Assert all 5 state machines have applied exactly 100 entries
8. `assertAllPeersIdentical()` — full `DatabaseComparator` comparison

## Consistency Verification Summary

| Test class | Verification method | Notes |
|---|---|---|
| `RaftReplicaCrashAndRecoverIT` | `assertClusterConsistency()` → `DatabaseComparator` | No override; all nodes running after recovery |
| `RaftLeaderCrashAndRecoverIT` | `assertClusterConsistency()` → `DatabaseComparator` | No override; old leader rejoins before comparison |
| `RaftFullSnapshotResyncIT` | `assertClusterConsistency()` → `DatabaseComparator` | No override; snapshot install followed by comparison |
| `RaftSplitBrain3NodesIT` | `assertAllPeersIdentical()` → `DatabaseComparator` | Close/reopen pattern before compare |
| `RaftSplitBrain5NodesIT` | `assertAllPeersIdentical()` → `DatabaseComparator` | Close/reopen pattern before compare |

## Files Changed / Created

### Modified
- `ha-raft/pom.xml` — add `ratis-test` test dependency
- `ha-raft/src/test/.../BaseRaftHATest.java` — add `persistentRaftStorage` flag, stable storage path derivation, `restartServer()` method
- `ha-raft/src/test/.../RaftSplitBrain3NodesIT.java` — replace @Disabled stub with real implementation (now extends `BaseMiniRaftTest`)
- `ha-raft/src/main/.../RaftHAServer.java` — expose configurable storage root path (used by `BaseRaftHATest` to inject stable paths in tests)

### Created
- `ha-raft/src/test/.../BaseMiniRaftTest.java`
- `ha-raft/src/test/.../RaftReplicaCrashAndRecoverIT.java`
- `ha-raft/src/test/.../RaftLeaderCrashAndRecoverIT.java`
- `ha-raft/src/test/.../RaftFullSnapshotResyncIT.java`
- `ha-raft/src/test/.../RaftSplitBrain5NodesIT.java`
