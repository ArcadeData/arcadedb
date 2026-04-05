# Design: Port apache-ratis improvements to ha-redesign

**Date:** 2026-04-06
**Branch:** ha-redesign
**Source branch:** apache-ratis

## Context

Both `ha-redesign` and `apache-ratis` rewrite ArcadeDB's HA stack on Apache Ratis. `ha-redesign` has superior architecture (separate `ha-raft/` module, plugin-based integration, 39 test files, chaos e2e suite) but lacks several performance and correctness features from `apache-ratis`. This design covers porting those features into `ha-redesign`.

## Decisions

- **Sequencing:** Critical-path first (group committer, then compression, then snapshot install), followed by operability (quorum enum, state machine upgrade), then polish (HALog, Studio UI, test ports).
- **Rollout:** All commits land on `ha-redesign` branch directly. No separate PRs.
- **Compatibility:** Clean break. `ha-redesign` is unreleased, so log entry format, state machine storage format, and config value changes require no backward compatibility.
- **Tests:** ha-redesign-style only (unit + BaseMiniRaftTest IT + e2e-ha chaos). No verbatim porting of apache-ratis tests.
- **Success gate:** All existing tests green + full e2e-ha chaos suite passes + RaftHAInsertBenchmark shows throughput improvement + RaftFullSnapshotResyncIT passes (no longer a stub).

## Out of Scope

- K8s auto-join (`tryAutoJoinCluster` / `leaveCluster`) - feature, not a "better part" port
- Command forwarding via Ratis `query()` path - ha-redesign already uses HTTP forwarding, simpler and tested
- Dynamic membership (`addPeer` / `removePeer` / `transferLeadership`) - useful but orthogonal

---

## Item 1: RaftGroupCommitter

**Goal:** Amortize Raft gRPC round-trip cost across concurrent transactions via batching. Current ha-redesign blocks each commit individually (~15ms RTT). Group commit batches up to N entries per round-trip.

### Files

- **New:** `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java`
- **Modified:** `RaftReplicatedDatabase` - `commit()` calls `groupCommitter.submitAndWait()` instead of `raftClient.io().send()`
- **Modified:** `RaftHAServer` - owns `RaftGroupCommitter` lifecycle (create after `raftClient`, stop on shutdown)
- **Modified:** `RaftHAPlugin` - wires committer through plugin startup/shutdown
- **Modified:** `GlobalConfiguration` - add `HA_RAFT_GROUP_COMMIT_BATCH_SIZE` (int, default 500)

### Design

`RaftGroupCommitter` contains:
- `LinkedBlockingQueue<PendingEntry>` for incoming entries
- Single daemon flusher thread (`arcadedb-raft-group-committer`)
- `submitAndWait(byte[] entry, long timeoutMs)`: enqueues, blocks caller on `CompletableFuture<Exception>`

Flusher loop:
1. `queue.poll(100ms)` for first entry (blocks until one arrives)
2. `queue.drainTo(batch, MAX_BATCH_SIZE - 1)` to collect all pending (non-blocking)
3. Send all entries via `raftClient.async().send()` (pipelined)
4. Wait for all replies, complete each caller's future
5. If `Quorum.ALL`: after majority commit, call `client.io().watch(logIndex, ALL_COMMITTED)` per entry

Shutdown: interrupt flusher, drain queue completing all futures with `QuorumNotReachedException`.

The committer is encoding-agnostic - it takes `byte[]` from `RaftLogEntryCodec` output. Single-thread-no-contention case: queue has one entry, flushed immediately, zero overhead beyond the queue add/poll.

### Ratis Config Additions

Port these `RaftProperties` settings from apache-ratis to `RaftHAServer.start()`:
- `Log.Appender.setBufferByteLimit(4MB)` - allow multiple entries per AppendEntries gRPC call
- `Log.Appender.setBufferElementLimit(256)` - cap per-batch element count
- `Read.setLeaderLeaseEnabled(true)` - consistent reads without round-trip
- `Read.setLeaderLeaseTimeoutRatio(0.9)`
- `Read.setOption(LINEARIZABLE)`
- `Log.setSegmentSizeMax(64MB)`
- `Log.setWriteBufferSize(8MB)`

### Tests

- **Unit:** `RaftGroupCommitterTest` - batch accumulation under concurrent submits, timeout handling, shutdown drains queue with errors, single-entry passes through without delay
- **IT:** 3-node `BaseMiniRaftTest` with 8 concurrent writer threads, verify all records replicated and that HALog shows batch sizes > 1
- **e2e:** existing chaos suite must pass with group commit enabled (no lost writes during partition)
- **Benchmark:** `RaftHAInsertBenchmark` before/after comparison

---

## Item 2: Log Entry Compression

**Goal:** Reduce WAL payload size in Raft log entries using ArcadeDB's existing `CompressionFactory`.

### Files

- **Modified:** `RaftLogEntryCodec` - compress WAL data in `encodeTxEntry()` and `encodeSchemaEntry()`, decompress in `decode()`

### Design

Only the WAL data blob is compressed. Type byte, database name, bucket deltas, schema JSON, and file maps stay uncompressed (small, useful for debugging).

Encode path (TX_ENTRY):
1. Write type byte, database name (unchanged)
2. Write `uncompressedLength` (int) - the original WAL data size
3. Compress WAL data via `CompressionFactory.getDefault().compress()`
4. Write compressed bytes
5. Write bucket deltas (unchanged)

Encode path (SCHEMA_ENTRY): same pattern for each embedded WAL entry in the `walEntries` list.

Decode path: read `uncompressedLength`, then `CompressionFactory.getDefault().decompress()` to recover original WAL data.

`INSTALL_DATABASE_ENTRY` has no WAL payload - no compression needed.

Clean break: old uncompressed entries are not decodable. Acceptable since ha-redesign is unreleased.

### Tests

- **Unit:** `RaftLogEntryCodecTest` updated - roundtrip encode/decode with compression, verify compressed size < uncompressed for non-trivial WAL data
- **IT:** existing replication ITs validate end-to-end correctness (encode on leader, decode on follower)

---

## Item 3: Snapshot Install Wired to Ratis

**Goal:** When a follower falls too far behind the leader's Raft log (entries already compacted), automatically resync via HTTP-based database snapshot download instead of requiring manual data copy.

### Files

- **New:** `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`
- **Modified:** `ArcadeStateMachine` - implement `notifyInstallSnapshotFromLeader()`, switch to `SimpleStateMachineStorage`
- **Modified:** `RaftHAPlugin` - register snapshot HTTP route
- **Modified:** `RaftHAServer` - add `getPeerHttpAddress(RaftPeerId)` public accessor

### Design

**Ratis config** (in `RaftHAServer.start()`):
- `Log.Appender.setInstallSnapshotEnabled(false)` - disable Ratis built-in snapshot transfer
- `Snapshot.setAutoTriggerEnabled(true)`, threshold 100,000 entries
- Ratis calls `notifyInstallSnapshotFromLeader()` instead of streaming chunks internally

**Leader side - `SnapshotHttpHandler`:**
- Registered at `/api/v1/ha/snapshot/{database}` by `RaftHAPlugin`
- Authenticates via Basic auth (existing server security)
- Acquires read lock + suspends page flushing for a consistent snapshot point
- Streams a ZIP containing: database config file, schema file, all `ComponentFile` data files
- Excludes WAL files (stale after snapshot install)
- Undertow `HttpHandler` implementation, same pattern as `GetClusterHandler`

**Follower side - `ArcadeStateMachine.notifyInstallSnapshotFromLeader()`:**
1. Resolve leader HTTP address via `raftHAServer.getPeerHttpAddress(leaderId)`
2. For each database: `GET http://leader/api/v1/ha/snapshot/{db}`
3. Close the local database
4. Extract ZIP over database directory (with zip-slip path traversal protection)
5. Delete stale `.wal` files
6. Return `firstTermIndexInLog` so Ratis replays log entries from that point
7. Database re-opens on next access

**Timeouts:** 30s connect, 300s read (5 minutes for large databases).

### Tests

- **Unit:** `SnapshotHttpHandlerTest` with a mock database, verify ZIP contains expected files
- **IT:** `RaftFullSnapshotResyncIT` (currently a stub, becomes real) - start 3 nodes, write data, stop follower, write enough to trigger log compaction past follower's last index, restart follower, verify it catches up via snapshot and has all data
- **e2e:** extend `NetworkPartitionRecoveryIT` or add variant that forces snapshot-based resync

---

## Item 4: ArcadeStateMachine Upgrade (SimpleStateMachineStorage + Election Metrics)

**Goal:** Replace hand-rolled `arcade-last-applied.txt` persistence with Ratis-native `SimpleStateMachineStorage`. Add election counters for operational visibility.

Folded into item 3 since they modify the same class.

### Files

- **Modified:** `ArcadeStateMachine`
- **Modified:** `GetClusterHandler`

### Design

**Storage migration:**
- Remove: `LAST_APPLIED_FILE` constant, `lastAppliedFile` field, `persistLastApplied()` method, manual file parsing in `initialize()`
- Add: `SimpleStateMachineStorage storage` field
- `initialize()`: call `storage.init(raftStorage)`, then `reinitialize()`
- `reinitialize()`: load latest snapshot info from storage, set `lastAppliedIndex`
- Override `getStateMachineStorage()` to return `storage`

**Election metrics:**
- Add: `AtomicLong electionCount`, `volatile long lastElectionTime`, `volatile long startTime`
- `notifyLeaderChanged()`: increment `electionCount`, set `lastElectionTime = System.currentTimeMillis()`
- Add `notifyConfigurationChanged()` override for logging (fires `REPLICA_ONLINE` callbacks)

**GetClusterHandler enrichment** - add to JSON response:
- `electionCount` (long)
- `lastElectionTime` (epoch ms)
- `uptime` (ms since start)

### Tests

- **Unit:** `ArcadeStateMachineTest` - verify election counter increments, verify `reinitialize()` restores from `SimpleStateMachineStorage`
- **IT:** existing leader failover ITs - add assertions on election count
- `GetClusterHandlerIT` - assert new JSON fields present

---

## Item 5: Quorum Enum

**Goal:** Replace string-based `HA_QUORUM` config handling with a proper enum. Add `ALL` quorum support via Ratis Watch API.

### Files

- **New:** `ha-raft/src/main/java/com/arcadedb/server/ha/raft/Quorum.java`
- **Modified:** `RaftHAPlugin` - use `Quorum.parse()` for config validation
- **Modified:** `RaftHAServer` - store `Quorum` field, expose `getQuorum()` / `getQuorumTimeout()` (reads existing `HA_QUORUM_TIMEOUT` config)
- **Modified:** `RaftGroupCommitter.flushBatch()` - ALL quorum enforcement via Watch API
- **Modified:** `GlobalConfiguration` - update `HA_QUORUM` description

### Design

```java
public enum Quorum {
    MAJORITY, ALL;

    public static Quorum parse(final String value) {
        return switch (value.toLowerCase()) {
            case "majority" -> MAJORITY;
            case "all" -> ALL;
            default -> throw new ConfigurationException(
                "Unsupported HA quorum '" + value + "'. Valid: 'majority', 'all'");
        };
    }
}
```

Drop `"none"` (meaningless with Raft - Ratis always requires majority). Clean break.

ALL quorum enforcement in `RaftGroupCommitter.flushBatch()`: after the async send returns success (majority), call `client.io().watch(reply.getLogIndex(), ReplicationLevel.ALL_COMMITTED)`. If the watch fails or times out, complete the entry's future with `QuorumNotReachedException`.

### Tests

- **Unit:** `QuorumTest` - parse valid values, invalid throws `ConfigurationException`
- **IT:** `RaftHAConfigurationIT` - updated for enum values
- **IT:** new test method: 3-node cluster with `ALL` quorum, kill one replica, verify write times out

---

## Item 6: HALog Utility

**Goal:** Structured HA-specific logging with configurable verbosity levels, replacing ad-hoc `LogManager.log(Level.FINE)` calls.

### Files

- **New:** `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HALog.java`
- **Modified:** `GlobalConfiguration` - add `HA_LOG_VERBOSE` (int, default 0, range 0-3)
- **Modified:** all ha-raft classes - replace `Level.FINE` HA debug logging with `HALog.log()`

### Design

```java
public final class HALog {
    public static final int BASIC = 1;    // elections, leader changes, peer add/remove
    public static final int DETAILED = 2; // command forwarding, WAL replication, schema changes
    public static final int TRACE = 3;    // every state machine apply, serialization, batch sizes

    public static void log(Object caller, int level, String message, Object... args) {
        if (GlobalConfiguration.HA_LOG_VERBOSE.getValueAsInteger() >= level)
            LogManager.instance().log(caller, Level.INFO, "[HA-" + level + "] " + message, null, args);
    }
}
```

No `System.out.println` (apache-ratis had this - likely a debug leftover).

Existing `Level.INFO` messages (e.g., "Raft cluster joined", "Leader elected") stay as-is - they are always-on operational messages. `HALog` replaces the `Level.FINE` debug messages that are currently invisible unless JUL is reconfigured.

### Callsites

- `ArcadeStateMachine`: apply, skip-on-leader, schema apply, snapshot
- `RaftGroupCommitter`: batch sizes, flush counts
- `RaftReplicatedDatabase`: commit path, forward path
- `RaftHAServer`: start, stop, refresh, lag check
- `RaftHAPlugin`: lifecycle events

### Tests

- **Unit:** `HALogTest` - `isEnabled()` respects config, `log()` only emits when level is met

---

## Item 7: Studio Cluster Monitor UI

**Goal:** Replace the minimal cluster tab with a richer UI showing leader/follower topology, election count, replication lag, and uptime.

### Files

- **Modified:** `studio/src/main/resources/static/cluster.html`
- **Modified:** `studio/src/main/resources/static/js/studio-cluster.js`

### Design

Port the apache-ratis Studio changes, adjusting JSON field names to match ha-redesign's `GetClusterHandler` response format (enriched with election metrics from item 4).

The UI:
- Polls `/api/v1/cluster` on a timer
- Displays leader badge, follower list with lag counters
- Shows election count, last election time, cluster uptime
- Uses jQuery + Bootstrap 5 only (per CLAUDE.md, no new frontend deps)

Manual diff of apache-ratis changes against main, then apply to ha-redesign. Review for any references to apache-ratis-specific API shapes.

### Tests

- Manual: start 3-node cluster, verify Studio cluster tab renders correctly
- No automated test (Studio UI is not covered by Java test suite)

---

## Item 8: Test Ports

**Goal:** Port `ReadConsistencyIT` and fill any remaining test gaps.

### Files

- **New:** `ha-raft/src/test/java/.../raft/RaftReadConsistencyIT.java`

### Design

`ReadConsistencyIT` tests that reads on a follower return data consistent with the leader's committed state. Relevant now that we're enabling `LeaderLeaseEnabled` and `LINEARIZABLE` reads in Ratis config.

Adapted to `BaseMiniRaftTest` base class. 3-node cluster, write on leader, read from follower, assert consistency.

Scan `RaftHAComprehensiveIT` (971 LOC in apache-ratis) for scenarios not already covered by ha-redesign's 39 test files. Add any gaps as new test methods on existing IT classes.

### Tests

- `RaftReadConsistencyIT` must pass on a 3-node cluster

---

## Implementation Order

| Step | Item | Depends on |
|------|------|-----------|
| 0 | Baseline benchmark run | - |
| 1 | RaftGroupCommitter | - |
| 2 | Log entry compression | - |
| 3 | Snapshot install + StateMachine upgrade | - |
| 4 | Quorum enum | RaftGroupCommitter (ALL enforcement lives there) |
| 5 | HALog utility | - |
| 6 | Studio cluster UI | StateMachine upgrade (election metrics in JSON) |
| 7 | Test ports | All items (tests exercise ported features) |
| 8 | Full validation | All items |

Steps 1, 2, 3, and 5 are independent and could be parallelized. Step 4 depends on step 1. Step 6 depends on step 3. Step 7 depends on all items. Step 8 is the final validation gate.
