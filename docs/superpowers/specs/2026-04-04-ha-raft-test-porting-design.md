# HA-Raft Test Porting Design

**Date:** 2026-04-04
**Branch:** ha-redesign
**Goal:** Port 13 meaningful integration tests from `server/src/test/java/com/arcadedb/server/ha/` to `ha-raft/src/test/java/com/arcadedb/server/ha/raft/`, covering scenarios not yet exercised by the existing Raft IT suite.

---

## Background

The `ha-raft` module introduces a Raft-based HA implementation as a replacement for the legacy `HAServer` protocol in the `server` module. The existing Raft IT tests cover core consensus scenarios (replication, leader failover, crash-and-recover, split brain, schema basics). However, the old `server/ha` test suite contains 14 additional test classes that cover the HTTP layer, write forwarding, schema lifecycle, materialized views, index operations, database utilities, and chaos. 13 of these are meaningful to port; 1 (`HASplitBrainIT`) is excluded because the Raft consensus-level split brain is already covered by `RaftSplitBrain3NodesIT` and `RaftSplitBrain5NodesIT`, and a real HTTP network-partition test would require OS-level network namespace manipulation not available in the JUnit environment.

---

## What is NOT ported (and why)

| Old test | Reason excluded |
|---|---|
| `HASplitBrainIT` | Split-brain at consensus level already covered; HTTP-partition simulation requires OS network tools |
| `ReplicationServerReplicaHotResyncIT` | Hot resync is a legacy-HA-only concept; Raft uses log replay and snapshot |
| `ReplicationServerFixedClientConnectionIT` | Disabled in old module; old connection strategy not applicable |
| `ReplicationServerLeaderChanges3TimesIT` | Disabled in old module |
| `ReplicationServerLeaderDownIT` | Disabled in old module |
| `ReplicationServerQuorumAllIT` | Raft has no "ALL" quorum mode; inherently majority-based |

---

## Architecture

### Location

All new tests go in:
```
ha-raft/src/test/java/com/arcadedb/server/ha/raft/
```

### Base class

All tests extend `BaseRaftHATest`, which already extends `BaseGraphServerTest`. No new base class is needed.

Key `BaseRaftHATest` utilities used by the ported tests:

| Utility | Purpose |
|---|---|
| `waitForReplicationIsCompleted()` | Waits for state machine to apply the leader's last log index |
| `waitAllReplicasAreConnected()` | Waits for Raft leader election to complete |
| `assertClusterConsistency()` | Combines both waits + verifies record counts match across nodes |
| `restartServer(int i)` | Stops and restarts server i with Raft storage preserved |
| `getRaftPlugin(int i)` | Returns `RaftHAPlugin` for server i |

### Key adaptations from old HA to Raft

| Old HA pattern | Raft equivalent |
|---|---|
| Extends `ReplicationServerIT` | Extends `BaseRaftHATest` |
| `getServer(i).getHA()` | `getRaftPlugin(i)` |
| Quorum `none` / `majority` / `all` | Quorum `none` / `majority` (no `ALL`) |
| `HAServer.setIOErrors()` for partition | Not used (excluded scenario) |
| Wait for replica connection (old protocol) | `waitAllReplicasAreConnected()` |

---

## Groups and Test Classes

### Group 1 - HTTP layer (3 tests)

**`RaftHTTP2ServersIT`**
- Cluster: 2 nodes, quorum: majority
- Tests:
  - `serverInfo()` - GET `/api/v1/server` returns cluster mode with `implementation=raft`
  - `propagationOfSchema()` - create type on leader, verify it appears on replica
  - `checkQuery()` - query execution on both nodes returns correct results
  - `checkDeleteGraphElements()` - create then delete graph elements, verify replication
  - `haConfiguration()` - HA config endpoint returns correct peer list and leader info

**`RaftHTTP2ServersCreateReplicatedDatabaseIT`**
- Cluster: 2 nodes, quorum: majority
- Tests:
  - `createReplicatedDatabase()` - create DB via HTTP POST on leader, create types on both servers, insert and verify 100 vertices per server

**`RaftHTTPGraphConcurrentIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `oneEdgePerTxMultiThreads()` - 4 threads x 100 SQL scripts each creating a Photo, User, and HasUploaded edge in REPEATABLE_READ isolation; verifies all edges and vertices replicated to all 3 nodes

### Group 2 - Write forwarding + schema/views (3 tests)

**`RaftReplicationWriteAgainstReplicaIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `writesForwardedFromReplicaToLeader()` - all writes target server 1 (a follower); Raft forwards to leader; verifies all 3 nodes consistent after replication

**`RaftReplicationChangeSchemaIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `schemaChangesReplicate()` - add/remove vertex types, properties, buckets, indexes via leader; verify all changes replicate
  - Verifies that direct schema modification on a replica throws `ServerIsNotTheLeaderException`
  - Verifies schema file consistency across all 3 nodes

**`RaftReplicationMaterializedViewIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `materializedViewReplicates()` - create document type, insert source data, create materialized view on leader; verify view exists and is queryable on all replicas; verify schema file contains view definition; drop view and verify removal replicates

### Group 3 - Index operations (2 tests)

**`RaftIndexCompactionReplicationIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `lsmTreeCompactionReplication()` - insert 5000 records, trigger compaction on leader, verify compacted state replicates to all nodes
  - `lsmVectorReplication()` - create vector index (dimensions=10), insert vectors on leader, verify index and data replicate
  - `lsmVectorCompactionReplication()` - vector index compaction replication
  - `compactionReplicationWithConcurrentWrites()` - concurrent writes during index compaction

**`RaftIndexOperations3ServersIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `rebuildIndex()` - insert 10,000 records with LSM index, rebuild index, verify consistency
  - `createIndexLater()` - insert data first, then create index, verify correct
  - `createIndexLaterDistributed()` - create index from different server than leader, verify distributed propagation
  - `createIndexErrorDistributed()` - duplicate key constraint violation handled correctly in cluster

### Group 4 - Database utilities + config + chaos (5 tests)

**`RaftServerDatabaseBackupIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `sqlBackup()` - SQL backup command creates backup file on each server independently
  - `sqlScriptBackup()` - SQL script backup command works correctly on all 3 nodes

**`RaftServerDatabaseSqlScriptIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `executeSqlScript()` - multi-statement SQL script (create vertex type, create edge type, insert vertices and edges with REPEATABLE_READ isolation) executes and replicates correctly; verify return values

**`RaftServerDatabaseAlignIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `alignNotNecessary()` - delete edge via replicated API; verify all nodes consistent; align command reports no action needed
  - `alignNecessary()` - bypass replication with direct DB transaction; verify inconsistency detected; align command repairs all nodes

**`RaftHAConfigurationIT`**
- Cluster: attempted startup only
- Tests:
  - `invalidPeerAddressRejected()` - peer list mixing `localhost` with non-localhost IPs (e.g. `192.168.0.1:2434,localhost:2434`) throws `ServerException` with message "Found a localhost"
- **Note:** This validation does not yet exist in `RaftHAServer`. Porting this test requires first adding the same localhost-mixing check to `RaftHAServer.start()` (or equivalent initialization path) that `HAServer` already performs.

**`RaftHARandomCrashIT`**
- Cluster: 3 nodes, quorum: majority
- Tests:
  - `replicationWithRandomCrashes()` - sustained replication of 1500 transactions x 10 vertices with a background thread randomly restarting servers every ~10 seconds; expects all records consistent on surviving nodes after test; expects at least 3 server restarts

---

## Naming Convention

All new test classes follow the `Raft*IT` prefix pattern already established in the module (e.g. `RaftReplication3NodesIT`, `RaftLeaderFailoverIT`).

---

## Verification

Each ported test is verified by:
1. Compiling the `ha-raft` module: `cd ha-raft && mvn test-compile`
2. Running the specific test: `mvn test -Dtest=<ClassName> -DskipITs=false`
3. Running the full ha-raft IT suite to confirm no regressions: `mvn verify -DskipITs=false`
