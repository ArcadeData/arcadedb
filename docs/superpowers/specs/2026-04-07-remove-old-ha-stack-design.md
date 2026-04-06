# Remove Old HA Stack from Server Module

**Date:** 2026-04-07
**Branch:** ha-redesign
**Status:** Approved

## Goal

Remove the legacy custom HA (High Availability) implementation from the `server` module, leaving the Raft-based implementation in `ha-raft/` as the single HA stack. This is the final step in the HA redesign effort.

## Context

The `ha-redesign` branch contains a complete Raft-based HA implementation (`ha-raft/` module, 14 classes, 42 tests) using Apache Ratis 3.2.2. The old custom HA stack (39 classes, ~6,600 LOC) in `server/src/main/java/com/arcadedb/server/ha/` coexists with the new stack via an `HA_IMPLEMENTATION` config toggle. The new stack is feature-complete and production-ready with comprehensive chaos testing.

## Approach: Bottom-Up Removal

Each step keeps the build compilable/testable. Relocate shared abstractions first, then delete old code.

## Step 1: Create HAServerPlugin Interface

Create `server/src/main/java/com/arcadedb/server/HAServerPlugin.java` in package `com.arcadedb.server`.

Extract from `HAServer.java`:
- **Enums:** `QUORUM` (with `quorum(int)` method), `ELECTION_STATUS`, `SERVER_ROLE`
- **Method signatures consumed outside old HA code:**
  - `boolean isLeader()`
  - `String getLeaderName()`
  - `ELECTION_STATUS getElectionStatus()`
  - `String getClusterName()`
  - `Map<String, Object> getStats()`
  - `int getConfiguredServers()`
- **New methods for operations previously handled by old HA internals:**
  - `void shutdownRemoteServer(String serverName)` - replaces direct `Leader2ReplicaNetworkExecutor` usage in `PostServerCommandHandler`
  - `void disconnectCluster()` - replaces direct `Replica2LeaderNetworkExecutor`/`disconnectAllReplicas()` usage

This interface extends `ServerPlugin`.

## Step 2: Move HAReplicatedDatabase

Move `server/src/main/java/com/arcadedb/server/ha/HAReplicatedDatabase.java` to `server/src/main/java/com/arcadedb/server/HAReplicatedDatabase.java`.

Change package declaration from `com.arcadedb.server.ha` to `com.arcadedb.server`.

Update `HAServer.QUORUM` reference in the interface to `HAServerPlugin.QUORUM`.

## Step 3: Update ha-raft Module

- `RaftHAServer` implements `HAServerPlugin` (instead of just `ServerPlugin`)
- Implement `shutdownRemoteServer(String)` - send shutdown via HTTP to the target server's REST API (POST /api/v1/server with `{"command":"shutdown"}`)
- Implement `disconnectCluster()` - close the local Ratis `RaftServer` and `RaftClient`, effectively leaving the cluster
- `RaftReplicatedDatabase` updates import for `HAReplicatedDatabase` to `com.arcadedb.server.HAReplicatedDatabase`
- `RaftReplicatedDatabase` changes `HAServer.QUORUM` references to `HAServerPlugin.QUORUM`
- `BaseRaftHATest` changes `HAServer.SERVER_ROLE` to `HAServerPlugin.SERVER_ROLE`

## Step 4: Update All Consumers

### Server main code (6 files):
- **`ArcadeDBServer.java`** - field type `HAServer` to `HAServerPlugin`, getter return type, remove `ReplicatedDatabase` import, update plugin discovery to look for `HAServerPlugin`
- **`PostCommandHandler.java`** - update `HAReplicatedDatabase` import to `com.arcadedb.server.HAReplicatedDatabase`
- **`PostServerCommandHandler.java`** - update `HAReplicatedDatabase` import, change `HAServer` to `HAServerPlugin`, replace `Leader2ReplicaNetworkExecutor`/`Replica2LeaderNetworkExecutor` usage with `HAServerPlugin.shutdownRemoteServer()`/`HAServerPlugin.disconnectCluster()`, remove `ServerShutdownRequest` import
- **`GetServerHandler.java`** - update `HAReplicatedDatabase` import, change `HAServer` to `HAServerPlugin`
- **`BackupTask.java`** - change `HAServer` to `HAServerPlugin`
- **`ServerStatusTool.java`** (MCP) - change `HAServer` to `HAServerPlugin`

### Test utils (2 files):
- **`test-utils/.../BaseGraphServerTest.java`** - change `HAServer.SERVER_ROLE` to `HAServerPlugin.SERVER_ROLE`
- **`server/src/test/.../BaseGraphServerTest.java`** - same change

## Step 5: Delete Old HA Implementation

### Main classes (39 files):
Delete the entire `server/src/main/java/com/arcadedb/server/ha/` package:
- `HAServer.java`
- `ReplicatedDatabase.java`
- `Leader2ReplicaNetworkExecutor.java`
- `Replica2LeaderNetworkExecutor.java`
- `LeaderNetworkListener.java`
- `ReplicationProtocol.java`
- `ReplicationMessage.java`
- `ReplicationLogFile.java`
- `ReplicationException.java`, `ReplicationLogException.java`
- All 27 files in `message/` subdirectory
- Both files in `network/` subdirectory (`ServerSocketFactory.java`, `DefaultServerSocketFactory.java`)

### Test classes (27 files):
Delete the entire `server/src/test/java/com/arcadedb/server/ha/` package:
- `ReplicationServerIT.java`, `HTTP2ServersIT.java`
- All leader/quorum/replica/crash/index test files
- `ManualClusterTests.java`

### Keep:
- `e2e-ha/` module (tests the Raft implementation)
- `ha-raft/` module (the new implementation)

## Step 6: Clean Up GlobalConfiguration

### Remove (14 entries):
- `HA_IMPLEMENTATION` - single implementation, no choice needed
- `HA_SERVER_ROLE` - Raft handles roles automatically
- `HA_REPLICATION_QUEUE_SIZE` - old message queue
- `HA_REPLICATION_FILE_MAXSIZE` - old file transfer
- `HA_REPLICATION_CHUNK_MAXSIZE` - old chunked transfer
- `HA_REPLICATION_INCOMING_HOST` - old TCP listener
- `HA_REPLICATION_INCOMING_PORTS` - old TCP listener
- `HA_LOG_SEGMENT_SIZE` - old replication log segments
- `HA_APPEND_BUFFER_SIZE` - old log append buffer
- `HA_ELECTION_TIMEOUT_MIN` - old election (Ratis has its own)
- `HA_ELECTION_TIMEOUT_MAX` - old election
- `HA_ERROR_RETRIES` - old retry logic
- `HA_K8S` - old K8s discovery
- `HA_K8S_DNS_SUFFIX` - old K8s discovery

### Keep (12 entries):
- `HA_ENABLED`, `HA_CLUSTER_NAME`, `HA_SERVER_LIST`
- `HA_QUORUM`, `HA_QUORUM_TIMEOUT`
- `HA_REPLICATION_LAG_WARNING`, `HA_LOG_VERBOSE`, `HA_CLUSTER_TOKEN`
- `HA_RAFT_PORT`, `HA_RAFT_PERSIST_STORAGE`, `HA_RAFT_SNAPSHOT_THRESHOLD`, `HA_RAFT_GROUP_COMMIT_BATCH_SIZE`

## Step 7: Verify Build

- Compile entire project: `mvn clean install -DskipTests`
- Run ha-raft tests: `cd ha-raft && mvn test`
- Run server tests (non-HA): `cd server && mvn test`

## What Does NOT Change

- `network/` module - `QuorumNotReachedException`, `ServerIsNotTheLeaderException`, `RemoteHttpComponent` stay as-is
- `e2e-ha/` module - kept, tests the Raft implementation
- Protocol plugins (Bolt, Postgres, Redis) - reference `ServerPlugin` not `HAServer`, no changes needed
- `ReplicationCallback.java` - already in `com.arcadedb.server` package, no move needed

## Risk Assessment

- **Low risk:** The new Raft implementation already covers all scenarios tested by the old HA tests (and more). The ha-raft module has 42 tests plus 9 e2e chaos tests.
- **Migration impact:** Users with old HA config entries will get unknown-config warnings. They need to update config files to remove legacy entries and ensure `HA_IMPLEMENTATION` is no longer set.
- **API compatibility:** `ArcadeDBServer.getHA()` return type changes from concrete `HAServer` to `HAServerPlugin` interface. Any external code calling HA-specific methods on the old type will need updating.
