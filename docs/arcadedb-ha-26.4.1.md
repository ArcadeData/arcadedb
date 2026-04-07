# ArcadeDB 26.4.1 - High Availability powered by Apache Ratis

## Overview

ArcadeDB 26.4.1 replaces the custom ad-hoc Raft-like replication protocol with **Apache Ratis** - a battle-tested, formally correct implementation of the Raft consensus protocol used in production by Apache Ozone (1000+ node clusters at Tencent), Apache IoTDB, and Alluxio.

This change is **transparent to users** - the HTTP API, database API, query languages, and client libraries remain unchanged. The only configuration difference is that `arcadedb.ha.enabled=true` now uses Ratis internally instead of the old custom protocol.

## What Changed

### Removed (old HA stack - ~6000 lines deleted)
- `HAServer.java` - custom election, quorum management, message routing
- `Leader2ReplicaNetworkExecutor.java` - leader-to-follower binary protocol
- `Replica2LeaderNetworkExecutor.java` - follower-to-leader binary protocol
- `LeaderNetworkListener.java` - TCP socket listener for replication
- `ReplicationLogFile.java` - custom replication log (64MB chunks)
- `ReplicationProtocol.java` - custom binary protocol definition
- 21 message classes (`TxRequest`, `TxForwardRequest`, `CommandForwardRequest`, etc.)
- Custom election protocol (sequential vote collection, no pre-vote)
- Custom quorum mechanism (CountDownLatch-based)

### Added (Ratis-based HA - ~2500 lines)
- `RaftHAServer.java` - Ratis server lifecycle, gRPC transport, peer management
- `ArcadeDBStateMachine.java` - Ratis state machine for WAL replication with wait/notify index tracking
- `RaftLogEntry.java` - binary serialization for Raft log entries
- `RaftGroupCommitter.java` - group commit batching (configurable batch size) to amortize gRPC round-trip cost
- `ClusterMonitor.java` - per-follower replication lag tracking with configurable warning threshold
- `ReplicatedDatabase.java` - rewritten to use Ratis (same class name for API compatibility)
- `HALog.java` - verbose logging utility with cached config level (`arcadedb.ha.logVerbose=0/1/2/3`)
- `SnapshotHttpHandler.java` - HTTP endpoint for database snapshot serving (cluster token + Basic auth)
- Log purge configuration: `arcadedb.ha.logPurgeGap` and `arcadedb.ha.logPurgeUptoSnapshot` for controlling how aggressively old Raft log segments are deleted after snapshots. Required for reliable snapshot-based catch-up of lagging followers
- Studio Cluster dashboard (Overview/Metrics/Management tabs)

## Advantages of Using Apache Ratis

| Feature | Old Custom Protocol | Apache Ratis |
|---|---|---|
| **Leader election** | Sequential vote collection, no pre-vote | Pre-vote protocol, parallel voting, term propagation |
| **Log replication** | Custom TCP binary, sequential per-replica | gRPC bidirectional streaming, parallel per-follower |
| **Membership changes** | Manual server list restart | Dynamic `addPeer`/`removePeer` via AdminApi |
| **Leader lease** | Not implemented | Built-in, configurable timeout ratio |
| **Snapshot transfer** | Custom page-by-page protocol | Notification mode + HTTP ZIP download |
| **Split brain** | No pre-vote, vulnerable to disruption | Pre-vote prevents disrupted elections |
| **Formal correctness** | Ad-hoc implementation | Formally verified Raft protocol |
| **Production track record** | ArcadeDB only | Apache Ozone, IoTDB, Alluxio at scale |
| **Transport** | Custom TCP binary | gRPC (shaded, no classpath conflicts) |
| **Dependencies** | None | ~20MB shaded JARs (gRPC, Protobuf, Netty, Guava) |

## New Features

### HA Management Commands
- `ha add peer <id> <address>` - add a server to the cluster at runtime
- `ha remove peer <id>` - remove a server from the cluster
- `ha transfer leader <peerId>` - transfer leadership to a specific server
- `ha step down` - make the current leader step down (transfers to random follower)
- `ha leave` - gracefully remove this server from the Raft cluster (transfers leadership if leader)
- `ha verify database <name>` - compare file checksums across all nodes

### Studio Cluster Dashboard
- **Overview tab**: cluster health badge, node cards with role/lag, databases table
- **Metrics tab**: election count, raft log size, uptime, last election time; replication lag chart, commit index chart
- **Management tab**: leadership transfer, peer management, database verification, danger zone

### Verbose Logging
```properties
arcadedb.ha.logVerbose=0  # Off (default)
arcadedb.ha.logVerbose=1  # Basic: elections, peer changes
arcadedb.ha.logVerbose=2  # Detailed: commands, WAL replication, schema
arcadedb.ha.logVerbose=3  # Trace: every state machine operation
```

### Cluster API Enrichment
`GET /api/v1/server?mode=cluster` now returns:
- `currentTerm`, `commitIndex`, `lastAppliedIndex`
- Per-peer `matchIndex`, `nextIndex` (replication lag)
- `protocol: "ratis"`
- Peer HTTP addresses for leader discovery

## Architecture Internals

### How Ratis is Used

```
Client (HTTP/Bolt/JDBC)
    |
ArcadeDB Server (HTTP handler)
    |
ReplicatedDatabase (wraps LocalDatabase)
    |
    +-- Reads (isIdempotent && !isDDL): execute locally on any server
    |
    +-- Writes (INSERT/UPDATE/DELETE): commit() -> 3-phase commit
    |       |
    |       +-- Phase 1 (read lock): commit1stPhase() captures WAL pages + delta
    |       +-- Phase 2 (no lock): sendToRaft() -> gRPC -> quorum ack
    |       +-- Phase 3 (read lock): commit2ndPhase() applies pages locally
    |
    +-- DDL/Non-idempotent commands: throw ServerIsNotTheLeaderException
            |
            +-- HTTP proxy forwards to leader transparently
```

### Key Design Decisions
- **Peer IDs**: `host_port` format (underscore for JMX compatibility, displayed as `host:port` in UI)
- **Replicate first, commit after**: The commit is split into 3 phases: (1) `commit1stPhase()` under read lock to capture WAL pages and delta, (2) `replicateTransaction()` with NO lock held to send WAL to Ratis and wait for quorum, (3) `commit2ndPhase()` under read lock to apply pages locally. If replication fails, phase 2 throws and phase 3 never runs - no local writes, no divergence. Matching the `ha-redesign` branch approach.
- **Leader skips state machine apply**: `applyTransaction()` on leader is a no-op - `commit2ndPhase()` handles the local page writes after Ratis confirms quorum
- **Command routing**: `isIdempotent() && !isDDL()` determines local vs forwarded execution
- **Snapshot mode**: Notification mode (`install.snapshot.enabled=false`) - follower downloads ZIP from leader HTTP, authenticated via cluster token
- **WAL-only replication**: Only page diffs replicate, not full records or SQL commands
- **No WAL in snapshots**: Snapshot ZIP contains data files + schema config only
- **Group commit**: Multiple concurrent transactions are batched into fewer Raft round-trips via `RaftGroupCommitter`, dramatically improving throughput under concurrent load
- **Wait/notify for read consistency**: `waitForAppliedIndex()` uses `Object.wait()/notifyAll()` signaled by `applyTransaction()`, eliminating polling latency for READ_YOUR_WRITES consistency
- **Cluster token**: SHA-256 derived from cluster name + root password (auto-computed). Can be overridden via `arcadedb.ha.clusterToken` for hardened deployments
- **Inter-node auth**: Cluster token (`X-ArcadeDB-Cluster-Token` header) used for HTTP proxy forwarding and snapshot downloads, avoiding credential transmission between nodes
- **Async server stop in callbacks**: Test callbacks that stop servers (e.g., `REPLICA_MSG_RECEIVED`) must use `new Thread(() -> server.stop()).start()` rather than calling `stop()` directly. Direct stop from within Ratis `applyTransaction()` corrupts the gRPC channels mid-flight

### Storage Layout
```
<serverRootPath>/ratis-storage/<peerId>/
    <groupId>/
        current/
            log_inprogress_<index>     # Active Raft log segment
            log_<start>-<end>          # Sealed log segments
        sm/                            # State machine snapshots
        metadata                       # Persisted term + vote
```
One per server, shared across all databases. Survives restarts for automatic catch-up.

## Configuration

### Quick Start

```properties
# Enable HA
arcadedb.ha.enabled=true
arcadedb.ha.serverList=host1:2424,host2:2424,host3:2424
arcadedb.ha.clusterName=my-cluster

# Quorum (MAJORITY or ALL)
arcadedb.ha.quorum=majority

# Timeouts
arcadedb.ha.quorumTimeout=10000

# Read consistency for follower reads
# EVENTUAL: read locally (fastest, may be stale)
# READ_YOUR_WRITES: wait for client's last write to be applied (default)
# LINEARIZABLE: wait for all committed writes to be applied
arcadedb.ha.readConsistency=read_your_writes

# Cluster token for inter-node auth (auto-derived from cluster name + root password if empty)
arcadedb.ha.clusterToken=

# Verbose logging for debugging
arcadedb.ha.logVerbose=0
```

### Ratis Tuning

These settings control the underlying Raft consensus behavior. Defaults work well for LAN clusters; adjust for WAN or high-latency environments.

```properties
# Election timeouts (ms) - increase for high-latency WAN clusters
arcadedb.ha.electionTimeoutMin=1500
arcadedb.ha.electionTimeoutMax=3000

# Snapshot: number of Raft log entries before auto-triggering a snapshot
arcadedb.ha.snapshotThreshold=100000

# Raft log segment max size
arcadedb.ha.logSegmentSize=64MB

# Log purging: controls how aggressively old Raft log segments are deleted after snapshots.
# purgeGap = gap between last applied index and purge index (lower = more aggressive)
# purgeUptoSnapshot = when true, purges log entries up to the latest snapshot index
arcadedb.ha.logPurgeGap=1024
arcadedb.ha.logPurgeUptoSnapshot=false

# AppendEntries batch byte limit for follower replication
arcadedb.ha.appendBufferSize=4MB

# Group commit: max transactions batched in a single Raft round-trip
arcadedb.ha.groupCommitBatchSize=500

# Replication lag warning threshold (Raft log index gap). 0 = disabled
arcadedb.ha.replicationLagWarning=1000
```

### GlobalConfiguration Reference

Complete reference of all `HA_*` entries in `GlobalConfiguration.java`. All settings have scope `SERVER` and are set via Java system properties (e.g. `-Darcadedb.ha.enabled=true`).

#### Cluster Setup

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_ENABLED` | `arcadedb.ha.enabled` | Boolean | `false` | Enables HA for this server |
| `HA_CLUSTER_NAME` | `arcadedb.ha.clusterName` | String | `arcadedb` | Cluster name. Useful when running multiple clusters in the same network |
| `HA_SERVER_LIST` | `arcadedb.ha.serverList` | String | (empty) | Comma-separated list of `host:raftPort` or `host:raftPort:httpPort` entries |
| `HA_SERVER_ROLE` | `arcadedb.ha.serverRole` | String | `any` | Server role: `any` (can be leader or follower) or `replica` (follower only). Values: `any`, `replica` |

#### Quorum and Consistency

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_QUORUM` | `arcadedb.ha.quorum` | String | `majority` | Write quorum: `majority` or `all` |
| `HA_QUORUM_TIMEOUT` | `arcadedb.ha.quorumTimeout` | Long | `10000` | Timeout in ms waiting for quorum acknowledgment |
| `HA_READ_CONSISTENCY` | `arcadedb.ha.readConsistency` | String | `read_your_writes` | Follower read consistency: `eventual` (read locally, may be stale), `read_your_writes` (wait for client's last write), `linearizable` (wait for all committed writes) |

#### Election and Timeouts

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_ELECTION_TIMEOUT_MIN` | `arcadedb.ha.electionTimeoutMin` | Integer | `1500` | Minimum election timeout (ms). Increase for WAN clusters |
| `HA_ELECTION_TIMEOUT_MAX` | `arcadedb.ha.electionTimeoutMax` | Integer | `3000` | Maximum election timeout (ms). Increase for WAN clusters |
| `HA_PROXY_READ_TIMEOUT` | `arcadedb.ha.proxyReadTimeout` | Integer | `30000` | Read timeout (ms) when proxying requests from followers to leader. Increase for long-running queries |

#### Raft Log and Snapshots

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_SNAPSHOT_THRESHOLD` | `arcadedb.ha.snapshotThreshold` | Long | `100000` | Number of Raft log entries before auto-triggering a snapshot |
| `HA_LOG_SEGMENT_SIZE` | `arcadedb.ha.logSegmentSize` | String | `64MB` | Maximum Raft log segment size (e.g. `64MB`, `128MB`) |
| `HA_LOG_PURGE_GAP` | `arcadedb.ha.logPurgeGap` | Integer | `1024` | Gap between last applied index and purge index. Lower values free disk faster but leave less room for slow followers to catch up via log replay |
| `HA_LOG_PURGE_UPTO_SNAPSHOT` | `arcadedb.ha.logPurgeUptoSnapshot` | Boolean | `false` | When true, purges Raft log entries up to the latest snapshot index. Combined with a low `logPurgeGap`, forces lagging followers to catch up via snapshot download instead of log replay |
| `HA_APPEND_BUFFER_SIZE` | `arcadedb.ha.appendBufferSize` | String | `4MB` | AppendEntries batch byte limit per gRPC call to followers |

#### Performance Tuning

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_GROUP_COMMIT_BATCH_SIZE` | `arcadedb.ha.groupCommitBatchSize` | Integer | `500` | Maximum transactions batched in a single Raft round-trip. Higher values improve throughput under concurrent load |
| `HA_REPLICATION_QUEUE_SIZE` | `arcadedb.ha.replicationQueueSize` | Integer | `512` | Queue size for replicating messages between servers |
| `HA_REPLICATION_FILE_MAXSIZE` | `arcadedb.ha.replicationFileMaxSize` | Long | `1073741824` | Maximum file size (bytes) for replication messages. Default 1GB |
| `HA_REPLICATION_CHUNK_MAXSIZE` | `arcadedb.ha.replicationChunkMaxSize` | Integer | `16777216` | Maximum channel chunk size (bytes) for replication. Default 16MB |

#### Security and Auth

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_CLUSTER_TOKEN` | `arcadedb.ha.clusterToken` | String | (empty) | Shared secret for inter-node HTTP forwarding and snapshot auth. If empty, auto-derived from SHA-256 of cluster name + root password |

#### Networking

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_REPLICATION_INCOMING_HOST` | `arcadedb.ha.replicationIncomingHost` | String | `0.0.0.0` | TCP/IP host for incoming replication connections |
| `HA_REPLICATION_INCOMING_PORTS` | `arcadedb.ha.replicationIncomingPorts` | String | `2424-2433` | TCP/IP port range for incoming replication connections |

#### Monitoring and Debugging

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_LOG_VERBOSE` | `arcadedb.ha.logVerbose` | Integer | `0` | Verbose logging: 0=off, 1=basic (elections, peers), 2=detailed (commands, WAL), 3=trace (all state machine ops) |
| `HA_REPLICATION_LAG_WARNING` | `arcadedb.ha.replicationLagWarning` | Integer | `1000` | Raft log index gap threshold for replication lag warnings. 0 = disabled |
| `HA_ERROR_RETRIES` | `arcadedb.ha.errorRetries` | Integer | `0` | Automatic retries on IO errors. 0 = retry against all configured servers |

#### Kubernetes

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_K8S` | `arcadedb.ha.k8s` | Boolean | `false` | Enable Kubernetes mode (auto-join, preStop hook) |
| `HA_K8S_DNS_SUFFIX` | `arcadedb.ha.k8sSuffix` | String | (empty) | DNS suffix for peer discovery (e.g. `arcadedb.default.svc.cluster.local`) |

## Kubernetes Support

### How It Works

ArcadeDB's Kubernetes deployment uses a **StatefulSet + Headless Service** pattern, which is the standard approach for Raft-based systems (used by etcd, Apache Ozone, CockroachDB).

**StatefulSet** provides predictable pod names: `arcadedb-0`, `arcadedb-1`, `arcadedb-2`
**Headless Service** provides predictable DNS: `arcadedb-0.arcadedb.default.svc.cluster.local`

The Helm chart pre-computes the full server list from `replicaCount` and injects it via environment variables. No runtime discovery is needed.

### Configuration

```properties
# Enable K8s mode
arcadedb.ha.k8s=true

# DNS suffix for peer discovery (derived from Helm chart)
arcadedb.ha.k8sSuffix=.arcadedb.default.svc.cluster.local

# Server list (auto-generated by Helm template)
arcadedb.ha.serverList=arcadedb-0.arcadedb.default.svc.cluster.local:2424,arcadedb-1.arcadedb.default.svc.cluster.local:2424,arcadedb-2.arcadedb.default.svc.cluster.local:2424
```

### Helm Chart Integration

The `_helpers.tpl` template generates the server list automatically:

```yaml
{{- define "arcadedb.nodenames" -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $fullname := (include "arcadedb.fullname" .) -}}
{{- $k8sSuffix := (include "arcadedb.k8sSuffix" .) -}}
{{- $rpcPort := int (default "2424" .Values.service.rpc.port) -}}
{{- range $i, $_ := until $replicas }}
{{- printf "%s-%d%s:%d" $fullname $i $k8sSuffix $rpcPort }}
{{- end }}
{{- end }}
```

### Auto-Join on Scale-Up

When `arcadedb.ha.k8s=true` and a new pod starts without existing Ratis storage, the server automatically attempts to join the existing cluster:

1. After Ratis server starts, `tryAutoJoinCluster()` contacts existing peers
2. Queries `GroupManagementApi.info()` to check if this server is already a member
3. If not, calls `admin().setConfiguration()` to add itself to the Raft group
4. If no existing cluster responds (fresh deployment), bootstraps a new cluster

This enables **zero-downtime scale-up**: `kubectl scale statefulset arcadedb --replicas=5` adds 2 new pods that automatically join the existing 3-node cluster.

### What Stays the Same from Old HA

| Setting | Purpose | Status |
|---|---|---|
| `arcadedb.ha.k8s` | Enable K8s mode | Unchanged |
| `arcadedb.ha.k8sSuffix` | DNS suffix for peer names | Unchanged |
| `HOSTNAME` env var | Pod identity (set by K8s) | Unchanged |
| Helm `_helpers.tpl` | Server list generation | Unchanged |
| Headless Service | DNS-based peer discovery | Unchanged |
| StatefulSet | Predictable pod names | Unchanged |

### What's New with Ratis

| Feature | Old HA | Ratis HA |
|---|---|---|
| Scale-up | Restart all pods with new server list | Auto-join via `tryAutoJoinCluster()` |
| Scale-down | Manual disconnect + restart | Auto-leave via preStop hook + `leaveCluster()` |
| Leader failover | Custom election, 3-5s | Ratis pre-vote + election, 1.5-3s |
| Rolling upgrade | Stop/start one by one, hope for the best | Ratis RECOVER mode auto-catches up |
| Storage persistence | Custom replication log | Ratis log segments + metadata (term, vote) |

## Tests

### Passing Tests (42 total: 19 existing + 17 comprehensive + 6 e2e)

#### Core Tests
| Test | Description | Servers | Status |
|---|---|---|---|
| `RaftLogEntryTest` (4) | Binary serialization round-trip | N/A | PASS |
| `RaftHAServerIT` (3) | Raw Ratis consensus: election, replication | 3 | PASS |

#### HTTP API Tests
| Test | Description | Servers | Status |
|---|---|---|---|
| `HTTP2ServersIT.serverInfo` | Cluster status API | 2 | PASS |
| `HTTP2ServersIT.propagationOfSchema` | Schema DDL replication | 2 | PASS |
| `HTTP2ServersIT.checkQuery` | Query execution across servers | 2 | PASS |
| `HTTP2ServersIT.checkDeleteGraphElements` | Cross-server CRUD with edges | 2 | PASS |
| `HTTP2ServersIT.verifyDatabase` | `ha verify database` command | 2 | PASS |
| `HTTP2ServersIT.hAConfiguration` | RemoteDatabase cluster config | 2 | PASS |

#### Failover Tests
| Test | Description | Servers | Status |
|---|---|---|---|
| `ReplicationServerLeaderDownIT` | Leader stop, new election, writes continue | 3 | PASS |
| `ReplicationServerLeaderChanges3TimesIT` | 3 leader kill/restart cycles | 3 | PASS |
| `HASplitBrainIT` | 5-node cluster, stop 2 minority, verify majority works, restart | 5 | PASS |

#### Configuration & Operations Tests
| Test | Description | Servers | Status |
|---|---|---|---|
| `HAConfigurationIT` | Invalid server list rejection | 3 | PASS |
| `ServerDatabaseBackupIT` (2) | SQL backup on HA cluster | 3 | PASS |

#### Comprehensive Tests (new)
| Test | Description | Servers | Status |
|---|---|---|---|
| `test01_dataConsistencyUnderLoad` | 1000 records, verify count & content on all nodes | 3 | PASS |
| `test02_followerRestartAndCatchUp` | Stop follower, write 100 records, restart, verify catch-up | 3 | PASS |
| `test03_fullClusterRestart` | Write data, stop all 3, restart all 3, verify data survives | 3 | PASS |
| `test04_concurrentWritesOnLeader` | 4 threads x 100 records, verify consistency | 3 | PASS |
| `test05_schemaChangesDuringWrites` | CREATE TYPE while data exists, verify propagation | 3 | PASS |
| `test06_indexConsistency` | Unique index enforcement across cluster | 3 | PASS |
| `test07_queryRoutingCorrectness` | SELECT local, INSERT rejected on follower | 3 | PASS |
| `test08_largeTransaction` | Single tx with 500 records, verify replication | 3 | PASS |
| `test09_rapidLeaderTransfers` | 5 rapid leadership transfers, verify stability | 3 | PASS |
| `test10_singleServerHAMode` | HA with 1 node, verify reads work, writes fail quorum | 3->1 | PASS |
| `test11_writeToFollowerViaHttpProxy` | 100 writes via HTTP to follower, proxied to leader | 3 | PASS |
| `test12_leaderElectionDuringTransaction` | Uncommitted tx on leader, kill leader, verify rollback (ACID) | 3 | PASS |
| `test13_concurrentWritesViaProxy` | 3 servers x 30 writes via HTTP simultaneously | 3 | PASS |
| `test14_writesDuringSlowFollower` | Stop 1 follower, writes continue (majority), restart, catch-up | 3 | PASS |
| `test15_veryLargeTransaction` | 2000 records x 500 bytes in single tx (~1MB+ WAL) | 3 | PASS |
| `test16_mixedReadWriteWorkload` | Concurrent reads on follower + writes on leader | 3 | PASS |
| `test17_rollingUpgradeSimulation` | Stop/restart each server one by one, verify data survives | 3 | PASS |

#### E2E Tests (Docker/TestContainers)
| Test | Description | Servers | Status |
|---|---|---|---|
| `HAReplicationE2ETest` (3) | Basic replication, leader failover, follower proxy | 3 | PASS (22s) |
| `HANetworkPartitionE2ETest` | Follower network disconnect/reconnect, catch-up via Raft log replay | 3 | PASS (8s) |
| `HAQuorumLossRecoveryE2ETest` | Network-isolate 2 of 3 nodes, writes fail, reconnect both, cluster recovers | 3 | PASS (20s) |
| `HALeaderPartitionE2ETest` | Leader network-partitioned, majority elects new leader, old leader reconnects and catches up | 3 | PASS (27s) |
| `HARollingRestartE2ETest` | Rolling network-isolation with writes on survivors, each node catches up | 3 | WIP |
| `HAColdStartE2ETest` | All 3 nodes restarted via docker restart, Ratis log recovery + data intact + index survives | 3 | DONE |
| `HASnapshotCatchUpE2ETest` | Follower lags behind log purge boundary, catches up via snapshot download | 3 | DONE |

### Known Limitations
- **State machine command forwarding**: The `query()` path for forwarding write commands to the leader has a page visibility issue. Currently using HTTP proxy fallback which works correctly.

### Removed Tests (not applicable to Ratis)
| Test | Reason |
|---|---|
| `ReplicationServerQuorumNoneIT` | Ratis doesn't support "none" quorum - only MAJORITY and ALL |

## TODO

### Resolved Issues
- **Snapshot persistence for cold restart**: `takeSnapshot()` was not persisting a snapshot marker file to `SimpleStateMachineStorage`. After a cold restart, `reinitialize()` found no snapshot, set `lastAppliedIndex=-1`, and Ratis replayed ALL committed log entries, double-applying WAL page diffs and corrupting the database. Fixed by writing a marker file (`snapshot.<term>_<index>`) and updating `reinitialize()` to restore both `lastAppliedIndex` and `BaseStateMachine`'s internal `TermIndex`. This also fixes snapshot-based catch-up: Ratis now knows a snapshot exists and can trigger `notifyInstallSnapshotFromLeader()` for lagging followers.
- **Database loading safety**: Added filter in `loadDatabases()` to skip `.snapshot-tmp` and `.snapshot-old` directories that could be leftover from a crash during snapshot installation.
- **Vector index replication**: Fixed 1-byte parsing misalignment in `LSMVectorIndex.applyReplicatedPageUpdate()` - the `quantization_type` byte (always written after `deleted` flag) was not being read, causing cumulative offset drift when parsing entries on followers.

### TODO: E2E Tests
These tests exercise full cluster scenarios using Docker containers (TestContainers). Each test is in `e2e/src/test/java/com/arcadedb/e2e/` and tagged `@Tag("e2e-ha")`.

| # | Test | Description | Key scenario | Status |
|---|---|---|---|---|
| 1 | `HALeaderPartitionE2ETest` | Leader gets network-partitioned. Majority elects new leader, accepts writes. Old leader reconnects, steps down, catches up | Leader stepdown + follower resync | DONE |
| 2 | `HAMultiDatabaseSnapshotE2ETest` | Cluster with 2-3 databases. Follower lags behind, snapshot installs all databases. Verify no partial failures | `notifyInstallSnapshotFromLeader` loops over all DBs | TODO |
| 3 | `HASnapshotDuringWritesE2ETest` | Follower reconnects while writes are actively happening on the leader. Verify snapshot install + concurrent Raft log apply don't conflict | Snapshot + concurrent writes | TODO |
| 4 | `HAColdStartE2ETest` | Write data, restart all 3 nodes, verify Ratis log recovery from disk + leader re-election + data intact | Full cluster restart from persisted state | DONE |
| 5 | `HAQuorumLossRecoveryE2ETest` | Network-isolate 2 of 3 nodes (quorum lost). Writes must fail. Reconnect both nodes. Cluster recovers, writes succeed again | Disaster recovery | DONE |
| 6 | `HADynamicDatabaseE2ETest` | Create a database on the leader after cluster formation. Verify schema + data replicate to followers. Then lag a follower, verify snapshot includes the new database | Post-formation DB creation + snapshot | TODO |
| 7 | `HALargeDataSnapshotE2ETest` | Insert large records (BLOBs, many properties) to exercise the ZIP streaming path with realistic data sizes | Snapshot HTTP streaming under load | TODO |

### WIP: Remaining E2E test issues

**`HARollingRestartE2ETest`**: Times out at 5 min. Three sequential disconnect/write/reconnect cycles accumulate latency (election ~5s + gRPC reconnection ~10s + catch-up per cycle). Needs investigation into whether iterations compound or if there's a specific failure point.

### Resolved issues during E2E testing

- **Docker network alias loss**: Docker does NOT preserve network aliases after `disconnect`/`connect`. Fixed by passing the alias explicitly via `ContainerNetwork.withAliases()` in `reconnectToNetwork()`. This was the root cause of the "old leader doesn't catch up" failures.
- **Peer ID collision**: `resolveLocalPeerId()` matched on `HA_REPLICATION_INCOMING_HOST` (`0.0.0.0`) + port, causing all nodes to get the same peer ID. Fixed by matching on server name, hostname, or unambiguous port.
- **gRPC reconnection tuning**: Added `ExponentialBackoffRetry` on RaftClient, `slownessTimeout=300s`, `closeThreshold=300s`, `flowControlWindow=4MB` for robust partition recovery.
- **HTTP API params**: E2E tests use direct HTTP with `INSERT ... CONTENT {}` syntax (not `RemoteDatabase`) to avoid cluster address discovery issues in Docker.

### Future Features
- **State machine command forwarding**: Fix the `query()` path page visibility issue to eliminate HTTP proxy dependency for command forwarding. Currently write commands on non-leader nodes are forwarded via HTTP proxy which works correctly but adds latency.
- **Multi-Raft groups**: One Raft group per database (currently all databases share one group). This would allow independent replication policies per database.
- **JWT-based auth for cluster**: Replace Basic auth forwarding in HTTP proxy with stateless JWT tokens that work across servers without session affinity.
- **Alert configuration in Studio**: Configurable thresholds for replication lag, election frequency, quorum health with notifications.
