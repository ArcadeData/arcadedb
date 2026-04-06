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
    +-- Writes (INSERT/UPDATE/DELETE): commit() -> replicateFromLeader()
    |       |
    |       +-- commit2ndPhase() (local page write)
    |       +-- sendToRaft() -> gRPC -> all followers apply via applyTransaction()
    |
    +-- DDL/Non-idempotent commands: throw ServerIsNotTheLeaderException
            |
            +-- HTTP proxy forwards to leader transparently
```

### Key Design Decisions
- **Peer IDs**: `host_port` format (underscore for JMX compatibility, displayed as `host:port` in UI)
- **Leader commits first**: `commit2ndPhase()` locally, then replicates WAL via Ratis. On replication failure, the exception propagates to the client (retry). Followers eventually catch up via Ratis log replay or snapshot installation.
- **Follower skips apply**: `applyTransaction()` on leader is a no-op (already committed)
- **Command routing**: `isIdempotent() && !isDDL()` determines local vs forwarded execution
- **Snapshot mode**: Notification mode (`install.snapshot.enabled=false`) - follower downloads ZIP from leader HTTP, authenticated via cluster token
- **WAL-only replication**: Only page diffs replicate, not full records or SQL commands
- **No WAL in snapshots**: Snapshot ZIP contains data files + schema config only
- **Group commit**: Multiple concurrent transactions are batched into fewer Raft round-trips via `RaftGroupCommitter`, dramatically improving throughput under concurrent load
- **Wait/notify for read consistency**: `waitForAppliedIndex()` uses `Object.wait()/notifyAll()` signaled by `applyTransaction()`, eliminating polling latency for READ_YOUR_WRITES consistency
- **Cluster token**: SHA-256 derived from cluster name + root password (auto-computed). Can be overridden via `arcadedb.ha.clusterToken` for hardened deployments
- **Inter-node auth**: Cluster token (`X-ArcadeDB-Cluster-Token` header) used for HTTP proxy forwarding and snapshot downloads, avoiding credential transmission between nodes

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

# AppendEntries batch byte limit for follower replication
arcadedb.ha.appendBufferSize=4MB

# Group commit: max transactions batched in a single Raft round-trip
arcadedb.ha.groupCommitBatchSize=500

# Replication lag warning threshold (Raft log index gap). 0 = disabled
arcadedb.ha.replicationLagWarning=1000
```

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

### Passing Tests (36 total: 19 existing + 17 comprehensive)

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

### Known Limitations
- **State machine command forwarding**: The `query()` path for forwarding write commands to the leader has a page visibility issue. Currently using HTTP proxy fallback which works correctly.

## TODO

### Future Tests
All planned tests have been implemented and are passing. See the comprehensive test suite below.

### Future Features
- **State machine command forwarding**: Fix the `query()` path page visibility issue to eliminate HTTP proxy dependency for command forwarding. Currently write commands on non-leader nodes are forwarded via HTTP proxy which works correctly but adds latency.
- **Multi-Raft groups**: One Raft group per database (currently all databases share one group). This would allow independent replication policies per database.
- **JWT-based auth for cluster**: Replace Basic auth forwarding in HTTP proxy with stateless JWT tokens that work across servers without session affinity.
- **Alert configuration in Studio**: Configurable thresholds for replication lag, election frequency, quorum health with notifications.
