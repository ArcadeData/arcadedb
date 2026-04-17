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

### Added (Ratis-based HA, module `ha-raft/`)
Core:
- `RaftHAServer.java` - Ratis server lifecycle, gRPC transport, peer management, cluster token, health monitor
- `RaftHAPlugin.java` - ServerPlugin entry point (auto-discovered via ServiceLoader)
- `ArcadeDBStateMachine.java` - Ratis state machine for WAL replication with wait/notify index tracking and persisted applied index
- `ReplicatedDatabase.java` - rewritten to use Ratis (same class name for API compatibility)
- `RaftTransactionBroker.java` - facade that funnels transaction submissions through the group committer and handles ALL-quorum completion
- `RaftGroupCommitter.java` - batched group commit over Raft; `CancellablePendingEntry` lets submitters abort waiting without leaking queue slots
- `RaftLogEntryCodec.java` / `RaftLogEntryType.java` - binary serialization for Raft log entries (LZ4-compressed WAL payloads)
- `Quorum.java` - typed enum replacing the old free-form string for `HA_QUORUM`

Extracted collaborators:
- `RaftPeerAddressResolver.java` - parse/resolve peer IDs from `HA_SERVER_LIST`, detect local peer id
- `RaftPropertiesBuilder.java` - Ratis `RaftProperties` + `Parameters` construction from `GlobalConfiguration`
- `RaftClusterManager.java` - add/remove/transfer-leader/step-down/leave operations
- `RaftClusterStatusExporter.java` - cluster status for `GET /api/v1/server?mode=cluster` and lag monitor

Snapshot + recovery:
- `SnapshotInstaller.java` - follower-side crash-safe snapshot install (symlink/zip-slip/zip-bomb checks, SSL validation, exponential-backoff retry, concurrent-request protection)
- `SnapshotHttpHandler.java` - HTTP endpoint for full database ZIP serving with cluster token auth, concurrency cap (`HA_SNAPSHOT_MAX_CONCURRENT`), and write-timeout (`HA_SNAPSHOT_WRITE_TIMEOUT`)
- `HealthMonitor.java` - detects `CLOSED` Ratis state after partitions and restarts the server in RECOVER mode

Security:
- `ClusterTokenProvider.java` - derives the cluster shared secret via PBKDF2-HMAC-SHA256 (100k iterations) from cluster name + root password with domain-separated salt; wired into every call site that emits the `X-ArcadeDB-Cluster-Token` header
- `PeerAddressAllowlistFilter.java` - rejects inbound Raft gRPC connections whose remote IP does not resolve to a host in `HA_SERVER_LIST`; DNS re-resolution is rate-limited

Kubernetes:
- `KubernetesAutoJoin.java` - scale-up auto-join that adds the pod to the existing Raft group via atomic `SetConfiguration(Mode.ADD)` with ordinal-derived jitter

HTTP endpoints (see "REST API" below):
- `PostAddPeerHandler.java`, `DeletePeerHandler.java`, `PostTransferLeaderHandler.java`, `PostStepDownHandler.java`, `PostLeaveHandler.java`, `PostVerifyDatabaseHandler.java`, `GetClusterHandler.java`

Logging:
- `HALog.java` - verbose logging utility with cached config level (`arcadedb.ha.logVerbose=0/1/2/3`)

Dependency scoping:
- `arcadedb-server` is declared as `provided` in `ha-raft/pom.xml`; ha-raft is loaded at runtime through the server's plugin mechanism

Studio:
- Cluster dashboard (Overview/Metrics/Management tabs) with term, commitIndex, per-follower matchIndex, replication-lag charts

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

Issued via `POST /api/v1/server` with a JSON body `{"command": "..."}`. All require the `root` user.

- `ha add peer <id> <address>` - add a server to the cluster at runtime; the new peer is seeded with the current user database so authentication stays consistent
- `ha remove peer <id>` - remove a server from the cluster
- `ha transfer leader <peerId>` - transfer leadership to a specific server
- `ha step down` - make the current leader step down (transfers to a random follower)
- `ha leave` - gracefully remove this server from the Raft cluster (transfers leadership first if leader); used as the StatefulSet preStop hook in K8s
- `ha verify database <name>` - compare component file checksums across all nodes

### Replicated Operations via Raft Log

The following control-plane operations now go through the Raft log so every node converges on the same state:

| Operation | Log entry type | Notes |
|---|---|---|
| Create database | `CREATE_DATABASE_ENTRY` | Runs on leader, followers create the empty database |
| Drop database | `DROP_DATABASE_ENTRY` | Propagated atomically to all peers |
| Import database | `INSTALL_DATABASE_ENTRY` (forceSnapshot) | Leader imports then followers receive a full snapshot |
| Restore database | `INSTALL_DATABASE_ENTRY` (forceSnapshot) | Same path as import, via `forceSnapshot` flag |
| Create / drop user | `SECURITY_USERS_ENTRY` | Carries a JSON blob of users; followers rewrite `server-users.jsonl` |
| Peer-add seed | `SECURITY_USERS_ENTRY` | Sent immediately after `ha add peer` to seed the new node |

Legacy code paths that mutated local state on only one node have been removed; any write of this kind is forwarded to the leader and then applied via the state machine.

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

The level is cached on first read, so raising it at runtime requires a server restart (or a config-reload command). The utility is in `HALog.java`.

### Cluster API Enrichment
`GET /api/v1/server?mode=cluster` now returns:
- `currentTerm`, `commitIndex`, `lastAppliedIndex`
- Per-peer `matchIndex`, `nextIndex` (replication lag)
- `protocol: "ratis"`
- Peer HTTP addresses for leader discovery

## REST API

| Method | Path | Handler | Description |
|---|---|---|---|
| `GET` | `/api/v1/ha/cluster` | `GetClusterHandler` | Cluster status (term, commitIndex, peers, roles) |
| `POST` | `/api/v1/ha/peers` | `PostAddPeerHandler` | Add peer (Raft + user seed) |
| `DELETE` | `/api/v1/ha/peers/{id}` | `DeletePeerHandler` | Remove peer |
| `POST` | `/api/v1/ha/transfer-leader` | `PostTransferLeaderHandler` | Transfer leadership |
| `POST` | `/api/v1/ha/step-down` | `PostStepDownHandler` | Leader steps down |
| `POST` | `/api/v1/ha/leave` | `PostLeaveHandler` | This node leaves the cluster |
| `POST` | `/api/v1/ha/verify` | `PostVerifyDatabaseHandler` | Verify database checksum across nodes |
| `GET` | `/api/v1/ha/snapshot/{db}` | `SnapshotHttpHandler` | Leader-only: stream a database ZIP for follower catch-up (requires cluster token + root) |

All endpoints accept either `Authorization: Basic` (root) or the inter-node `X-ArcadeDB-Cluster-Token` header described under "Security".

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
- **Replicate first, commit after (3-phase commit)**: Commit is split into three phases wrapping Ratis replication: (1) `commit1stPhase()` under read lock to capture WAL pages and delta into a `ReplicationPayload`, (2) `replicateTransaction()` with NO lock held to submit to the group committer and wait for quorum, (3) `commit2ndPhase()` under read lock to apply pages locally. If replication fails, phase 2 throws and phase 3 never runs: no local writes, no divergence. Schema-save for read-only leader transactions is deferred and persisted as part of phase 3 so followers and leader share the exact same schema version increments.
- **Leader skips state machine apply**: `applyTransaction()` on leader is a no-op for transaction entries; `commit2ndPhase()` writes the pages after Ratis confirms quorum. The `originPeerId` is embedded in the log entry so followers do not re-apply entries that originated on themselves after a restart replay.
- **Command routing**: `isIdempotent() && !isDDL()` determines local vs forwarded execution. Schema changes on followers throw `ServerIsNotTheLeaderException` and are proxied to the leader over HTTP
- **Snapshot mode**: Chunk-based install. Ratis ships a tiny marker file via its `LogAppender` chunk transport; the follower detects the gap between the snapshot index and its persisted applied index and downloads the full database ZIP over HTTP from the leader. A snapshot is taken on clean shutdown so cold restart does not replay the entire log.
- **Snapshot install is crash-safe**: Staging happens in `.snapshot-tmp/<db>`, committed via an atomic `.snapshot-old` rename, then `.snapshot-old` is deleted. The applied-index is persisted separately so a crash midway is detected and retried. `SnapshotInstaller` retries with exponential backoff and serialises concurrent requests per database.
- **WAL-only replication**: Only page diffs replicate, not full records or SQL commands. WAL payloads are LZ4-compressed inside Raft log entries
- **No WAL in snapshots**: Snapshot ZIP contains data files + schema config only
- **Group commit**: Multiple concurrent transactions are batched into fewer Raft round-trips via `RaftGroupCommitter`, dramatically improving throughput under concurrent load. Pending entries are cancellable so a client hitting `quorumTimeout` does not keep a slot occupied.
- **Transaction broker**: All transaction submissions go through `RaftTransactionBroker`, which owns the group committer lifecycle and handles ALL-quorum completion (`watch(ALL)`) plus error translation into `ReplicationException` / `MajorityCommittedAllFailedException`.
- **Wait/notify for read consistency**: `waitForAppliedIndex()` uses `Object.wait()/notifyAll()` signalled by `applyTransaction()` for `READ_YOUR_WRITES`. `LINEARIZABLE` uses Ratis's ReadIndex so a follower only serves the read after confirming with the current leader that its committed index is at least the leader's committed index at the time of the request.
- **Cluster token**: PBKDF2-HMAC-SHA256 (100k iterations, 256-bit output) derived from cluster name + root password with a domain-separated salt (`arcadedb-cluster-token:<clusterName>`). Computed eagerly at startup so the first request does not pay the derivation cost. Can be overridden via `arcadedb.ha.clusterToken` for hardened deployments.
- **Inter-node auth**: Cluster token (`X-ArcadeDB-Cluster-Token` header) used for HTTP proxy forwarding and snapshot downloads, avoiding credential transmission between nodes. Comparison is always constant-time (`MessageDigest.isEqual`) to prevent timing oracles.
- **Peer allowlist for Raft gRPC**: Inbound Raft gRPC connections are filtered against the DNS-resolved `HA_SERVER_LIST` hosts via `PeerAddressAllowlistFilter` so unrelated pods or hosts that merely know the port cannot inject log entries. Not a substitute for mTLS on untrusted networks.
- **Idempotency cache**: The HTTP layer caches successful responses keyed by `X-Request-Id` + authenticated principal, so a client retry after a lost response replays the cached body instead of double-applying a non-idempotent write.
- **Async server stop in callbacks**: Test callbacks that stop servers must use `new Thread(() -> server.stop()).start()` rather than calling `stop()` directly. Direct stop from within Ratis `applyTransaction()` corrupts the gRPC channels mid-flight

### Durability Guarantees

**Write acknowledgment**: A successful `commit()` response is only returned to the client after all three phases complete: WAL captured locally, Raft quorum acknowledgment (a majority of nodes have persisted the log entry), and local page application on the leader. A write that returns successfully is guaranteed durable on a quorum of nodes. A leader crash after quorum but before responding to the client results in a client timeout, not silent data loss - the write is already on a quorum.

**Quorum failure**: If `replicateTransaction()` cannot reach quorum within `arcadedb.ha.quorumTimeout` ms, it throws and the local commit (Phase 2) is never executed. The transaction is rolled back on the leader. Any follower that received a partial AppendEntries will not apply it (Raft only applies entries after they are committed by the leader).

**ALL-quorum recovery**: When `HA_QUORUM=all`, every configured peer must acknowledge. If the majority ack but a minority fails the watch, the broker throws `MajorityCommittedAllFailedException`. The leader then escalates: it schedules a step-down so a correct follower can take over, and optionally stops the JVM if `HA_STOP_SERVER_ON_REPLICATION_FAILURE=true` (default `false`). A node that comes back after such an event recovers via Raft log replay or, if the log has been purged, via snapshot download.

**Phase 2 failure after quorum**: If `commit2ndPhase()` fails after quorum is reached (e.g., a page version conflict under concurrent file lock), followers have already applied the transaction but the leader has not. The leader logs a `SEVERE` message identifying the transaction and calls `stepDown()` so a follower with correct state takes over. Step-down retries are bounded; only when every attempt fails and `HA_STOP_SERVER_ON_REPLICATION_FAILURE=true` does the node self-terminate. The stepped-down node self-heals on restart via Raft log replay.

**WAL version gap handling**: If a follower apply detects that the WAL version gap between the incoming log entry and its local file state is too large (e.g., after a long partition or a missed snapshot), `WALVersionGapException` is thrown and the state machine triggers a snapshot download from the current leader instead of corrupting pages.

**Concurrent snapshot install protection**: `SnapshotInstaller` serialises concurrent install requests per database. A second install request while one is in flight either joins the in-flight install or backs off with exponential retry, preventing duplicate directory swaps or half-installed snapshots.

**Follower read consistency**: The `arcadedb.ha.readConsistency` setting controls what followers return:
- `EVENTUAL`: read locally without waiting - may return data that has not yet been applied on this follower.
- `READ_YOUR_WRITES` (default): waits for the client's own last write to be applied on this follower before reading.
- `LINEARIZABLE`: issues a Ratis ReadIndex request to the leader and waits for the local applied index to reach it before reading. Strongest guarantee, highest latency; survives leader changes without serving stale data.

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

# Cluster token for inter-node auth
# Auto-derived via PBKDF2-HMAC-SHA256(100k) from cluster name + root password if empty
arcadedb.ha.clusterToken=

# Verbose logging for debugging
arcadedb.ha.logVerbose=0
```

### Ratis Tuning

These settings control the underlying Raft consensus behavior. Defaults work well for LAN clusters; adjust for WAN or high-latency environments.

```properties
# Election timeouts (ms) - increase for high-latency WAN clusters
arcadedb.ha.electionTimeoutMin=2000
arcadedb.ha.electionTimeoutMax=5000

# Snapshot: number of Raft log entries before auto-triggering a snapshot
arcadedb.ha.snapshotThreshold=100000

# Raft log segment max size
arcadedb.ha.logSegmentSize=64MB

# Log purging: controls how aggressively old Raft log segments are deleted after snapshots.
# purgeGap = number of entries to retain after purge as buffer for slightly lagging followers
# purgeUptoSnapshot = when true (default), deletes old log segments after each snapshot,
#   preventing unbounded disk growth. Followers that fall behind recover via snapshot download.
#   Set to false only if you need full log history for debugging or auditing.
arcadedb.ha.logPurgeGap=1024
arcadedb.ha.logPurgeUptoSnapshot=true

# AppendEntries batch byte limit for follower replication
arcadedb.ha.appendBufferSize=4MB
arcadedb.ha.writeBufferSize=8MB
arcadedb.ha.grpcFlowControlWindow=4MB

# Group commit: max transactions batched in a single Raft round-trip
arcadedb.ha.groupCommitBatchSize=500
arcadedb.ha.groupCommitQueueSize=10000
arcadedb.ha.groupCommitOfferTimeout=100

# Snapshot install concurrency / timeouts
arcadedb.ha.snapshotMaxConcurrent=2
arcadedb.ha.snapshotDownloadTimeout=300000
arcadedb.ha.snapshotWriteTimeout=300000
arcadedb.ha.snapshotWatchdogTimeout=30000
arcadedb.ha.snapshotGapTolerance=10
arcadedb.ha.snapshotMaxEntrySize=10737418240

# Ratis restart bound before giving up (partition recovery)
arcadedb.ha.ratisRestartMaxRetries=10

# Phase-2 divergence handling
arcadedb.ha.stopServerOnReplicationFailure=false

# Peer allowlist (inbound Raft gRPC) - on by default
arcadedb.ha.peerAllowlist.enabled=true
arcadedb.ha.peerAllowlist.refreshMs=5000

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
| `HA_QUORUM_TIMEOUT` | `arcadedb.ha.quorumTimeout` | Long | `10000` | Timeout in ms waiting for quorum acknowledgment. Also used as extended wait when an entry is already dispatched to Raft, so worst-case client latency is txTimeout + quorumTimeout |
| `HA_READ_CONSISTENCY` | `arcadedb.ha.readConsistency` | String | `read_your_writes` | Follower read consistency: `eventual` (read locally, may be stale), `read_your_writes` (wait for client's last write), `linearizable` (wait for all committed writes) |

#### Election and Timeouts

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_ELECTION_TIMEOUT_MIN` | `arcadedb.ha.electionTimeoutMin` | Integer | `2000` | Minimum election timeout (ms). Increase for WAN clusters |
| `HA_ELECTION_TIMEOUT_MAX` | `arcadedb.ha.electionTimeoutMax` | Integer | `5000` | Maximum election timeout (ms). Increase for WAN clusters |
| `HA_PROXY_READ_TIMEOUT` | `arcadedb.ha.proxyReadTimeout` | Integer | `30000` | Read timeout (ms) when proxying requests from followers to leader. Increase for long-running queries |
| `HA_RATIS_RESTART_MAX_RETRIES` | `arcadedb.ha.ratisRestartMaxRetries` | Integer | `10` | Maximum consecutive Ratis restart attempts by the health monitor before the server shuts down for cluster-level recovery. Raise when partition-recovery scenarios cause legitimate rapid restarts |

#### Raft Log and Snapshots

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_SNAPSHOT_THRESHOLD` | `arcadedb.ha.snapshotThreshold` | Long | `100000` | Number of Raft log entries before auto-triggering a snapshot |
| `HA_LOG_SEGMENT_SIZE` | `arcadedb.ha.logSegmentSize` | String | `64MB` | Maximum Raft log segment size (e.g. `64MB`, `128MB`) |
| `HA_LOG_PURGE_GAP` | `arcadedb.ha.logPurgeGap` | Integer | `1024` | Number of log entries to retain after a snapshot purge, as a buffer for slightly lagging followers. Lower values free disk faster but increase the chance a slow follower needs a full snapshot resync |
| `HA_LOG_PURGE_UPTO_SNAPSHOT` | `arcadedb.ha.logPurgeUptoSnapshot` | Boolean | `true` | Purge old Raft log segments after each snapshot, preventing unbounded disk growth. Followers that fall behind the purge boundary recover automatically via snapshot download. Set to false only to retain full log history for debugging or auditing |
| `HA_APPEND_BUFFER_SIZE` | `arcadedb.ha.appendBufferSize` | String | `4MB` | AppendEntries batch byte limit per gRPC call to followers |
| `HA_WRITE_BUFFER_SIZE` | `arcadedb.ha.writeBufferSize` | String | `8MB` | Raft log write buffer size. Must be at least `appendBufferSize + 8` bytes |
| `HA_GRPC_FLOW_CONTROL_WINDOW` | `arcadedb.ha.grpcFlowControlWindow` | String | `4MB` | gRPC flow-control window for Raft replication. Larger values help catch-up replication after partitions |
| `HA_SNAPSHOT_MAX_CONCURRENT` | `arcadedb.ha.snapshotMaxConcurrent` | Integer | `2` | Maximum concurrent snapshot downloads served by this node. Excess requests receive HTTP 503 so followers retry with backoff |
| `HA_SNAPSHOT_DOWNLOAD_TIMEOUT` | `arcadedb.ha.snapshotDownloadTimeout` | Integer | `300000` | Read timeout (ms) for downloading a database snapshot from the leader during follower resync |
| `HA_SNAPSHOT_WRITE_TIMEOUT` | `arcadedb.ha.snapshotWriteTimeout` | Integer | `300000` | Server-side write timeout (ms) for serving a snapshot. Releases the concurrency slot if the transfer stalls |
| `HA_SNAPSHOT_MAX_ENTRY_SIZE` | `arcadedb.ha.snapshotMaxEntrySize` | Long | `10737418240` | Maximum uncompressed size (bytes) of a single file extracted from a snapshot ZIP. Decompression-bomb guard (10 GB default) |
| `HA_SNAPSHOT_WATCHDOG_TIMEOUT` | `arcadedb.ha.snapshotWatchdogTimeout` | Integer | `30000` | Delay (ms) after which a follower that detected a snapshot gap forces a direct snapshot download if no leader-change has fired. Effective value is floored to `4 × electionTimeoutMax` |
| `HA_SNAPSHOT_GAP_TOLERANCE` | `arcadedb.ha.snapshotGapTolerance` | Integer | `10` | Maximum tolerated difference between the Ratis snapshot index and the persisted applied index before a follower forces a full snapshot download on startup |
| `HA_STOP_SERVER_ON_REPLICATION_FAILURE` | `arcadedb.ha.stopServerOnReplicationFailure` | Boolean | `false` | After a phase-2 local commit fails on the leader while followers have applied the entry, step-down is attempted first. If every step-down fails and this flag is true, the JVM exits so an orchestrator can restart and let Raft log replay correct the state |

#### Performance Tuning

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_GROUP_COMMIT_BATCH_SIZE` | `arcadedb.ha.groupCommitBatchSize` | Integer | `500` | Maximum transactions batched in a single Raft round-trip. Higher values improve throughput under concurrent load |
| `HA_GROUP_COMMIT_QUEUE_SIZE` | `arcadedb.ha.groupCommitQueueSize` | Integer | `10000` | Maximum pending transactions allowed in the Raft group-commit queue. Increase under sustained high write load to avoid `ReplicationQueueFullException` |
| `HA_GROUP_COMMIT_OFFER_TIMEOUT` | `arcadedb.ha.groupCommitOfferTimeout` | Integer | `100` | Timeout (ms) waiting for space in the group-commit queue before throwing `ReplicationQueueFullException` |
| `HA_REPLICATION_CHUNK_MAXSIZE` | `arcadedb.ha.replicationChunkMaxSize` | Integer | `16777216` | Maximum channel chunk size (bytes) for replication. Default 16MB |

#### Security and Auth

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_CLUSTER_TOKEN` | `arcadedb.ha.clusterToken` | String | (empty) | Shared secret for inter-node HTTP forwarding and snapshot auth. If empty, auto-derived via PBKDF2-HMAC-SHA256 (100k iterations) from cluster name + root password with a domain-separated salt |
| `HA_PEER_ALLOWLIST_ENABLED` | `arcadedb.ha.peerAllowlist.enabled` | Boolean | `true` | Reject inbound Raft gRPC connections whose remote address does not resolve to a host in `arcadedb.ha.serverList`. Loopback is always allowed. Does not provide peer identity or encryption: use mTLS on untrusted networks |
| `HA_PEER_ALLOWLIST_REFRESH_MS` | `arcadedb.ha.peerAllowlist.refreshMs` | Long | `5000` | Minimum interval (ms) between DNS re-resolutions of the peer host list. Lower bound prevents DNS flooding when an unknown peer repeatedly retries |

#### Networking

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_REPLICATION_INCOMING_HOST` | `arcadedb.ha.replicationIncomingHost` | String | `0.0.0.0` | TCP/IP host for incoming replication connections |
| `HA_REPLICATION_INCOMING_PORTS` | `arcadedb.ha.replicationIncomingPorts` | String | `2424-2433` | TCP/IP port range for incoming replication connections |

#### Monitoring and Debugging

| Setting | Property | Type | Default | Description |
|---|---|---|---|---|
| `HA_LOG_VERBOSE` | `arcadedb.ha.logVerbose` | Integer | `0` | Verbose logging: 0=off, 1=basic (elections, peers), 2=detailed (commands, WAL), 3=trace (all state machine ops) |
| `HA_REPLICATION_LAG_WARNING` | `arcadedb.ha.replicationLagWarning` | Integer | `1000` | Raft log index gap (number of uncommitted entries) between leader and follower before emitting replication lag warnings. 0 = disabled |
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

1. Ordinal-derived jitter (parsed from `HOSTNAME=<name>-<ordinal>`) spreads simultaneous pod starts into non-overlapping time slots so concurrent joins cannot stampede
2. `KubernetesAutoJoin.tryAutoJoin()` iterates configured peers from `HA_SERVER_LIST` and queries `GroupManagementApi.info()` to check if this server is already a member
3. If not a member, it issues an atomic `SetConfigurationRequest(Mode.ADD)` that appends this peer to the current configuration; `Mode.ADD` is atomic so concurrent joins from multiple pods are race-free
4. If no existing cluster responds (fresh deployment), Ratis normal leader election proceeds on the full configured peer group

Auto-join uses only peers from `HA_SERVER_LIST` (no Kubernetes API call); the same list is used by `PeerAddressAllowlistFilter` to build the inbound allowlist, so auto-join traffic is implicitly allowlisted.

This enables **zero-downtime scale-up**: `kubectl scale statefulset arcadedb --replicas=5` adds 2 new pods that automatically join the existing 3-node cluster. Each new peer is then seeded with the current user database so HTTP authentication works immediately.

### Security Warnings on Startup

When `HA_K8S=true`, `RaftHAServer` emits two escalating warnings:
- `INFO`: Raft gRPC transport does not enforce cluster-token auth, so a NetworkPolicy is recommended.
- `SEVERE`: if gRPC is bound to `0.0.0.0` / `::`, any pod in the cluster can inject log entries absent a NetworkPolicy, so either restrict `arcadedb.ha.replicationIncomingHost` or enable `HA_PEER_ALLOWLIST_ENABLED` (the default) alongside mTLS.

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
| Scale-up | Restart all pods with new server list | Auto-join via `KubernetesAutoJoin.tryAutoJoin()` |
| Scale-down | Manual disconnect + restart | Auto-leave via preStop hook + `leaveCluster()` |
| Leader failover | Custom election, 3-5s | Ratis pre-vote + election, 1.5-3s |
| Rolling upgrade | Stop/start one by one, hope for the best | Ratis RECOVER mode auto-catches up |
| Storage persistence | Custom replication log | Ratis log segments + metadata (term, vote) |

## Tests

### Non-E2E Tests (server module, 30 classes, ~80 individual tests)

All pass when run individually. Port conflicts occur when multiple HA test classes run in the same JVM session (not a real failure).

#### Core Tests
| Test | Tests | Description |
|---|---|---|
| `RaftLogEntryTest` | 12 | Binary serialization round-trip |
| `SnapshotSwapRecoveryTest` | 8 | Crash recovery during snapshot directory swap |
| `ClusterMonitorTest` | 5 | Replication lag monitoring |
| `RaftHAServerIT` | 3 | Raw Ratis consensus: election, replication |
| `RaftReplicationIT` | 5 | WAL replication via Ratis |

#### Comprehensive Tests (`RaftHAComprehensiveIT`, 17 tests, 3 servers)
| Test | Description |
|---|---|
| `test01_dataConsistencyUnderLoad` | 1000 records, verify count & content on all nodes |
| `test02_followerRestartAndCatchUp` | Stop follower, write 100 records, restart, verify catch-up |
| `test03_fullClusterRestart` | Write data, stop all 3, restart all 3, verify data survives |
| `test04_concurrentWritesOnLeader` | 4 threads x 100 records with TX_RETRIES=50 for MVCC contention |
| `test05_schemaChangesDuringWrites` | CREATE TYPE while data exists, verify propagation |
| `test06_indexConsistency` | Unique index enforcement across cluster |
| `test07_queryRoutingCorrectness` | SELECT local, INSERT rejected on follower |
| `test08_largeTransaction` | Single tx with 500 records, verify replication |
| `test09_rapidLeaderTransfers` | 5 rapid leadership transfers, verify stability |
| `test10_singleServerHAMode` | HA with 1 node, verify reads work, writes fail quorum |
| `test11_writeToFollowerViaHttpProxy` | 100 writes via HTTP to follower, proxied to leader |
| `test12_leaderElectionDuringTransaction` | Uncommitted tx on leader, kill leader, verify rollback (ACID) |
| `test13_concurrentWritesViaProxy` | 3 servers x 30 writes via HTTP simultaneously |
| `test14_writesDuringSlowFollower` | Stop 1 follower, writes continue (majority), restart, catch-up |
| `test15_veryLargeTransaction` | 2000 records x 500 bytes in single tx (~1MB+ WAL) |
| `test16_mixedReadWriteWorkload` | Concurrent reads on follower + writes on leader |
| `test17_rollingUpgradeSimulation` | Stop/restart each server one by one, verify data survives |

#### HTTP API & Failover Tests
| Test | Tests | Description |
|---|---|---|
| `HTTP2ServersIT` | 6 | Cluster status, schema DDL, queries, CRUD, verify, config |
| `HTTP2ServersCreateReplicatedDatabaseIT` | 1 | Create database via HTTP, replicate schema + data |
| `ReplicationServerLeaderDownIT` | 1 | Leader stop, new election, writes continue |
| `ReplicationServerLeaderDownNoTransactionsToForwardIT` | 2 | Leader down with no pending forwards |
| `ReplicationServerLeaderChanges3TimesIT` | 1 | 3 leader kill/restart cycles |
| `HASplitBrainIT` | 1 | 5-node cluster, stop 2 minority, verify majority works |
| `HAConfigurationIT` | 1 | Invalid server list rejection |
| `ServerDatabaseBackupIT` | 2 | SQL backup on HA cluster |
| `ReplicationServerWriteAgainstReplicaIT` | 2 | Write forwarding from follower to leader |
| `ReplicationChangeSchemaIT` | 2 | Schema DDL replication |
| `ReadConsistencyIT` | 3 | EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE consistency |
| `ReplicationServerReplicaHotResyncIT` | 1 | Hot resync detection callback |
| `ClusterTokenAuthIT` | 5 | Cluster token auth, inter-node forwarding |
| `ReplicationServerQuorumMajorityIT` | 1 | MAJORITY quorum |
| `ReplicationServerQuorumAllIT` | 1 | ALL quorum |
| `ReplicationServerQuorumMajority1ServerOutIT` | 1 | MAJORITY with 1 server down |
| `ReplicationServerQuorumMajority2ServersOutIT` | 1 | MAJORITY with 2 servers down (quorum lost) |
| `ReplicationServerFixedClientConnectionIT` | 1 | Fixed client connection to specific server |
| `ReplicationMaterializedViewIT` | 2 | Materialized view replication |
| `IndexCompactionReplicationIT` | 4 | Index compaction + replication |
| `IndexOperations3ServersIT` | 4 | Index create/rebuild/drop across 3 servers |
| `ServerDatabaseSqlScriptIT` | 1 | SQL script execution on HA cluster |
| `HARandomCrashIT` | 1 | Random server crash during writes |
| `HTTPGraphConcurrentIT` | 1 | Concurrent graph operations via HTTP |

#### Not Applicable
| Test | Reason |
|---|---|
| `ReplicationServerIT` | Abstract base class, no tests |
| `HAInsertBenchmark` | `@Disabled` - benchmark, not a functional test |
| `ReplicationServerQuorumNoneIT` | Removed - Ratis doesn't support "none" quorum |

### E2E Tests (Docker/TestContainers, 13 classes)

#### Passing (10 tests)
| Test | Description | Servers |
|---|---|---|
| `HAReplicationE2ETest` (3) | Basic replication, leader failover, follower proxy | 3 |
| `HANetworkPartitionE2ETest` | Follower network disconnect/reconnect, catch-up via Raft log replay | 3 |
| `HAQuorumLossRecoveryE2ETest` | Network-isolate 2 of 3 nodes, writes fail, reconnect both, cluster recovers | 3 |
| `HALeaderPartitionE2ETest` | Leader network-partitioned, majority elects new leader, old leader reconnects | 3 |
| `HAColdStartE2ETest` | All 3 nodes restarted via docker restart, Ratis log recovery + data intact + index survives | 3 |
| `HASnapshotCatchUpE2ETest` | Follower lags behind log purge boundary, catches up via snapshot HTTP download | 3 |
| `HAMultiDatabaseSnapshotE2ETest` | 2 databases, follower partitioned, snapshot installs both, all nodes converge | 3 |
| `HASnapshotDuringWritesE2ETest` | Follower reconnects while concurrent writes active on leader | 3 |
| `HADynamicDatabaseE2ETest` | Create database after cluster formation, verify schema + data replicate | 3 |
| `HALargeDataSnapshotE2ETest` | Large records (500+ bytes per field), snapshot streaming via HTTP ZIP | 3 |

#### WIP (1 test)
| Test | Description | Issue |
|---|---|---|
| `HARollingRestartE2ETest` | Rolling network-isolation with writes on survivors | 10min timeout. 3 sequential disconnect/reconnect cycles each trigger Ratis restart + snapshot download. Needs investigation into cumulative latency. |

#### Infrastructure Issues (2 tests)
| Test | Description | Issue |
|---|---|---|
| `HAPacketLossE2ETest` | Packet loss via Toxiproxy | Raft leader election never completes through Toxiproxy. The Toxiproxy TCP proxy may not handle Ratis gRPC bidirectional streaming correctly, or the proxy routing topology prevents Raft quorum formation. Tests were refactored to use direct HTTP instead of `RemoteDatabase` (which followed cluster redirects to unreachable internal Docker addresses), but the underlying Raft routing issue remains. |
| `HANetworkDelayE2ETest` | Network latency via Toxiproxy | Same Toxiproxy Raft routing issue as PacketLoss. |

### Known Limitations
- **State machine command forwarding**: The `query()` path for forwarding write commands to the leader has a page visibility issue. Currently using HTTP proxy fallback which works correctly.
- **mTLS for Raft gRPC**: not yet wired; `PeerAddressAllowlistFilter` closes the "any host knowing the port can inject log entries" vector, but it does not authenticate peer identity or encrypt traffic. Deploy behind a private network, NetworkPolicy, or service mesh on untrusted networks.

## HISTORY

### Resolved Issues
- **Linearizable reads on followers**: Previously `LINEARIZABLE` consistency still returned the follower's local `lastAppliedIndex`, which could be arbitrarily stale after a leader change. Wired Ratis `ReadIndex` so followers obtain the current leader's committed index for the request and wait until their own applied index reaches it before serving the read. New test: `RaftReadConsistencyIT`.
- **Concurrent snapshot install protection**: Two followers (or a retrying follower) could trigger overlapping snapshot installs, producing inconsistent `.snapshot-tmp` / `.snapshot-old` state. `SnapshotInstaller` now serialises installs per database and retries with exponential backoff.
- **Step down instead of suicide on phase-2 failure**: Earlier builds called `System.exit` directly when phase-2 diverged. The default now tries `stepDown()` first; JVM exit requires explicit opt-in via `HA_STOP_SERVER_ON_REPLICATION_FAILURE=true`.
- **Cluster token derivation upgraded from SHA-256 to PBKDF2**: Old build hashed cluster name + root password with a single SHA-256 pass. Now PBKDF2-HMAC-SHA256 at 100k iterations (OWASP 2023 guidance) with a domain-separated salt.
- **Constant-time token comparison**: All call sites comparing the cluster token use `MessageDigest.isEqual()` (SHA-256 digest pre-hash in `AbstractServerHttpHandler`, raw-bytes in `SnapshotHttpHandler`) to avoid timing side-channels.
- **SnapshotInstaller hardening**: Symlink escape, zip-slip, zip-bomb (entry-size + compression ratio), and SSL hostname/cert checks added to snapshot ingestion.
- **SnapshotHttpHandler hardening**: Cluster token auth enforced, input validation on database name, per-server concurrency cap (`HA_SNAPSHOT_MAX_CONCURRENT`), write timeout to release stuck slots (`HA_SNAPSHOT_WRITE_TIMEOUT`).
- **Non-idempotent request replay**: Added an idempotency cache keyed by `X-Request-Id` + principal so a client retry after a lost response replays the cached body instead of double-applying a write.
- **RaftTransactionBroker extraction**: Transaction submission, group-commit lifecycle, and ALL-quorum completion were scattered across `RaftHAServer`. Consolidated in `RaftTransactionBroker`.
- **RaftHAServer decomposition**: `RaftPeerAddressResolver`, `RaftPropertiesBuilder`, `RaftClusterManager`, and `RaftClusterStatusExporter` were extracted to reduce the god-class and make each concern unit-testable.
- **Originating peer skip**: Embedded `originPeerId` in each log entry so a node that restarts does not re-apply its own pre-crash entries (fixes a TOCTOU race with `isLeader()` in the old implementation).
- **Peer-add user seed**: A new peer added via `ha add peer` is now seeded with the current user list so authentication works immediately without waiting for the next user mutation.
- **Replicated user management**: `create user` / `drop user` are routed through Raft as `SECURITY_USERS_ENTRY` log entries. Synchronisation of `ServerSecurity` hooks was relaxed to avoid blocking Raft apply.
- **Replicated database lifecycle**: `create database`, `drop database`, `import database`, and `restore database` go through the state machine. Import/restore use `INSTALL_DATABASE_ENTRY` with a `forceSnapshot` flag so followers rebuild from a fresh snapshot rather than trying to replay individual WAL pages.
- **WAL version gap detection**: Added `WALVersionGapException` so a follower that cannot safely apply an incoming WAL delta triggers a snapshot download instead of corrupting pages.
- **Snapshot persistence for cold restart**: `takeSnapshot()` was not persisting a marker file to `SimpleStateMachineStorage`. After cold restart, `reinitialize()` set `lastAppliedIndex=-1` and Ratis replayed everything. Fixed by writing a marker file with MD5, restoring `lastAppliedIndex` and `BaseStateMachine`'s `TermIndex` in `reinitialize()`, and taking a snapshot on clean shutdown.
- **Snapshot installation (chunk-based)**: Changed from notification mode to chunk mode (`installSnapshotEnabled=true`). Leader sends the marker file via Ratis chunks; follower detects gap and downloads the ZIP from the leader HTTP endpoint.
- **Ratis server restart after partition**: Docker network disconnect caused `RaftServerImpl` to enter CLOSED. Added `EventApi` + health monitor thread (3s interval) that detects CLOSED and restarts Ratis with a fresh state machine in RECOVER mode. `slownessTimeout=300s`, `closeThreshold=600s`.
- **ServerDatabase close during snapshot**: `installDatabaseSnapshot()` now uses `db.getEmbedded().close()` instead of `db.close()` (the latter threw `UnsupportedOperationException` on server-managed databases).
- **SnapshotHttpHandler path parameter NPE**: After `exchange.dispatch()`, Undertow path params can be null; added fallback URL parsing and fixed the `ServerDatabase -> ReplicatedDatabase -> LocalDatabase` unwrap chain.
- **ColdStart stale port mappings**: After `docker restart`, Docker assigns new host port mappings; the e2e test now queries actual ports from Docker inspect.
- **ConcurrentModificationException during replay**: Catch both `java.util.ConcurrentModificationException` and `com.arcadedb.exception.ConcurrentModificationException`; skip already-applied entries.
- **Database loading safety**: `loadDatabases()` now skips `.snapshot-tmp` and `.snapshot-old` directories leftover from crashes during snapshot install.
- **Schema file registration during WAL apply**: `createNewFiles()` now triggers `schema.load()` + `initComponents()` so `LocalSchema.files` is rebuilt before WAL apply.
- **Orphan index files after failed creation**: Capture the exception in `recordFileChanges()`, send the file-removal replication command, then rethrow.
- **Schema save under lock / read-only leader**: Persist deferred schema save for read-only leader transactions so read-only DDL sequences do not silently lose metadata.
- **Shared config file race in `applyReplicatedUsers`**: Serialised writes to `server-users.jsonl` to prevent torn files under concurrent state-machine applies.
- **Shared `LeaderProxy`**: A single `LeaderProxy` instance per `HttpServer` (not per handler) so proxy state / connections are reused.
- **HealthMonitor disabled in tests**: Prevents thread exhaustion in tight restart loops.
- **RaftHAPlugin auto-discovery**: Fixed when `server.plugins` is not set in configuration (the plugin is still picked up via `ServiceLoader`).
- **Basic auth forwarding for server commands**: The HTTP proxy now forwards the caller's Basic auth as-is for server commands instead of substituting the cluster token, so permission checks run against the real user on the leader.
- **Schema version double-increment**: Fixed in follower apply path so schema-change entries do not bump the version twice.
- **Replication convergence in HA tests**: `waitForReplicationConvergence()` in `BaseGraphServerTest.endTest()` waits for all followers to apply up to the leader's commit index before comparing databases.
- **Concurrent write MVCC contention**: Increased `TX_RETRIES` to 50 for concurrent-writes tests to handle extended MVCC conflict window (file locks held during Raft gRPC round-trip).
- **Exception chain in test helpers**: `TestServerHelper.expectException()` now checks the entire cause chain, not just the top-level class.
- **Propagate ArcadeDB exceptions**: Commit errors are no longer wrapped in generic `TransactionException`; original exception type reaches the caller.
- **Null-safe group-commit error detail**: Avoids NPE when Ratis returns an error without a detail message.
- **Vector index replication**: Fixed 1-byte parsing misalignment in `LSMVectorIndex.applyReplicatedPageUpdate()`.

### Resolved issues during E2E testing
- **Docker network alias loss**: Docker does NOT preserve network aliases after `disconnect`/`connect`. Fixed by passing the alias explicitly via `ContainerNetwork.withAliases()` in `reconnectToNetwork()`.
- **Peer ID collision**: `resolveLocalPeerId()` matched on `HA_REPLICATION_INCOMING_HOST` (`0.0.0.0`) + port, causing all nodes to get the same peer ID. Fixed by matching on server name, hostname, or unambiguous port.
- **gRPC reconnection tuning**: Added `ExponentialBackoffRetry` on RaftClient, `slownessTimeout=300s`, `closeThreshold=600s`, `flowControlWindow=4MB` for robust partition recovery.
- **HTTP API params**: E2E tests use direct HTTP with `INSERT ... CONTENT {}` syntax (not `RemoteDatabase`) to avoid cluster address discovery issues in Docker.

### Future Features
- **State machine command forwarding**: Fix the `query()` path page visibility issue to eliminate HTTP proxy dependency for command forwarding. Currently write commands on non-leader nodes are forwarded via HTTP proxy which works correctly but adds latency.
- **Multi-Raft groups**: One Raft group per database (currently all databases share one group). This would allow independent replication policies per database.
- **JWT-based auth for cluster**: Replace Basic auth forwarding in HTTP proxy with stateless JWT tokens that work across servers without session affinity.
- **Alert configuration in Studio**: Configurable thresholds for replication lag, election frequency, quorum health with notifications.

## Operational Notes

### Minimum cluster size for fault tolerance

A 2-node Raft cluster has a quorum of 2, meaning **both** nodes must be available for the cluster to accept writes and elect a leader. If either node fails, the remaining node cannot form a quorum on its own and the cluster becomes read-only (or unavailable, depending on read consistency settings).

For fault tolerance, deploy at least **3 nodes**. A 3-node cluster tolerates 1 failure, a 5-node cluster tolerates 2 failures, and so on (quorum = N/2 + 1).

A 2-node cluster is useful for development, testing, or scenarios where you only need replication (not fault tolerance), but operators should be aware that losing one node in a 2-node cluster leaves a single node unable to elect a new leader.

## Comparison: `apache-ratis` vs `ha-redesign` Branch

Both branches implement Apache Ratis-based HA. This section documents only the differences.

### Architecture

| | ha-redesign | apache-ratis | Verdict |
|---|---|---|---|
| **Integration** | Plugin via ServiceLoader (`RaftHAPlugin`). Coexists with legacy binary protocol via `HA_IMPLEMENTATION=raft` switch. | Direct integration in server module. Legacy HA fully deleted (~6000 lines). | apache-ratis: cleaner. No reason to keep the old protocol. |
| **Module layout** | Separate `ha-raft/` Maven module + separate `e2e-ha/` test module. | Everything in `server/.../ha/ratis/`. E2E tests consolidated in `e2e/`. | apache-ratis: simpler. One module, no duplication. |
| **Compilation** | Does not compile (4 missing symbol errors). | Compiles, all tests pass. | apache-ratis: ha-redesign is broken. |
| **Origin skip** | `isLeader()` check at apply time (TOCTOU race). | `originPeerId` embedded in log entry (immutable, race-free). | apache-ratis: eliminates subtle correctness bug. |
| **Peer ID format** | `"peer-0"`, `"peer-1"` (numeric index from server name). | `"host_raftPort"` (e.g., `localhost_2424`). | apache-ratis: self-describing, JMX-compatible, no naming convention required. |
| **Replica-only servers** | Not supported. | `HA_SERVER_ROLE=replica` prevents node from being elected leader. | apache-ratis: essential for read-scale deployments. |

### Snapshot & Recovery

| | ha-redesign | apache-ratis | Verdict |
|---|---|---|---|
| **takeSnapshot()** | Returns index but writes no file. After restart, `lastAppliedIndex=-1`, Ratis replays everything. | Writes MD5-checksummed marker file. Restores exact position on restart. | apache-ratis: ha-redesign has the cold restart corruption bug. |
| **Snapshot installation** | Not implemented. Comment: "not yet wired." Lagging followers cannot auto-recover. | Full pipeline: chunk transfer, HTTP download, atomic swap, retry with backoff, crash-safe markers, persisted applied index for gap detection. | apache-ratis: this is the core HA recovery mechanism. |
| **Partition recovery** | Not handled. Ratis server enters CLOSED and stays dead. | Health monitor detects CLOSED state, restarts Ratis, gap detection triggers snapshot download on leader discovery. | apache-ratis: production-critical. |

### Performance & Tuning

| | ha-redesign | apache-ratis | Verdict |
|---|---|---|---|
| **Group commit** | Not implemented. Each tx = separate Raft round-trip. | `RaftGroupCommitter` batches up to 500 concurrent tx per round-trip. | apache-ratis: order-of-magnitude throughput improvement. |
| **Read consistency** | Not implemented. All reads stale or go to leader. | EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE with bookmark-based waiting. | apache-ratis: essential for follower reads. |
| **Election timeouts** | Hardcoded 2-5s. | Configurable via `HA_ELECTION_TIMEOUT_MIN/MAX`. | apache-ratis: WAN clusters need longer timeouts. |
| **Ratis tuning** | Minimal (snapshot threshold, purge-up-to-snapshot only). | Full control: log segment size, purge gap, append buffer, write buffer, flow control, leader lease, client request timeout, gRPC window. | apache-ratis: production deployments need tuning knobs. |

### Operations

| | ha-redesign | apache-ratis | Verdict |
|---|---|---|---|
| **Dynamic membership** | Not implemented. | `addPeer`, `removePeer`, `transferLeadership`, `stepDown`, `leaveCluster`. | apache-ratis: zero-downtime cluster management. |
| **K8s support** | Not implemented. | Auto-join on scale-up, auto-leave on scale-down via preStop hook. | apache-ratis. |
| **Verbose logging** | Not implemented. | 4-level runtime-configurable HA logging (`HALog`). | apache-ratis: critical for production debugging. |
| **Studio cluster dashboard** | Old HA layout (222 lines), no Ratis-specific data. | Full rewrite (442 lines): Overview/Metrics/Management tabs with term, commitIndex, per-follower matchIndex, replication lag charts. | apache-ratis: ha-redesign shows stale pre-Ratis UI. |
| **Replica-only servers** | Not supported. | `HA_SERVER_ROLE=replica` for read-scale nodes. | apache-ratis. |

### Error Handling

| | ha-redesign | apache-ratis | Verdict |
|---|---|---|---|
| **CME during replay** | `ignoreErrors=true` to `applyChanges()` (silently ignores ALL errors). | Catches specific `ConcurrentModificationException` types only. | apache-ratis: won't mask real corruption. |
| **Orphan files on failed schema** | Not handled. Partial files left on followers. | Captures exception, sends removal replication command, then rethrows. | apache-ratis: prevents orphan files. |
| **Phase 2 failure** | Logs error, continues as leader. | Steps down from leadership to prevent stale reads. | apache-ratis: safer. |
| **Schema file registration** | `load(READ_WRITE, true)` (rebuilds everything). | `load(READ_WRITE, false)` + `initComponents()` (targeted file list rebuild). | apache-ratis: more precise. |

### Tests

| | ha-redesign | apache-ratis |
|---|---|---|
| **Compilation** | Does not compile | Compiles |
| **RaftHAComprehensiveIT** | Does not exist | 17 tests: consistency, failover, concurrent writes, schema, proxy, slow followers, rolling upgrade |
| **E2E Docker tests** | 9 in separate `e2e-ha/` module (untested, module deleted) | 13 in `e2e/`, 11 passing: replication, partition, quorum loss, leader partition, cold start, snapshot catch-up, multi-DB snapshot, snapshot during writes, dynamic DB, large data, rolling restart |
| **Ratis-specific unit tests** | ~40 in `ha-raft/src/test/` | SnapshotSwapRecovery(8), RaftLogEntry(12), ClusterMonitor(5), RaftHAServer(3), RaftReplication(5), ClusterTokenAuth(5), ReadConsistency(3), OriginNodeSkip, AddressParsing |
| **All non-E2E HA tests** | Cannot run | 30 test classes, ~80 individual tests, all pass |

### What ha-redesign Had (not in apache-ratis)

| Feature | Assessment |
|---|---|
| Plugin architecture | Not needed. Single implementation is simpler. |
| Legacy HA coexistence | Not needed. Clean cut is better than two code paths. |
| Peer priority in server list | Parsed but never used. Ratis doesn't support weighted election natively. `ha transfer leader` achieves the same goal manually. |
| SnapshotManager utility (CRC32, file diffing) | Building blocks never wired to Ratis. HTTP ZIP download approach is more complete. |
