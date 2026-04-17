# Port apache-ratis Hardening to ha-redesign - Design Spec

**Date:** 2026-04-18
**Branch source:** `apache-ratis` (15 commits from 2026-04-17)
**Branch target:** `ha-redesign`
**Approach:** Adapt each feature to ha-redesign's refactored structure (class renames, SnapshotManager extraction, etc.)

---

## Context

The `apache-ratis` branch received a burst of HA hardening on 2026-04-17 covering production resilience, security, and correctness. The `ha-redesign` branch has diverged structurally (renamed `ArcadeDBStateMachine` to `ArcadeStateMachine`, `ReplicatedDatabase` to `RaftReplicatedDatabase`, extracted `SnapshotManager`, added `package-info.java`). This spec covers porting all 6 feature areas to ha-redesign by adapting the code, not cherry-picking.

### Structural Mapping

| apache-ratis | ha-redesign |
|---|---|
| `ArcadeDBStateMachine` | `ArcadeStateMachine` |
| `ReplicatedDatabase` | `RaftReplicatedDatabase` |
| `HAPlugin` (server module) | `HAServerPlugin` (server module) |
| `IdempotencyCache` (server/http) | New file (same location) |
| `PeerAddressAllowlistFilter` | New file (same package) |
| `RaftGrpcServicesCustomizer` | New file (same package) |

---

## Section 1: Linearizable Reads on Followers

### Problem
ha-redesign has `waitForReadConsistency()` in `RaftReplicatedDatabase` that handles READ_YOUR_WRITES via `waitForAppliedIndex()`. But LINEARIZABLE consistency on followers is incomplete - there is no ReadIndex protocol where a follower asks the leader "what's your current committed index?" and waits for local catch-up.

### Changes

**RaftHAServer.java** - add 3 new methods:
- `ensureLinearizableRead()` (leader-side): Call Ratis `sendReadOnly` to confirm lease, then wait for local state machine to apply up to the returned index.
- `ensureLinearizableFollowerRead()` (follower-side): Call `sendReadOnly` to obtain leader's committed index, wait for local state machine to catch up.
- `fetchReadIndex(boolean expectSelfIsLeader)`: Shared helper wrapping Ratis `sendReadOnly` with error classification - distinguishes `NotLeaderException` (redirect hint) from timeout/other failures (`ReplicationException`).

**RaftReplicatedDatabase.java** - modify `waitForReadConsistency()`:
- For LINEARIZABLE on leader: call `raftHAServer.ensureLinearizableRead()`
- For LINEARIZABLE on follower: call `raftHAServer.ensureLinearizableFollowerRead()`
- Replace current `waitForLocalApply()` call with the appropriate method based on leadership.

**HAServerPlugin.java** - add interface methods:
- `ensureLinearizableRead()` (default throws UnsupportedOperationException)
- `ensureLinearizableFollowerRead()` (default throws UnsupportedOperationException)

### Config Keys
None new - uses existing `HA_QUORUM_TIMEOUT` for the ReadIndex RPC timeout.

### Tests
- Unit test for `fetchReadIndex` error classification (mock Ratis client)
- Integration test: 3-node cluster, write on leader, LINEARIZABLE read on follower returns fresh data

---

## Section 2: Step-Down on Phase-2 Failure

### Problem
ha-redesign calls `stepDown()` once on phase-2 failure and spawns emergency stop on failure. apache-ratis is more resilient with retry and configurability.

### Changes

**RaftReplicatedDatabase.java** - modify `recoverLeadershipAfterPhase2Failure()`:
- Add retry loop: 3 attempts with 500ms delay between step-down calls before fallback
- Make server stop conditional on `HA_STOP_SERVER_ON_REPLICATION_FAILURE` config (default: true)
- Improve logging: distinguish `ConcurrentModificationException` (page version conflict) from other errors
- Add `TEST_POST_REPLICATION_HOOK` static `Runnable` field (null in production) that fires after replication succeeds but before phase-2 runs

**GlobalConfiguration.java**:
- Add `HA_STOP_SERVER_ON_REPLICATION_FAILURE` (boolean, default: true)

### Tests
- Unit test: verify retry loop attempts step-down 3 times before server stop
- Integration test (`RaftLeaderCrashBetweenCommitAndApplyIT`): use `TEST_POST_REPLICATION_HOOK` to simulate leader crash in the narrow window, verify follower elects new leader and data is consistent

---

## Section 3: Snapshot Hardening

### Problem
ha-redesign has solid snapshot infrastructure (retry, 3-phase swap, symlink/zip-slip protection) but is missing several hardening features from apache-ratis.

### Changes

**SnapshotHttpHandler.java**:
- Add write timeout watchdog: `ScheduledExecutorService` that force-closes stalled connections after `HA_SNAPSHOT_WRITE_TIMEOUT` ms. This prevents a disconnected follower from permanently occupying a semaphore slot. The watchdog is armed before streaming begins and cancelled in the finally block.
- Add plain HTTP warning: log WARN once per server lifecycle when serving snapshots over unencrypted HTTP.

**SnapshotInstaller.java**:
- Add `CountingInputStream` (inner class or private helper) that tracks raw compressed bytes consumed per zip entry.
- Add compression ratio check: reject entries where `uncompressed / compressed > 200` (skip check for entries < 64KB compressed).
- Add `syncDatabasesFromLeader()`: fetch leader's database list via HTTP GET, drop local databases not present on leader.
- Add leader address refresh on each retry attempt in `downloadWithRetry()` to handle elections during snapshot download.

**ArcadeStateMachine.java**:
- Change watchdog timeout computation: `max(configured HA_SNAPSHOT_WATCHDOG_TIMEOUT, 4 * HA_ELECTION_TIMEOUT_MAX)` instead of fixed 30s. This prevents premature firing on WAN clusters with long election timeouts.
- Add gap tolerance via `HA_SNAPSHOT_GAP_TOLERANCE` config.

**GlobalConfiguration.java**:
- Add `HA_SNAPSHOT_WRITE_TIMEOUT` (long, default: 300000 - 5 minutes)
- Add `HA_SNAPSHOT_WATCHDOG_TIMEOUT` (long, default: 30000 - makes existing hardcoded 30s value configurable, floored by 4x election timeout)
- Add `HA_SNAPSHOT_GAP_TOLERANCE` (long, default: 10)
- Add `HA_SNAPSHOT_MAX_ENTRY_SIZE` (long, default: 10737418240 - 10GB)

### Tests
- Unit test: `CountingInputStream` byte tracking accuracy
- Unit test: compression ratio rejection at 200:1 threshold
- Unit test: watchdog timeout floor computation
- Integration test: stale DB cleanup after leader drops a database

---

## Section 4: IdempotencyCache

### Problem
No request deduplication exists on ha-redesign. A client retrying a POST whose original response was lost can cause double-commits.

### Changes

**New file: `server/src/main/java/com/arcadedb/server/http/IdempotencyCache.java`**:
- `ConcurrentHashMap<String, CachedEntry>` keyed by `X-Request-Id` header value
- `CachedEntry`: statusCode, body, binary (byte[]), principal, timestampMs
- `get(requestId)`: return cached entry or null (removes expired on access)
- `putSuccess(requestId, status, body, binary, principal)`: cache only 2xx responses
- `cleanupExpired()`: periodic maintenance
- `evictOldest()`: drop oldest entry when max size reached
- Principal verification: cached response only returned if requesting principal matches

**HttpServer.java** - lifecycle:
- Create `IdempotencyCache` on server start with configured TTL and max entries
- Expose via `getIdempotencyCache()`
- Cleanup on shutdown

**AbstractServerHttpHandler.java** - request interception:
- Before executing POST requests: check `X-Request-Id` header against cache. If hit, return cached response immediately.
- After successful POST execution: cache response with request ID.
- GET/PUT/DELETE are naturally idempotent - skip cache.

**GlobalConfiguration.java**:
- Add `HA_IDEMPOTENCY_CACHE_TTL_MS` (long, default: 60000)
- Add `HA_IDEMPOTENCY_CACHE_MAX_ENTRIES` (int, default: 10000)

### Tests
- Unit test: cache put/get, TTL expiration, principal mismatch rejection, max size eviction
- Integration test (`IdempotencyReplayIT`): send POST with `X-Request-Id`, verify retry returns cached response without re-execution

---

## Section 5: PeerAddressAllowlistFilter + RaftGrpcServicesCustomizer

### Problem
The Raft gRPC port has no access control. Any host that discovers the port can inject log entries without authentication.

### Changes

**New file: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java`**:
- Implements gRPC `ServerTransportFilter`
- On `transportReady()`: extract remote IP from connection attributes, reject if not in peer set
- Peer set derived from `HA_SERVER_LIST` config
- Lazy DNS resolution with rate-limiting (`refreshIntervalMs`) for K8s pod-IP churn
- Loopback addresses (127.0.0.1, ::1) always allowed
- `AtomicReference<Set<String>>` for thread-safe allowlist updates
- `extractPeerHosts(serverList)`: parse host:port entries, strip IPv6 brackets

**New file: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGrpcServicesCustomizer.java`**:
- Implements Ratis `GrpcServices.Customizer`
- Constructor takes `ServerTransportFilter[]`
- `customize(NettyServerBuilder)`: applies filters sequentially

**RaftHAServer.java** - wiring in `start()`:
- Create `PeerAddressAllowlistFilter` from `HA_SERVER_LIST` and refresh interval
- Wrap in `RaftGrpcServicesCustomizer`
- Pass customizer to Ratis server builder via `RaftPropertiesBuilder`

**RaftPropertiesBuilder.java** - extend:
- Accept optional `GrpcServices.Customizer` parameter
- Set it on the Ratis GrpcConfigKeys if provided

**GlobalConfiguration.java**:
- Add `HA_GRPC_ALLOWLIST_REFRESH_MS` (long, default: 30000)

### Tests
- Unit test: `PeerAddressAllowlistFilter` - allowed peer passes, unknown IP rejected, loopback allowed, DNS refresh rate-limiting
- Unit test: `extractPeerHosts` parsing with IPv4, IPv6, hostnames

---

## Section 6: Remote Client Safety + Edge-Case Fixes

### Problem
Several smaller hardening improvements across the codebase address real failure modes seen in testing.

### Changes

**RemoteHttpComponent.java** (remote module):
- Add retry loop for election-related HTTP failures: on 503/connection-refused during leader election, wait `HA_CLIENT_ELECTION_RETRY_DELAY_MS` and retry up to `HA_CLIENT_ELECTION_RETRY_COUNT` times before failing.

**RemoteSchema.java** (remote module):
- Add synchronization to prevent `ConcurrentModificationException` when multiple threads share a `RemoteDatabase` and trigger concurrent schema initialization.

**PostVerifyDatabaseHandler.java** (ha-raft module):
- Port improved error handling and validation from apache-ratis.

**KubernetesAutoJoin.java** (ha-raft module):
- Port edge-case fixes for DNS resolution.

**RaftLogEntryCodec.java** (ha-raft module):
- Handle null/unknown entry type IDs gracefully: return null instead of throwing, allowing forward compatibility during rolling upgrades.

**RetryStep.java** (engine module):
- Port small fix from apache-ratis.

**GlobalConfiguration.java**:
- Add `HA_CLIENT_ELECTION_RETRY_COUNT` (int, default: 5)
- Add `HA_CLIENT_ELECTION_RETRY_DELAY_MS` (long, default: 1000)

### Tests
- Unit test: `RemoteHttpComponent` retry on 503 during election
- Unit test: `RemoteSchema` concurrent initialization safety
- Unit test: `RaftLogEntryCodec` null type handling
- Integration test: `RemoteClientLivenessIT` - verify no thread leaks
- Integration test: `RemoteSchemaConcurrentInitIT` - concurrent schema access

---

## Execution Order

Sections should be implemented in dependency order:

1. **Section 6 (edge-case fixes)** - prerequisite fixes that other sections may depend on (RaftLogEntryCodec, GlobalConfiguration entries)
2. **Section 2 (step-down hardening)** - foundational resilience, no dependencies on other sections
3. **Section 1 (linearizable reads)** - depends on correct RaftHAServer state, benefits from step-down safety
4. **Section 3 (snapshot hardening)** - independent, can run in parallel with Section 1
5. **Section 5 (gRPC security)** - independent, can run in parallel with Sections 1/3
6. **Section 4 (idempotency cache)** - HTTP-layer concern, fully independent, lowest risk

Sections 1, 3, 4, and 5 are independent of each other and can be parallelized after Section 6 and 2 are complete.

---

## New Files Summary

| File | Module | Purpose |
|---|---|---|
| `IdempotencyCache.java` | server | HTTP response cache for retry deduplication |
| `PeerAddressAllowlistFilter.java` | ha-raft | gRPC transport filter for peer IP validation |
| `RaftGrpcServicesCustomizer.java` | ha-raft | Ratis gRPC customizer for installing filters |

## Modified Files Summary

| File | Module | Sections |
|---|---|---|
| `GlobalConfiguration.java` | engine | 2, 3, 4, 5, 6 |
| `RaftHAServer.java` | ha-raft | 1, 5 |
| `RaftReplicatedDatabase.java` | ha-raft | 1, 2 |
| `ArcadeStateMachine.java` | ha-raft | 3 |
| `SnapshotInstaller.java` | ha-raft | 3 |
| `SnapshotHttpHandler.java` | ha-raft | 3 |
| `RaftPropertiesBuilder.java` | ha-raft | 5 |
| `HAServerPlugin.java` | server | 1 |
| `HttpServer.java` | server | 4 |
| `AbstractServerHttpHandler.java` | server | 4 |
| `RemoteHttpComponent.java` | remote | 6 |
| `RemoteSchema.java` | remote | 6 |
| `PostVerifyDatabaseHandler.java` | ha-raft | 6 |
| `KubernetesAutoJoin.java` | ha-raft | 6 |
| `RaftLogEntryCodec.java` | ha-raft | 6 |
| `RetryStep.java` | engine | 6 |

## New Config Keys Summary

| Key | Type | Default | Section |
|---|---|---|---|
| `HA_STOP_SERVER_ON_REPLICATION_FAILURE` | boolean | true | 2 |
| `HA_SNAPSHOT_WRITE_TIMEOUT` | long | 300000 | 3 |
| `HA_SNAPSHOT_WATCHDOG_TIMEOUT` | long | 30000 | 3 |
| `HA_SNAPSHOT_GAP_TOLERANCE` | long | 10 | 3 |
| `HA_SNAPSHOT_MAX_ENTRY_SIZE` | long | 10737418240 | 3 |
| `HA_IDEMPOTENCY_CACHE_TTL_MS` | long | 60000 | 4 |
| `HA_IDEMPOTENCY_CACHE_MAX_ENTRIES` | int | 10000 | 4 |
| `HA_GRPC_ALLOWLIST_REFRESH_MS` | long | 30000 | 5 |
| `HA_CLIENT_ELECTION_RETRY_COUNT` | int | 5 | 6 |
| `HA_CLIENT_ELECTION_RETRY_DELAY_MS` | long | 1000 | 6 |
