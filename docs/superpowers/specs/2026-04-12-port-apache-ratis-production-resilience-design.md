# Design: Port apache-ratis production-resilience features to ha-redesign

**Date:** 2026-04-12
**Branch:** ha-redesign
**Source branch:** apache-ratis
**Supersedes/extends:** `2026-04-06-port-apache-ratis-to-ha-redesign-design.md`

## Context

Three prior rounds of porting brought `ha-redesign` to parity with `apache-ratis` on performance (group committer, LZ4 compression, leader lease, Ratis tuning), correctness (ALL quorum, snapshot install, 3-phase commit), cluster management (dynamic membership, K8s auto-join), and read consistency (server-side). A fresh comparison identified eight remaining gaps, all related to production resilience and operational hardening:

**Critical (production safety):**
1. Network partition auto-recovery (Health Monitor)
2. Server-side HTTP proxy to leader
3. Client-side HA failover (read consistency + election retry) in `RemoteDatabase`
4. Constant-time cluster token comparison in `AbstractServerHttpHandler`

**Important (operational):**

5. Concurrent snapshot throttling
6. Symlink rejection in snapshot ZIPs
7. gRPC flow control window tuning
8. HTTP proxy read timeout (couples to item 2)

`ha-redesign` is unreleased, so clean breaks are acceptable. This design covers all eight items in a single spec, grouped into two rollout phases.

## Decisions

- **Scope:** Single spec, phased rollout. Phase A = items 1-4 (production safety). Phase B = items 5-8 (operational hardening).
- **Style:** Port the mechanism from `apache-ratis`, rewrite tests in ha-redesign style (unit + `BaseMiniRaftTest` IT + e2e chaos when applicable). Use `HALog` for debug output. All tunables go in `GlobalConfiguration`.
- **Client failover (item 3):** Opt-in via `RemoteDatabase.setReadConsistency(ReadConsistency)`. Default `EVENTUAL`, zero behavior change for non-HA users.
- **Server proxy (item 2):** Triggered in the existing `AbstractServerHttpHandler` catch clauses that today return HTTP 400. Narrow surface, single code path, mirrors `apache-ratis`.
- **Health monitor (item 1):** Dedicated component inside `RaftHAServer`, separate thread. Decoupled from `ClusterMonitor`.
- **Test gate:** All existing tests green + new unit/IT tests for each item + at least one `BaseMiniRaftTest` IT per critical item. No flaky Toxiproxy test for the health monitor; use a test hook to inject `CLOSED` state.
- **No backward compatibility shims.** Config key additions only, no removals.
- **No new external dependencies.**

## Out of Scope

- Streaming response relay in the leader proxy (buffered only)
- Auto-tuning gRPC flow control
- Unifying `ReadConsistency` enum across `network/` and `server/` modules
- Shared `SecurityUtils.constantTimeEquals` helper (below duplication threshold at 2 call sites)
- Porting `TRANSACTION_FORWARD` log entry type (known page-visibility issue in apache-ratis, currently unused there)

---

## Phase A: Production Safety

### Item 1: Health Monitor (network partition auto-recovery)

**Problem:** After a network partition heals, Ratis can remain in `CLOSED` / `EXCEPTION` state and not self-recover. A partitioned follower stays dead until an operator restarts the process. `apache-ratis` runs a background 3-second health check that detects the state and recreates the `RaftServer` in place via `StartupOption.RECOVER`.

#### Files

- **New:** `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HealthMonitor.java`
- **Modified:** `ha-raft/.../raft/RaftHAServer.java`
  - Add `restartRatisIfNeeded()` method
  - Add package-private `forceRaftStateForTesting(LifeCycle.State)` test hook
  - Start `HealthMonitor` after `RaftClient` initialization in `start()`
  - Stop `HealthMonitor` before closing Ratis in `stop()`
  - Add a `shutdownRequested` volatile flag and a dedicated `recoveryLock` (ReentrantLock)
- **Modified:** `engine/.../GlobalConfiguration.java`
  - New key `HA_HEALTH_CHECK_INTERVAL` (long ms, default 3000, 0 disables)

#### Design

`HealthMonitor` owns a single-threaded `ScheduledExecutorService` named `arcadedb-raft-health-monitor` (daemon). It runs `tick()` at `HA_HEALTH_CHECK_INTERVAL` after a startup grace period of `2 * intervalMs` (lets Ratis complete its initial handshake).

`tick()` logic:
1. If `shutdownRequested` is set, return immediately.
2. Read `server.getRaftServer().getLifeCycleState()`.
3. If state is `CLOSED` or `EXCEPTION`, call `server.restartRatisIfNeeded()`.
4. Otherwise no-op.

`restartRatisIfNeeded()` on `RaftHAServer`:

1. `HALog.log(BASIC, "Health monitor detected Ratis %s state, attempting recovery")`.
2. `tryLock(recoveryLock)`. If already held, return (another recovery is in flight).
3. Best-effort close of the existing `RaftClient` and `RaftServer` (swallow exceptions, log at FINE).
4. Build a fresh `ArcadeStateMachine` instance. The state machine's durable state lives in `SimpleStateMachineStorage` on disk, so the fresh instance replays from where the old one stopped, no data loss.
5. Re-build `RaftProperties` by calling a package-private `buildRaftProperties()` helper extracted from `start()` as part of this item. This helper re-reads all config on every call, so config-driven values (election timeouts, gRPC flow control window from item 7, etc.) are re-applied automatically on recovery.
6. Build a new `RaftServer` via `RaftServer.newBuilder().setOption(StartupOption.RECOVER)` with the same peers.
7. Start the new server. Build a new `RaftClient`.
8. Re-attach `RaftGroupCommitter` to the new client.
9. Release `recoveryLock`. Log `BASIC "Ratis recovered successfully"`.
10. On failure, release the lock, log at SEVERE, let the next tick retry.

**Guard rails:**
- `shutdownRequested` is set by `RaftHAServer.stop()` before calling `healthMonitor.stop()`. Prevents a tick that is already running from recovering during shutdown.
- The recovery lock is separate from any start/stop mutex to avoid deadlocking with a concurrent `stop()`.
- Startup grace period prevents recovery during Ratis initial election.

**Test hook:** `forceRaftStateForTesting(LifeCycle.State)` on `RaftHAServer` stores a one-shot override consulted by `getLifeCycleState()`. After one read, the override clears. Package-private. Used only in tests.

#### Tests

- **Unit** `HealthMonitorTest`: fake `RaftHAServer` with a stateful `getLifeCycleState()`. Verify tick triggers recovery on `CLOSED` and `EXCEPTION`, no recovery on `RUNNING` / `STARTING` / `NEW`. Verify the recovery lock serializes concurrent ticks. Verify `shutdownRequested` bails out of the tick.
- **Unit** `RaftHAServerRecoveryTest`: fake Ratis builder, call `restartRatisIfNeeded()`, assert old client is closed, new state machine is built, new server uses `StartupOption.RECOVER`, new client is attached to group committer.
- **IT** `RaftHealthMonitorRecoveryIT` extends `BaseMiniRaftTest`: 3 nodes. Call `forceRaftStateForTesting(CLOSED)` on one follower. Wait up to 10s. Assert the follower resumes accepting appends and catches up to the leader's commit index.
- **Chaos regression:** existing `e2e-ha/NetworkPartitionRecoveryIT` keeps passing.

---

### Item 2: Server-side leader proxy

**Problem:** `AbstractServerHttpHandler` catches `ServerIsNotTheLeaderException` and returns HTTP 400 with the leader address. Clients that do not re-target fail. `apache-ratis` transparently proxies the request to the leader and relays the response.

#### Files

- **New:** `server/src/main/java/com/arcadedb/server/http/handler/LeaderProxy.java`
- **Modified:** `server/.../http/handler/AbstractServerHttpHandler.java`
  - Instantiate one `LeaderProxy` per `HttpServer` (shared `HttpClient`)
  - Replace the 400-return in the two `ServerIsNotTheLeaderException` catches with `tryProxy` + fallback
- **Modified:** `engine/.../GlobalConfiguration.java`
  - `HA_PROXY_READ_TIMEOUT` (long ms, default 30000)
  - `HA_PROXY_CONNECT_TIMEOUT` (long ms, default 5000)
  - `HA_PROXY_MAX_BODY_SIZE` (int bytes, default 16 * 1024 * 1024)

#### Design

`LeaderProxy` owns one `java.net.http.HttpClient` instance (HTTP/1.1, `connectTimeout = HA_PROXY_CONNECT_TIMEOUT`). `tryProxy(HttpServerExchange exchange, String leaderAddress)` returns `boolean`. `true` means the response has been written to the exchange and no further action is required. `false` means the caller should fall back to the existing 400 response.

Proxy sequence in `tryProxy`:

1. **Loop prevention:** if the incoming request already carries `X-ArcadeDB-Cluster-Token`, return `false` immediately. Prevents a cycle where a stale follower proxies to a new follower.
2. **Validate leader address.** Blank or null, return `false`.
3. **Read body.** The exchange is already on a worker thread at this catch point (Undertow dispatches before invoking the handler). Read up to `HA_PROXY_MAX_BODY_SIZE` bytes from the blocking input stream. If the stream has more bytes, log at WARNING and return `false`.
4. **Build target URL:** `http://<leaderAddress><path>?<query>`, where path and query come from the exchange.
5. **Build the forwarded request** with the original HTTP method. Copy request headers except hop-by-hop (`Connection`, `Transfer-Encoding`, `Keep-Alive`, `Proxy-*`, `TE`, `Trailer`, `Upgrade`, `Host`, `Content-Length`) and `Authorization`. Inject:
   - `X-ArcadeDB-Cluster-Token: <HA_CLUSTER_TOKEN>`
   - `X-ArcadeDB-Forwarded-User: <username from HttpAuthSession>`

   This is the same cluster-forwarded-auth path `validateClusterForwardedAuth` already accepts on the peer side.
6. **Per-request timeout:** `HttpRequest.timeout(Duration.ofMillis(HA_PROXY_READ_TIMEOUT))`.
7. **Send** with `BodyPublishers.ofByteArray(body)` and `BodyHandlers.ofByteArray()`.
8. **Relay the response:** set exchange status from leader response. Copy response headers except hop-by-hop and `Content-Length` (Undertow recomputes). Write body bytes to `exchange.getResponseSender()`. Return `true`.

Exception handling in `tryProxy`:
- `HttpTimeoutException` -> return `false`, log WARNING.
- `ConnectException` / `IOException` -> return `false`, log WARNING.
- Any other exception -> return `false`, log SEVERE.

**Usage in `AbstractServerHttpHandler`:**
```java
} catch (final ServerIsNotTheLeaderException e) {
  if (!leaderProxy.tryProxy(exchange, e.getLeaderAddress())) {
    sendErrorResponse(exchange, 400, "Cannot execute command", e, e.getLeaderAddress());
  }
}
```
Applied identically to both catch sites (direct catch and `TransactionException` unwrap).

**Safety cap:** `HA_PROXY_MAX_BODY_SIZE` guards against a misbehaving client triggering OOM on a follower by streaming a huge body.

#### Tests

- **Unit** `LeaderProxyTest`: mock leader via an embedded Undertow test server that echoes headers back. Verify `X-ArcadeDB-Cluster-Token` is added, `Authorization` is stripped, `X-ArcadeDB-Forwarded-User` matches the authenticated session, hop-by-hop headers are removed, body bytes match, response status and headers are relayed.
- **Unit** loop prevention: request with existing `X-ArcadeDB-Cluster-Token` -> `tryProxy` returns false without touching the network.
- **Unit** timeout: mock leader sleeps longer than `HA_PROXY_READ_TIMEOUT`. Assert fallback.
- **Unit** oversize body: request body larger than `HA_PROXY_MAX_BODY_SIZE`. Assert fallback with WARNING log message containing "body too large".
- **IT** `RaftLeaderProxyIT` extends `BaseMiniRaftTest`: 3 nodes. Send `POST /api/v1/command/*` and `POST /api/v1/document/*` to a follower. Assert 200 OK and the document lands on all nodes via the leader.
- **IT** leader unreachable fallback: 3 nodes, kill the leader, send a command to a follower. Assert 400 with populated `leaderAddress` (no hang waiting for the dead leader).

---

### Item 3: Client-side HA failover

**Problem:** `DatabaseAbstractHandler` on the server already reads `X-ArcadeDB-Read-Consistency` + `X-ArcadeDB-Commit-Index` (the latter used as a read bookmark on the request side) and writes `X-ArcadeDB-Commit-Index` in the response (the last-applied index on the peer). `RemoteDatabase` on the client does not send these headers, does not capture the commit index, and does not retry on 503 during elections. Read-your-writes works server-to-server but not through `RemoteDatabase`.

Note: `X-ArcadeDB-Commit-Index` is used for both request (bookmark = "I need data up to this index") and response (actual = "last index the peer has applied"). Same header, different semantics by direction.

#### Files

- **New:** `network/src/main/java/com/arcadedb/remote/ReadConsistency.java`
- **Modified:** `network/.../remote/RemoteDatabase.java`
  - Add `readConsistency`, `lastCommitIndex`, `electionRetryCount`, `electionRetryDelayMs` fields
  - Add public setters/getters
  - Package-private `updateLastCommitIndex(long)`
- **Modified:** `network/.../remote/RemoteHttpComponent.java`
  - Inject headers on request
  - Capture `X-ArcadeDB-Commit-Index` from response
  - Split request execution into `executeOnce` + retry wrapper
  - Map 503 with `NeedRetryException` body to retry
  - Map 400 with `ServerIsNotTheLeaderException` body to leader retarget
- **Modified:** `engine/.../GlobalConfiguration.java`
  - `HA_CLIENT_ELECTION_RETRY_COUNT` (int, default 3)
  - `HA_CLIENT_ELECTION_RETRY_DELAY_MS` (long, default 2000)

#### Design

**New enum:**
```java
public enum ReadConsistency {
  EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE
}
```
Placed in `network/` so `RemoteDatabase` can reference it without a server-module dependency. A parallel enum already exists on the server side. The two live in different modules and are intentionally duplicated; consolidation is out of scope.

**`RemoteDatabase` changes:**
- `private volatile ReadConsistency readConsistency = ReadConsistency.EVENTUAL;`
- `private final AtomicLong lastCommitIndex = new AtomicLong(-1);`
- `private volatile int electionRetryCount;` initialized from `HA_CLIENT_ELECTION_RETRY_COUNT`
- `private volatile long electionRetryDelayMs;` initialized from `HA_CLIENT_ELECTION_RETRY_DELAY_MS`
- New API: `setReadConsistency`, `getReadConsistency`, `getLastCommitIndex`, `setElectionRetryCount`, `setElectionRetryDelay`, package-private `updateLastCommitIndex(long newValue)` which does `lastCommitIndex.accumulateAndGet(newValue, Math::max)` (monotonic, thread-safe).

**`RemoteHttpComponent` request-side injection:**
1. If `database.getReadConsistency() != EVENTUAL`, set `X-ArcadeDB-Read-Consistency: <value.name().toLowerCase()>` (server parses via `valueOf(toUpperCase())` and accepts lowercase per `HA_READ_CONSISTENCY` enum).
2. If `readConsistency == READ_YOUR_WRITES` and `lastCommitIndex >= 0`, set `X-ArcadeDB-Commit-Index: <lastCommitIndex>` (request-side bookmark).

**`RemoteHttpComponent` response-side capture:**
1. Read `X-ArcadeDB-Commit-Index` header if present.
2. Parse as long, call `database.updateLastCommitIndex(value)`.

**Retry loop:** extract the existing request execution body into a private `executeOnce(peer)` method. Wrap with:

```java
int attempt = 0;
while (true) {
  try {
    return executeOnce(currentPeer);
  } catch (final NeedRetryException e) {
    if (++attempt > electionRetryCount) throw e;
    Thread.sleep(electionRetryDelayMs);
  } catch (final ServerIsNotTheLeaderException e) {
    currentPeer = parseLeaderAddress(e);
    // retry immediately, same attempt count (leader retarget, not election wait)
  }
  // existing ConnectException / IOException peer-failover path stays as-is
}
```

**503 detection:** the existing response parser already extracts the exception class name from the error body. If the class is `NeedRetryException`, throw it. If the class is `ServerIsNotTheLeaderException`, parse `leaderAddress` from the body and throw the typed exception (which the outer catch handles).

**Backward compatibility:** zero. Callers that do not touch the new API keep `EVENTUAL`, which sends no headers and triggers no retry beyond the existing peer-failover path.

#### Tests

- **Unit** `ReadConsistencyHeadersTest`: mock `HttpURLConnection`. Verify headers are set/not-set per the `ReadConsistency` value. Verify `lastCommitIndex` updates monotonically from the response header and never goes backward.
- **Unit** `ElectionRetryTest`: mock 503 responses with `NeedRetryException` in the body. Verify retry count is respected, total elapsed time approximately `retryCount * delayMs`, eventual success is returned after recovery.
- **Unit** `LeaderFailoverTest`: mock 400 with `ServerIsNotTheLeaderException` + leader address. Verify the client re-targets that peer and succeeds on the retry.
- **IT** `RaftRemoteReadYourWritesIT` extends `BaseMiniRaftTest`: 3 nodes. Open `RemoteDatabase` on a follower, `setReadConsistency(READ_YOUR_WRITES)`. Write on the leader via one client, read on the follower via a second client that has observed the write's commit index. Assert the row is visible.
- **IT** `RaftRemoteElectionRetryIT`: 3 nodes, force a leadership step-down via `PostStepDownHandler`. Assert in-flight clients retry through the election window and see success.

---

### Item 4: Constant-time cluster token comparison

**Problem:** `AbstractServerHttpHandler.validateClusterForwardedAuth` compares tokens with `String.equals`, which short-circuits on the first mismatching character and leaks timing information.

#### Files

- **Modified:** `server/.../http/handler/AbstractServerHttpHandler.java`

#### Design

Replace line 305:
```java
if (clusterToken.isBlank() || !clusterToken.equals(providedToken)) {
```
with:
```java
if (clusterToken.isBlank() || !constantTimeEquals(clusterToken, providedToken)) {
```

Add a private static helper on the same class:
```java
private static boolean constantTimeEquals(final String a, final String b) {
  if (a == null || b == null) return false;
  final byte[] aBytes = a.getBytes(StandardCharsets.UTF_8);
  final byte[] bBytes = b.getBytes(StandardCharsets.UTF_8);
  return MessageDigest.isEqual(aBytes, bBytes);
}
```

`MessageDigest.isEqual` is documented to be time-constant on JDK 7+ including on length mismatches. Null guard runs before secret inspection.

Why not extract to a shared `SecurityUtils.constantTimeEquals`? Two call sites (this and `SnapshotHttpHandler`) is below the extraction threshold. Defer until a third appears.

#### Tests

- **Unit** `AbstractServerHttpHandlerTokenTest`: mock exchange. Set `HA_CLUSTER_TOKEN = "secret"`. Verify matching token authenticates, wrong-same-length token returns 401, wrong-different-length token returns 401, empty token returns 401, null token returns 401.
- No dedicated timing-attack test. Relies on the standard library guarantee.

---

## Phase B: Operational Hardening

### Item 5: Concurrent snapshot throttling

**Problem:** Nothing limits concurrent snapshot downloads. A mass follower restart can saturate the leader's NIC.

#### Files

- **Modified:** `ha-raft/.../raft/SnapshotHttpHandler.java`
- **Modified:** `engine/.../GlobalConfiguration.java`
  - `HA_SNAPSHOT_MAX_CONCURRENT` (int, default 2)

#### Design

Static `Semaphore semaphore = new Semaphore(HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger())`. Read once at class load.

`handleRequest()`:
```java
if (!semaphore.tryAcquire()) {
  sendErrorResponse(exchange, 503, "Too many concurrent snapshots", null, null);
  return;
}
try {
  // existing snapshot serving logic
} finally {
  semaphore.release();
}
```

503 lets Ratis retry the snapshot install later.

#### Tests

- **Unit** `SnapshotThrottleTest`: spawn 4 concurrent handlers against a mock exchange with `HA_SNAPSHOT_MAX_CONCURRENT=2`. Assert exactly 2 acquire and serve, 2 return 503. Assert all permits released after handlers complete.
- **IT** piggyback on `RaftFullSnapshotResyncIT` (add a variant): trigger 4 concurrent snapshot installs. Assert 2 succeed, 2 are rejected with 503, Ratis retries the rejected ones to success.

---

### Item 6: Symlink rejection in snapshot ZIPs

**Problem:** `SnapshotHttpHandler` walks database files without checking for symlinks. A misconfigured deploy with a symlink in the database directory could leak arbitrary host files into the ZIP.

#### Files

- **Modified:** `ha-raft/.../raft/SnapshotHttpHandler.java`

#### Design

Inside the loop that writes each `ComponentFile` to the `ZipOutputStream`, check before writing:
```java
final Path filePath = file.toPath();
if (Files.isSymbolicLink(filePath)) {
  LogManager.instance().log(this, Level.WARNING, "Skipping symlink in snapshot: %s", filePath);
  continue;
}
```
Defense in depth. ArcadeDB should never create symlinks under the database dir, but an external deploy might.

#### Tests

- **Unit** `SnapshotSymlinkTest`: create a temp database directory containing a regular file plus a symlink pointing outside the directory. Run the ZIP builder. Verify the symlink is not in the resulting ZIP entries, verify the WARNING is logged, verify the regular file is included.

---

### Item 7: gRPC flow control window tuning

**Problem:** Ratis default gRPC flow control window may be too small for catch-up replication after long partitions. No config lever exists.

#### Files

- **Modified:** `ha-raft/.../raft/RaftHAServer.java`
  - In `start()` (and whatever helper builds `RaftProperties`): call `GrpcConfigKeys.setFlowControlWindow(props, SizeInBytes.valueOf(HA_GRPC_FLOW_CONTROL_WINDOW.getValueAsLong()))`
- **Modified:** `engine/.../GlobalConfiguration.java`
  - `HA_GRPC_FLOW_CONTROL_WINDOW` (long bytes, default 4 * 1024 * 1024)

#### Design

Single-line Ratis config set on `RaftProperties`. Because `restartRatisIfNeeded()` in item 1 rebuilds `RaftProperties` from scratch via the same helper, the window is automatically re-applied on recovery.

#### Tests

- **Unit** `RaftHAServerFlowControlTest`: reflect on `RaftProperties` after `RaftHAServer.start()`. Assert the flow-control-window value matches `HA_GRPC_FLOW_CONTROL_WINDOW`.

---

### Item 8: HTTP proxy read timeout

Covered in Item 2. The config keys are:
- `HA_PROXY_READ_TIMEOUT` (long ms, default 30000)
- `HA_PROXY_CONNECT_TIMEOUT` (long ms, default 5000)
- `HA_PROXY_MAX_BODY_SIZE` (int bytes, default 16 * 1024 * 1024)

`LeaderProxy` reads them in its constructor.

---

## Implementation Order

| Step | Item | Depends on |
|------|------|-----------|
| 0 | Baseline: run full test suite on current `ha-redesign`, capture pass count | - |
| 1 | Item 4: constant-time token (smallest, no risk) | - |
| 2 | Item 1: Health Monitor + test hook | - |
| 3 | Item 7: gRPC flow control window | - |
| 4 | Item 2 + Item 8: Leader proxy with timeouts and body cap | - |
| 5 | Item 3: Client-side failover (ReadConsistency, retry loop) | Item 2 for IT validation |
| 6 | Item 5: Snapshot throttling | - |
| 7 | Item 6: Symlink rejection | - |
| 8 | Full validation: all tests + chaos suite | All items |

Steps 1, 2, 6, 7 are independent and could run in parallel across feature branches inside `ha-redesign`. Steps 3 and 4 can also run in parallel but step 5 benefits from having step 4 landed first (for the end-to-end `RaftRemoteElectionRetryIT`). Step 8 is the final gate.

## Success Criteria

- All pre-existing tests pass (unit, IT, e2e-ha chaos suite).
- Every new unit and IT test passes.
- `RaftHealthMonitorRecoveryIT` recovers a forced-CLOSED follower within 10 seconds.
- `RaftLeaderProxyIT` commands sent to a follower land on the leader and replicate to all nodes.
- `RaftRemoteReadYourWritesIT` demonstrates read-your-writes through two independent `RemoteDatabase` instances.
- `SnapshotThrottleTest` proves the semaphore rejects beyond capacity and releases on completion.
- `AbstractServerHttpHandlerTokenTest` proves token validation uses the constant-time path.
- `RaftHAInsertBenchmark` throughput matches or beats current baseline (no regression from the leader proxy catch path).

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Health monitor recovery corrupts state machine | Low | High | `SimpleStateMachineStorage` replays from disk; Ratis `RECOVER` option is designed for this; unit test asserts clean re-init |
| Leader proxy hangs on dead leader | Medium | High | `HttpRequest.timeout` + `connectTimeout`, IT kills leader and asserts fallback |
| Client retry loop amplifies load during an election | Medium | Medium | Default 3 retries * 2s = 6s worst case; tunable per-client; documented |
| Proxy body buffering OOMs a follower | Low | High | `HA_PROXY_MAX_BODY_SIZE` cap, fallback to 400 on oversize |
| Symlink skip breaks a legitimate deploy that uses symlinks | Low | Low | Warning logged; rejecting is the safer default; document in release notes |
| `HA_GRPC_FLOW_CONTROL_WINDOW` too small causes stall | Low | Medium | Default 4MB matches apache-ratis; tunable via config |
| Race between health monitor tick and normal `stop()` | Low | Medium | `shutdownRequested` flag checked at start of tick and inside `restartRatisIfNeeded`; separate recovery lock |
