# Port Remaining Apache-Ratis Gaps to ha-redesign

**Date:** 2026-04-18
**Branch:** ha-redesign
**Source:** apache-ratis branch (commits ed7967340..a494fb676)

## Context

The ha-redesign branch has already ported the bulk of the apache-ratis HA stack:
linearizable reads, snapshot hardening, idempotency cache, gRPC security,
step-down retry loop, and TEST_POST_REPLICATION_HOOK. Three production-code gaps
and several test/hardening items remain.

## Approach

Incremental per-feature commits in dependency order. Each commit is isolated,
reviewable, and testable independently.

---

## Commit 1: RaftGroupCommitter Timeout Refactor

### Problem

`submitAndWait(byte[] entry, long timeoutMs)` accepts an explicit timeout from
callers. Callers can pass values shorter than a Raft round-trip, causing
spurious `QuorumNotReachedException` failures.

### Change

- Remove the `timeoutMs` parameter from `submitAndWait()`.
- Compute timeout internally as `2 * quorumTimeout` (field already available).
- The grace-period logic (wait an additional `quorumTimeout` if the entry was
  already dispatched) remains unchanged but uses the internally derived value.
- Update all callers in `RaftReplicatedDatabase` and `RaftTransactionBroker` to
  drop the timeout argument.
- Update `RaftGroupCommitterTest` to match the new signature.

### Files Modified

- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java`

---

## Commit 2: Snapshot Swap 503 Protection

### Problem

During snapshot installation a follower closes and reopens databases. HTTP
requests arriving during this window produce uncontrolled errors (NPE or
"database not found") instead of a clean retry signal.

### Change

**ArcadeDBServer:**
- Add `AtomicBoolean snapshotInstallInProgress` field.
- Add `setSnapshotInstallInProgress(boolean)` and
  `isSnapshotInstallInProgress()` methods.

**SnapshotInstaller:**
- Call `server.setSnapshotInstallInProgress(true)` BEFORE closing databases.
- Call `server.setSnapshotInstallInProgress(false)` after reopen completes
  (in a finally block to ensure cleanup on failure).

**AbstractServerHttpHandler:**
- Early check in `handleRequest()`: if `server.isSnapshotInstallInProgress()`,
  return HTTP 503 with `Retry-After: 5` header immediately.

**RaftLogEntryCodec:**
- Add corrupted-stream bounds checks on entry length fields.
- Add trailing-byte validation to detect truncated entries.

### Files Modified

- `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java`
- `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`

### Tests

- `SnapshotInstallInProgressResponseIT` - verifies 503 during swap window

---

## Commit 3: RemoteHttpComponent Watchdog Timeout

### Problem

`httpCommand()` uses `HttpClient.send()` synchronously. A known JDK bug with
HTTP/2 stream resets can cause the call to block indefinitely even though the
TCP connection is alive. The existing `NETWORK_SOCKET_TIMEOUT` does not catch
this because only the HTTP/2 stream is stalled.

### Change

- Add `sendWithWatchdog(HttpRequest, HttpResponse.BodyHandler)` method that
  calls `HttpClient.sendAsync()` and waits via `.get(timeout)` on the
  `CompletableFuture`.
- Timeout derived from `NETWORK_SOCKET_TIMEOUT` value.
- On timeout, cancel the future and throw `TimeoutException`, which the
  existing retry logic in `httpCommand()` already handles.
- Replace the synchronous `send()` call in `httpCommand()` with
  `sendWithWatchdog()`.

### Files Modified

- `network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java`

---

## Commit 4: Tests and Incremental Hardening

### New Tests to Port

- `SnapshotSymlinkProtectionTest` - symlink escape detection in zip extraction
- `SnapshotHttpHandlerConcurrencyIT` - concurrent snapshot download under
  semaphore gating
- `ClusterTokenProviderTest` - cluster token validation edge cases
- `ReplicatedDatabasePhase2RecoveryTest` - phase-2 failure recovery paths
- `RaftLoadConvergenceIT` updates - replication convergence under load

### Incremental Hardening

**PostVerifyDatabaseHandler:**
- Response byte limit enforcement.
- Expanded path traversal checks.

**SnapshotInstaller:**
- Extract `MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES` (10GB) constant replacing inline
  value for zip-bomb defense.

**RaftGroupCommitter:**
- Minor cleanup in batch flushing logic after timeout signature change.

### Files Modified

- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/` (new test files)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java`
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java`

---

## Verification

Each commit must:
1. Compile cleanly (`mvn clean install -DskipTests` from project root)
2. Pass its own new/modified tests
3. Pass existing HA tests (`cd ha-raft && mvn test`)
