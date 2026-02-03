# Bidi Streaming Flow Control Fix Design

**Date:** 2026-02-03
**Goal:** Fix timing issues in `ingestBidi` that cause test timeouts, enabling the 5 disabled tests in `BidiIngestionIT.java`

## Background

The `grpc-client` module has 5 disabled integration tests for bidirectional streaming ingestion:
- `ingestBidi_basicFlow`
- `ingestBidi_backpressure`
- `ingestBidi_largeVolume`
- `ingestBidi_withTransaction`
- `ingestBidi_upsertOnConflict`

All are disabled with: `@Disabled("Bidi streaming has server-side timing issues that cause timeouts - needs investigation")`

## Root Cause Analysis

Three issues identified in `RemoteGrpcDatabase.ingestBidiCore()`:

### Issue 1: Missing `drain()` Call After STARTED Response

```java
case STARTED -> {
  /* ok */  // Does nothing - should call drain()
}
```

After receiving STARTED, the client should call `drain()` to continue sending chunks. Currently relies on BATCH_ACK to drive progress.

### Issue 2: Thread Safety Violation

The `sendCommitIfNeeded` runnable executes on a **scheduler thread**:
```java
final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(...);
scheduler.schedule(sendCommitIfNeeded, ackGraceMillis, TimeUnit.MILLISECONDS);
```

But it calls `req.onNext()` and `req.onCompleted()` while `drain()` runs on the gRPC callback thread and also calls `req.onNext()`. **gRPC StreamObserver is NOT thread-safe**.

### Issue 3: Stall When `isReady()` Returns False

When `drain()` is called but `isReady()` returns false, it returns immediately. The code relies on `onReadyHandler` to resume, but there's a race condition where the ready state changes between check and return, causing `onReadyHandler` to not fire.

## Solution Design

### Fix 1: Add Synchronization Lock

Add a lock object to protect all stream operations:

```java
final Object streamLock = new Object();
```

### Fix 2: Synchronized `drain()` Method

Wrap the drain body in synchronized block with early-exit for committed state:

```java
private void drain() {
  synchronized (streamLock) {
    if (commitSent.get()) return;

    if (!req.isReady())
      return;

    if (!started) {
      req.onNext(InsertRequest.newBuilder().setStart(Start.newBuilder().setOptions(effectiveOpts)).build());
      started = true;
      req.request(1);
    }

    while (req.isReady() && (sent.get() - acked.get()) < maxInflight) {
      final int start = cursor.get();
      if (start >= protoRows.size())
        break;

      final int end = Math.min(start + chunkSize, protoRows.size());
      final var slice = protoRows.subList(start, end);

      final var chunk = InsertChunk.newBuilder()
          .setSessionId(sessionId)
          .setChunkSeq(seq.getAndIncrement())
          .addAllRows(slice)
          .build();

      req.onNext(InsertRequest.newBuilder().setChunk(chunk).build());
      cursor.set(end);
      sent.incrementAndGet();
    }

    if (cursor.get() >= protoRows.size()) {
      if (acked.get() >= sent.get())
        sendCommitIfNeeded.run();
      else
        armAckGraceTimer.run();
    }
  }
}
```

### Fix 3: Synchronized `sendCommitIfNeeded`

```java
final Runnable sendCommitIfNeeded = () -> {
  synchronized (streamLock) {
    if (commitSent.compareAndSet(false, true)) {
      try {
        final ClientCallStreamObserver<InsertRequest> r = observerRef.get();
        if (r != null) {
          r.onNext(InsertRequest.newBuilder().setCommit(Commit.newBuilder().setSessionId(sessionId)).build());
          r.onCompleted();
        }
      } catch (Throwable ignore) {
        /* best effort */
      }
    }
  }
};
```

### Fix 4: Add `drain()` Call in STARTED Handler

```java
case STARTED -> {
  drain();  // Resume sending after session established
}
```

## Files to Modify

| File | Change |
|------|--------|
| `grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java` | Add `streamLock`, synchronize `drain()` and `sendCommitIfNeeded`, add `drain()` in STARTED case |
| `grpc-client/src/test/java/com/arcadedb/remote/grpc/BidiIngestionIT.java` | Remove `@Disabled` annotations from 5 tests |

## Implementation Tasks

### Task 1: Add synchronization to ingestBidiCore

**Files:**
- Modify: `grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java:1384-1626`

**Steps:**
1. Add `final Object streamLock = new Object();` after line 1423 (with other state variables)
2. Wrap the `drain()` method body (lines 1485-1517) in `synchronized (streamLock) { ... }`
3. Add early-exit `if (commitSent.get()) return;` at start of synchronized block
4. Wrap the `sendCommitIfNeeded` lambda body (lines 1438-1449) in `synchronized (streamLock) { ... }`
5. Add `drain();` call inside the `case STARTED ->` block (line 1522-1524)

### Task 2: Enable disabled tests

**Files:**
- Modify: `grpc-client/src/test/java/com/arcadedb/remote/grpc/BidiIngestionIT.java`

**Steps:**
1. Remove `@Disabled(...)` annotation from `ingestBidi_basicFlow` (line 135)
2. Remove `@Disabled(...)` annotation from `ingestBidi_backpressure` (line 148)
3. Remove `@Disabled(...)` annotation from `ingestBidi_largeVolume` (line 161)
4. Remove `@Disabled(...)` annotation from `ingestBidi_withTransaction` (line 174)
5. Remove `@Disabled(...)` annotation from `ingestBidi_upsertOnConflict` (line 188)

### Task 3: Verify all tests pass

**Steps:**
1. Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client -Dtest="BidiIngestionIT" -DfailIfNoTests=false`
2. Expected: All 8 tests pass (3 existing + 5 newly enabled)
3. Run full grpc-client test suite: `mvn test -pl grpc-client`
4. Verify no regressions

## Expected Outcome

- All 5 previously disabled bidi streaming tests pass
- No thread safety issues or race conditions
- Robust flow control that doesn't stall under various timing conditions

## Out of Scope

- Server-side changes (the server implementation appears correct)
- Performance optimization (focus is on correctness first)
- Additional test coverage beyond enabling existing tests
