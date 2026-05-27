# Fix #4367 - LockManager.tryLock never re-attempts putIfAbsent after await timeout

## Root Cause

In `LockManager.tryLock`, the do-while loop contains:

```java
if (!currentLock.lock.await(timeout, TimeUnit.MILLISECONDS))
    continue;   // BUG: skips putIfAbsent retry
currentLock = lockManager.putIfAbsent(resource, lock);
```

When `await(timeout, MILLISECONDS)` returns `false` (the wait timed out), `continue`
jumps back to the top of the loop, bypassing the `putIfAbsent` call entirely.
A second bug: `await` always uses the full `timeout` instead of the remaining time,
so the total wait can be much longer than the caller requested.

Together these cause:
- A waiter that times out on an already-free lock keeps busy-looping and returns NO
  even though it could acquire the lock immediately.
- Each await re-uses the full `timeout` value, not the remaining window.

## Fix

1. Remove the `continue` so that `putIfAbsent` is always attempted after any `await` return.
2. Pass the remaining time (`timeout - elapsed`) to each `await` call so the total wait
   never exceeds `timeout`.

## Affected file

`engine/src/main/java/com/arcadedb/utility/LockManager.java` (lines 66-80)

## Test

New test `tryLockSucceedsWhenLockReleasedAfterTimeout` in `LockManagerTest.java`:
- Thread A holds the lock.
- Thread B calls `tryLock` with a 300ms timeout.
- Thread A releases the lock at ~150ms (after Thread B's first await would have expired
  with a short per-iteration wait that we can observe).
- With the bug, Thread B misses the release and returns NO; with the fix it returns YES.

## Verification

```
mvn test -pl engine -Dtest=LockManagerTest
```
