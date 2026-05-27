# Fix #4367 - LockManager.tryLock never re-attempts putIfAbsent after await timeout

## Root Cause

In `LockManager.tryLock`, the do-while loop contained:

```java
if (!currentLock.lock.await(timeout, TimeUnit.MILLISECONDS))
    continue;   // BUG: skips putIfAbsent retry
currentLock = lockManager.putIfAbsent(resource, lock);
```

When `await(timeout, MILLISECONDS)` returns `false` (the wait timed out), `continue`
jumps back to the top of the loop, bypassing the `putIfAbsent` call entirely.
A second bug: `await` always uses the full `timeout` instead of the remaining time,
so the total wait can be much longer than the caller requested (up to 2x per extra
loop iteration caused by OS scheduling jitter).

Together these cause:
- A waiter that times out on an already-free lock keeps busy-looping and returns NO
  even though it could acquire the lock immediately.
- Each await re-uses the full `timeout` value, not the remaining window.

## Fix

1. Remove the `continue` so that `putIfAbsent` is always attempted after any `await` return.
2. Track remaining time using `System.nanoTime()` (monotonic, immune to NTP/clock changes)
   and pass `remaining` to each `await` call so the total wait never exceeds `timeout`.
3. Add `if (remaining <= 0) break;` guard so a zero/negative budget exits immediately.

## Affected file

`engine/src/main/java/com/arcadedb/utility/LockManager.java` (lines 64-82)

## Tests added

In `LockManagerTest.java`:

- `tryLockSucceedsWhenReleasedWithinTotalTimeout`: Thread A holds the lock, Thread B
  waits with a 2-second budget, Thread A releases at ~100ms; asserts Thread B gets YES.
- `tryLockDoesNotOverwaitBeyondRequestedTimeout`: Thread A holds indefinitely, Thread B
  requests 300ms; asserts elapsed < 550ms (not the ~600ms a double full-timeout iteration
  would produce).

## Verification

```
mvn test -pl engine -Dtest=LockManagerTest
```

Result: 17 tests, 0 failures.

---

## PR

https://github.com/ArcadeData/arcadedb/pull/4375

## Review Cycles

### Cycle 1 — SHA `8778842b`

gemini-code-assist reviewed with COMMENTED state. Two actionable items:

1. **Use `System.nanoTime()` instead of `System.currentTimeMillis()`** (LockManager.java:69)
   Applied: changed `startTime`, remaining-time calculation, and loop condition to use
   `System.nanoTime()` with `TimeUnit.NANOSECONDS.toMillis()` for monotonic elapsed time.

2. **Assert return value of `waiterEntered.await(1, SECONDS)` in test** (LockManagerTest.java:243)
   Applied: changed to `assertThat(waiterEntered.await(1, TimeUnit.SECONDS)).isTrue()` for
   fast-fail on thread-setup failure.

Both applied in commit `24aa5074`. All 17 tests continue to pass.

### Cycle 2 — SHA `24aa5074`

No review received. This is consistent with the known pattern that `gemini-code-assist`
does not re-review follow-up pushes in this repository.

## Final State

`timeout` — gemini reviewed cycle 1 only; follow-up push (cycle 2) received no bot review.
All feedback from cycle 1 was addressed. The PR is ready for developer review and merge.
