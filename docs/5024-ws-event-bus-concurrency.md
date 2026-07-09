# Issue #5024 - WebSocket event bus concurrency & serialization fixes

## Symptom
Five interacting defects in `server` WebSocket event bus:
1. Watcher thread self-deadlocks when the last subscriber is a zombie (`publish` -> `unsubscribeAll` -> `stopDatabaseWatcher` -> `shutdown().await()` on its own thread).
2. Check-then-act in `subscribe` starts duplicate watcher threads -> events published twice; symmetric `unsubscribe` race stops a watcher underneath a live subscribe.
3. `EventWatcherSubscription` mutates plain `HashSet` values read concurrently by the watcher thread.
4. `publish` zombie list is a plain `ArrayList` mutated from async send callbacks -> `ConcurrentModificationException` / lost zombie.
5. Event re-serialized once per subscriber (`event.toJSON()` inside the loop) + a new callback per subscriber -> O(N x record size).

## Root cause
Non-atomic map coordination and non-thread-safe collections shared across the watcher thread and Undertow IO threads, plus per-subscriber serialization on the single watcher thread.

## Fix
- `DatabaseEventWatcherThread.shutdown()`: skip `await()` when `Thread.currentThread() == this` (self-shutdown returns immediately; `run()`'s finally still unregisters listeners as the loop unwinds).
- `WebSocketEventBus`: per-database lock guards subscriber-presence + watcher lifecycle transitions; watcher creation via `computeIfAbsent`; the blocking `shutdown()` join is performed OUTSIDE the lock (watcher removed from the map under lock, joined after release) to avoid a cross-thread deadlock with the watcher's own `unsubscribeAll`.
- `EventWatcherSubscription`: `ConcurrentHashMap.newKeySet()` instead of `HashSet`.
- `publish`: `ConcurrentLinkedQueue` for zombies drained via poll-loop after the send loop; `final String json = event.toJSON()` computed once before the loop; a single shared `WebSocketCallback` reused for all subscribers.

## Tests
- `Issue5024WebSocketEventBusConcurrencyTest` (unit, embedded DB):
  - self-shutdown on the watcher thread terminates within a timeout (Defect 1, deterministic fail-before).
  - `publish` serializes the event exactly once regardless of subscriber count (Defect 5, deterministic fail-before).
  - concurrent `add`/`isMatch` on a subscription is thread-safe and correct (Defect 3).
  - `publish` drains zombies without `ConcurrentModificationException` under concurrent callback adds (Defect 4).
- `WebSocketEventBusIT` new method: concurrent subscribes to the same DB create exactly one watcher (each client receives each event exactly once) (Defect 2).

## Impact
`server` module only. No API change. Removes a leaked/hung watcher thread, duplicate WS frames, and N-fold serialization on the change-stream hot path.
