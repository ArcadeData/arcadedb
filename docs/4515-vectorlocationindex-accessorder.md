# Issue #4515 - VectorLocationIndex LinkedHashMap accessOrder=true iterated without holding wrapper monitor

URL: https://github.com/ArcadeData/arcadedb/issues/4515

## Problem

`VectorLocationIndex` (bounded mode, `maxSize > 0`) backs its cache with:

```java
Collections.synchronizedMap(new LinkedHashMap<>(initialCapacity, 0.75f, true) { ... })
```

`accessOrder=true` turns every `get()` into a structural modification of the
LinkedHashMap's doubly-linked list. Two methods then iterate the map without
holding the wrapper monitor required by `Collections.synchronizedMap`:

- `getActiveVectorIds()` streams `keySet()` and calls `locations.get(id)`
  inside the filter. Each `get()` mutates the very map being iterated, so a
  concurrent search/insert load throws `ConcurrentModificationException` or
  silently corrupts the linked list (lost/duplicated entries -> data loss).
- `getAllVectorIds()` streams `keySet()` without the monitor.
- `getActiveCount()` streams `values()` without the monitor.

## Root cause

1. `accessOrder=true` makes reads structural mutations.
2. `Collections.synchronizedMap` requires `synchronized (map)` around any
   iteration (including stream pipelines over `keySet()`/`values()`); the
   streaming methods omit it.

## Fix

Per the issue's suggested fix (combine the two safe options):

1. Drop access-order: use insertion-order LRU
   (`new LinkedHashMap<>(initialCapacity, 0.75f, false)`). The eviction policy
   stays LRU-by-insertion which is the conventional, correct LRU for this
   cache and removes the mutate-on-`get()` hazard entirely. Reads no longer
   reorder the list, so a concurrent `get()` can never corrupt an iteration.
2. Snapshot under the wrapper monitor: every method that iterates the
   synchronized map (`getAllVectorIds`, `getActiveVectorIds`, `getActiveCount`)
   takes `synchronized (locations)` and copies the needed data into a local
   array/list before streaming, satisfying the `Collections.synchronizedMap`
   contract.

Both changes are dependency-free (no Caffeine) and preserve existing public
behavior and LRU eviction semantics.

## Verification

- New regression test `VectorLocationIndexConcurrencyTest` drives concurrent
  `get`/`getActiveVectorIds`/`getActiveCount` against a bounded index and
  asserts no exception and consistent results. Before the fix it reproduced
  `ConcurrentModificationException` (verified); after the fix all 4 methods
  pass, including an unlimited-mode (`ConcurrentHashMap`) concurrency case.
- Existing vector index tests pass: `LSMVectorIndexTest`,
  `LSMVectorIndexSimpleUpdateTest`, `LSMVectorIndexConcurrentUpdateTest`,
  `LSMVectorIndexRebuildTest`, `LSMVectorIndexPersistenceTest`,
  `LSMVectorIndexRecoveryTest` (66 tests, 0 failures).

## Pull request

https://github.com/ArcadeData/arcadedb/pull/4528

## Review cycles

- cycle 1: `abb24dc` - initial fix (accessOrder=false + always-synchronized
  snapshot in the three iterating methods). Gemini reviewed with one high-priority,
  actionable point: in unlimited mode the backend is a `ConcurrentHashMap`, so
  synchronizing on it and snapshotting keys into an `int[]` adds O(N) allocation
  and GC pressure for no concurrency benefit. The `claude` bot did not post a
  review within the 15-minute window.
- cycle 2: `498dc3e` - addressed Gemini's feedback: gated the synchronized
  snapshot on `maxSize > 0` (bounded `synchronizedMap`/`LinkedHashMap` backend
  only) and kept the lazy `ConcurrentHashMap` stream for unlimited mode; added
  an unlimited-mode concurrency regression test. No bot review arrived within
  the polling window (neither `gemini-code-assist` re-review nor `claude`).

## Deferred items

None. All actionable review feedback was applied; no comments were unclear or
disputed.

## Final state

`timeout` - the review loop's gating reviewer set (`claude` + `gemini-code-assist`)
never both responded on the same head SHA within the per-cycle 15-minute window.
The `claude` bot was unresponsive across both cycles. All actionable feedback
received (Gemini, cycle 1) was addressed in cycle 2. The PR is left open for the
developer; merge remains the developer's responsibility.
