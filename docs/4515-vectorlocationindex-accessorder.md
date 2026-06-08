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
  asserts no exception and consistent results.
- Existing vector index tests must still pass.
