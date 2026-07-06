# Issue #5030 - Salt cache stores plaintext passwords as keys and serializes all authentication on a single lock

## Symptom
`ServerSecurity.saltCache` keys the entry on the cleartext password
(`password + "$" + salt + "$" + iterations`), so plaintext credentials live as reachable
map keys in a heap-resident LRU cache long after the auth call returns (heap/core dump / swap
exposure). The cache is a `Collections.synchronizedMap(new LRUCache<>(size))`; `LRUCache` is
access-ordered, so even a `get` hit is a structural mutation that takes the single monitor -
every Basic-auth request serializes on one lock (lock convoy across ~500 Undertow workers).

## Root cause
1. Cache key = raw password string, retained as a live map key.
2. Access-ordered `LinkedHashMap` behind a global `synchronizedMap` monitor; reads mutate order
   and therefore contend on the same lock as writes.

## Fix
- New `ConcurrentSaltCache` (server security package): `ConcurrentHashMap` storage +
  `ConcurrentLinkedQueue` for approximate-FIFO bounded eviction. `get` is lock-free; no global
  monitor on the auth hot path.
- Key the cache on a SHA-256 hex digest of `password + "$" + salt + "$" + iterations` so the
  plaintext password is never reachable as a map key.

## Tests
- `ConcurrentSaltCacheTest` - bounded eviction, get/put correctness, lock-free reads.
- `ServerSecuritySaltCacheTest` - (1) plaintext never present in cache keys after
  `encodePassword`; (2) cache still returns identical encoded hash for same
  password+salt+iterations; (3) concurrent `encodePassword` correctness.

## Impact
Server module only. `passwordMatch` / `encodePassword` semantics unchanged (same encoded output).
Removes plaintext-in-heap exposure and the single-lock auth bottleneck.
