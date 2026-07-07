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

## Latent bug also fixed
With the cache disabled (`arcadedb.server.securitySaltCacheSize=0`) the old code held
`Collections.emptyMap()` and still ran an unconditional `saltCache.put(...)` at the end of
`encodePassword`, throwing `UnsupportedOperationException` on every encode. The new
`saltCache != null` guard fixes that; `shouldEncodeAndMatchWithCacheDisabled` locks in the regression.

## PR
https://github.com/ArcadeData/arcadedb/pull/5056

## Review cycles
- Cycle 1 - SHA `96762c7f7`: Gemini flagged a possible int overflow in `ConcurrentSaltCache`'s
  initial-capacity calc for very large `maxSize`. Applied: cap the initial capacity at `1 << 30`;
  added `shouldNotOverflowInitialCapacityForVeryLargeMaxSize` regression.
- Cycle 2 - SHA `b7c6ecdf5`: Claude (LGTM) suggested reusing the SHA-256 `MessageDigest` on the
  hot path, adding a cache-disabled regression, and documenting the residual risk. Applied all three
  (`ThreadLocal<MessageDigest>`, `shouldEncodeAndMatchWithCacheDisabled`, residual-risk note).
- Cycle 3 - SHA `c23d28652`: Claude (LGTM) noted the explicit `digest.reset()` is redundant (one-shot
  `digest(byte[])` self-resets) and that an internal review-notes file should not be committed.
  Applied: dropped the `reset()`; removed the notes file. Gemini re-posted the cycle-1 overflow
  finding, already fixed - no change.
- Cycle 4 - SHA (this commit): Claude (LGTM, "the fix is correct") suggested an explicit FIFO-vs-LRU
  note in the class Javadoc. Applied. Remaining items (`map.size()` micro-cost, `@Tag("slow")`) are
  non-blocking and declined with rationale; they are already fast/negligible at the default size 64.
  Gemini re-posted the same already-fixed overflow finding a third time (stale).

## Final state
clean-approval - both reviewers converged on LGTM; all actionable feedback applied, remaining nits are
non-blocking and either already addressed or declined with rationale.

## Residual security note
The cache key is `SHA-256(password$salt$iterations)`. The salt and iteration count are also embedded
in the cached value (and the salt is not secret), so a full heap dump still lets an attacker
brute-force low-entropy passwords with one fast SHA-256 per guess. This is inherent to caching a slow
KDF under any recomputable-without-plaintext key: strictly better than the previous plaintext-key
exposure, but weaker than the PBKDF2 hash it protects. Prefer session-token auth for hot clients, and
set `arcadedb.server.securitySaltCacheSize=0` to disable the cache entirely when even this residual
exposure is unacceptable.
