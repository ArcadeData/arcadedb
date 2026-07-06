# Review items intentionally skipped - PR #5056 (base SHA 96762c7f)

Feedback from the claude bot review that was assessed and **not** applied, with rationale.
All items were flagged by the reviewer itself as non-blocking / optional.

## Skipped (with rationale)

1. **`AtomicInteger`/`LongAdder` instead of `ConcurrentHashMap.size()` in the eviction loop.**
   Reviewer verdict: "At the salt-cache's scale this is negligible... I wouldn't bother here."
   The cache is bounded to a small size (default 64) and eviction runs only on a genuinely new key.
   Adding a separate counter adds a second source of truth to keep in sync with the map for no
   measurable benefit. Skipped.

2. **LRU -> approximate-FIFO behavioral change.**
   Reviewer verdict: "With the default size 64 and typical small user counts this is a non-issue,
   just flagging the semantic change." A hot credential can be evicted while still hot only once the
   working set exceeds `maxSize`; a miss simply recomputes. No correctness impact. Documented as
   best-effort in `ConcurrentSaltCache`'s class javadoc. Skipped.

3. **`ConcurrentSaltCache.isEmpty()` now unused by production code.**
   Reviewer verdict: "effectively dead API, fine to keep." It is part of the small, cohesive cache
   API and is exercised by `ConcurrentSaltCacheTest`. Kept.

## Applied in this cycle (for the record)
- Reuse `MessageDigest` via a `ThreadLocal` (reset per use) to avoid a `getInstance` provider lookup
  on the Basic-auth hot path.
- Added a cache-disabled (`SERVER_SECURITY_SALT_CACHE_SIZE=0`) regression test.
- Documented the residual offline-brute-force risk of the SHA-256 key in code and the tracking doc.
