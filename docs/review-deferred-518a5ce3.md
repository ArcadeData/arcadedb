# Review Deferred Items - HEAD 518a5ce3

## Cycle 1 - gemini-code-assist review on PR #4378

### Applied

**Comment 1 (HIGH) - currentServer/currentPort split-state race:**

Gemini correctly pointed out that adding `volatile` to the individual `currentServer` and
`currentPort` fields does not guarantee atomicity of combined reads across the two fields.
A concurrent reader of `getUrl()` could observe a new `currentServer` with a stale `currentPort`.

Action taken: reverted `volatile` from `currentServer` and `currentPort`. The class is
explicitly documented as "not thread safe" - making individual fields volatile without
providing atomicity of the pair creates false confidence. The core fix (`leaderServer`
snapshot + null guard) requires only `leaderServer` to be volatile, since it is the specific
field involved in the null-read bug from issue #4372.

### Deferred

**Comment 2 (HIGH) - currentReplicaServerIndex / replicaServerList not thread-safe:**

Gemini noted that `currentReplicaServerIndex` is incremented non-atomically and
`replicaServerList` is a plain `ArrayList` with no synchronization.

Rationale for deferring:
1. `RemoteHttpComponent` is documented "not thread safe" - these are pre-existing concerns,
   not introduced by this PR.
2. This PR addresses the specific bug from issue #4372 (leaderServer null-read). Broadening
   to full thread-safety would require AtomicInteger + CopyOnWriteArrayList across many
   call sites - appropriate for a separate, dedicated issue.
3. Filing a follow-up issue is recommended to track the broader concurrency hardening.
