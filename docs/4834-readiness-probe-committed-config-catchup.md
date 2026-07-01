# Issue #4834: Readiness probe reports Ready before node is in committed Raft config or caught up

## Problem

`/api/v1/ready` returns 204 as soon as `ArcadeDBServer` status is `ONLINE`. With
`arcadedb.server.readinessRequiresHA=true` the handler additionally required only
`ELECTION_STATUS.DONE` (a leader is known). Neither condition guarantees that:

1. the local peer is a member of the **committed** Raft configuration, nor
2. the local applied index has **caught up** to the commit index.

During a Kubernetes StatefulSet RollingUpdate, a restarted follower (with wiped Raft
storage) advertises Ready with an empty/lagging log before catch-up. K8s then terminates
the next pod, the committed-state replica count drops, and the write quorum is lost
mid-rollout.

The `SERVER_READINESS_REQUIRES_HA` config description already promises the node will "be
caught up", but the code never implemented the catch-up / membership gate.

## Root cause

`GetReadyHandler.execute` only inspected `HAServerPlugin.getElectionStatus()`. There was no
HA method exposing committed-config membership or applied-vs-commit lag to the readiness probe.

## Fix

1. New `HAServerPlugin.getReadinessSignal(long maxLagEntries)` returning a `READINESS_SIGNAL`
   enum (`READY`/`NOT_READY`), or `null` when the HA implementation provides no consensus
   readiness signal (the probe applies no extra gating). A nullable enum is used rather than a
   `Boolean` because a Mockito mock defaults an unstubbed `Boolean`-returning method to `false`,
   which would have silently broken the existing `flagOnAndElectionDoneReturns204` test; an
   unstubbed enum-returning method defaults to `null` ("no signal").
2. `RaftHAPlugin.getReadinessSignal` delegates to `RaftHAServer.isReadyForTraffic(maxLagEntries)`,
   which reads a single `getDivision(...)` snapshot and requires: leader known, local peer present
   in the current configuration (`getRaftConf().getCurrentPeers()`), and (for a follower)
   `0 <= commitIndex - lastAppliedIndex <= maxLagEntries`. The leader is always caught up with
   itself; unreadable or inconsistent (`applied > commit`) state fails closed. Decision split into
   pure static `isReadyForTrafficState(...)` for unit testing.
3. New config `SERVER_READINESS_HA_MAX_LAG` (`arcadedb.server.readinessHAMaxLag`, default 100)
   for the catch-up bound, read by the handler (clamped to `>= 0`) and passed to the plugin.
4. `GetReadyHandler` keeps the existing election gate, then applies the new catch-up/membership
   gate when `getReadinessSignal` returns `NOT_READY`.

### A note on "committed" vs "current" configuration

Membership is checked against the *current* Raft configuration (the most recent conf in the log,
which during a joint-consensus change may not yet be committed). For the targeted scenario - a
wiped/lagging restarted follower - this is conservative and correct: such a node is not in the
current conf either way. Naming and Javadoc say "current configuration" to avoid overstating the
guarantee.

### Deferred / follow-up

- An end-to-end test asserting a freshly (re)joined follower in a *running* multi-node Raft cluster
  returns 503 until catch-up would lock in the integrated contract (membership read + index
  plumbing are currently exercised through the pure predicate and handler mocks). Deferred: it
  requires a real multi-node cluster with a wiped follower and is slow/flaky for the unit suite.

## Tests

- `RaftHAServerReadinessTest` (ha-raft): pure-function matrix for `isReadyForTrafficState`.
- `GetReadyHandlerHATest` (server): new methods for caught-up vs not-caught-up under the flag
  (existing methods untouched).

## Files changed

- `engine/.../GlobalConfiguration.java` - new `SERVER_READINESS_HA_MAX_LAG`.
- `server/.../HAServerPlugin.java` - new `isReadyForTraffic(long)` default method.
- `server/.../http/handler/GetReadyHandler.java` - wire the catch-up gate.
- `ha-raft/.../RaftHAPlugin.java` - delegate.
- `ha-raft/.../RaftHAServer.java` - `isReadyForTraffic` + pure `isReadyForTrafficState`.
