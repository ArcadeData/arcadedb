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

1. New `HAServerPlugin.isReadyForTraffic(long maxLagEntries)` returning a nullable `Boolean`
   (null = this HA implementation provides no consensus readiness signal, so the probe applies
   no extra gating; the Raft implementation returns a concrete `true`/`false`).
2. `RaftHAPlugin` delegates to `RaftHAServer.isReadyForTraffic(maxLagEntries)`, which requires:
   leader known, local peer present in the committed config (`getLivePeers()`), and (for a
   follower) `commitIndex - lastAppliedIndex <= maxLagEntries`. The leader is always caught up
   with itself. Decision split into pure static `isReadyForTrafficState(...)` for unit testing.
3. New config `SERVER_READINESS_HA_MAX_LAG` (`arcadedb.server.readinessHAMaxLag`, default 100)
   for the catch-up bound, read by the handler and passed to the plugin.
4. `GetReadyHandler` keeps the existing election gate, then applies the new catch-up/membership
   gate when `isReadyForTraffic` returns a non-null `false`.

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
