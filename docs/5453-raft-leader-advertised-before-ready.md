# Issue #5453 - Raft leader is advertised as available before it is ready to serve

## Symptom

A freshly elected Raft leader was advertised as available the instant Ratis elected it, while Ratis
itself still rejected client requests with the retryable `LeaderNotReadyException` until the leader
had committed a no-op entry in its current term and caught its state machine up.

A client that wrote immediately after seeing `"isLeader": true` absorbed a run of retries. The client
retry policy is a fixed 1-second cadence and the caller's budget is `arcadedb.ha.quorumTimeout`
(default 10000 ms), so there is a hard cliff at ten retries and the write surfaced as

```
ReplicationDispatchedTimeoutException:
  Group commit entry failed: TimeoutException (entry was dispatched to Raft; outcome unknown)
```

even though nothing was wrong with the cluster. Observed while investigating the nightly `e2e-ha`
suite (PR #5452): the durations of successful `drop database` calls issued right after
`"isLeader": true` were sharply bimodal, with a slow bucket sitting in a 0.38-second band
immediately below the 10-second timeout - the signature of a fixed retry cadence, not of work
proportional to data.

## Root cause

`RaftHAServer` never consulted Ratis' own readiness signal. `DivisionInfo` exposes both

```java
default boolean isLeader();     // current role == LEADER
boolean isLeaderReady();        // current role == LEADER *and* ready to serve
```

and `isLeaderReady()` was unused anywhere in the codebase. Two places assumed role implies
readiness:

1. `isReadyForTraffic` / `isReadyForTrafficState` - the readiness probe added in #4834 carefully
   checked follower lag, membership and in-flight resync, then short-circuited unconditionally for
   the leader:

   ```java
   if (leader)
     return true;   // "the leader is always caught up with itself"
   ```

   That holds for a leader in steady state, but not for one that has just won an election and has
   not yet committed its current-term no-op. The probe therefore advertised Ready on a node that
   would reject the very next write.

2. `GET /api/v1/cluster` and the HA stats reported only the raw `isLeader` role, so a client had no
   way to distinguish "is the leader" from "can serve now".

## Fix

### 1. Expose the Ratis readiness signal

`RaftHAServer.isLeaderReady()` mirrors the existing `isLeader()` shape, including the defensive
try/catch that degrades a status read to "unknown" rather than propagating the
`IllegalStateException` Ratis throws while an in-place restart re-initializes the division
(issue #5271).

### 2. Fail the readiness probe closed on a not-yet-ready leader

`isReadyForTrafficState` gains a `leaderReady` parameter and the leader branch becomes

```java
if (leader)
  return leaderReady;
```

`isReadyForTraffic` feeds it `info.isLeaderReady()` from the same single `getDivision(...)` snapshot
the other inputs come from, so leader/readiness/membership/commit/applied stay a consistent view.

`leaderReady` had to be a distinct input rather than a redefinition of the existing `leader`
parameter. Passing `isLeaderReady()` in as `leader` would let a not-yet-ready leader fall through to
the follower lag branch, where a freshly elected leader typically has `commitIndex == appliedIndex`
and would be reported Ready anyway - defeating the fix.

The pre-existing 6- and 7-argument overloads delegate with `leaderReady = leader`, preserving their
documented behaviour for existing callers; the 8-argument overload is the production entry point.
This mirrors how the `resyncInProgress` parameter was added in #5273.

### 3. Report readiness distinctly, without a compatibility break

`isLeader` keeps meaning the raw Raft role on every surface. A new `leaderReady` field is added
alongside it in:

- `GET /api/v1/cluster` (`GetClusterHandler`)
- the HA stats map (`RaftHAServer.getStats`)
- the exported cluster status JSON (`RaftClusterStatusExporter`)

Callers - Studio, the cluster tooling, and application code that polls for a leader before writing -
opt in to the stricter signal rather than having `isLeader` change meaning underneath them.

### Deliberately not changed

The ~12 `isLeader()` guards on leader-only write paths in `RaftHAServer` and `ArcadeStateMachine`
keep using the raw role. Those paths submit to Ratis, which rejects with the retryable
`LeaderNotReadyException` and retries on the client's behalf; gating them on readiness would convert
a transparent retry into a hard local rejection without improving the outcome. The problem this
issue describes is that *external* callers had no way to see the distinction, which is what the new
field and the probe gate address.

## Tests

`ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerReadinessTest.java`

- `freshlyElectedLeaderNotYetReadyIsNotReadyForTraffic` - the regression: leader present, in config,
  no resync, `commitIndex == appliedIndex` (so the follower lag branch would pass), but
  `leaderReady = false`. Must be false.
- `readyLeaderIsReadyForTraffic` - same inputs with `leaderReady = true` are Ready.
- `notYetReadyLeaderIsNotRescuedByZeroLag` - explicitly pins that the un-ready leader does not fall
  through to the follower lag calculation.
- `followerIgnoresLeaderReadyFlag` - a caught-up follower is Ready regardless of `leaderReady`,
  which is a leader-only signal.
- `sevenArgOverloadTreatsAnyLeaderAsReady` / `sixArgOverloadTreatsAnyLeaderAsReady` - the legacy
  overloads keep their previous semantics.

`ha-raft/src/test/java/com/arcadedb/server/ha/raft/GetClusterHandlerIT.java`

- `clusterEndpointReportsLeaderReadiness` - every node carries `leaderReady`; the leader of a
  settled cluster reports it true and a follower reports it false.

## Verification

```
mvn -pl ha-raft -am -DskipTests install
mvn -pl ha-raft test -Dtest=RaftHAServerReadinessTest
mvn -pl ha-raft test -Dtest=GetClusterHandlerIT
```
