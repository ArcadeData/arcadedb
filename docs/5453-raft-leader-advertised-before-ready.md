# Issue #5453 - Raft leader is advertised as available before it is ready to serve

PR: https://github.com/ArcadeData/arcadedb/pull/5458

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

All three read the pair through `RaftHAServer.getLeadershipState()`, which resolves the division
once and returns both flags. Two separate `isLeader()` / `isLeaderReady()` calls could straddle a
leadership change and publish the impossible pair `isLeader=false, leaderReady=true`; a payload that
carries both fields should never be internally contradictory. `isLeaderReady()` is retained as the
public single-flag companion to `isLeader()` and delegates to the same accessor.

### Deliberately not changed

The ~12 `isLeader()` guards on leader-only write paths in `RaftHAServer` and `ArcadeStateMachine`
keep using the raw role. Those paths submit to Ratis, which rejects with the retryable
`LeaderNotReadyException` and retries on the client's behalf; gating them on readiness would convert
a transparent retry into a hard local rejection without improving the outcome. The problem this
issue describes is that *external* callers had no way to see the distinction, which is what the new
field and the probe gate address.

## Operational impact

This is not purely an additive JSON field: `isReadyForTraffic` backs the readiness endpoint via
`RaftHAPlugin`, so a freshly elected leader now reports **not ready** for the brief window between
winning the election and committing its current-term no-op. That is the intended effect - it is what
stops a Kubernetes rolling restart from sending traffic to a leader that will reject it - but
operators who expect a leader to be Ready the instant it is elected will see a short additional
delay. Worth calling out in the release notes.

A leader stays ready once Ratis marks it ready, until it loses leadership, so steady-state leaders
are unaffected.

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

- `clusterEndpointReportsLeaderReadiness` - every node carries `leaderReady`; a follower reports it
  false, and the elected leader reports it true. The leader assertion polls with Awaitility rather
  than sampling once: the role flips to LEADER before Ratis marks the node ready, which is the very
  window this issue is about, so a single sample would be racy by construction.

## Verification

```
mvn -pl ha-raft -am -DskipTests install
mvn -pl ha-raft test -Dtest=RaftHAServerReadinessTest,GetClusterHandlerIT,RaftClusterStatusExporterReemitTest,RaftHTTP2ServersIT
mvn -pl server test -Dtest=GetReadyHandlerHATest,HealthProbesIT
```

All green (44 + 11 tests). The gate was confirmed to genuinely fail closed by temporarily restoring
`if (leader) return true;`, which fails exactly
`freshlyElectedLeaderNotYetReadyIsNotReadyForTraffic` and `notYetReadyLeaderIsNotRescuedByZeroLag`
and nothing else.

## Review cycles

### Cycle 1 - fa1743d (claude[bot])

No blocking findings; the reviewer confirmed the core semantics, the defensive read and the legacy
overload delegation. Three non-blocking observations, two of which were applied:

- *Torn read on the reporting surfaces* - flagged as cosmetic, applied anyway. The three payloads
  now read both flags from one snapshot via `getLeadershipState()`, so the contradictory pair can no
  longer be published.
- *IT robustness* - flagged as "only if it actually flakes", applied. The single-sample assertion on
  the leader's `leaderReady` was racy by construction, for exactly the reason this issue exists; it
  is now a bounded Awaitility poll.
- *`isReadyForTraffic` is strictly stricter for leaders* - intended behaviour, no change. A leader
  stays ready once ready until it loses leadership, so steady-state leaders are unaffected.

gemini-code-assist did not review within the 15-minute polling window.

### Cycle 2 - f8d243d (claude[bot])

"Recommend merge", no blocking findings. The reviewer independently confirmed that Ratis 3.2.2
exposes `isLeaderReady()` and that it implies `isLeader()`, that `studio-cluster.js` reads only
`isLeader` so nothing breaks, and that no other compile-scope consumer is affected. Three
non-blocking notes:

- *The readiness probe behaviour changes, not just the JSON* - correct, and worth operator
  visibility. Captured in the "Operational impact" section above.
- *WARNING log volume from `getLeadershipState()` on polled surfaces* - no action. The reviewer
  concluded there is no net increase, since those callers already invoked `isLeader()`, which logs
  identically.
- *Studio could render `leaderReady`* - explicitly out of scope for this PR; see "Deferred" below.

gemini-code-assist again did not review within the polling window (a known inconsistency for this
repository, unrelated to this change).

## Deferred

Studio's `studio-cluster.js` still renders only `isLeader`. Surfacing the new signal - for example a
"leader (initializing)" state while `leaderReady` is false - would make the UI reflect the same
distinction the API now exposes. Left out deliberately to keep this PR scoped to the server-side
defect; worth a follow-up issue.
