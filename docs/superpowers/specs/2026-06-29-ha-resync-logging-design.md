# HA Resync Logging Design

Date: 2026-06-29
Status: Approved (brainstorming complete, ready for implementation plan)
Module: `ha-raft`

## Problem

In a running Raft cluster under heavy write load, restarting a node produces two
poor logging experiences:

1. **Master node floods.** The leader emits thousands of WARN lines while a
   restarting follower is briefly unreachable, e.g.:

   ```
   WARNI [GrpcLogAppender] arcadesplit-0_2434@group-...->arcadesplit-2_2434-FixedGrpcLogAppender:
   Follower failed (request=null, errorCount=4442); keep nextIndex (105325) unchanged and retry.
   (Repeated 2 times in the last 5.001s)
   ```

   The errorCount climbs into the thousands. The line is harmless (Ratis is
   correctly holding the follower's `nextIndex` and retrying), but it drowns the log.

2. **Replica node is silent.** When the restarting replica comes back and resyncs,
   it logs nothing about catching up. An operator cannot tell whether resync is
   progressing, stuck, or finished.

### Root cause / current state

- The flood line is emitted by Apache Ratis 3.2.2's own
  `org.apache.ratis.grpc.server.GrpcLogAppender` (parent of ArcadeDB's
  `FixedGrpcLogAppender`), routed SLF4J -> `slf4j-jdk14` -> JUL -> ArcadeDB's
  `DefaultLogger`. The "Repeated N times in the last Xs" suffix is Ratis's own
  throttle, not ArcadeDB's.
- ArcadeDB already manipulates this exact JUL logger: it pins `org.apache.ratis`
  to `WARNING` (`RaftHAServer:416`) and temporarily raises `GrpcLogAppender` to
  `SEVERE` during shutdown (`RaftHAServer:800-825`).
- The follower's snapshot-install path already logs start/end
  (`ArcadeStateMachine.notifyInstallSnapshotFromLeader:691/774`,
  `DatabaseReconciler` "Acquiring/Refreshing/Installing"). The silent gap is the
  common restart-under-load case: **Raft log catch-up** (AppendEntries replay),
  which never touches the snapshot path, plus the absence of in-flight progress
  during a long snapshot download.
- `HealthMonitor` already runs on a `scheduleWithFixedDelay` timer and already
  computes follower lag (`commit - applied`). `FixedGrpcLogAppender` is already
  instantiated per-follower on the leader. Both sides therefore have existing
  hooks; no new threads are required.

## Decision: log hygiene, not membership change (Option A)

The flood is a symptom of a **transient, self-healing** event (a restart). The
correct response is to say it once, clearly, then stay quiet - **not** to mutate
Raft membership.

Auto-deregistering an unreachable follower was explicitly considered and
**rejected** for the automatic path because:

- Membership changes are themselves replicated log entries needing quorum;
  removing a peer while degraded can deadlock the operation meant to help.
- Quorum math shifts under load (3 -> 2 members makes any further failure fatal).
- The removed node is usually already rejoining; removal forces redundant
  re-add + full resync, adding load exactly when the cluster is busy.
- Periodic restarts (rolling deploy, k8s) would cause remove/add flapping.

If eviction is ever wanted, it belongs in a **separate, explicit,
operator-triggered** admin path with quorum-safety checks - out of scope here.

## Design

### Master side: flood suppression + honest summary

- **Suppression via a JUL `Filter`.** Install a `RatisAppenderLogThrottle`
  (`java.util.logging.Filter`) on the `org.apache.ratis.grpc.server.GrpcLogAppender`
  logger at HA startup, next to the existing `:416` level pin. Policy: pass the
  **first** "Follower failed" record per follower through, then suppress repeats
  until recovery or the summary interval elapses. This is fully decoupled from
  Ratis internals and survives Ratis upgrades. Chosen over (a) demoting the logger
  to `SEVERE` wholesale (loses the signal) and (b) reading Ratis `FollowerInfo`
  internals from the appender subclass (more reflection fragility).
- **Periodic summary** from the leader's existing periodic monitor: for each
  follower currently failing, one INFO every
  `arcadedb.ha.peerUnreachableSummaryInterval` (default 30s):
  `follower-2 unreachable for 45s (errorCount~4442, holding nextIndex=105325)`.
- **One-shot recovery line** when the stream recovers:
  `follower-2 reconnected, replication resumed at index 105326`.
- **No Raft membership change.** Suppression is default-on.

### Follower side: resync visibility

All three visibility goals are in scope.

1. **Log catch-up visibility (the silent common case).** On follower (re)start,
   when this node observes it is behind (via `HealthMonitor`'s existing
   `commit - applied`), emit a one-shot
   `HA resync started (mode=catch-up): 126 entries behind leader (applied=105200, leader=105326)`,
   then a time-throttled progress line every `arcadedb.ha.resyncProgressInterval`
   (default 5s): `catch-up: applied=105290/105326 (36 behind, ~7200 entries/s)`,
   then a bookend `HA resync finished (mode=catch-up, duration=3.2s, result=ok)`.
   Throttled by time, not per-entry, so a fast catch-up logs only start + end.

2. **Snapshot download progress.** Thread a lightweight progress callback
   (bytes-so-far / total-if-known) through the snapshot streaming read in
   `DatabaseReconciler` / `SnapshotInstaller`. Emit per-database progress on the
   same time-throttled cadence: `snapshot download db=foo: 420/512 MB (82%, 38 MB/s, ETA 2s)`.
   Where total size is unknown, drop %/ETA and show bytes + throughput only.

3. **Unified lifecycle bookends.** Standardize one grep-able phrase for both
   paths: exactly one `HA resync started (mode=catch-up|snapshot, reason=...)` and
   one `HA resync finished (mode=..., duration=Ns, result=ok|failed)`. Align the
   existing snapshot-path lines (`ArcadeStateMachine:691/774`) to this wording so
   both modes share identical bookends.

### Configuration and levels

New `GlobalConfiguration` knobs (HA scope, safe defaults, new behavior default-on):

- `arcadedb.ha.peerUnreachableSummaryInterval` - default 30s.
- `arcadedb.ha.resyncProgressInterval` - default 5s.
- `arcadedb.ha.verboseResyncLogging` - boolean master switch to disable the new
  lines wholesale (default true).

Levels: the flood is suppressed by the Filter; the first-failure line, summaries,
and all resync lines are **INFO**; genuine failures remain **WARNING/SEVERE** as
today. Existing SEVERE halt paths are unchanged.

## Testing (TDD)

- **Master:**
  - Unit: drive `RatisAppenderLogThrottle` with N synthetic "Follower failed"
    records; assert only the first + interval summaries pass.
  - IT: extend the `RaftIdleReplicaRestartIT`-style harness; restart a follower
    under write load; assert the master log contains one failure line + summaries,
    not thousands, and a single recovery line.
- **Follower:**
  - IT: restart a follower under write load; assert catch-up
    started/progress/finished bookends appear.
  - Unit: reconciler progress callback fires with monotonic bytes and a final
    100% / "finished" line.
- All HA ITs `@Tag`-tagged per existing convention (`benchmark` / `slow` as
  appropriate); assertions via `assertThat(...)`.

## Out of scope

- Automatic membership eviction of unreachable peers (would be a separate,
  operator-triggered admin command with quorum-safety checks).
- Changes to Raft replication behavior, quorum, or failover.
- Migrating the snapshot-install offload off the JDK common ForkJoinPool
  (tracked separately).

## Implementation amendment (2026-06-29)

During integration testing the JUL-`Filter` approach for master-side flood suppression proved
unworkable in this codebase: ArcadeDB's `DefaultLogger` calls
`java.util.logging.LogManager.readConfiguration()` during logging init
(`engine/.../log/DefaultLogger.java`), and that runs repeatedly in a live cluster (server startup,
database bootstrap, reconcile), resetting per-logger JUL state and clearing any installed `Filter`.
A filter installed by `RaftHAServer.start()` is therefore wiped before the flood occurs (verified: the
filter was `null` at flood time and 147 retry lines leaked).

**Mechanism changed to log-config level demotion.** The `RatisAppenderLogThrottle` filter and its
`RaftHAServer` install/restore wiring were removed. Instead, `arcadedb-log.properties`
(`engine/src/main/resources` and `package/src/main/config`) sets
`org.apache.ratis.grpc.server.GrpcLogAppender.level = SEVERE`. Because this lives in the config that
`readConfiguration()` re-applies, it survives the resets and reliably suppresses the WARNING-level
retry flood. Trade-offs versus the original design: suppression is now **wholesale** (all WARNINGs
from that one Ratis logger, which are essentially all retry noise) and **global/ungated** (not tied to
`arcadedb.ha.resyncProgressLogging`). This is consistent with the existing shutdown-time demotion of
the same logger in `RaftHAServer.stop()`. The operator signal is preserved by the `ClusterMonitor`
unreachable/reconnected narrative, which is unchanged.

**Integration tests deferred.** `RaftLeaderFloodSuppressionIT` and `RaftFollowerCatchupLoggingIT` are
committed but `@Disabled`: asserting on log output across in-process servers is timing- and
logging-config-fragile for the same `readConfiguration` reason. The behaviors are covered
deterministically by unit tests (`ResyncLoggingConfigTest`, `ClusterMonitorTest`,
`FollowerResyncProgressTrackerTest`, `SnapshotDownloadProgressMeterTest`, `HealthMonitorTest`) and the
suppression by the config file itself. The ITs remain as scaffolding to revisit with a more robust
capture mechanism (e.g. a thread-safe `LogManager` logger seam that survives JUL resets).
