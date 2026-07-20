# Issue #5346 - leader's replication channel to a re-IP'd follower stays permanently dead

## Symptom

On a 3-node Kubernetes Raft cluster a follower (`server-2`) was recreated with a new pod IP. It rejoined
membership cleanly, but the leader never delivered a single `AppendEntries` to it (`matchIndex = -1`) for
5.5+ minutes. The leader ran all 5 in-process replication-channel resets
(`arcadedb.ha.peerChannelResetDuration`) and never recovered the channel. DNS from the leader resolved to
the follower's new IP and TCP to the Raft port was open the whole time, so this was not stale DNS and not
an L3/L4 reachability failure. Only restarting the leader process fixed it: the new leader built a fresh
appender, shipped a snapshot, and the follower reached `lag = 0` within seconds.

## Root cause

Two distinct defects in the leader-side channel-recovery path, both in `ha-raft`.

### 1. Exhausting the reset budget is a dead end

`ClusterMonitor.trackUnreachableForChannelReset` fires one reset per `peerChannelResetDurationMs` up to
`CHANNEL_RESET_MAX_ATTEMPTS` (5). When the budget is exhausted it sets `channelResetGaveUp = true`, logs a
single SEVERE line telling the operator to transfer leadership, and then does nothing else - forever.

The streak only re-arms when the follower becomes reachable again. For a genuinely wedged channel that
never happens, so the leader stays in the given-up state until the process is restarted. The log message
already prescribes the correct remedy (`transfer leadership to that follower's healthy peer to rebuild the
appender`) and `RaftHAServer` already has every piece needed to perform it - `selectStepDownTargets` and
`transferLeadership(String, long)` - but nothing wires the two together. The automated 5-attempt recovery
therefore gives the operator false confidence while leaving a manual leader restart as the only real fix.

### 2. The reset itself is fire-and-forget, and runs on the lag-monitor thread

`RaftHAServer.resetPeerReplicationChannel` calls Ratis `PeerProxyMap.resetProxy(peerId)`, which closes and
drops the cached channel. Rebuilding is **lazy**: a fresh channel is only created when the appender next
calls `getProxy(peerId)`. If the peer's `GrpcLogAppender` daemon is itself wedged, no send is ever
attempted, so no new channel is ever created and the reset is silently a no-op that still burns one of the
five attempts. This is consistent with the reported incident, where connectivity was demonstrably healthy
yet no append ever landed.

The reset also ran inline on the single lag-monitor thread, and logged nothing an operator could use to
tell whether re-resolution actually took effect.

## Fix

Scoped to the two items above, which correspond to suggestions 1 and 2 in the issue.

1. **Automatic escalation (`ClusterMonitor` + `RaftHAServer`).** `ClusterMonitor` gained an optional
   `exhaustedPeerChannelHandler`, invoked exactly once per unreachable streak at the moment the reset
   budget is exhausted. `RaftHAServer.escalateWedgedPeerChannel` implements it: it picks a healthy
   step-down target (reusing `selectStepDownTargets`, explicitly excluding the wedged follower) and
   transfers leadership to it, which rebuilds the appender on the new leader - the exact remedy the old
   log line only suggested. When no healthy candidate exists it falls back to the previous
   operator-intervention log rather than flapping the cluster.

2. **Off-thread reset with resolved-address logging.** The `resetProxy` call and the new escalation both
   run on the existing bounded single-thread recovery executor instead of the lag-monitor thread, so
   neither DNS resolution nor a 10 s leadership transfer can stall replica monitoring. The reset path now
   logs the peer's resolved IP addresses before and after the reset, so an operator can see directly
   whether re-resolution took effect.

Gated by a new `arcadedb.ha.peerChannelResetEscalation` (Boolean, default `true`). Setting it to `false`
restores the previous log-and-stop behaviour.

## Out of scope

- Suggestion 3 (peer build/protocol version handshake on the Raft channel) is a feature, not a fix for
  this defect, and is better tracked separately. The issue's own caveat notes the incident ran mixed
  SNAPSHOT builds, so a handshake would have improved diagnosability but is orthogonal to the wedged
  recovery path.
- Suggestion 4 (`matchIndex=-1` as a first-class alert) already landed as the distinct SEVERE message
  added by issue #5295.

## Verification

- `ClusterMonitorTest` - new deterministic clock-driven tests covering escalation on budget exhaustion,
  fire-once-per-streak, re-arm after the follower reconnects, and the disabled/absent-handler paths.
- `RaftHAServerChannelEscalationTest` - new unit test for the escalation target selection seam, including
  exclusion of the wedged follower itself.
- Full `ha-raft` module test suite for regressions.
