# #5099 - HA offline bootstrap: leadership-transfer path never commits baseline

## Symptom
`RaftBootstrapLeadershipTransferIT.leadershipTransfersToTheFresherPeerBeforeBootstrapCommit`
times out: after the offline-bootstrap protocol transfers Raft leadership to the elected source
peer, that new leader never commits the `BOOTSTRAP_FINGERPRINT_ENTRY`, so
`getBootstrapBaseline(db)` stays `null` on every peer.

## Root cause
`BootstrapElection`'s first-formation gate was `commitIndex == 0`
(`isConfirmedFirstFormation`). The leadership transfer plus the term-2 leader election make Ratis
append and commit internal no-op / configuration entries, so the new leader (the elected source)
reads `commitIndex = 2`, not `0`. The exact-`0` gate (tightened in #4800 to reject a running
cluster) therefore rejects the post-transfer leader and the baseline is never committed.

The gate cannot simply be loosened to `<= 2`: #4800 keys first-formation on an exact `0` to stop
bootstrap re-triggering on an already-running cluster (which would risk overwriting live data).

## Fix
Add a durable first-formation signal that survives an internal term bump but still cannot fire once
any application data has committed:
`ArcadeStateMachine.hasNeverAppliedApplicationEntry()`.

Ratis internal no-op/config entries never flow through `applyTransaction`, so they advance neither
the in-memory `lastAppliedIndex` nor the persisted applied index - while every real mutation
(TX/SCHEMA/INSTALL/DROP/SECURITY/BOOTSTRAP entry) advances both. The signal checks both the
in-memory and the persisted index so a restarted, already-bootstrapped cluster (log compacted below
the Ratis snapshot) is never mistaken for a fresh one.

`BootstrapElection` gate is widened: it opens on either the exact-`0` empty log (no-transfer path,
unchanged) OR, when commitIndex > 0, on the "no application entry has ever been applied" signal. A
negative (unready/failed) commit index still never opens the gate (#4800 preserved). The static
`isConfirmedFirstFormation(long)` is left unchanged.

## Tests
- `ArcadeStateMachineFirstFormationSignalTest` (new, unit) - the durable signal: fresh SM true;
  false once a persisted applied index exists; false across a simulated restart.
- `BootstrapElectionTransferGateTest` (new, unit) - the widened gate: post-transfer leader
  (commitIndex 2) with no application data engages bootstrap; with committed data or a null state
  machine it skips.
- `RaftBootstrapLeadershipTransferIT` (existing IT) - end-to-end reproduction, now passes.

## Related
- #4147 (offline bootstrap protocol), #4800 (first-formation gate), #5098 (transient `-1` fix,
  prerequisite), #5100/#5104 (restart baseline persistence).
