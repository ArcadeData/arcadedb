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

## Review hardening (cycle 1)
Both bot reviewers flagged the same failure mode: `hasNeverAppliedApplicationEntry()` could falsely
report first formation on a running cluster if the persisted `.raft/applied-index` read degrades to
`-1` (a transient I/O error or corrupt file). Added a defense-in-depth guard: the file is created
only after an application entry is applied, so its mere presence proves the node is not fresh -
`hasNeverAppliedApplicationEntry()` now returns `false` when the file exists, regardless of whether
its contents parse. New unit case
`ArcadeStateMachineFirstFormationSignalTest.presentButCorruptAppliedIndexFileKeepsTheSignalClosed`.

The reviewer's alternative "require `lastAppliedIndex >= commitIndex`" is infeasible: internal
no-op/config entries advance the commit index but never flow through `applyTransaction`, so applied
can never catch up to commit in the transfer case (that is the whole reason the exact-`0` gate had to
be replaced). The residual "never-applied node with committed-but-unapplied application entries wins
an election" window is bounded by Raft's election restriction (a lagging node cannot win leadership
until its log is up to date) and backstopped by the existing `localLastTxId > baseline`
refusing-to-overwrite guard.

## PR
https://github.com/ArcadeData/arcadedb/pull/5113

## Review cycles
- **Cycle 1** (`3f558b8`) - both bots flagged the same failure mode: a degraded/corrupt read of the
  persisted `.raft/applied-index` file degrading to `-1` could re-open bootstrap on a running
  cluster. Applied the file-existence guard (`8fba97a0`): the file exists only after an application
  entry is applied, so its presence proves the node is not fresh regardless of parse result. Added
  `presentButCorruptAppliedIndexFileKeepsTheSignalClosed`. Declined the alternative
  `lastAppliedIndex >= commitIndex` guard (infeasible: internal entries advance commit but never
  applied).
- **Cycle 2** (`8fba97a0`) - claude LGTM; applied its clarifying comment on the fallthrough line
  (`b723394e`). Gemini re-posted its already-applied suggestion.
- **Cycle 3** (`b723394e`) - claude LGTM; applied all three polish items (`167a5be3`): residual-window
  reasoning moved into the `isFirstFormation` code comment, `unwiredStateMachineReportsFirstFormation`
  null-path test, and narrowed `hasNeverAppliedApplicationEntry()` to package-private. Gemini re-posted
  the already-applied suggestion again.
- **Cycle 4** (`167a5be3`) - claude verdict "looks good to merge"; all observations non-blocking and
  already covered, no code changes. Gemini did not re-review this head.

**Final state:** clean-approval (claude LGTM, no actionable items outstanding; working tree clean).
Merge is the maintainer's decision - this workflow does not merge.

## Related
- #4147 (offline bootstrap protocol), #4800 (first-formation gate), #5098 (transient `-1` fix,
  prerequisite), #5100/#5104 (restart baseline persistence).
