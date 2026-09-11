# 6965 - two nodes writing the same page: the second one is refused, not merged

Two counter documents on one data page, each written by exactly one node - the leader increments document A, a
replica increments document B - lost increments. The replica read back a value smaller than the one its own
acknowledged commit had written, no exception on either side, nothing in any log, and depending on which way the
local race went the leader and the replica could even end up holding different bytes for the page.

## Why

Every replicated transaction is validated on the node that originates it, against that node's copy of the pages,
and ships each page's *next* version. Two nodes validating against the same base version therefore ship two entries
that both claim `v+1`, and Raft orders both. On apply, the second one hit the equal-version branch of
`TransactionManager.applyChanges` - the torn-write repair of #4926, which re-applies a delta whose version matches
the page - and was spliced over the first, region by region. A delta that carries the slot table or a neighbouring
record image carries a stale copy of the other document, and that copy wins.

The leader had a second race of its own. Its state machine origin-skipped the leader's entries and left the page
write to `commit2ndPhase`, which ran on the committing thread after the acknowledgement. Nothing ordered that write
against the apply of the neighbouring entries on the same page: the state machine could apply a replica's `v+1`
first, the leader's own `commit2ndPhase` then failed validation, and the reconcile path re-applied the leader's
delta at equal version - the same splice, on the leader only.

Neither the per-file commit lock (#5503) nor the explicit type lock can help: both are node-local, and the collision
is between nodes. Taking the lock in the apply path, the first proposal, still merges two same-base deltas.

## What changed

**The leader validates every transaction entry against the versions the log already gave away**, in
`ArcadeStateMachine.startTransaction`, before Ratis touches the request. A new `PageVersionLedger` holds, per
database, the pages of the entries accepted but not yet applied on the leader, with the version each will take.
Validation runs under the database's ledger lock (a check-then-act), seeds a page with no reservation from the
leader's local copy, and refuses an entry whose target is not `last assigned + 1` with a
`ReplicatedPageConflictException` - a `ConcurrentModificationException`, so every retry loop already handles it. An
accepted entry reserves its versions; `preAppendTransaction`, which Ratis calls under the log lock in log order,
confirms them; the apply releases them, because from then on the local copy carries the version itself.

The seed is correct because Ratis makes a leader ready only after it has applied every entry preceding its term,
and every entry accepted since is either still reserved or applied locally. The ledger is cleared on step-down and
on leader-ready.

**The leader publishes its own entries at their log position, on the apply thread.** `commit2ndPhase` is split in
`TransactionContext`: `publishCommittedPages` (validate and bump, WAL append, publish, counters) and
`completeCommit` (records clean, `onAfterCommit`, callbacks, reset). The committing thread registers the prepared
transaction with the state machine (`LocalCommitRegistry`) right before dispatching the entry; the apply thread
claims it when the entry comes up and runs `publishCommittedPages` there. Every node now writes its pages in log
order, the leader included, so the leader's commit can neither overtake nor trail a neighbouring entry. The
committing thread only completes the bookkeeping once the entry is acknowledged.

The registration is a compare-and-set handshake (`LocalCommit`). If replication fails or its outcome is unknown, the
committing thread *withdraws*: it rolls back, and should the entry commit anyway the apply thread finds no claim and
applies it from its own WAL bytes, as a follower would. If the apply thread *claimed* first, a timeout learned
afterwards is not an unknown outcome any more: the entry committed, its pages are published, and `commit()`
completes. This retires the origin-skip, the abandoned marks of #4790/#6848 and the phase-2 tickets of #5407/#5410
in one go - the applied index is the durable position on the leader exactly as it is on a follower, so
`takeSnapshot` no longer clamps. The `arcadedb.ha.phase2.*` gauges keep their names and now count registered
transactions the apply thread has not reached; the replay floor is always `-1`.

**Local validation sees the ledger too.** `PageManager.checkPageVersion` consults
`LocalDatabase.getPageVersionReservations()`, which the state machine installs on the leader: a transaction on the
leader that touches a page reserved by an in-flight entry fails its own phase 1 with the same retryable error, one
round trip earlier. A standalone database and every replica pay one volatile read.

**A refused replica waits for the page to catch up.** A replica whose apply trails the leader by a couple of entries
would otherwise re-read the same stale page and be refused every time while the leader keeps writing it - the
reproducer showed thousands of refusals per increment. The refusal carries the page and the version the cluster is
at, and `RaftReplicatedDatabase` waits (bounded) for the local copy to reach it before returning the conflict, which
turns a starvation into a fair race.

## The Ratis trap

Refusing from `preAppendTransaction` would be the textbook place - it is the one hook with a total order - and it
does not work: Ratis 3.3.0 acquires the leader's pending-write permit before `appendLog` and releases it only when
the pending request is removed, so a `StateMachineException` thrown from the pre-append hook leaks one permit per
refusal. After `raft.server.write.element-limit` (4096) refusals the leader answers every write with
"Failed to acquire a pending write request" until it steps down. The reproducer wedged the cluster this way in its
first run. A refusal expressed through the transaction context (`setException`) in `startTransaction` costs no
permit; that is where the validation lives, and pre-append only confirms.

Two consequences of validating before the append are handled in the ledger: Ratis retries a request it could not
append with the same client id and call id, so a reservation remembers the entry that made it and the retry is
accepted as the same entry; and a request dropped between the reservation and the append leaves a reservation
nothing will confirm, which is discarded once it is 30 seconds old and unconfirmed.

The Ratis client logs each refusal as a SEVERE with a stack trace (`OrderedAsync`); `RatisRefusedEntryErrorFilter`
drops that one record, the same way `RatisSnapshotDigestWarningFilter` drops the snapshot-digest warning. The client
also delivers the refusal as an exceptional future rather than a failed reply, and `RaftGroupCommitter` recognises
it on both paths so it surfaces as the definite, retryable conflict it is and never as "dispatched, outcome unknown".

## Tests

- `Issue6965SharedPageCrossNodeWritersTest`: the issue's scenario on a two-node cluster, one bucket, two writers.
  Fails on the previous tree within seconds (the leader's own commit failed as committed-remotely, or an increment
  was lost); passes with every increment accounted for on both nodes and the pages identical.
- `Issue6965PreAppendValidationTest`: the collision against a real database, and the phase-1 refusal on a reserved
  page.
- `PageVersionLedgerTest`, `LocalCommitRegistryTest`, `ReplicatedPageConflictExceptionTest`,
  `RatisRefusedEntryErrorFilterTest`, and the two `RaftGroupCommitterTest` cases for the refusal on the failed-reply
  and exceptional paths.
- `Issue6965LocalCommitHandshakeTest` replaces the ticket and abandoned-mark unit tests
  (`Issue5407Phase2TicketLifecycleTest`, `Issue5410AbandonedPhase2TicketTest`, `Issue6848LocalOriginHandshakeTest`,
  `ArcadeStateMachinePendingPhase2SnapshotTest`, `Issue5064ApplyLocallyAfterMajorityCommitTest`): every exit of
  `replicateAndCommitLocally` is pinned to what it does with the registration.
- The dispatched-timeout ITs (#4790, #5410, #6848) keep their convergence assertions; the commit outcome they
  assert follows the handshake now - `Issue6848AbandonedMarkRaceIT`, which parks the committer past the apply,
  expects a completed commit.

## Not covered here

A DDL on the leader runs under the database write lock and publishes its pages locally before its `SCHEMA_ENTRY`
is appended, so a replica transaction on the same pages accepted in between is still merged the old way. Closing
it means excluding replica writes for the duration of a leader DDL; tracked separately.
