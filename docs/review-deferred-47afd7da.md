# Deferred review item — PR #7210 (issue #6990), head 47afd7da

One item from the cycle-4 `claude` review is **documented, not resolved**, and needs the developer's decision. The
review loop reached its `--max-cycles=4` limit here.

## The comment, verbatim in substance

> ### 1. Widening the recording session past `HA_QUORUM_TIMEOUT` re-opens the #4083 race for ordinary writers
>
> `RaftReplicatedDatabase.commit()` makes every ordinary (non-DDL) commit on the leader call
> `waitForActiveRecordingSession()`, which polls `FileManager.getRecordedChanges()` for up to `HA_QUORUM_TIMEOUT`
> (default 10s) and then *gives up and proceeds anyway* (logged, not thrown). [...] `LocalSchema.bulkChange` now
> holds that same session for the entire script, with no bound [...] Suggested next step: either (a) add a regression
> test that runs a large-enough batch concurrently with an ordinary write into a type the batch just created,
> or (b) bound the batch, or (c) at minimum call this out explicitly in the config description and default
> `arcadedb.schemaBulkDDLScript` to `false` until it's addressed.

## What was verified

**The mechanism is real.** `RaftReplicatedDatabase.commit()` (line 439) calls `waitForActiveRecordingSession()`
before anything else on the leader, and that method (line 2691) returns after `HA_QUORUM_TIMEOUT` with
`"commit waited %dms for FileManager recording session and gave up; proceeding with TX_ENTRY"`. The comment above
the call names issue #4083 and says the write is otherwise "silently dropped" on followers.

**One supporting claim in the review is wrong, and the correction matters.** It states that
`database.executeInWriteLock` "is only ever called from `TypeBuilder.create()`" and concludes that "the lock-hold
framing doesn't actually describe the mechanism that changed". `LocalDatabase.recordFileChanges` IS
`executeInWriteLock(callback)` (`LocalDatabase.java:2309`), so every recording session - the bulk scope included -
holds the write lock for its whole duration, and an ordinary commit blocks on it via `executeInReadLock`. The lock
framing was accurate; what it was missing is that `waitForActiveRecordingSession` runs BEFORE that lock is taken,
so the writer stops waiting for the session at 10s, then blocks on the lock, and then races the batch's
`replicateSchema` to the broker.

**The hazard is pre-existing, and this change widens it rather than creating it.** `TypeIndexBuilder.create()`
already wraps a whole index build in one session; on a populated type that is minutes, far past the 10s give-up.
`RaftReplicatedDatabase.flushSchemaWalBufferIfFull`'s javadoc (issue #6136) names the same concurrent-writer wait
and accepts it. What a bulk scope adds is a new shape that can reach the same window: a long DDL script.

## What was done in this PR

Option (c), the documentation half of it: `Schema.bulkChange`'s javadoc and the `arcadedb.schemaBulkDDLScript`
description now name the recording-session window, `waitForActiveRecordingSession`'s give-up, the #4083 guard, and
the fact that a long index build already opens the same window today.

## What was NOT done, and why it is the developer's call

- **Defaulting `arcadedb.schemaBulkDDLScript` to `false`** would ship the feature inert and is a product decision,
  not one to take unilaterally at the end of a review loop. The argument for leaving it `true`: the window it
  widens is one the engine already opens for index builds, and the acceptance criterion in #6990 is about the
  entry count a script produces.
- **Bounding the batch** (statement count or elapsed time) cannot be done by flush-and-reopen without giving up
  the atomicity the feature exists to provide - the PR body argues this - and refusing to batch past N statements
  would defeat the issue's own headline case (1209 types).
- **A concurrency regression test** (an ordinary write against the leader while a batch is in flight) is the right
  next artefact whichever way the decision goes. It belongs with whatever bound or acceptance is chosen, so that
  it asserts the decided behaviour rather than today's.

## Other cycle-4 notes, addressed

- `DROP GRAPH ANALYTICAL VIEW` / `ALTER GRAPH ANALYTICAL VIEW` being left at the conservative default is now stated
  in the `DDLStatement.isBulkSchemaScopeSafe` javadoc, so the omission is not mistaken for an oversight.
- `RemoteSchema` inheriting the default `bulkChange` was flagged as an assumption to double-check; it holds - every
  remote DDL call was always its own HTTP round trip, so there is nothing there to batch.
