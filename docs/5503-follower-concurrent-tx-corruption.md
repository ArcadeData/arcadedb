# #5503 - concurrent transactions on a shared follower handle corrupt records and lose writes

## Symptom

On a 3-node Raft cluster, several threads sharing one follower's embedded `Database` handle and issuing
single-record transactions produced:

```
SEVER [LocalBucket] Invalid record size 570425357 for record #29:20: deleting record
SEVER [LocalBucket] Error on reusing hole in page PageId(graph/29/0), invalid length 0
```

Every transaction reported success; a node ended up holding a third of them. Reproduced here at
155/450 records with **zero** conflicts raised - no transaction was ever told it had lost a race.

The same load against the leader, and single-threaded load against a follower, were both clean.

## Root cause

A replica captured its transaction in `TransactionContext.commit1stPhase(false)`, which deliberately
skipped the per-file commit locks:

```java
} else
  // IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION
  lockedFiles = new ArrayList<>();
```

The page-version validation that follows (`PageManager.checkPageVersion`) still ran, but it is a
check-then-act and the file lock is what makes it atomic. The assumption recorded in
`PageManager.publishPages` was that "the single-threaded Raft apply provides the serialization" - but the
apply only orders the **write** side. Nothing ordered the **capture** side, so:

1. Threads A, B and C each load bucket page `v=N` and allocate a record into it.
2. All three pass `checkPageVersion` against the local page manager, which still reports `N` - a replica
   never runs phase 2, so its pages only change when the state machine applies.
3. All three ship a WAL delta stamped `currentPageVersion = N+1`.
4. `TransactionManager.applyChanges` applies A (`N` -> `N+1`), then meets B at **equal** version. Equal
   version is not skipped: it re-applies, by design, because that path repairs torn writes (#4926).
5. B's delta is a partial byte range computed against a page that no longer exists. Splicing it onto A's
   page leaves a header, a slot table and record bytes from different transactions - hence the invalid
   record sizes and the deleted records.

Losing writes and corrupting pages are the same bug: step 4 both discards A's records and produces a
physically inconsistent page.

## Fix

Two halves, both required.

**1. `TransactionContext.commit1stPhase` - replicas take the commit locks too.** The lock acquisition
(and the #4937 late-joiner union check that keeps it complete) is no longer gated on `isLeader`.
`indexChanges.commit()` stays leader-only: that genuinely is the leader's job. This serializes local
committers, so the second one now fails the version check with a retryable
`ConcurrentModificationException` instead of shipping a colliding delta.

**2. `RaftReplicatedDatabase` - hold those locks until the entry is applied locally.** Locking alone
only moved the failure (155 -> 302 records): `reset()` released the locks as soon as the quorum acked,
while this node's pages were still at `N`, so the next transaction took the lock and read a stale
version anyway.

The replica now waits for **its own** entry's log index before releasing. The existing
`waitForLocalApply()` was not usable: it waits for the local commit index, which on a follower still
trails the leader and therefore does not cover the entry just written. So the committed index is
plumbed back from Ratis:

- `RaftGroupCommitter.CancellablePendingEntry` gains a `volatile long logIndex`, published by the
  flusher from `reply.getLogIndex()` before it completes the future.
- `submitAndWait` and `RaftTransactionBroker.replicateTransaction` return it (-1 when unknown).
- The replica branch calls `waitForAppliedIndex(committedLogIndex)`.

On timeout the wait returns silently, degrading to the previous behaviour rather than failing a
transaction the cluster has already committed.

No deadlock: `applyChanges` takes no file locks, so the apply thread never waits on the committer.

## Verification

`Issue5503ConcurrentFollowerWritesIT` - 3 nodes, 3 threads x 150 single-record transactions through a
follower handle, retrying `NeedRetryException` the way an application must. Asserts a scan count
(`count(@rid)`, not the cached `count(*)` counter, which would hide a record deleted as malformed) and
the full set of distinct values on **every** node.

Both halves of the fix were shown to be load-bearing:

| State | Records on a node | Conflicts raised | Corruption log lines |
|---|---|---|---|
| Before the fix | 155 / 450 | 0 | 200+ |
| Locking only | 302 / 450 | 4 | many |
| Locking + apply wait | 450 / 450 | some, all retried | 0 |

Passed 3 consecutive runs.

### Regression runs

- **ha-raft unit tests**: 770 tests, 0 failures.
- **ha-raft integration tests**: 69 tests across four targeted batches, 0 failures - the write-against-replica
  and commit-path suites (`RaftEmbeddedWriteAgainstReplicaIT`, `RaftReplicationWriteAgainstReplicaIT`,
  `Raft3PhaseCommitIT`, `Issue4740Phase2ReconcileIT`, `Issue4790PhantomCommitOriginSkipIT`,
  `Issue5064CommittedRemotelyContractIT`, `Issue5410AbandonedTicketReleaseIT`, `OriginNodeSkipIT`), the
  replication/concurrency/index suites (`RaftReplicationIT`, `RaftHTTPGraphConcurrentIT`,
  `SuperNodeAppendHAConsistencyIT`, `Issue5381SlotMergeRaftIT`, `Issue5443FollowerIndexGapIT`,
  `RaftIndexOperations3ServersIT`), the read-consistency suites that share `waitForAppliedIndex`, and the
  crash/failover suites. The full 105-class IT suite was not run: it takes hours, and CLAUDE.md calls for
  the connected tests rather than the whole suite.
- **engine unit tests**: 10074 tests, 50 failures - **all pre-existing**, every one of them
  `NoClassDefFoundError: org.graalvm.polyglot.Engine$ImplHolder` or "Query engine 'js' was not found".
  Reproduced identically on a pristine worktree at `origin/main` (eb7739e9d), so the Graal JS language is
  simply not loadable on this JDK. No transaction, bucket, locking or index test failed.

`RaftDictionaryManyNewKeysIT.httpInsertContentWithProgressivelyMoreKeysOnFollower` failed once inside a
batch with `DatabaseIsClosedException` (a shutdown race in the HTTP path), then passed on three
subsequent runs including a re-run of the identical batch. Recorded as an observed flake rather than
proven pre-existing: this fix does make follower writes slower, so a timing-sensitive follower-write test
becoming flakier is a plausible - if unproven - interaction worth watching in CI.

## Impact and follow-ups

- Follower writes now hold per-file commit locks across the Raft round trip **and** the local apply, so
  concurrent writers to the same bucket serialize. The leader already held its locks across its own
  round trip, so this is symmetric, but a write-heavy follower workload will see lower throughput and
  more retries. That is the correct trade against silent corruption; writes routed to the leader are
  unaffected.
- **Lock hold time now exceeds `COMMIT_LOCK_TIMEOUT` by a wide margin.** With defaults, a replica holds a
  bucket's commit lock for `submitAndWait` (up to `2 * quorumTimeout` = 20 s, plus the grace window) plus
  the apply wait (up to `quorumTimeout` = 10 s), while other writers give up on that lock after
  `COMMIT_LOCK_TIMEOUT` = 5 s. On a healthy cluster the apply is sub-millisecond and none of this is
  observable, but under leader churn or apply lag same-bucket writers serialize and burn retries. Note the
  20 s half is not new and not replica-specific: the leader has always held its commit locks across
  `submitAndWait`. What is new is that replicas hold locks at all, plus the apply wait.

  **Shortening the apply wait was considered and rejected.** It looks attractive - the wait is a purely
  local catch-up, not a quorum operation, so 10 s is generous - but the timeout path is precisely the path
  that releases the locks with stale pages and reopens the corruption window. A shorter bound makes the
  dangerous branch *more* reachable to buy back a few seconds of lock contention, which is the wrong
  direction for a silent-corruption fix. The wait stays at `quorumTimeout`, the timeout is loudly logged,
  and the contention is accepted and documented instead.
- **New failure mode for follower writers: `LockTimeoutException`.** Because the locks are now held across
  the round trip and the apply wait, concurrent writers to the same bucket can exhaust
  `COMMIT_LOCK_TIMEOUT` (5000 ms by default) where previously a replica took no locks at all. It is
  retryable - `LockTimeoutException` extends `NeedRetryException`, as does
  `ConcurrentModificationException` - but an application that only caught the latter on follower writes
  will now see the former. Worth a release note.
- `db.transaction(...)` on a follower now implies read-your-writes on that node, which it did not before.
- **If the apply wait times out**, the locks are released with the local pages still behind, which is the
  pre-#5503 condition. `waitForAppliedIndex` already logs, but as a READ_YOUR_WRITES *consistency*
  warning, which reads like a stale-read risk rather than a corruption one; the replica branch therefore
  logs its own WARNING naming the risk and pointing at state machine apply lag.
- `lockFilesFromChanges()` includes `indexChanges.addFilesToLock(...)`, so replicas now take locks on index
  files even though `indexChanges.commit()` stays leader-only. Harmless for correctness, and it keeps the
  late-joiner union check consistent, but it is extra contention on index files for follower writes - and
  another reason the `indexChanges`-on-replica question below deserves its own issue.
- Not addressed here: `indexChanges.commit()` is still skipped on a replica, so it is worth confirming
  separately whether index entries for follower-originated writes are produced by the leader as the
  comment claims. That is orthogonal to this corruption and out of scope.
