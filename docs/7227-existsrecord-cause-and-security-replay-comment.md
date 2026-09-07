# Issue #7227 - two leftovers from #7141 and #7137

<https://github.com/ArcadeData/arcadedb/issues/7227>

## Finding ledger

- [x] 1. `LocalBucket.existsRecord` throws `DatabaseOperationException` without chaining the `IOException`
  - **fixed** - `LocalBucket.java:534` now passes `e`, with the #7141 rationale restated for this call site
- [x] 2. `ArcadeStateMachine.applySecurityUsersEntry`'s javadoc claims a failed `SECURITY_USERS_ENTRY`
  "replays on the next start", which contradicts the SEVERE it logs and the contract note in
  `ServerSecurity.applyReplicatedUsers`
  - **fixed** - the paragraph now says the entry is NOT replayed and the change has to be reissued, and
    names the mechanism; the behaviour it describes is pinned by a new test

## Analysis

### Item 1

`engine/src/main/java/com/arcadedb/engine/LocalBucket.java:534`

```java
} catch (final IOException e) {
  throw new DatabaseOperationException("Error on checking record existence for " + rid);
}
```

`readRecordSizeMarker` resolves the record's page through `database.getTransaction().getPage(...)`,
so the `IOException` here is a real page read failure - a permission problem, a full volume, a short
file, a closed channel. Dropping it leaves the caller with a message and nothing else.
`isRecordStoredInSinglePage`, twenty lines below and reading the same marker through the same
helper, already chains.

### Item 2

`ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:3466`

The javadoc says the durability of the user change "is outstanding, and the entry replays on the next
start". It does not. Traced in the tree:

- `applySecurityUsersEntry` throws `ReplicationException` on a persistence failure.
- `applyTransaction` catches `ReplicationException` and returns a failed future **before**
  `lastAppliedIndex.getAndSet(index)` / `writePersistedAppliedIndex(index, ...)`, so this entry does
  not advance the position - but it also does not halt the node.
- The node therefore keeps applying, and the **next** entry runs those same two lines with its own,
  higher index. `writePersistedAppliedIndex` overwrites `globalAppliedIndex` unconditionally, and
  `updateLastAppliedTermIndex` moves the Ratis-side position past the failed entry too.
- `reinitialize()` seeds `lastAppliedIndex` from the snapshot/persisted position, so a restart
  resumes above the failed index. Nothing replays it.

That is exactly what the SEVERE in the same method and the contract note at
`ServerSecurity.applyReplicatedUsers` already say ("a restart before this is fixed reads the previous
`server-users.jsonl`" / "so a restart reloads the stale file with nothing left to replay. Fixing the
volume is therefore only half the repair: the user change has to be reissued"). The javadoc is the
only place in the tree that says the opposite, and it is the place an operator reads while deciding
what to do.

## Completeness

### Invariant 1

> Every `IOException` a main-source `catch` turns into a thrown exception carries the original as the
> cause (or, where a retry supersedes it, is handled without a throw).

Sweep - a script that parses every `catch (X var) { ... }` block in `*/src/main/java`, keeps the ones
that `throw` and never mention `var`, was run over all modules (script kept at
`scratchpad/scan7227.py`, 61 such blocks in `engine` alone). Filtered to IO-shaped catches across the
whole tree:

```
$ for m in */src/main/java; do python3 scan7227.py $m; done | grep -iE 'catch\((.*IOException|.*ClosedChannel|.*FileNotFound)'
engine/src/main/java/com/arcadedb/engine/LocalBucket.java:534        catch(IOException e)  throw new DatabaseOperationException("Error on checking record existence for " + rid);
engine/src/main/java/com/arcadedb/engine/PaginatedComponentFile.java:392  catch(ClosedChannelException e)  LogManager...SEVERE...; reopenChannelUnderWriteLock(); ...
```

Two hits, one of them the reported one. `PaginatedComponentFile:392` is **argued**: it does not
rethrow that exception at all - it names the file in a SEVERE, reopens the channel and retries the
read, so there is no exception whose cause could be dropped.

Same-shape siblings inside the file itself:

```
$ grep -n -A3 "catch (final IOException" engine/src/main/java/com/arcadedb/engine/LocalBucket.java | grep "throw new"
535:      throw new DatabaseOperationException("Error on checking record existence for " + rid);      <-- the only one
558:      throw new DatabaseOperationException("Error on checking record layout for " + rid, e);
742:      throw new DatabaseOperationException("Cannot scan bucket '" + componentName + "'", e);
1213:     throw new DatabaseOperationException("Cannot count bucket '" + componentName + "'", e);
2143:     throw new DatabaseOperationException("Error on lookup of record " + rid, e);
2296:     throw new DatabaseOperationException("Cannot add a new record to the bucket '" + componentName + "'", e);
2387:     throw new DatabaseOperationException("Cannot restore record at position " + position + ...", e);
2436:     throw new DatabaseOperationException(... , e);
3027:     throw new DatabaseOperationException("Error on slot delete rebase for page " + page.getPageId(), e);
3584:     throw new DatabaseOperationException("Error on update record " + rid, e);
4056:     throw new DatabaseOperationException("Error on deletion of record " + rid, e);
```

Line 535 is the single outlier among the 11 throwing `IOException` catches in `LocalBucket`.

The non-IO catches the script found (`NumberFormatException` on a string the message already quotes,
`IllegalArgumentException` on an enum name the message already lists) are a different defect class
and outside this issue: the cause carries no information the message lacks. Not filed - reported here
so the count is not silently dropped.

### Invariant 2

> No comment in the tree tells an operator that a `SECURITY_USERS_ENTRY` whose local persist failed
> comes back on its own.

```
$ grep -rn "replays on the next start" --include="*.java" .
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:3466

$ grep -rn "replay" --include="*.java" ha-raft/src/main/java server/src/main/java | grep -i "user\|securit"
server/.../ServerSecurity.java:865:  // the stale file with nothing left to replay. Fixing the volume is therefore only half the repair
server/.../http/IdempotencyCache.java:248            (unrelated: HTTP response replay)
server/.../http/handler/AbstractServerHttpHandler.java:983  (unrelated: HTTP response replay)
```

One occurrence. Also checked the other two places that discuss this entry type
(`ArcadeStateMachine:401` on the node-wide halt flag, `:1178` on `handleUnexpectedApplyError`): both
speak about a failure that DOES halt, and both stay correct.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `Bucket.existsRecord(rid)` direct (GraphEngine, BucketIterator, RestoreEdgeStatement) → `LocalBucket.existsRecord` | yes | yes - `theCauseOfAFailedExistenceCheckSurvivesOnTheBucketApi` |
| `Database.existsRecord(rid)` → `LocalDatabase:963` → `LocalBucket.existsRecord` (SQL `FetchFromRidsStep`, Gremlin, server/HA wrappers) | yes | yes - `theCauseSurvivesThroughTheDatabaseLevelEntryPoint` |
| `LocalBucket.isRecordStoredInSinglePage` (same helper, same shape) | already correct before this PR | argued - chains since it was written |
| `PaginatedComponentFile:392` `ClosedChannelException` | n/a, does not rethrow | argued - reopens and retries |
| `ArcadeStateMachine.applySecurityUsersEntry` javadoc | yes (comment) | yes - the behaviour it now describes is pinned by `Issue7227SecurityEntryIsNotReplayedTest` |

## Residual risk

The `IOException` half changes an exception's cause, nothing else: no control flow, no message, no
catch site keys on the cause type (`grep -rn "DatabaseOperationException" --include="*.java" | grep
getCause` → none). The state-machine half is a comment; the code it describes is unchanged, which is
why the new test asserts the behaviour rather than the text.

Not covered: the ~50 non-IO cause-dropping catches the sweep counted across `engine` (parse and enum
validation sites). They are a separate class of defect - the discarded cause adds nothing the message
does not already carry - and this issue does not claim them.

## Changes

| File | Change |
|---|---|
| `engine/src/main/java/com/arcadedb/engine/LocalBucket.java` | `existsRecord` chains the `IOException` (item 1) |
| `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` | `applySecurityUsersEntry` javadoc corrected (item 2) |
| `engine/src/test/java/com/arcadedb/engine/Issue7227ExistsRecordCauseTest.java` | new - 3 tests |
| `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7227SecurityEntryIsNotReplayedTest.java` | new - 1 test |

## Test results

Both new engine tests were run BEFORE the fix and failed for the right reason
(`Expecting a throwable with cause being an instance of java.io.IOException but current throwable has
no cause`, at `LocalBucket.java:535`), while the healthy-disk control passed. After the fix:

```
mvn -o -pl engine test -Dtest='Issue7227ExistsRecordCauseTest,Issue6282BrokenChainDeleteAndProbeTest,Issue7141DiagnosticsTest,LocalBucket*Test,Bucket*Test'
  -> Tests run: 40, Failures: 0, Errors: 0, Skipped: 0

mvn -o -pl engine test -Dtest='com.arcadedb.graph.**,com.arcadedb.engine.**' -DexcludedGroups=benchmark,slow,vector
  -> Tests run: 1541, Failures: 0, Errors: 0, Skipped: 0

mvn -o -pl ha-raft test -Dtest='Issue7227SecurityEntryIsNotReplayedTest,Issue7137SecurityEntryWriteFailureTest'
  -> Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

## Reachability

`existsRecord` is on live paths, not test-only: `BucketIterator:205` (every bucket scan, skipping
deleted slots), `GraphEngine:848`/`:1125` (edge-endpoint and vertex validation), `LocalDatabase:963`
behind `Database.existsRecord`, which `FetchFromRidsStep`, the Gremlin deleted-element filters,
`ServerDatabase` and `RaftReplicatedDatabase` all reach. No flag gates it. The state-machine change is
a comment, so its reachability is the reader's.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a `general-purpose` subagent for this. **No `Task` tool was
available in this session**, so the pass was run manually against the same three inputs (issue body,
`git diff main...HEAD`, the worktree) with the same brief. Recorded here so the difference is visible:
it lacks the "has not been convinced already" property the subagent is there for.

Findings:

1. **Real, out of scope - filed as #7247.** The sweep for the same *shape* rather than the same
   *type* found six `catch (Exception e)` blocks that throw and never mention `e`: four in
   `ArcadeDBServer` around `lifecycleEvent(...)` (start and stop, so the message on a server that
   will not come up is the server's own name and nothing else), `AbstractServerHttpHandler:445`
   (reached only by NON-security auth failures, since `ServerSecurityException` is re-thrown one
   catch above), and `BinarySerializer:555` (whose log call passes no throwable either). Verified by
   reading each site. Out of scope for #7227, which is about the `IOException` site the reporter
   named, so filed rather than folded in.
2. **Not real: "chaining the cause changes an error message someone asserts on."**
   `grep -rn "Error on checking record existence" --include="*.java" .` returns only the throw site
   itself - no test, no handler, no client keys on that string, and nothing in main sources inspects
   a `DatabaseOperationException`'s cause
   (`grep -rn "DatabaseOperationException" */src/main/java | grep -i "getCause\|instanceof IOException"`
   → no hits).
3. **Not real: "the comment fix is untestable, so the test is decoration."** The new ha-raft test
   does not assert on comment text; it asserts the two positions a restart consults (the persisted
   applied index and the Ratis-side `getLastAppliedTermIndex`) both end up above the failed entry.
   Its intermediate assertion (`isLessThan(5L)` right after the failure) is what arms it: it proves
   the failed entry genuinely did not record itself, so the final `isEqualTo(6L)` is the NEXT entry
   moving the floor and not a value that was always there.
4. **Argued: the JVM-wide fault injector.** `PageManager.setPageReadFaultInjector` is static and, as
   its own javadoc says, would not survive parallel test execution. The engine module runs
   sequentially today, `Issue6282BrokenChainDeleteAndProbeTest` already relies on that, and the new
   test clears the injector both in an inner `finally` and in `@AfterEach`. No new constraint.
