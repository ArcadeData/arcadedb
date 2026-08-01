# #5655 - OpenCypherQueryEngine commits on the inner LocalDatabase, bypassing Raft replication

Cypher half of #5492. The SQL half shipped as #5652.

## Root cause

`OpenCypherQueryEngine` holds `private final DatabaseInternal database`, assigned in the constructor. On an
HA leader that is the inner `LocalDatabase`: `RaftReplicatedDatabase.getQueryEngine()` delegates to
`proxied.getQueryEngine()`, so the engine is built with the inner instance. The engine then handed that
instance to everything it executed.

`LocalDatabase.commit()` writes pages locally. `RaftReplicatedDatabase.commit()` proposes them to Raft and
*then* writes. A commit on the inner instance succeeds, applies its pages on the leader, and replicates
nothing. Nothing throws where the mistake is made.

Two paths reach such a commit, and they fail independently:

1. **The explicit `COMMIT` statement.** `executeTransaction()` ran `database.begin()` / `database.commit()` /
   `database.rollback()` straight against the field. `commit()` is the only one of the three where the two
   instances differ; `begin()` and `rollback()` delegate to the inner database on the wrapper too.

2. **Auto-commit of a write step.** `CypherExecutionPlan` does `context.setDatabase(database)` with whatever
   the engine handed it, and `SetStep`, `DeleteStep`, `MergeStep`, `RemoveStep`, `ForeachStep` and a writing
   `CALL` subquery each call `begin()`/`commit()` **directly** on `context.getDatabase()` when no transaction
   is already open.

### Why a Cypher smoke test would have stayed green

`CreateStep` is not affected. It wraps its work in `context.getDatabase().transaction(...)`, and
`LocalDatabase.transaction()` drives `wrappedDatabaseInstance` internally rather than `this`
(`LocalDatabase.java:1395-1403`). So a bare `CREATE` always replicated correctly while an equally ordinary
`SET` in the same session did not. That asymmetry is the reason this survived alongside a passing Cypher HA
suite, and it is worth remembering: "does it go through `transaction()` or call `commit()` directly?" is the
question that separates the safe steps from the exposed ones.

### Resolving the two items the issue left unverified

The issue filed this as code-shape-matches, not reproduced. Both open questions are now answered:

1. **Is an explicit Cypher transaction reachable across the wrapper on a leader?** Yes. `LocalDatabase.command()`
   opens no transaction of its own, so `START TRANSACTION` / write / `COMMIT` issued as three commands on one
   thread binds through the thread-local `DatabaseContext` and commits on the inner instance.
2. **Are autocommit Cypher writes already covered by the wrapper's commit finalization?** No, and the
   exposure is not uniform. It depends on which step runs: the `transaction()`-based steps were always safe,
   the ones that call `commit()` directly were not.

Both are reproduced by the new test, and the harness's own `DatabaseComparator` teardown independently
reports the divergence as `Page PageId(graph/28/0) has different versions on databases. DB1 2 <> DB2 1`.

## Fix

Same resolution as #5652: resolve `getWrappedDatabaseInstance()` for anything that can commit. Off HA it
returns the instance itself, so it is a no-op there.

- New `executionDatabase(CypherStatement)` supplies the database the execution plan is built with, and
  therefore what `CommandContext.getDatabase()` returns for every step in the tree, subqueries included -
  all of it flows from that one argument.
- `executeTransaction()` resolves the wrapper for `begin`/`commit`/`rollback`, so the statement never
  straddles both instances.

**The switch is on `statement.isReadOnly()`, not on the entry point.** A read-only statement cannot commit,
so resolving the wrapper would change nothing about durability, while it *would* put the nested reads the
steps issue on a different footing. Precisely which ones matters:
`RaftReplicatedDatabase.lookupByRID`/`iterateType`/`lookupByKey` delegate straight to the inner instance and
are indifferent, but its `query(language, ...)` overloads open with `waitForReadConsistency()`, and
`ExistsExpression`, `CountExpression` and `CollectExpression` each evaluate their subquery through that call.
Routing read-only statements through the wrapper would add a read barrier to every
`EXISTS`/`COUNT`/`COLLECT` subquery, follower-local ones included. #5652 achieved the same separation by leaving `query()`
alone, but that shortcut does not hold for Cypher: `PROFILE` bypasses the idempotency gate and executes, so
`PROFILE MATCH ... SET ...` arrives through `query()` and does write. `isReadOnly()` is computed at parse
time and already accounts for writes nested in a `CALL` subquery (`SimpleCypherStatement:152`), so it is the
right authority.

Not changed, and why:

- `executeDDL()` goes through `database.getSchema()`; `LocalSchema` is constructed with
  `wrappedDatabaseInstance` (`LocalDatabase.java:2433`), so schema DDL already replicates regardless of which
  instance the engine holds - which is also why SQL DDL replicated before #5492 was found. Since the whole
  class of bug is "committed on the wrong instance", this one is pinned by a test
  (`cypherDDLReachesFollowers`) rather than left as an argument from the constructor. That test is a guard,
  not a reproducer: it passes with or without the fix, by design.
- `executeAdmin()` operates on the server security manager, not on database pages.
- `executeSession()` only evaluates an expression to store a session parameter; it never commits.

## Files

| File | Change |
|---|---|
| `engine/.../query/opencypher/query/OpenCypherQueryEngine.java` | `executionDatabase()` helper; used in `execute()` and `executeTransaction()` |
| `ha-raft/src/test/java/.../Issue5655CypherCommitsOnInnerDatabaseIT.java` | new regression test |

## Test

`Issue5655CypherCommitsOnInnerDatabaseIT`, a Cypher analogue of `Issue5492TruncateBatchNotReplicatedIT`:
2-node cluster, one test per exposed path, plus one that pins the entry-point decision
(`PROFILE MATCH ... SET` through `query()`).

Each asserts `ArcadeStateMachine.TEST_WAL_GAP_COUNTER` *before* querying the follower. A resync reinstalls
the follower's database and a count issued after it throws `DatabaseIsClosed`, which reads like
infrastructure noise rather than the divergence that caused it.

Proven to fail before the fix:

```
[ERROR] Issue5655CypherCommitsOnInnerDatabaseIT.autocommitCypherWriteStepReachesFollowers:151
        [the value written by the auto-committing SET step must reach the follower] expected: 1L but was: 0L
[ERROR] Issue5655CypherCommitsOnInnerDatabaseIT.explicitCypherCommitReachesFollowers:102
        [a write committed through the Cypher COMMIT statement must reach the follower] expected: 1L but was: 0L
```

## Verification

| Suite | Result |
|---|---|
| `Issue5655CypherCommitsOnInnerDatabaseIT` | 4/4 pass; the 3 reproducers each proven to fail without the fix |
| `engine` module, full | 10684 pass, 0 failures, 23 skipped |
| `Issue5492*IT` (SQL half, cross-regression) | 3/3 pass |

Server-module Cypher ITs (`Issue4141SessionManagementIT` and siblings, the transaction control over HTTP
path) could not be run locally: port 2480 is held by a local service, so those ITs are CI-only here.

## Impact

Every HA deployment issuing Cypher writes through the `opencypher` engine was affected whenever a write step
auto-committed or an explicit `COMMIT` ran. Off HA nothing changes:
`LocalDatabase.getWrappedDatabaseInstance()` returns `this`.

## Follow-ups

- The rule is recorded in `ha-raft/CLAUDE.md` under "Anything that commits must hold the WRAPPED database
  instance, not the inner one".
- The steps that call `begin()`/`commit()` directly rather than going through `transaction()` also forgo its
  MVCC retry loop, which `CreateStep` gets for free. That is a separate concern from replication and is not
  touched here.

## PR

https://github.com/ArcadeData/arcadedb/pull/5688 - four review cycles, all approving. Cycle-by-cycle
process notes live in the PR thread; what follows is only the part that stayed relevant to the code.

### Points settled during review

- **`PROFILE MATCH ... SET` is why the switch is on `isReadOnly()`.** Review asked for the design decision to
  be pinned by a test rather than a comment, which is fair: it is the case a refactor back to entry-point
  switching would silently break. `profiledCypherWriteThroughQueryReachesFollowers` covers it, and was
  confirmed to fail with `executionDatabase()` stubbed to `return database`.
- **Which read path actually carries the read-consistency barrier.** An early draft of the rationale listed
  `lookupByRID`/`iterateType` alongside `query()`. Only `query(language, ...)` opens with
  `waitForReadConsistency()` (`RaftReplicatedDatabase.java:1274`, `:1281`, `:1287`); the other three delegate
  straight to the inner instance. Since `ExistsExpression`, `CountExpression` and `CollectExpression` all
  evaluate their subqueries through `query()`, the conclusion holds - but on that one method, not on reads
  generally.
- **Mixed instances inside an explicit transaction are intentional.** After `START TRANSACTION` a read-only
  `MATCH` runs on the raw instance and a write on the wrapper. Safe because the transaction lives in the
  thread-local `DatabaseContext` keyed by database path, shared by both handles, and reads never commit.
  Recorded in the `executionDatabase()` Javadoc.
- **`ROLLBACK` has no dedicated replication test.** It commits nothing, so such a test could only assert an
  absence. Deliberately skipped.

## Follow-up worth filing

`gremlin`, `graphql` and `mongodbw` hold the same `private final DatabaseInternal database` field shape and
have never been audited against the "anything that commits must hold the wrapped instance" rule. Two engines
have now needed it (#5492 SQL, #5655 Cypher), and both hid the same way: nothing throws at the mistake, and
the paths that happen to route through `database.transaction(...)` stay green and mask the ones that do not.
Raised in review three times; not filed here because that is the maintainer's call.
