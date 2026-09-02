# #7052 - Cypher `timestamp()` re-reads the wall clock per call

## Symptom

`OpenCypherScalarFunctionsComprehensiveTest.timestampConsistency` fails intermittently:

```
expected: 1788346805145L
 but was: 1788346805144L
```

`RETURN timestamp() AS ts1, timestamp() AS ts2` returns two values that differ by 1 ms whenever the two
evaluations straddle a millisecond tick.

## Root cause

`com.arcadedb.function.temporal.TimestampFunction.execute` returns `System.currentTimeMillis()` on every
invocation, and `ExpressionOptimizer.isDeterministicFunction` deliberately marks `timestamp` non-foldable
(correctly - the value must not be constant-folded into a cached plan), so every occurrence in a statement is
evaluated separately against a freshly-read clock.

Neo4j documents `timestamp()` as the milliseconds of the *transaction* clock: stable for the whole
transaction, so `RETURN timestamp() AS a, timestamp() AS b` always yields `a = b`, and repeated calls inside
one query never advance. The class javadoc already claims "Compatible with Neo4j's timestamp() behavior", so
the test asserts the documented contract and the implementation is the half that is wrong.

ArcadeDB already has the machinery for this: `CypherFunctionHelper.getStatementTime(CommandContext)` freezes
`date`/`localtime`/`time`/`localdatetime`/`datetime` in the `$statementTime` context variable on first use, and
the five constructor functions read from it. `timestamp()` was simply never wired into it.

### Second defect in the same helper

`getStatementTime` built its five entries from five *separate* `now()` calls:

```java
statementTime.put("date", CypherDate.now());
statementTime.put("localtime", CypherLocalTime.now());
statementTime.put("time", CypherTime.now());
statementTime.put("localdatetime", CypherLocalDateTime.now());
statementTime.put("datetime", CypherDateTime.now());
```

Five clock reads means the "frozen" statement time is not a single instant. `RETURN date() AS d, datetime() AS dt`
can straddle midnight and report a date one day behind its own datetime, and `datetime()` and `localdatetime()`
can disagree by a millisecond in exactly the way `timestamp()` did. The issue explicitly asks for these to be
checked "while in there".

## Fix

`CypherFunctionHelper.getStatementTime` now takes **one** clock reading and derives every entry from it,
including the new `timestamp` entry:

```java
final ZonedDateTime now = ZonedDateTime.now();
statementTime.put("timestamp", now.toInstant().toEpochMilli());
statementTime.put("date", new CypherDate(now.toLocalDate()));
...
```

Each derivation is value-identical to the `now()` factory it replaces (`CypherTime.now()` was
`OffsetTime.now(ZoneOffset.UTC)`, so it becomes `now.toInstant().atOffset(ZoneOffset.UTC).toOffsetTime()`), so
no observable value changes except that they now share an instant.

`TimestampFunction.execute` reads the pinned value instead of the clock.

`timestamp` stays non-foldable in `ExpressionOptimizer`: the value is per *execution*, not per *plan*, and the
plan cache outlives a single execution. Non-foldable + statement-pinned is the combination that gives both
"stable within a statement" and "advances between statements".

## Scope / non-goals

- `timestamp()` is registered only for Cypher (`CypherFunctionFactory` line 420); SQL's
  `DateCurrentTimestamp` is a different function with no Neo4j contract and is untouched.
- The pin is per **statement** (per `CommandContext`), not per transaction, matching what `date()`/`datetime()`
  have always done in ArcadeDB. Neo4j's transaction clock is a strictly wider guarantee; matching it would need
  the freeze to move onto the transaction object and would change `date()` too. Left as-is deliberately.
- `date.realtime`/`datetime.realtime` are aliased to the statement-pinned constructors already (see
  `CypherFunctionFactory` lines 466-471). That pre-existing deviation from Neo4j, where `.realtime` is
  documented to read the clock on every call, is out of scope here.

## Tests

New class `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherStatementClockTest.java`:

| test | asserts |
|---|---|
| `timestampIsPinnedAcrossManyEvaluationsInOneStatement` | 200k evaluations of `timestamp()` in one statement yield exactly one distinct value - deterministic, since the scan spans many milliseconds |
| `timestampIsPinnedAcrossTwoProjectionsInOneStatement` | the original `RETURN timestamp() AS ts1, timestamp() AS ts2` contract |
| `timestampAdvancesBetweenStatements` | a later statement sees a strictly greater value - proves the pin is per statement and not frozen forever |
| `timestampAgreesWithDatetimeEpochMillis` | `timestamp() == datetime().epochMillis` in the same statement - the Neo4j relationship, and the regression test for the five-clock-reads defect |
| `statementClockEntriesShareOneInstant` | `date()`, `localdatetime()` and `datetime()` in one statement all report the same calendar day, and `localdatetime()`/`datetime()` the same wall-clock second |

Two of them fail deterministically before the fix, so neither is a coin flip that merely happens to be red:

```
timestampIsPinnedAcrossManyEvaluationsInOneStatement  Expected size: 1 but was: 84
timestampAgreesWithDatetimeEpochMillis                expected: 1788380281544L but was: 1788380281738L
```

`timestampIsPinnedAcrossTwoProjectionsInOneStatement` is the issue's own shape and, like the original test, only
fails on the fraction of runs that straddle a tick - it is kept as the literal statement of the contract, with the
200,000-evaluation test as the deterministic guard behind it. Nothing asserts on elapsed wall-clock time: the
assertions are on the *number of distinct values* and on equality between two values, both of which hold for any
duration, and `timestampAdvancesBetweenStatements` is a lower bound that a stall can only make more true.

The existing `OpenCypherScalarFunctionsComprehensiveTest.timestampConsistency` is left untouched and stops
flaking as a side effect.

## Design notes

- **`getStatementTime` does not guard against a null `CommandContext`.** No caller can supply one: the built-in
  path is `FunctionCallExpression.invoke` -> `function.execute(args, context)` with a context that the executor
  always has, and `CallStep`'s no-context `execute(Object[])` overload belongs to `FunctionDefinition` (user
  function libraries), not `StatelessFunction`. A fallback to `System.currentTimeMillis()` would silently
  un-pin the clock on some future path instead of failing loudly there, which is the defect this issue is
  about. The five temporal constructors have always dereferenced the context on their zero-argument path for
  the same reason.
- **`timestamp` stays in `ExpressionOptimizer`'s non-deterministic list.** Statement-pinned is not
  plan-pinned: the plan cache outlives an execution, so folding the value into a cached plan would freeze it
  across every later run of the same query text. Non-foldable evaluation reading a per-execution frozen value
  is the combination that gives both halves of the contract.

## Test results

- New class: 5/5 green (2 of them red before the fix).
- `engine` temporal/scalar regression set (`*Temporal*`, `OpenCypherScalarFunctions*`, `OpenCypherDate*`,
  `OpenCypherTime*`, `*Duration*`): 277 tests, 0 failures - including `OpenCypherTimestampTest` (14) and the
  previously flaky `OpenCypherScalarFunctionsComprehensiveTest` (85).
- `com.arcadedb.query.opencypher.**` + `com.arcadedb.function.**`: 6,579 tests, 0 failures, 13 skipped.
- Full `engine` module (`-DexcludedGroups=benchmark,vector,slow`): see PR body.

## Impact

- `timestamp()` is stable within a statement and still advances between statements.
- `timestamp()` now equals `datetime().epochMillis` in the same statement, as in Neo4j.
- `date()`, `localtime()`, `time()`, `localdatetime()` and `datetime()` in one statement now describe a single
  instant rather than five successive ones. No individual value changes shape or timezone.
- One fewer `System.currentTimeMillis()` call per `timestamp()` occurrence per row; the frozen map was already
  being allocated for any statement using a temporal constructor.
