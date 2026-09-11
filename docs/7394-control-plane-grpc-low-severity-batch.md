# #7394 - seven low-severity items from the control-plane and gRPC delta

Issue: https://github.com/ArcadeData/arcadedb/issues/7394
Branch: `fix/7394-low-severity-control-plane-grpc`

## Finding ledger

- [x] 1. Three search RPCs never set `ProtocolContext` - **fixed** on all three, on both the inline and the
      transaction-executor thread. The issue's "metered as internal" is half right: the context really did
      report `internal` (proven by reverting the fix), but these legs reach no metric at all on any protocol,
      which is **#7418**. `insertStream`/`insertBidirectional` carry the same gap: **#7407**
- [x] 2. `TS_MAX_BATCH_SIZE` bounds rows, not bytes - **fixed**: the row cap is restated as a heuristic and
      paired with a 2 MiB serialized-size budget on both the raw-row and the aggregated-bucket stream
- [x] 3. Read-path tag filter accepted what the write path refuses - **fixed** at `TimeSeriesGateway.andTag`,
      the chokepoint all four protocol paths already share; the refusal also widened to catch `JSONArray`
- [x] 4. Profiler documented an open-ended recording it never gives - **fixed**: all three claims corrected,
      and the effective timeout is now reported in the HTTP JSON and in `ProfilerStateResponse`
- [x] 5. Late `expand` type check - **fixed**: both type checks moved ahead of the searches they followed, so
      the verdict no longer depends on whether the corpus happened to match
- [x] 6. `ProfilingResultSet`'s stale #7330 citation - **fixed**
- [x] 7. Gremlin sibling of #7330 - **fixed**, and it was a data-correctness bug, not a cost one: a mutating
      traversal under `$profileExecution` ran twice and mutated twice (reproduced: 2 vertices, 2 edges). The
      profiling pass is now skipped for mutating traversals. The read-only double execution remains: **#7408**

## Analysis

### 1. ProtocolContext on the search RPCs

`ArcadeDbGrpcService.vectorSearch/hybridSearch/fullTextSearch` (lines 5086/5097/5108) hand their body
to `GrpcUnaryCall.respond` without ever calling `ProtocolContext.set("grpc")`, unlike every other
handler in the file.

The subtlety the fix has to respect: `searchInTransaction()` runs the body on the *transaction's own
executor thread* when the request names a live transaction (`submitToActiveTransaction`), not on the
gRPC worker. `ProtocolContext` is a `ThreadLocal`, so setting it in the RPC method would tag the wrong
thread on exactly the path #7326 added. The precedent in this same file is `executeCommandInternal()`,
which sets it *inside* the body that runs on whichever thread was chosen. The fix follows that: the
set/clear pair wraps `body.apply(...)` inside `searchInTransaction`, so both the inline and the
transaction-executor paths are tagged.

Which of the three actually executes SQL today:

- `vectorSearch` -> `VectorSearch.search` -> `database.query("sql", ...)` (VectorSearch.java:93) - yes
- `hybridSearch` -> `HybridSearch.search` -> `database.query("sql", ...)` (HybridSearch.java:377/495/627) - yes
- `fullTextSearch` -> `FullTextQuery.search` -> reads the index directly, no SQL

`fullTextSearch` is tagged anyway so the three siblings cannot drift, and so the tag is already right
if that surface ever grows a SQL leg.

**Correction to the issue's diagnosis, found while building the test.** The item says this SQL is
"metered and traced as `protocol=\"internal\"`". The context half is exactly right - an
`Issue7394SearchProtocolContextIT` probe run against the unfixed tree reports `internal` on all three
paths. The metering half is not: these legs are not metered at all, on any protocol.
`arcadedb.query.duration` is opened by `LocalDatabase.query`/`command` (LocalDatabase.java:2053-2057),
and every leg reaches SQL through `AnalyzedQuery.execute()` instead, with `database.query(...)` only as
a fallback arm that is unreachable for `sql` because `SQLQueryEngine`'s `AnalyzedQuery.execute()` never
returns null (SQLQueryEngine.java:202-210). So a metric-count assertion cannot see this fix, and the
traffic is absent from the metric rather than misattributed. Filed as **#7418**; the test asserts the
invariant that does hold - the thread running the search reports `grpc` - from inside the SQL itself,
via a custom function called from the search's own `filter`.

### 2. TS_MAX_BATCH_SIZE

`ArcadeDbGrpcService:3266-3270` documents the constant as "A message holds this many rows, so an
unbounded batch size is an unbounded message: the cap keeps one answer under maxInboundMessageSize
regardless of what was asked for." It caps rows. 10,000 rows of a wide, string-heavy series crosses
gRPC's 4 MiB default client inbound limit long before the row cap bites, and the failure is the client
rejecting the frame rather than a status the caller can act on.

Fix: the row cap stays as a cheap first bound and is restated as a heuristic, and a byte budget
(`TS_MAX_BATCH_BYTES`) is accumulated from each row's/bucket's own `getSerializedSize()` so a batch is
flushed on whichever bound is reached first.

### 3. Tag-filter asymmetry

Write path: `GrpcTimeSeriesSupport.toSamples` calls `TimeSeriesGateway.requireStorableTagValue`, which
refuses an array, a `Collection` or a `Map` because a tag is stored by its text form.

Read path: `GrpcTimeSeriesSupport.toTagMap` does not. The value reaches `ColumnDefinition.coerceValue`,
which for a STRING column answers `value.toString()` - `[B@6bc7c054` for a byte array - which matches
no stored tag. The client is told "no data" instead of "that value is not valid".

Fix: the check moves into `TimeSeriesGateway.andTag`, the single point every protocol's tag selection
already converges on (its own javadoc says so), so read and write share one rule across gRPC and HTTP.
`requireStorableTagValue` is widened from `Collection` to `Iterable` so it also catches `JSONArray`,
which the HTTP `tags` object path can produce (`JSONArray implements Iterable<Object>` but not
`Collection`; `JSONObject implements Map` and was already caught).

### 4. Profiler timeout default

Three places state that a zero timeout records until stopped, and none of them is true:

- `ServerControlPlane.profilerStart` javadoc: "A `timeoutSec` of zero or less starts an open-ended
  recording, which is what the HTTP `profiler start` command does when it carries no timeout."
- `arcadedb-server.proto` `ProfilerStartRequest.timeout_seconds`: "0 records until ProfilerStop."
- `RemoteGrpcServer.profilerStart` javadoc: "`timeoutSeconds` of 0 records until `profilerStop()`."

`ServerQueryProfiler.start(int)` does `timeoutSeconds = timeoutSec > 0 ? timeoutSec : DEFAULT_TIMEOUT_SECONDS`
and `start()` passes `DEFAULT_TIMEOUT_SECONDS` (60). Both spellings of "no timeout" get 60 seconds and
an auto-stop timer.

Fix: the reporter's second option - keep the bounded default (it is a safety property: an unattended
recording keeps every server query on the `ProfilingResultSet` wrapping path) and *report* it. The
effective timeout is now returned from `ServerControlPlane.profilerStart`, so it reaches the HTTP JSON
response, and it is added to `ProfilerStateResponse.timeout_seconds` so it reaches gRPC callers. All
three documentation claims are corrected.

### 5. Late expand type check

`HybridSearch.search:171-174`:

```java
if (expandArgs != null) {
  requireVertexType(database, vectorQuery.index().typeIndex().getTypeName());
  if (!fullTextLeg.rows().isEmpty())
    requireVertexType(database, fullTextLeg.typeName());
```

The full-text type check is gated on the text query having *matched*. A full-text index declared on a
document type, with `expand` requested, is a configuration error that reports success whenever the
query happens to match nothing. `FullTextLeg.typeName()` is non-null whenever the leg ran, so the
emptiness test is not needed to tell "leg ran" from "leg absent".

Fix: both checks move ahead of the searches they used to follow - the vector type is checked right
after `VectorLeg.build` and before `runVectorLeg` spends a search, and the full-text type is checked
inside `runFullTextLeg` as soon as the index resolves and before `FullTextSearch.search` runs. A
rejected request now does no retrieval I/O at all, which is what "validate up front, where the other
configuration checks are" asks for.

### 6. Stale ProfilingResultSet citation

`ProfilingResultSet.java:38-45` explains `startNanos` with "...every write statement, and every
OpenCypher statement while the profiler is recording, because the profiler routes those through
`CypherExecutionPlan.profile()` which drains the plan eagerly...". Commit `a6a31f7092`/`f7fa4950a6`
(#7330) removed that reroute: `asksForTimedExecution()` now times the ordinary streaming run and the
eager `profile()` path is reached only by the explicit `PROFILE` keyword. The write-statement half of
the claim still holds; the OpenCypher half now points at something that no longer exists.

Fix: restate the parenthetical as what is true today.

### 7. Gremlin sibling of #7330

Confirmed, and it is a data-correctness defect rather than a cost-reporting one.
`ArcadeGremlin.execute()` (gremlin/ArcadeGremlin.java:72-108):

```java
final Iterator<?> resultSet = executeStatement();      // run 1
...
if (profileExecution) {
  query += ".profile()";
  final Iterator<?> profilerResultSet = executeStatement();   // run 2
  profilerResultSet.hasNext(); profilerResultSet.next();      // ... and drained
```

`ServerDatabase.command(language, query, Map)` injects `$profileExecution` for **every** statement
while the server profiler is recording, whatever the language. So on a recording server a Gremlin
`g.addV('Person')` builds the traversal twice and iterates both: the `.profile()` pass applies the
mutation, and the caller's iteration of `resultSet` applies it again. Two vertices.

This is #7330's shape exactly - switching the diagnostic on changes what the statement does - but
structurally different in its remedy: Gremlin has no per-step timer to read off the ordinary run, so
`.profile()` genuinely *is* a second execution. There is nothing to reroute away from.

Fix: refuse to buy a plan with a second mutation. Before the profiling pass runs, the traversal shape
is inspected the way `parse()` already inspects it (the analysis engine builds the step list without
executing it) and the pass is skipped when any step is `Mutating`. A read-only traversal keeps its
execution plan, so Studio's `profileExecution: "detailed"` is unchanged for reads; a mutating one
reports no plan rather than mutating twice.

## Completeness

### Invariant

1. Every gRPC RPC that executes SQL tags the executing thread with `ProtocolContext.set("grpc")`, on
   the thread that actually runs it.
2. A streamed `TimeSeriesQueryResult` never exceeds the byte budget the constant claims to enforce.
3. A tag value the write path refuses can never be silently accepted as a read filter.
4. No surface documents the profiler's zero timeout as unbounded, and every start response says when
   the recording will end.
5. `expand` against an index declared on a non-vertex type is refused whether or not the retrieval
   legs matched anything.
6. No comment cites the OpenCypher profiling reroute #7330 removed.
7. `$profileExecution` never changes what a statement does to the database.

### Sweep

```
$ grep -n "ProtocolContext" grpcw/.../ArcadeDbGrpcService.java
757, 975, 1591, 1691, 2052, 2217, 2630, 2692, 3280, 3295, 3338, 3371, 3407, 3447, 3636, 3662
$ grep -c "ProtocolContext" grpcw/.../ArcadeDbGrpcAdminService.java
0
```

Mapping SQL execution to the enclosing RPC (`awk` over `\.(query|command)\(`):

```
811   executeCommandInternal   <- executeCommand            ProtocolContext set (757)
1616  executeQueryInternal     <- executeQuery              ProtocolContext set (1591)
2261  streamQuery                                           ProtocolContext set (2052)
2332  streamQuery                                           ProtocolContext set (2052)
2430  streamQuery (paged)                                   ProtocolContext set (2052)
4169  tryUpsertByRecord   <- bulkInsert / insertStream / insertBidirectional
4189  keyExistsByRecord   <- bulkInsert / insertStream / insertBidirectional
$ grep -n "\.query(\|\.command(" engine/.../query/search/*.java
HybridSearch.java:377, 495, 627 ; VectorSearch.java:93   <- vectorSearch / hybridSearch RPCs, NOT set
```

`bulkInsert` sets the context (2630); `insertStream` (2698) and `insertBidirectional` (3842) do not,
so the upsert/conflict lookup SQL those two run is metered as `internal`. Same defect, different
shape - see the coverage table.

```
$ grep -rn "toTagMap" --include="*.java" .
ArcadeDbGrpcService.java:3420  (timeSeriesQuery)
ArcadeDbGrpcService.java:3644  (timeSeriesLatest)
$ grep -rn "buildTagFilter\|andTag(" --include="*.java" . | grep -v TimeSeriesGateway.java
GetTimeSeriesLatestHandler:118    -> buildTagFilterFromQueryParams (values are always String)
PostTimeSeriesQueryHandler:241    -> TimeSeriesHandlerUtils.buildTagFilter(JSONObject)  <- JSONArray reachable
PostGrafanaQueryHandler:116       -> TimeSeriesHandlerUtils.buildTagFilter(JSONObject)  <- JSONArray reachable
PromQLEvaluator:547               -> LabelMatcher values are String
```

```
$ grep -rn "profilerStart\|ProfilerStateResponse" --include="*.java" --include="*.proto" . | grep -v /test/
arcadedb-server.proto:1016, 1427
RemoteGrpcServer.java:631-635
ServerControlPlane.java:1059
PostServerCommandHandler.java:360
ArcadeDbGrpcAdminService.java:697-702
$ grep -rn "profileExecution" --include="*.java" . | grep -v /test/
ArcadeGremlin.java:72-79            <- second execution
ServerDatabase.java:549,562,575,618 <- injects for EVERY language while recording
PostCommandHandler.java:250         <- Studio 'detailed'
OpenCypherQueryEngine.java:307      <- timing only since #7330
BasicCommandContext.java:542        <- SQL: timing only
```

### Coverage table

| # | Entry point | Covered by fix? | Covered by a test? |
|---|---|---|---|
| 1 | gRPC `VectorSearch` (no transaction) | yes | yes |
| 1 | gRPC `VectorSearch` (inside a client transaction, runs on the tx executor thread) | yes | yes |
| 1 | gRPC `HybridSearch` | yes | yes |
| 1 | gRPC `FullTextSearch` | yes (tagged; executes no SQL today) | argued: shares `searchInTransaction`, the one place the tag is set |
| 1 | the search legs reach no metric at all, on any protocol | **filed #7418** | no |
| 1 | gRPC `insertStream` / `insertBidirectional` upsert-lookup SQL | **filed #7407** | no |
| 1 | gRPC admin service RPCs | argued: none of the 43 executes SQL; they call `ServerControlPlane`, which is schema/file work | n/a |
| 1 | HTTP / Postgres / Bolt / Redis / Mongo | argued: each sets its own protocol at the request boundary (grep above) | pre-existing |
| 2 | gRPC `TimeSeriesQuery` raw-row stream | yes | yes |
| 2 | gRPC `TimeSeriesQuery` aggregated-bucket stream | yes | yes |
| 3 | gRPC `TimeSeriesQuery` tag filter | yes | yes |
| 3 | gRPC `TimeSeriesLatest` tag filter | yes | yes |
| 3 | HTTP `POST /ts/{db}/query` `tags` object | yes (same `andTag` chokepoint) | yes |
| 3 | HTTP Grafana `POST /ts/{db}/grafana/query` `tags` object | yes (same chokepoint) | argued: identical call, same test |
| 3 | HTTP `GET /ts/{db}/latest` `?tag=n:v` | argued: the value is a `String` substring, never an array/collection/map | n/a |
| 3 | PromQL selector | argued: does not use `andTag` at all - `PromQLEvaluator.buildTagFilter` builds its own `TagFilter`, from `LabelMatcher.value()`, always a parser-produced `String` | n/a |
| 4 | gRPC `ProfilerStart` | yes | yes |
| 4 | HTTP `POST /api/v1/server` `profiler start` | yes | yes |
| 4 | MCP `profiler_start` | argued: `ProfilerStartTool` already requires 1..3600, so zero never reaches it (`Issue6762ToolInputBoundsTest`) | pre-existing |
| 5 | `HybridSearch.search` with `expand` + a full-text leg that matches nothing | yes | yes |
| 5 | `HybridSearch.search` with `expand` + a vector index on a non-vertex type | yes (now checked before the vector search runs) | yes |
| 6 | `ProfilingResultSet` javadoc | yes | n/a (comment) |
| 7 | Gremlin mutating traversal under `$profileExecution` | yes | yes |
| 7 | Gremlin read-only traversal under `$profileExecution` | argued: still profiled, plan preserved | yes (regression guard) |
| 7 | OpenCypher / SQL under `$profileExecution` | argued: timing-only since #7330 / `BasicCommandContext` | pre-existing |

### Reachability

- `ProtocolContext` is read by `QueryMetricsRecorder.Holder.record` and `QueryTracer.Holder.begin`, both
  called from `LocalDatabase.query/command`. The search legs do NOT take that path (#7418), so the fix is
  reachable but currently observable only by something else that reads the thread-local - which is what
  the test does, through a SQL function evaluated by the search itself. Verified failing before the fix
  (`internal` on all three paths) and passing after.
- `ServerDatabase` wraps the database for every server-side request, and `getProfiler()` is non-null
  once `ArcadeDBServer` builds `queryProfiler` (ArcadeDBServer.java:1528), so the Gremlin double
  execution is reachable on any server with the profiler recording.
- `TimeSeriesGateway.andTag` is called by four production sites (grep above), not only by tests.
- `ProfilerStateResponse.timeout_seconds` is a new proto field; the admin service sets it, so it is
  populated rather than defaulted.

### Residual risk

- Item 1 makes the search RPCs report `grpc` to anything reading `ProtocolContext`, but it does not make
  `arcadedb.query.duration` appear for them: those legs bypass the metered entry point entirely (#7418).
  A dashboard is not fixed by this PR, it is only no longer being lied to about the protocol.
- Item 1 is not fixed for `insertStream`/`insertBidirectional` (filed as #7407). Those two are
  bidirectional streams whose work hops onto a transaction executor from an observer callback rather
  than from a single request body, so the set/clear placement is a different change from this one.
- Item 2 bounds a *batch*. A single row larger than the byte budget is still emitted alone and will
  still be rejected by a client whose inbound limit is below it; nothing can split one row.
- Item 4 keeps the 60-second default rather than making a zero timeout unbounded. A caller that wants
  an unbounded recording still cannot ask for one - that is a feature request, not this fix.
- Item 7 leaves a read-only Gremlin traversal executing twice under `$profileExecution`, which is
  what `.profile()` costs in Gremlin. Only the mutation is prevented. Filed as #7408.

## Test results

| Module | Command | Result |
|---|---|---|
| engine | `-Dtest='Issue7394*'` | 13/13 pass |
| engine | `-Dtest='*TimeSeries*,*HybridSearch*,*VectorSearch*,*FullText*,*PromQL*,*Grafana*'` | 590/590 pass |
| server | `-Dtest='Issue7394*'` | 4/4 pass |
| server | profiler / control-plane / TS-handler / metrics unit classes | 76/76 pass |
| grpcw | full unit suite | 238/238 pass |
| grpcw | `-Dit.test=Issue7394SearchProtocolContextIT` | 4/4 pass |
| gremlin (via gremlin-it) | `-Dtest='Issue7394*'` | 5/5 pass |
| gremlin-it | full suite | 404 run, 7 errors - **pre-existing**, see below |
| mcp | full unit suite | 316/316 pass |
| grpc-client | full unit suite | 154 run, 6 errors - **pre-existing**, see below |

### Tests proven able to fail

- Item 7: before the fix, `anAddVertexUnderProfileExecutionCreatesOneVertexNotTwo` reported `expected: 1L but
  was: 2L` and the addE case likewise - the defect reproduced directly.
- Item 5: before the fix, `aNonVertexFullTextIndexWithExpandIsRefusedEvenWhenTheTextQueryMatchesNothing` was
  the single red test of the four in its class, which is exactly the corpus-dependence the item describes.
- Item 1: with the `asGrpcProtocol` wrapper removed, all three search assertions report `internal` instead of
  `grpc`, while the `theSameSqlRunOutsideAnyProtocolStillReportsInternal` control stays green - so the test
  cannot pass against a `ProtocolContext` that answers "grpc" for everything.

### Failures that are not this change

A server unrelated to this work has been listening on port 2480 in this environment for six days
(`lsof -nP -iTCP:2480 -sTCP:LISTEN` -> a java process with an elapsed time of `06-08:49`). Every HTTP-driven
test in the tree therefore talks to it. The failures it causes read as `503`, `Transaction Error on
transaction begin`, and `NeedRetry Server is installing a snapshot, please retry`, never as a port conflict -
the trap `CLAUDE.md` documents.

Both sets were re-run against the pristine files to confirm they are not this change:

- gremlin-it's 7 errors all abort in `AbstractGremlinServerIT.beginTest:66`, before any statement is parsed.
  Restoring the unmodified `ArcadeGremlin.java` and re-running produced the identical 7.
- grpc-client's 6 errors (`Issue4562RollbackDeleteTest`, `RemoteGrpcDatabaseRegressionTest`) reproduce
  identically with `ArcadeDbGrpcService.java` and `RemoteGrpcServer.java` restored to `HEAD`.

## Adversarial pass

The orchestrator's Phase 1.5 spawns one subagent that has not seen the author's reasoning. **The `Task`
tool is not available in this session** (`ToolSearch` for it returns nothing), so no independent pass ran.
What follows is a self-review of the diff, recorded as such rather than as the independent check it is
meant to replace - the whole value of that pass is the reviewer not having been persuaded already, and this
one had been.

| Question asked of the diff | Answer |
|---|---|
| Does widening `requireStorableTagValue` from `Collection` to `Iterable` refuse anything legitimate on the WRITE path? | No. Its only write caller converts values through `GrpcTypeConverter.fromGrpcValue`, which produces boxed primitives, `String`, `byte[]`, `List` and `Map`. `String` is not `Iterable`. Engine TS suite (590) and grpcw (238) both green. |
| Does `andTag` throwing for an unresolvable NAME break the documented "unknown name contributes nothing"? | No - the value is judged, the name is not. An unknown name with a valid value still contributes nothing; only an unstorable VALUE throws, and it throws under any name. Pinned by `anUnstorableValueIsRefusedEvenUnderANameThatResolvesToNoColumn`. |
| Does anything reach `andTag` that would now throw where it did not? | `GET /ts/{db}/latest` passes a `String` substring; PromQL does not call it at all (verified: `PromQLEvaluator.buildTagFilter` builds its own `TagFilter`). Only the two JSON-bodied HTTP endpoints and the two gRPC RPCs can carry a non-scalar, which is the fix. |
| Does the `andTag` javadoc's "every protocol converges here" survive? | It did not - PromQL is a reader that does not converge. Corrected in this PR rather than left standing, since the new `@throws` sits directly under it. |
| Does moving the vector-type check before `runVectorLeg` change which error a doubly-invalid request reports? | Yes, deliberately: a non-vertex vector index with `expand` now reports the type error instead of whatever the vector leg would have raised. That is the "up front" ordering the issue asks for. No existing test asserted the old order (engine suite green). |
| Does the Gremlin gate lose an execution plan that used to be produced? | For mutating traversals, yes - by design, that plan cost a second mutation. For statements ending in an eager terminal step, no: appending `.profile()` to those does not parse, so the attempt already landed in the existing `// NO EXECUTION PLAN` catch. Read-only traversals are unaffected, pinned by `aReadOnlyTraversalStillGetsItsExecutionPlan`. |
| Is the `$profileExecution` flag still consumed when the pass is skipped? | Yes - the `remove()` happens before the gate. Pinned by `theFlagIsRemovedFromTheParametersEvenWhenTheProfilingPassIsSkipped`. |
| Can `batchBytes` overflow? | Only if one row serialized to ~2 GiB, which protobuf cannot produce and gRPC would reject first. The accumulator is reset at every flush, so it is bounded by 2 MiB plus one row. |
| Is the terminal TimeSeries message bounded? | Yes - it carries whatever was left after the last flush, which is by construction under the budget. |
| Does `asGrpcProtocol` clearing unconditionally strand a thread? | No. On the gRPC worker the context was previously unset, so clearing restores exactly that. The transaction executor is dedicated to one transaction, and `executeCommandInternal` already sets/clears on it the same way. |
| Does the issue's own claim for item 1 hold? | Half of it. See the item 1 correction above - #7418. |

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7420

## Review cycles

### Cycle 1 - `4d7b14a1` - clean approval, no changes applied

The `claude` reviewer read the diff and the eight test files and reported **no blocking issues**. It
independently re-derived the three placement decisions this PR turns on, which is the useful part of the
review: that `asGrpcProtocol` belongs on `searchInTransaction`'s body rather than the RPC method because the
in-transaction search runs on the transaction's executor thread; that the `expand` reordering correctly
leaves the no-full-text-leg early return alone rather than false-triggering the type check on it; and that
`batch`/`batchBytes` are reset together at every flush point with the terminal message bounded by
construction.

Three notes, none asking for a change:

| Note | Disposition |
|---|---|
| `TimeSeriesPoint` is a public client-facing record, so widening the tag check to `Iterable` is a public-API behaviour change for any external caller passing a custom Iterable-but-not-Collection tag value | Agreed, and verified rather than taken on trust: `requireStorableTagValue` has exactly three production callers (`TimeSeriesPoint`'s compact constructor, `LineProtocolWriter`, `GrpcTimeSeriesSupport.toSamples`), all of which take a caller-supplied `Map<String, Object>`. A custom `Iterable` there was previously stored as the text of an object identity - the exact corruption the check exists to prevent - so refusing it is the fix, not a casualty of it. No change. |
| `RemoteGrpcServer.profilerStart` going from `void` to `int` is a public API surface change on the gRPC client | Correct, and deliberate: without a return value the Java client is the one caller that cannot see the effective timeout the rest of item 4 exists to report. It is source-compatible (a discarded return value compiles), the method is new in the unreleased 26.10.1 window, and its only in-tree caller (`Issue7304RemoteGrpcServerControlPlaneIT:128`) uses it as a statement. No change. |
| The reviewer could not run Maven in its environment and read the changed files and call sites manually instead; asks for a green CI run before merge | Not a code note. The verification this branch did run is in the Test results section above; CI is the developer's gate at merge time. No change. |

Working tree empty after the cycle, no deferred items, no actionable comments - the loop's early-exit
condition, so no second cycle was run. (`docs/review-deferred-47afd7da.md` in this directory predates this
branch: it came in with PR #7210.)

## Final state

**clean-approval** after 1 review cycle. Merge is the developer's.
