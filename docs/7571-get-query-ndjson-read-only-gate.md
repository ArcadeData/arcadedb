# #7571 — `GET /query` streams ndjson with no read-only gate

Issue: https://github.com/ArcadeData/arcadedb/issues/7571
Follow-up from #7569 (PR #7572, merged), which deliberately scoped itself to the two POST operations.

## The defect

Three operations advertise `application/x-ndjson` under their `200`. Two of them refuse a statement that
is not provably read-only before it runs; the third does not.

| Operation | Handler | Gate before streaming |
|---|---|---|
| `POST /api/v1/command/{database}` | `PostCommandHandler` | `requireStreamableStatement()` |
| `POST /api/v1/query/{database}` | `PostQueryHandler extends PostCommandHandler` | same call, inherited |
| `GET /api/v1/query/{database}/{language}/{command}` | `GetQueryHandler` | none |

`GetQueryHandler.execute()` computes `streaming = isNdJsonRequested(exchange)` and then calls
`database.query(language, text)`. The only thing standing in for the gate is `SQLQueryEngine.query()`:

```java
if (!statement.isIdempotent())
  throw new QueryNotIdempotentException("Query '" + query + "' is not idempotent");
```

which is strictly weaker. `BackupDatabaseStatement.isIdempotent()` returns `true` while
`getOperationTypes()` reports `{READ, CREATE}`, so `BACKUP DATABASE` - which writes a whole archive to
the server filesystem - is refused with 400 when streamed at `POST /command` and streamed happily at
`GET /query` with `Accept: application/x-ndjson`.

## Analysis

`requireStreamableStatement()` is `private static` on `PostCommandHandler`. `GetQueryHandler` is a sibling
under `AbstractQueryHandler`, not a subclass, so it cannot reach the method even accidentally.

The transactional half of the gate's javadoc is genuinely POST-only: `GetQueryHandler.requiresTransaction()`
returns `false`, so there is no auto-commit wrapper to commit a half-executed statement and no
`database.transaction(..., retries)` to re-run one into a closed exchange. What is *not* POST-only is the
half that makes the refusal a refusal at all: once the first ndjson line is on the wire the status code has
already been chosen, so a statement that writes and then fails cannot be reported as a failure. That reason
holds for every ndjson-capable operation, and it is the reason the gate belongs in the shared base class.

## Completeness

### Invariant

> No HTTP operation streams the `application/x-ndjson` row encoding for a statement whose analysis does not
> prove it writes nothing.

### Sweep

```
$ grep -rn "streamResultSetAsNdJson(" server/src/main --include='*.java'
GetQueryHandler.java:93:          final SerializationOutcome outcome = streamResultSetAsNdJson(exchange, database, serializer, limit,
PostCommandHandler.java:324:            outcome = streamResultSetAsNdJson(exchange, database, serializer, limit, maxResultRows, qResult,
AbstractQueryHandler.java:316:  protected SerializationOutcome streamResultSetAsNdJson(final HttpServerExchange exchange, final Database database,
```

Two row-streaming call sites, both under `AbstractQueryHandler`. That is the whole population the invariant
has to cover.

```
$ grep -rn "isNdJsonRequested(exchange)" server/src/main --include='*.java'
GetQueryHandler.java:69:      final boolean streaming = isNdJsonRequested(exchange);
GetQueryHandler.java:160:    return isNdJsonRequested(exchange);
PostCommandHandler.java:182:    final boolean streaming = isNdJsonRequested(exchange);
PostBatchHandler.java:296:    final boolean streaming = isNdJsonRequested(exchange);
AbstractServerHttpHandler.java:556:          && !(supportsNdJsonEncoding() && isNdJsonRequested(exchange));
```

A third handler negotiates ndjson - `PostBatchHandler` - which the issue does not name. Argued below.

```
$ grep -rn "requireStreamableStatement" server/src/main --include='*.java'
PostCommandHandler.java:199:      requireStreamableStatement(database, language, command);
PostCommandHandler.java:492:  private static void requireStreamableStatement(final Database database, final String language,
CoreApiSpec.java:83:  // ... so both reach the very same requireStreamableStatement() call in execute() before the
```

One caller before this change.

Which statements the gate actually separates, as an intersection of two greps rather than from memory:

```
$ for f in engine/src/main/java/com/arcadedb/query/sql/parser/*.java; do \
    grep -A3 "public boolean isIdempotent" "$f" | grep -q "return true" && basename "$f"; done
BackupDatabaseStatement.java  ExplainStatement.java  FindReferencesStatement.java  MatchStatement.java
ProfileStatement.java  ReturnStatement.java  SelectStatement.java  TraverseStatement.java

$ grep -rln "OperationType.CREATE\|OperationType.UPDATE\|OperationType.DELETE" \
    engine/src/main/java/com/arcadedb/query/sql/parser/
InsertStatement.java  UpdateStatement.java  MoveVertexStatement.java  Statement.java
BackupDatabaseStatement.java  CreateVertexStatement.java  DeleteStatement.java  CreateEdgeStatement.java
```

The intersection is exactly one class: `BackupDatabaseStatement`. So `BACKUP DATABASE` is the only statement
the stronger gate turns away that `SQLQueryEngine.query()`'s idempotency check admits - which is why the
issue could name it and no other.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/command/{db}` + `Accept: application/x-ndjson` → `PostCommandHandler.execute` | gate relocated, behaviour unchanged | yes - existing `Issue7306HttpStreamingQueryIT.backupDatabaseIsRefusedOnTheStreamingEncodingThoughItIsIdempotent` and `aStatementThatWritesIsRefusedOnTheStreamingEncoding` (both still green), plus the POST half of `Issue7571...getQueryRefusesToStreamBackupDatabaseExactlyAsPostCommandDoes` |
| `POST /api/v1/query/{db}` + ndjson → `PostQueryHandler` (inherits `execute`) | gate relocated, behaviour unchanged | yes - the POST half of `Issue7571...getQueryRefusesAWritingStatementWithTheSameMessageThePostOperationsSend` |
| `GET /api/v1/query/{db}/{lang}/{cmd}` + ndjson → `GetQueryHandler.execute` | **yes, this fix** | yes - `Issue7571GetQueryStreamingReadOnlyGateIT`, 4 methods: `BACKUP DATABASE`, a writing `UPDATE`, three spellings of the backup verb, and the two things that must NOT change (a streamed `SELECT`, and `BACKUP DATABASE` over the buffered encoding) |
| OpenAPI: `executeQueryGet` operation description | **yes, this fix** | yes - `CoreApiSpecTest.allThreeNdJsonOperationsDescribeTheSameReadOnlyRestriction` and `theReadOnlyStreamingRestrictionNamesBackupDatabase` |
| `POST /api/v1/batch/{db}` + ndjson → `PostBatchHandler` | **argued** - not a result-set stream | n/a |
| `GET /query` + ndjson + `EXPLAIN` | **filed: #7575** | n/a |
| any operation + ndjson + `language: sqlscript` | **filed: #7576** | n/a |

### Falsifiability

Every gate assertion was shown to fail against the unfixed handler, not merely to pass against the fixed one.
With the single new line in `GetQueryHandler` commented out and the module rebuilt:

```
[ERROR] Tests run: 4, Failures: 3, Errors: 0 -- Issue7571GetQueryStreamingReadOnlyGateIT
  getQueryRefusesToStreamBackupDatabaseExactlyAsPostCommandDoes:86
    expected: 400 but was: 200, body
    {"record":{"operation":"backup database","result":"OK","backupFile":"graph-backup-20260912-233046577.zip"}}
  theGateReadsTheParsedStatementSoNoSpellingOfBackupGetsThrough:136  (same, lowercase spelling)
  getQueryRefusesAWritingStatementWithTheSameMessageThePostOperationsSend:117
    body was {"error":"Query is not idempotent", ...} - the weaker engine-level refusal, not the gate's
```

The archive named in that body is the defect: the backup completed and the server streamed a 200 about it.
The fourth method (`theGateChangesNothingForAReadOrForTheBufferedEncoding`) passes in both states by
construction - it is the regression guard, not a gate assertion.

An earlier draft of this IT had a fourth gate test asserting that an *unregistered* language is refused. It
passed before the fix, because `database.getQueryEngine("notalanguage")` throws at the engine lookup and never
reaches the gate - a test that did not touch the branch it named. It was replaced by the verb-spelling test,
which does.

### Argued: `PostBatchHandler`

`PostBatchHandler` negotiates `application/x-ndjson` (line 296) but never calls `streamResultSetAsNdJson` -
the grep above lists that method's two call sites and neither is in that file. Its ndjson is the per-chunk
acknowledgement encoding added by #7311, not a row stream of a statement's result set: the endpoint's whole
purpose is a bulk *write*, it has no statement to analyze, and its "200 with the verdict in band" discipline
is documented on the operation itself. A read-only gate there would refuse the endpoint's only use. Outside
the invariant's population rather than an uncovered row.

### Filed: `EXPLAIN` over `GET /query` + ndjson (#7575)

`PostCommandHandler` refuses a streamed `EXPLAIN` outright (line 280, "EXPLAIN produces a plan, not a row
stream"). `GetQueryHandler` has no `ExplainResultSet` branch at all, so `GET /query/.../sql/EXPLAIN%20...`
with `Accept: application/x-ndjson` streams one row carrying `executionPlan` and `executionPlanAsString`.
`ExplainStatement.isIdempotent()` is true and its operation types carry no write, so the gate this PR adds
does not - and should not - catch it. Same *shape* of divergence, different right answer, so it is a separate
issue rather than a silent extension of this one.

### Filed: `sqlscript` classifies a backup as a plain READ (#7576)

`SQLScriptQueryEngine.analyze()` implements only `isIdempotent()` and `isDDL()`, so it inherits
`AnalyzedQuery.getOperationTypes()`'s default - which derives the types from those two booleans and therefore
reports `{READ}` for a script whose statements are all idempotent. `BackupDatabaseStatement`'s own
`{READ, CREATE}` override is discarded on the way through the script engine, so
`{"language":"sqlscript","command":"BACKUP DATABASE"}` is admitted by the gate on **all three** operations.

Not fixed here for two reasons. It is not the GET/POST asymmetry this issue is about - it weakens the gate
equally everywhere, including the two operations the issue names as already correct. And the same
classification is an authorization input, not only a streaming precondition: `ExecuteCommandTool` and
`MCPToolUtils` pass `analyzed.getOperationTypes()` straight to `checkPermission`, and MCP's `language` is
caller-supplied, so the fix changes what a READ-only identity may run through MCP and wants its own review
rather than a paragraph in a PR about a GET handler.

### Residual risk

The invariant now holds at both row-streaming call sites, which the first sweep grep shows are the whole
population - for every language whose `analyze()` reports operation types faithfully. #7576 is the known
exception and is filed.

What this PR deliberately does **not** change:

- **The buffered `application/json` encoding, on all three operations.** `BACKUP DATABASE` over `GET /query`
  with `Accept: application/json` still runs, and `theGateChangesNothingForAReadOrForTheBufferedEncoding`
  asserts it does. Nothing about the buffered encoding is unsound, so the gate is scoped to streaming and the
  fix narrows no capability - it redirects. The statement stays gated on `UPDATE_SECURITY` by
  `BackupDatabaseStatement.executeSimple` either way, so this is not a security boundary being moved.
- **Non-SQL languages.** `requireStreamableStatement` refuses anything its language's `analyze()` throws on.
  That conservative reading is pre-existing on the two POST operations and now applies to `GET /query` too, so
  a language whose engine cannot analyze a statement loses the streaming encoding on GET where it had it
  before. `grep -rn "getOperationTypes()" --include='*.java'` shows `OpenCypherQueryEngine`,
  `ArcadeGremlin`, `GraphQLQueryEngine`, `MongoQueryEngine` and `RedisQueryEngine` each overriding it rather
  than inheriting the default, so those languages report real operation types to the gate. A language that
  cannot be analyzed at all could not have streamed soundly in the first place, which is the judgement #7306
  already made for the two POST operations.
- **The error message text changed** from "before the transaction that produced them commits" to "before the
  statement has finished", because the transaction wording was only true of the two POST operations and this
  message is now sent by three. Anything matching on the substring `read-only statement` is unaffected; the
  existing #7306 assertions match on exactly that and stay green.

## Changes

- `AbstractQueryHandler`: `requireStreamableStatement(Database, String, String)` moved here from
  `PostCommandHandler`, `private static` → `protected static`, javadoc rewritten so the reason that holds
  for every operation is stated first and the two transactional hazards are named as POST-only.
- `PostCommandHandler`: method removed, call site unchanged; the two imports it was the only user of
  (`com.arcadedb.query.OperationType` and `com.arcadedb.query.QueryEngine`) dropped in review cycle 1 -
  the first commit claimed to have dropped them and had not, which the reviewer caught.
- `GetQueryHandler`: calls `requireStreamableStatement(database, language, text)` when the ndjson encoding
  was negotiated, next to the existing `ndJsonRowSerializer(...)` precondition, before `database.query(...)`.
- `CoreApiSpec`: `NDJSON_READ_ONLY_DESCRIPTION` now appended to `executeQueryGet` as well, and its wording
  generalized so it is true of all three operations (it named only the transactional reason, which is
  POST-only) and names `BACKUP DATABASE` explicitly, since that is the one statement a client author would
  otherwise expect to stream.

## Tests

- `server/src/test/java/com/arcadedb/server/http/Issue7571GetQueryStreamingReadOnlyGateIT.java` (new)
- `server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java` (method added)

## Adversarial pass

The orchestrator's Phase 1.5 spawns an isolated subagent that has not seen the author's reasoning. The `Task`
tool was disabled for this session, so that isolation was not available and the pass was run by the author
instead - which is weaker, and is recorded as such rather than reported as a clean result. The questions were
answered against the source, not against this document.

| # | Question | Verdict |
|---|---|---|
| 1 | Does the new call site run before anything executes? | **No defect.** `GetQueryHandler.java:78-82` - the gate is inside `if (streaming)`, above `database.query(language, text)` at line 90. Proven rather than read: with the line commented out the refused response carries `"backupFile":"graph-backup-...zip"`, and with it in place the 400 body carries no such field, which `theGateReadsTheParsedStatementSoNoSpellingOfBackupGetsThrough` now asserts. |
| 2 | Other entry points that stream a result set and bypass the gate? | **No defect, and the population is bounded by command output.** `grep -rn "x-ndjson" --include='*.java'` outside `server/` returns only `network/.../RemoteDatabase.java:97,1286` - the *client*. `RemoteDatabase.streamingCommand` builds `createRequestBuilder("POST", ...)` (line 858), so the driver never streams over the GET operation and is unaffected. gremlin, graphql and mongodbw contain no ndjson at all; grpcw streams over gRPC, whose trailer can still carry a failure status after the first message, so the "status already on the wire" reasoning does not transfer. |
| 3 | Do the added comments over-claim? | **One did, removed.** A first draft of the `GetQueryHandler` comment said the operation had been ungated "for three releases" - a claim no command in this session established. The remaining exhaustive claim, that `BACKUP DATABASE` is the only statement answering `isIdempotent()` true whose operation types declare a write, is the intersection of the two greps in the sweep above and holds. |
| 4 | Do the new tests reach the branch they name? | **One did not, replaced.** See Falsifiability above. |
| 5 | Does relocating the method change POST behaviour? | **Two real changes, both acknowledged, neither observed by anything in the tree.** (a) The FINE-level diagnostic now logs against `AbstractQueryHandler.class` instead of `PostCommandHandler.class`, so a log scraper keyed on the logger name would see a different one; the gate is now genuinely shared, so the base class is the more accurate attribution. (b) The exception message's second clause changed from "before the transaction that produced them commits" to "before the statement has finished", because the transaction wording was only true of the two POST operations and the message is now sent by three. `grep -rn "before the transaction commits\|transaction that produced them"` over `*.java` finds no assertion or client matching it, and `grep -rn "streaming encoding is available only"` finds only the throw site; every existing assertion matches the unchanged substring `read-only statement`. |
| 6 | Is the OpenAPI contract now inconsistent with the handlers? | **One gap, filed as #7575.** The restriction text is accurate for all three operations and `createQueryResponses` already declares the `400` the GET operation now uses for it. The `EXPLAIN` divergence stays undocumented, which is exactly what #7575 asks to settle - it needs a decision about which behaviour is right before the contract can state one. |

## Review cycles

### Cycle 1 - `a5a09db`

| Reviewer | Outcome |
|---|---|
| `claude` | One actionable finding, applied. Everything else was confirmation: gate ordering, the classification matching `BackupDatabaseStatement.getOperationTypes()`, the `protected static` relocation as the right answer to the sibling-class problem, the message rewording, the OpenAPI sharing, and the scope calls on #7575/#7576. |
| `coderabbitai` | "No actionable comments were generated in the recent review." Merge risk: minimal. |
| `codacy-production` | 0 new issues, 0 complexity. |

**Applied.** *Two unused imports left in `PostCommandHandler.java`.* Verified before agreeing rather than
taken on the reviewer's word: `grep -n "OperationType\|QueryEngine" PostCommandHandler.java` returns lines
25 and 26 (the imports) and line 162, which is a comment - so neither symbol is referenced by any code in the
file. The reviewer was also right that the PR body and this document both claimed these had already been
dropped, which was false. Both imports removed and the claim above corrected. A scan of every import in all
six changed files for the same defect found nothing else; the only other hits are two pre-existing
`java.util.*` wildcards, which the scan cannot resolve and which are used.

**Not applied.** *CodeRabbit's "Docstring Coverage 40.91%, threshold 80%" pre-merge check.* It counts every
function touched by the diff, which here is dominated by the IT's private request-building helpers
(`baseUrl`, `authorization`, `getRequest`, `postRequest`, `send`, `countTouched`) and `CoreApiSpecTest`'s
`suffixFrom`. Each is three to ten lines whose name states exactly what it does, and every method that
carries a non-obvious decision - the gate itself, all four IT test methods, both new spec tests - already has
javadoc explaining *why*. Adding a docstring to `private String baseUrl()` to move a percentage is the kind
of comment the repo's own style avoids. The threshold is a generic bot metric rather than a finding about
this code, so it is recorded here rather than satisfied.

### Cycle 2 - `1e23fc3`

| Reviewer | Outcome |
|---|---|
| `claude` | "No blocking issues found." The import removal was re-verified by the reviewer. One non-blocking observation, answered below. |

**Applied (documentation only, no behaviour change).** *"Worth double-checking [that `analyze()` is cheap]
holds for the non-SQL engines mentioned in Residual risk if any of them do non-trivial work in `analyze()`."*
The reviewer is right that the javadoc only claimed it for `sql` and left the reader to guess about the rest,
and right that this is not a regression - the two POST operations have made the same call since #7306 - but
this PR does newly apply that cost to `GET /query`, so the claim should be complete rather than SQL-shaped.
Checked each engine and wrote the answer into the gate's javadoc:

| Language | `analyze()` cost | Evidence |
|---|---|---|
| `sql` | free | `SQLQueryEngine.parse` → `database.getStatementCache().get(query)` |
| `opencypher` | free | `OpenCypherQueryEngine.analyze` → `database.getCypherStatementCache().get(query)` |
| `mongo`, `graphql`, `redis` | cheap, no parse | `detectMongoOperationTypes` / `classify` / `parseCommand` classify from the command text |
| `gremlin` | a real parse | `ArcadeGremlin.parse()` calls `executeStatement(true)` and walks the resulting traversal's steps |
| `sqlscript` | a real parse | `SQLScriptQueryEngine.parseScript`; that class's own javadoc says "this parse is NOT free - there is no script statement cache" |

So two of the seven parse twice on a streamed request. That is the price of refusing before the first byte
instead of after, it is the price #7306 already accepted for the two POST operations, and it is now written
down where the next reader will find it instead of inferred from a claim about `sql`.

**Not applied.** Nothing else; the rest of the review was confirmation.

### Cycle 3 - `3ad27c7`

| Reviewer | Outcome |
|---|---|
| `claude` | **No actionable items.** "Overall: solid, well-tested fix." Three non-blocking observations, all of them acknowledgements of decisions already recorded here: the double-parse cost (now documented in the gate's javadoc, cycle 2), the two filed gaps (#7575 and #7576, with the reviewer independently agreeing #7576 deserves a fast follow because it reaches MCP's `checkPermission`), and this document's length, which the reviewer checked against the repo's existing `docs/75xx-*.md` convention and did not object to. |
| `coderabbitai` | Rate-limited for this commit; its review of the substantive diff at `a5a09db` returned "No actionable comments were generated", merge risk minimal. The two later commits are an import removal and a javadoc edit. |
| `codacy-production` | 0 new issues, 0 complexity. |

No changes were applied in this cycle, no deferred-items notes file was produced, and the working tree is
empty - the loop's clean-approval condition. (`docs/review-deferred-*.md` files exist in this tree, but
`git log -1 --` on each shows all five belong to earlier merged PRs: #7210, #7442 and #7556. None was
produced by this run.)

## Outcome

- **PR:** https://github.com/ArcadeData/arcadedb/pull/7582
- **Final state:** `clean-approval` after 3 review cycles.
- **Deferred items:** none. Every review point was either applied or answered here with its reasoning.
- **Follow-ups filed before the PR opened:** #7575 (`EXPLAIN` GET/POST divergence), #7576 (`sqlscript`
  discards the declared write, weakening the gate on all three operations and reaching MCP's permission
  check - the one the reviewer flagged as worth prioritizing).

| Cycle | Head | What changed |
|---|---|---|
| 1 | `a5a09db` | Initial implementation. One actionable finding: two imports left unused in `PostCommandHandler`, verified and removed. |
| 2 | `1e23fc3` | No blocking issues. One non-blocking point applied as documentation: the gate's javadoc now states `analyze()`'s cost per language instead of claiming it is free on the strength of `sql` alone. |
| 3 | `3ad27c7` | No actionable items. Clean approval. |
