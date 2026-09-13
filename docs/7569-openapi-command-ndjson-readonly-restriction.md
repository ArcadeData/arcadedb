# Issue #7569: OpenAPI /command declares application/x-ndjson but doesn't say streaming is read-only-only

## Ledger (single finding, from the issue body)

1. `POST /api/v1/command/{database}` declares `application/x-ndjson` as a `200` content type but
   neither its `summary` nor `description` says that the ndjson response is available only for a
   read-only statement, and that a mutating statement is refused with HTTP 400 before it runs.
   Reporter also suggested (as an "ideally") a line noting that ndjson is selected via `Accept`.

## Root cause

`PostCommandHandler.requireStreamableStatement()` (server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java:492)
refuses to stream any statement that is not provably read-only - `analyzed.isIdempotent()` and not
DDL/CREATE/UPDATE/DELETE/SCHEMA - throwing `IllegalArgumentException` mapped to HTTP 400, before the
statement runs. `CoreApiSpec.createCommandPath()` declares `application/x-ndjson` as a response
media type (via `addNdJsonAlternative`) and the `Accept` parameter that selects it
(`ndJsonAcceptParam()`), but the operation's own `description` never mentions the restriction.

The `Accept`-header opt-in the reporter asked for as an "ideally" is already documented: `ndJsonAcceptParam()`
attaches a description ("Send 'application/x-ndjson' to receive the result as a stream...") to the `Accept`
parameter on GET query, POST query and POST command alike. That part of the ask is already satisfied - no
change needed there.

## Completeness sweep

**Invariant:** every OpenAPI operation whose execution path can refuse the ndjson encoding with 400 for a
non-read-only statement documents that restriction in its own `description`.

Grep for every caller of the gate and every operation offering the ndjson media type:

```
$ grep -n "requireStreamableStatement" server/src/main/java/com/arcadedb/server/http/handler/*.java
PostCommandHandler.java:199:      requireStreamableStatement(database, language, command);
PostCommandHandler.java:492:  private static void requireStreamableStatement(...)

$ grep -n "class PostQueryHandler" server/src/main/java/com/arcadedb/server/http/handler/PostQueryHandler.java
public class PostQueryHandler extends PostCommandHandler {
   // overrides only executeCommand() -> database.query(); execute() itself, including the streaming
   // gate at line 199, is inherited unchanged.

$ grep -n "requireStreamableStatement\|supportsNdJsonEncoding" server/src/main/java/com/arcadedb/server/http/handler/GetQueryHandler.java
(no matches - GetQueryHandler streams unconditionally via a separately-implemented execute())
```

| Entry point | Calls the gate? | Status |
|---|---|---|
| `POST /api/v1/command/{database}` (`PostCommandHandler`) | Yes, directly | **Fixed here** - description now states the restriction |
| `POST /api/v1/query/{database}` (`PostQueryHandler extends PostCommandHandler`) | Yes, inherited via the same `execute()` | **Fixed here** - same restriction, same shared description text, for parity with the existing `commandRequestDeclaresLimitMatchingQueryRequest` precedent of keeping shared-behavior text identical between the two operations |
| `GET /api/v1/query/{database}/{language}/{command}` (`GetQueryHandler`) | No - separate `execute()`, calls `database.query()` directly with no analysis gate | **Filed as #7571** - there is no explicit read-only gate on this path, so nothing to document identically here; but the adversarial pass below found the absence is itself a divergence worth tracking (`BACKUP DATABASE` streams on GET while `POST /command` refuses it) |
| `Accept` header opt-in (both POST operations, and GET query) | n/a | **Already covered** - `ndJsonAcceptParam()` already documents `Accept: application/x-ndjson` on all three operations |

Every in-scope row is fixed in this PR; the one out-of-scope row is tracked by #7571.

## Fix

Added a shared `NDJSON_READ_ONLY_DESCRIPTION` constant to `CoreApiSpec` and appended it to the
`description` of both `POST /api/v1/command/{database}` and `POST /api/v1/query/{database}`, kept
byte-identical between the two operations (mirroring how `STALE_SESSION_404_DESCRIPTION` is shared).

## Tests

`CoreApiSpecTest.commandAndQueryDescribeTheReadOnlyStreamingRestriction()` (new) asserts:
- both operations' `description` mention the 400 refusal and the read-only requirement;
- the shared restriction text is identical between the two operations.

## Test results

`mvn -o -pl server -am test -Dtest=CoreApiSpecTest` - see run log in PR.

## Residual risk

This is a documentation-only change to the generated OpenAPI contract; no runtime behavior changed.

The one thing this PR does NOT cover: `GET /api/v1/query/{database}/{language}/{command}` also offers
`application/x-ndjson` and has no read-only gate at all, so it still streams a `BACKUP DATABASE` that
`POST /command` refuses. Tracked by #7571 - see the adversarial pass below.

## Adversarial pass (Phase 1.5)

One finding, verified independently before filing:

1. **GET `/api/v1/query/{database}/{language}/{command}` offers `application/x-ndjson` with no read-only
   gate at all.** `GetQueryHandler.execute()` calls `database.query()` directly and never reaches
   `PostCommandHandler.requireStreamableStatement()` (which is `private static` on a sibling class). Its
   only stand-in is `SQLQueryEngine.query()`'s `if (!statement.isIdempotent()) throw
   QueryNotIdempotentException`, which is strictly weaker: `BackupDatabaseStatement.isIdempotent()`
   returns `true` while `getOperationTypes()` reports `{READ, CREATE}`, so `BACKUP DATABASE` streams on
   GET while `POST /command` refuses it with 400.

   Verified by reading `engine/src/main/java/com/arcadedb/query/sql/parser/BackupDatabaseStatement.java:56-72`
   and `engine/src/main/java/com/arcadedb/query/sql/SQLQueryEngine.java:86-93`.

   **Disposition: real, out of scope -> filed as #7571.** Out of scope because closing it means either a
   behavior change on a third handler or a deliberate documentation of the divergence, neither of which
   #7569 (documentation-only, about `/command`) asked for.

   Narrower than the subagent framed it: the transactional hazards `requireStreamableStatement()`'s javadoc
   describes are POST-only, since `GetQueryHandler.requiresTransaction()` returns `false` so there is no
   auto-commit wrapper and no retry loop. #7571 says so explicitly rather than overstating the exposure.

## PR

https://github.com/ArcadeData/arcadedb/pull/7572

## Review cycles

### Cycle 1 - 9463c26df5

- **CodeRabbit:** "No actionable comments were generated." Merge risk: minimal. One pre-merge check
  warning, "Docstring Coverage 16.67%" - **skipped as a nitpick**: the functions it counts are JUnit
  test methods and private spec builders, and this repo documents intent in targeted javadoc/comments
  (the new constant and the new test both carry one) rather than in per-method docstrings. Adding
  boilerplate javadoc to satisfy a percentage would be noise.
- **claude:** no blocking issues. Confirmed the new text matches
  `requireStreamableStatement()`'s actual conditions, that sharing one constant between the two POST
  operations is right, and that deferring the GET gap to #7571 keeps the PR narrow.
- **claude, one non-blocking observation:** `requireStreamableStatement()` excludes
  CREATE/UPDATE/DELETE/SCHEMA/DDL but not `OperationType.ADMIN`, so "if any statement reports ADMIN
  while `isIdempotent()` returns true, it could still stream" - suggested as a possible follow-up.

  **Assessed and NOT filed: the premise does not hold.** `OperationType.ADMIN` has exactly one
  producer in the tree - `OpenCypherQueryEngine.analyze()` returns it for a `CypherAdminStatement`
  (`engine/src/main/java/com/arcadedb/query/opencypher/query/OpenCypherQueryEngine.java:116`) - and
  `CypherAdminStatement.isReadOnly()` returns `false` unconditionally
  (`engine/src/main/java/com/arcadedb/query/opencypher/ast/CypherAdminStatement.java:66-68`), which is
  what `isIdempotent()` delegates to. So every ADMIN statement fails the gate's very first conjunct
  and is refused before the operation-type set is consulted at all. There is no statement that is both
  ADMIN and idempotent, so there is nothing to leak and no follow-up to file.

  Verified by: `grep -rn "OperationType.ADMIN" --include="*.java" engine/src/main/java server/src/main/java`
  (one hit) and reading `CypherAdminStatement`.

No code changes were required by the review; no items were deferred.

## Final state

`clean-approval`
