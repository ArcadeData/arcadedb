# #5628 - Client-error classification across the wire protocols

## Problem

`#5602` (PR `#5612`) gave arithmetic failures - 64-bit overflow, division and modulo by zero - their own
type, `ArithmeticErrorException`, so a wire layer can report them as a client error rather than a server
fault. Only two layers were wired:

- HTTP: 400, in `AbstractServerHttpHandler` (the direct arm and the auto-commit `TransactionException` arm)
- Bolt: `Neo.ClientError.Statement.ArithmeticError`, in `BoltNetworkExecutor.classifyExecutionError`

`postgresw`, `mongodbw` and `redisw` reported every execution failure through one generic arm, so a caller
who divided by zero got the code that means "the server broke". `graphql` had a related defect: it rewrapped
*execution* failures as `CommandParsingException`, relabelling a runtime error as invalid syntax.

The issue asks for this to be looked at as "how does each wire protocol map ArcadeDB's exception hierarchy"
rather than one exception at a time.

## Root cause

Each wire module ended its query path with a single `catch (Exception)` that produced one protocol code.
Nothing walked the cause chain, and a failure arrives wrapped differently depending on the path it took, so
even the modules that inspected `getCause()` saw the wrapper rather than the real error.

## Approach

One shared classifier in the engine, `com.arcadedb.exception.ErrorCategory`, answers the single question all
five layers ask - *which category of failure is this?* - by walking the cause chain with `CauseChain`. Each
wire module then owns only the table that translates a category into its own vocabulary:

| `ErrorCategory` | Postgres SQLSTATE | Redis RESP prefix | MongoDB code |
|---|---|---|---|
| `RETRY` | `40001` serialization_failure | `TRYAGAIN` | 112 `WriteConflict` |
| `ARITHMETIC` | `22012` division_by_zero / `22003` numeric_value_out_of_range | `ERR` | 2 `BadValue` |
| `DUPLICATED_KEY` | `23505` unique_violation | `ERR` | 11000 `DuplicateKey` |
| `NOT_FOUND` | `02000` no_data | `ERR` | (uncoded) |
| `SCHEMA` | `42P01` undefined_table | `ERR` | 26 `NamespaceNotFound` |
| `SECURITY` | `42501` insufficient_privilege | `NOPERM` | 13 `Unauthorized` |
| `VALIDATION` | `22023` invalid_parameter_value | `ERR` | 2 `BadValue` |
| `PARSING` | `42601` syntax_error | `ERR` | 9 `FailedToParse` |
| `TIMEOUT` | `57014` query_canceled | `ERR` | 50 `MaxTimeMSExpired` |
| `SERVER` | `XX000` internal_error | `ERR` | (uncoded) |

`RETRY` is tested before `ARITHMETIC`, matching the precedence `BoltNetworkExecutor` already documents: a
chain carrying both must keep the transient classification, because that is the one a driver acts on.

HTTP and Bolt keep the ladders they already had. Both predate `ErrorCategory` and are covered by the
regression suites of several shipped issues; rewriting them buys nothing this issue asks for.

## Changes

- `engine` - new `com.arcadedb.exception.ErrorCategory`
- `postgresw` - `PostgresNetworkExecutor.sqlStateFor(...)`, used by every query/parse/bind error arm
- `redisw` - `RedisNetworkExecutor.respErrorPrefix(...)`, plus a RESP-safe single-line error reply
- `mongodbw` - `MongoDBDatabaseWrapper.wireException(...)`, so `putLastError` can emit `code`/`codeName`
- `graphql` - `ArcadeDBException` passthrough in `GraphQLQueryEngine` and `GraphQLSchema`

## Tests

New: `ErrorCategoryTest` (engine), `PostgresErrorClassificationTest`, `RedisErrorClassificationTest`,
`MongoDBErrorClassificationTest`, `GraphQLExecutionErrorClassificationTest`.

Fail-first was verified where a regression test could fail: with the GraphQL passthrough reverted,
`anExecutionFailureIsNotReportedAsASyntaxError` fails while the two guard tests still pass. The arithmetic
GraphQL test is a pin rather than a regression proof - the projection is evaluated as rows are pulled, so
that failure already reached the caller from outside the rewrapping block.

Green: `graphql` 30/30, `mongodbw` 78/78, `postgresw` unit 258/258, `redisw` `RedisWTest` 13/13 (the RESP
wire path), `bolt` `BoltErrorClassificationTest` 13/13, `engine` `com.arcadedb.exception` 42/42.

Not verifiable locally: `RedisQueryLanguageTest` and the server-backed ITs, because a local ArcadeDB holds
ports 2480-2482 and the tests connect to it instead of the one they start. Identical failures confirmed on
unmodified `main`, so CI is the gate for those.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5700

## Review cycles

### Cycle 1 - 9ffa8e267

Claude's review found one substantive gap, which I verified and fixed:

- **`SchemaException` fell through to `SERVER`.** It is the exception behind `SELECT FROM NonExistentType` -
  demonstrably the most common caller mistake there is - so it reported as Postgres `XX000` and an uncoded
  MongoDB server fault: exactly the misdirection this change exists to remove, for a different exception.
  Added a `SCHEMA` category. The review also correctly noted that my GraphQL test asserted only
  `isNotEqualTo(PARSING)`, which passed while the real classification was `SERVER`; it now asserts the exact
  category. Fail-first re-verified: removing the `SCHEMA` arm fails
  `namingATypeTheSchemaDoesNotDefineIsTheCallersMistake` and `theRemainingClientErrorCategoriesAreRecognised`.

Points assessed and answered rather than changed:

- **`IllegalArgumentException -> VALIDATION` is broad.** Correct, and it is the one entry that is not
  self-evidently a client error. Kept, because the HTTP handler has answered it with 400 since long before
  this enum and having the two disagree would be worse than either verdict. Now recorded in the javadoc.
- **Arithmetic split via message text.** Fragile, already acknowledged; the class-22 verdict holds either way.
- **GraphQL passthrough drops the wrapper's context.** Accurate cost. Rewrapping is what destroyed the
  classification, so the trade is deliberate.
- **No end-to-end wire ITs for the Postgres/MongoDB SQLSTATE and codes.** Fair, and worth doing - but the
  server-backed ITs cannot run on this machine (ports 2480-2482 are held locally), so writing one I cannot
  execute would be worse than not writing it. Deferred; noted below.

## Deferred

- One IT per protocol asserting the SQLSTATE / MongoDB code actually reaches a driver over the wire. Redis
  already has this coverage via `RedisWTest`; Postgres and MongoDB are unit-tested at the helper level only.
