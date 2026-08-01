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
| `SECURITY` | `42501` insufficient_privilege | `NOPERM` | 13 `Unauthorized` |
| `VALIDATION` | `22023` invalid_parameter_value | `ERR` | 2 `BadValue` |
| `PARSING` | `42601` syntax_error | `ERR` | 9 `FailedToParse` |
| `TIMEOUT` | `57014` query_canceled | `ERR` | 50 `MaxTimeMSExpired` |
| `SERVER` | `XX000` internal_error | `ERR` | (uncoded) |

`RETRY` is tested before `ARITHMETIC`, matching the precedence `BoltNetworkExecutor` already documents: a
chain carrying both must keep the transient classification, because that is the one a driver acts on.

HTTP and Bolt keep the ladders they already had. Both predate `ErrorCategory` and are covered by the
regression suites of five shipped issues; rewriting them buys nothing this issue asks for.

## Changes

- `engine` - new `com.arcadedb.exception.ErrorCategory`
- `postgresw` - `PostgresNetworkExecutor.sqlStateFor(...)`, used by every query/parse/bind error arm
- `redisw` - `RedisNetworkExecutor.respErrorPrefix(...)`, plus a RESP-safe single-line error reply
- `mongodbw` - `MongoDBDatabaseWrapper.wireException(...)`, so `putLastError` can emit `code`/`codeName`
- `graphql` - `CommandExecutionException` passthrough in `GraphQLQueryEngine` and `GraphQLSchema`

## Tests

See the PR test plan.
