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
| `NOT_FOUND` | `P0002` no_data_found | `ERR` | (uncoded) |
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
- `graphql` - `CommandParsingException | CommandExecutionException` passthrough in `GraphQLQueryEngine` and
  `GraphQLSchema` (deliberately not `ArcadeDBException` - see the cycle-2 note)

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

### Cycle 2 - 12e9b668c

- **Postgres bypassed the `ARITHMETIC`-before-`PARSING` precedence it documents.** Both execution arms caught
  `CommandParsingException` first and hardcoded `42601`, so on that path the ordering in `ErrorCategory.of` was
  dead: any failure arriving wrapped in a parsing exception reported as a syntax error. Latent today (the
  Postgres path executes an already-parsed statement) but exactly the shape that made GraphQL need fixing in
  this same PR. All four arms now route through `sqlStateFor`, which keeps genuine parse errors at `42601` via
  the `PARSING` arm while letting a wrapped arithmetic, conflict or schema error win. Pinned by
  `aParsingWrapperDoesNotHideTheRealFailure`.
- **`isClientError()` had no production caller.** Dropped, along with its test, rather than left as an API that
  reads as if it were in use.
- **`NOT_FOUND` used `02000`.** SQLSTATE class 02 is a completion condition, not an error class. Changed to
  `P0002` no_data_found, which carries the client-error verdict honestly.
- **The GraphQL passthrough was narrowed, because the broad version regressed an HTTP status.** Review cycle 2
  flagged that propagating the underlying exception changes what the HTTP handler sees. Checking rather than
  acknowledging: `SchemaException` is *not* in that handler's ladder, so it falls to `catch (Throwable)` and
  reports 500. The broad `ArcadeDBException` passthrough therefore took GraphQL's unknown-type case from 400 to
  500 - a regression I introduced.

  The obvious repair, an HTTP arm mapping `SchemaException` to 400, is **not** taken. The class is not purely a
  caller error: `Dictionary`, `TransactionManager` and `TransactionContext` raise it for genuine server faults,
  and issue #4122 is specifically an HTTP 500 `Error on updating dictionary for key '...'` on a follower.
  Mapping the class to 400 would relabel a known server bug as the caller's fault, which is the mirror image of
  the defect this PR fixes. It also needs two coordinated sites (the top-level arm and the auto-commit
  `TransactionException` arm, since `POST /command` and `POST /query` wrap), in a handler whose behaviour is
  pinned by several shipped issues - and the server ITs that would validate it cannot run on this machine.

  So the passthrough covers only `CommandParsingException` and `CommandExecutionException`, both of which the
  HTTP ladder already models. `SchemaException` keeps the status it always had, pinned by
  `anUnknownTypeKeepsTheStatusItAlwaysHad` so the deliberate narrowness is not "tidied up" into a regression.
  The HTTP gap is recorded under Deferred.

  Consequence worth stating: a `CommandExecutionException` from a delegated statement now answers the same as
  the identical statement issued directly (500), instead of being relabelled a 400 parse error. That is a status
  change, and it is the consistent one.
- **`SECURITY` javadoc** now names which `SecurityException` it targets (`java.lang`, raised by
  `LocalDatabase.checkPermissionsOn*`) rather than the server's `ServerSecurityException`.

### Cycle 3 - b5385847c

Nothing blocking; four points, all answered in comments rather than behaviour:

- **Postgres message/SQLSTATE can diverge.** The `CommandParsingException` arm still says "Syntax error" while the
  code now comes from `sqlStateFor`. Today only genuine parse failures reach it, so they agree; the assumption is
  now pinned in a comment rather than left implicit.
- **`IllegalArgumentException -> VALIDATION` now applies to every protocol, not just HTTP.** Recorded in the
  javadoc as the conscious trade it is: an internal invariant violation can reach a MongoDB client as `BadValue`.
- **Redis `TRYAGAIN` is a weaker retry hint** than Postgres `40001` or Bolt's transient status - it is a
  cluster-mode error in real Redis and not every client auto-retries on it. Noted; RESP2 offers nothing better.
- **MongoDB's four literal codes** are literals because the bundled `de.bwaldvogel` `ErrorCode` enum does not
  define them. Verified against the bundled sources and noted.

The suggestion to collapse `ErrorCategory.of` into a single cause-chain walk was **not** taken, because it is not
equivalent: classification is by category priority, not by which type the walk meets first. A chain whose
`NeedRetryException` sits below an `ArithmeticErrorException` must still answer `RETRY`. Now pinned by
`categoryPriorityBeatsPositionInTheChain` so the claim in the javadoc has a test behind it.

### Cycle 4 - 9efdea5e6

No blocking items. One change taken:

- **A code the MongoDB backend already assigned was being thrown away.** A `de.bwaldvogel` `MongoServerError` is
  not an `ArcadeDBException`, so it classified as `SERVER` and got re-wrapped uncoded - losing the very code the
  client needs. `wireException` now returns it untouched. The specific `16459` insert case is caught locally and
  never reaches here, but the bundled aggregation code raises coded errors that do. Pre-existing behaviour, not a
  regression from this PR; fixed because it is one line. Pinned by `aCodeTheBackendAlreadyAssignedIsNotThrownAway`,
  fail-first verified.

The remaining points were already answered in earlier cycles: the Postgres message/SQLSTATE divergence is
documented and cosmetic (drivers branch on the code), `IllegalArgumentException`'s widened surface is a recorded
trade, and the end-to-end wire ITs stay deferred below. The review independently re-verified the parts of the
ordering that matter - `DuplicatedKeyException` not extending `NeedRetryException`, `ArithmeticErrorException`
being tested before its supertype, and `SecurityException` resolving to the `java.lang` one.

**Watch on merge:** the Redis reply format changed from `-<message>` to `-ERR <message>`. `RedisWTest` (which
exercises the RESP wire) is green locally, but `RedisQueryLanguageTest` and the server ITs are CI-gated here.

## Deferred

- **`SchemaException` has no HTTP arm**, so `SELECT FROM NonExistentType` over `/query` or `/command` reports 500
  today - for plain SQL as well, independently of this PR. Fixing it needs a decision this change should not make
  unilaterally: the class mixes caller errors (~75% of throw sites: unknown type, bucket, property, index) with
  genuine server faults (dictionary update failures, schema IO, issue #4122), and no discriminator exists short
  of matching messages. Verified that nothing currently depends on the 500, and that no HTTP test covers
  `SELECT FROM <nonexistent>` at all.
- One IT per protocol asserting the SQLSTATE / MongoDB code actually reaches a driver over the wire. Redis
  already has this coverage via `RedisWTest`; Postgres and MongoDB are unit-tested at the helper level only.
