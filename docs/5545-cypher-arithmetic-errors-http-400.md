# Issue #5545 - Cypher arithmetic errors return HTTP 500 instead of 400

## Problem

Arithmetic domain errors (integer overflow, division/modulo by zero, `abs()` of a MIN_VALUE) are decided
entirely by the values the caller supplied. Neo4j classifies the whole category as a client error
(`Neo.ClientError.Statement.ArithmeticError`), but ArcadeDB answered HTTP 500, which tells the client the
server failed and a retry is reasonable. These queries fail deterministically, so every retry burns a round
trip, and each one wrote a full stack trace into the server log.

## What the analysis found

Most of the issue was already resolved by **#5602** (merged after #5545 was filed). That work introduced
`com.arcadedb.exception.ArithmeticErrorException` - a subclass of `CommandExecutionException`, so embedded
code catching the broader type is unaffected - and taught `AbstractServerHttpHandler` to single it out on
both the direct arm and the wrapped `TransactionException` arm, answering 400. `BoltNetworkExecutor` does
the same over Bolt.

Checked every row of the issue's table against `main` (8a4cf19aa):

| Error | Site | State before this change |
|---|---|---|
| overflow on `+`, `-`, `*` | `ArithmeticExpression.integerArithmetic` | fixed by #5602 |
| `Long.MIN_VALUE / -1` | `ArithmeticExpression.integerArithmetic` | fixed by #5602 |
| integer division by zero | `ArithmeticExpression.checkIntegerDivisorNotZero` | fixed by #5602 |
| integer modulo by zero | `ArithmeticExpression.checkIntegerDivisorNotZero` | fixed by #5602 |
| `abs()` of a MIN_VALUE (Cypher) | `AbsFunction` | fixed by #5602 |
| **`abs()` of a MIN_VALUE (SQL)** | **`SQLFunctionAbsoluteValue.absExact`** | **still a bare `CommandExecutionException`** |

So exactly one site remained. The consequence was worse than a plain leftover: the identical client mistake
answered 400 in Cypher and 500 in SQL, which is the cross-engine inconsistency #5545 explicitly asked to
avoid.

The SQL arithmetic operators (`+`, `-`, `*`, `/`) were deliberately left alone - they do not perform exact
arithmetic at all and wrap silently, which is a separate behavioural question outside this issue's scope.

## Change

`engine/src/main/java/com/arcadedb/function/sql/math/SQLFunctionAbsoluteValue.java`

- `absExact()` throws `ArithmeticErrorException` instead of `CommandExecutionException`. One-line behavioural
  change plus the import; the message wording (`long overflow`, `integer overflow`, `short overflow`,
  `byte overflow`) is unchanged.
- Javadoc records why the subclass matters and that the supertype is preserved.

No handler change was needed - the classification arms from #5602 already match on the subclass anywhere in
the cause chain, so both the read path and the auto-commit write path pick it up.

## Verification

Tests written first and proven to fail against unmodified code (`CommandExecutionException` raised at
`SQLFunctionAbsoluteValue.absExact:98`), then green after the change.

Added to `engine/src/test/java/com/arcadedb/function/sql/math/SQLFunctionAbsoluteValueTest.java`:

- `everyIntegralMinValueOverflowIsAnArithmeticError` - all four fixed-width signed types raise
  `ArithmeticErrorException` with the per-type message.
- `fromQueryOverStoredLongMinValueIsAnArithmeticError` - the same end to end through the SQL engine. The value
  must arrive as a stored property: a `Long.MIN_VALUE` literal never survives the SQL parser, which rejects
  the unsigned digits ("Invalid integer: 9223372036854775808") before `abs()` is reached.

New `server/src/test/java/com/arcadedb/server/Issue5545SqlArithmeticErrorHttpStatusIT.java`:

- `sqlAbsOverflowReturns400OnTheReadPath` - asserts the status, the `exception` field, and the message.
- `sqlAbsOverflowReturns400OnTheWritePath` - the auto-commit wrapper re-wraps the failure as a
  `TransactionException`, the shape that historically degraded a client error back to 500. Also asserts the
  response is not the misleading "Error on transaction commit".
- `sqlAbsWithoutOverflowStillReturns200` - the guard fires on exactly one value per type, so ordinary data
  cannot become a client error.

The existing `#5494` overflow tests assert `isInstanceOf(CommandExecutionException.class)` and stay green
unchanged, since `ArithmeticErrorException` extends it - which is the compatibility guarantee in practice, not
just on paper.

## Impact

- Embedded API: none. The supertype is preserved, so every existing `catch (CommandExecutionException)` still
  matches.
- HTTP: SQL `abs()` on a MIN_VALUE moves 500 -> 400, and stops writing a stack trace per request.
- Bolt: the same call now maps through the client-error branch in `BoltNetworkExecutor`.

## Follow-ups (not in scope)

- SQL arithmetic operators wrap silently on overflow instead of failing like their Cypher counterparts. Worth
  its own issue - it is a behaviour change, not a status-code change.
- `SQLFunctionAbsoluteValue`'s `Duration` branch guards with `seconds > -1 && nanos > -1`, which looks wrong
  for durations between -1s and 0s. Pre-existing and untouched here.
