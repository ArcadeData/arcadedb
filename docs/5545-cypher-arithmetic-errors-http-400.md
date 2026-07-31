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

## Review decisions

PR https://github.com/ArcadeData/arcadedb/pull/5631. Both review cycles approved with no blocking items.
The non-obvious calls, recorded because the reasoning is not visible in the diff:

- **`@author` tag removed from the new IT rather than reassigned.** The reviewer called it a copy-paste
  artifact. That premise is wrong - `@author Luca Garulli` is house convention, on 76 of 235 files under
  `server/src/test/java`, including both siblings this test is modeled on. The conclusion still holds:
  naming a real person as the author of a file they did not write is a misattribution however common the
  tag is. Removed rather than reassigned, since the remaining ~2/3 of server test files carry no `@author`
  at all, so omitting it claims nothing false.
- **The `executeSql` helper is deliberately not hoisted into `BaseGraphServerTest`.** Its existing
  `command()` / `executeCommand()` hardcode a 200 assertion and cannot serve the 400 cases. Changing a
  widely-inherited fixture for one caller is the worse trade; revisit if a third error-status IT lands.
- **No `@Tag("slow")` on the new IT.** CLAUDE.md's bar is "multi-second elapsed time"; this class runs in
  1.1 s. Neither comparable sibling (`Issue5602ArithmeticErrorHttpStatusIT`,
  `Issue5484AbsNonNumericHttpStatusIT`) is tagged, and only 9 of 117 server ITs are. Tagging it would
  wrongly drop a fast regression test out of the regular CI run.
- **The `detail` field is mode-dependent, and that is now documented.** `buildErrorBody` emits `detail`
  only when verbose, so the assertions on it depend on `SERVER_MODE` not being production (it defaults to
  development). Recorded in the IT's class Javadoc, and the write-path test now also asserts the
  `exception` field, so the strongest assertion in each test no longer rests on a mode-dependent field.

### Review cycle log

| Cycle | Head | Reviewer verdict | Applied |
|---|---|---|---|
| 1 | `97dd04e51` | approve, "nothing blocking" | removed the misattributed `@author` tag |
| 2 | `c867f1c96` | approve, "nothing blocking" | deleted the review-cycle scratch note; `@Tag("slow")` verified inapplicable and skipped |
| 3 | `cd05a5917` | LGTM | documented the `detail`/`SERVER_MODE` coupling; write path now asserts `exception` |
| 4 | `be0179620` | pending at hand-off | - |

The engine change itself was never modified after the first commit; every review cycle touched only tests,
Javadoc and docs.

## Follow-ups (filed)

Both were probed empirically before filing, which turned up more than expected.

- **#5647** - SQL integer arithmetic. Two defects, not one. `Long.MAX_VALUE * 2` returns `-2` and
  `Long.MAX_VALUE + 1` returns `Long.MIN_VALUE`, silently, so a wrong number can be persisted by an
  `UPDATE ... SET`. Separately, SQL `1/0` and `1%0` raise a **raw** `java.lang.ArithmeticException`, which
  misses the #5602 classification arms entirely and falls through to the generic `catch (Throwable)` arm -
  still HTTP 500 today. That second half is literally the defect #5545 described, still live on the SQL
  side; #5631 only converted `SQLFunctionAbsoluteValue`.
- **#5649** - the `Duration` branch of `SQLFunctionAbsoluteValue`. The guard reads `toSecondsPart()` /
  `toNanosPart()` (components, not the whole), and `Duration.ofSeconds(abs(seconds), abs(nanos))` does not
  reconstruct the magnitude of a negative duration, which `Duration` normalizes as negative seconds plus a
  positive nanos adjustment. The branch has no test at all.
