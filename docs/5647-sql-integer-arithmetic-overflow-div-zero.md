# 5647 - SQL integer arithmetic silently wraps on overflow, and SQL division by zero returns HTTP 500

Issue: https://github.com/ArcadeData/arcadedb/issues/5647

Split out of the review of #5631, which closed the last row of #5545 by converting `SQLFunctionAbsoluteValue`
alone. Both defects below live in `MathExpression.Operator`, are pre-existing, and were out of scope for #5631.

## Root cause

Both are the SQL side of behavior the Cypher engine already has, so the two engines disagreed on the same
caller mistake. `com.arcadedb.query.opencypher.ast.ArithmeticExpression` is the reference implementation
(`integerArithmetic` / `checkIntegerDivisorNotZero`, issues #5163, #5164, #5602).

### 1. 64-bit `+`, `-`, `*` wrapped around silently

`MathExpression.Operator.STAR/PLUS/MINUS.apply(Long, Long)` were bare `left * right` / `left + right` /
`left - right`. With `v` a stored `LONG` holding `Long.MAX_VALUE`, `select v*2` answered `-2` and `select v+1`
answered `-9223372036854775808`, both with HTTP 200.

The `Integer` overloads are fine: they widen to `long` on overflow, so they always have a representable answer.
The `Long` overload has nowhere left to widen to. Because `UpdateItem` routes `UPDATE ... SET x *= v` through
the same operators, the wrong number could be persisted, which makes this a silent data-corruption path rather
than only a wrong display value.

`SLASH.apply(Long, Long)` belongs to the same family for a less obvious reason: `Long.MIN_VALUE / -1` overflows
without the JDK raising anything at all, because JLS 15.17.2 defines it to return the dividend.

### 2. Integer `/` and `%` by zero raised a raw `java.lang.ArithmeticException`

`SLASH`/`REM` computed `left % right` with no divisor guard, so `select 1/0` and `select 1%0` let the JDK
exception escape uncaught. It is not an `ArithmeticErrorException`, so it missed the classification arms #5602
added in `AbstractServerHttpHandler` and fell through to the generic `catch (Throwable)` arm: HTTP 500 plus a
full stack trace in the server log, for a pure caller mistake.

The `BigDecimal` overloads had the same defect, confirmed empirically rather than assumed: `BigDecimal.divide`
and `BigDecimal.remainder` also raise a raw `java.lang.ArithmeticException` on a zero divisor. Leaving that one
would have meant `v/0` answering 400 for a `LONG` column and 500 for a `DECIMAL` one - the same cross-path
inconsistency this issue is about.

## Changes

`engine/src/main/java/com/arcadedb/query/sql/parser/MathExpression.java`, two new private helpers in the
`Operator` enum plus six overload bodies rewired to them:

- `exactIntegerArithmetic(op, left, right)` - `Math.addExact` / `subtractExact` / `multiplyExact` / `divideExact`,
  rethrowing the resulting `ArithmeticException` as `ArithmeticErrorException("long overflow")`. Wired into the
  `Long` overloads of `STAR`, `PLUS`, `MINUS` and `SLASH`.
- `checkDivisorNotZero(op, zero)` - raises `ArithmeticErrorException("/ by zero")` or `("% by zero")`. Wired into
  the `Integer`, `Long` and `BigDecimal` overloads of `SLASH` and `REM`. The caller evaluates zero-ness in the
  operand's own type (`right == 0` vs `right.signum() == 0`), so `BigDecimal` scale does not defeat the check.

`SLASH.apply(Integer, Integer)` additionally widens through `long` so `Integer.MIN_VALUE / -1` returns the
correct `2147483648`, narrowing back to `Integer` whenever the answer fits. That preserves the existing
`MathExpressionTest.types()` contract that `1/1` is an `Integer`, and matches the widening idiom `STAR` and
`PLUS` already used on their `Integer` overloads.

`ArithmeticErrorException extends CommandExecutionException`, so embedded callers catching the broader type are
unaffected and no HTTP or Bolt handler change was needed.

### Deliberately unchanged

- Floating-point `/` and `%` keep IEEE 754 semantics (`±Infinity` / `NaN`). Only integer division has no
  representable answer for a zero divisor; this matches the decision taken on the Cypher side in #5163.
- The `Integer` overloads of `PLUS`/`MINUS`/`STAR` keep their long-upgrade rather than gaining an exact-math
  guard: they have a representable answer, and `MathExpressionTest:63-64` pins that upgrade.

### Known behavior change beyond the two reported defects

`PLUS`/`MINUS`.`apply(Object, Object)` convert date operands to timestamps and then call the `Long` overload, so
nanosecond-precision date arithmetic that previously wrapped now raises `ArithmeticErrorException`. Reaching it
requires summing two timestamps past year 2262; the previous answer was a garbage `Duration`, so failing is the
better of the two. Noted here rather than special-cased, to avoid a branch with no test to justify it.

Item 1 is a behavior change in the sense the issue calls out - queries that today return a silently wrapped
number start failing - so it deserves a release note even though the current answer is simply wrong.

## Tests

Both are new files; no existing test was modified or deleted.

- `engine/src/test/java/com/arcadedb/query/sql/parser/Issue5647SqlIntegerArithmeticTest.java` (11 tests) -
  operator-level assertions for each overflow and each zero divisor, the `Integer` widening boundary, plus
  end-to-end SQL through `TestHelper.executeInNewDatabase` for the read path, the `1/0` and `1%0` from the issue,
  and an `UPDATE` that re-reads the record to prove no wrapped value was persisted. Three of the eleven are
  regression guards that were green before the fix (IEEE semantics, null propagation, arithmetic without
  overflow).
- `server/src/test/java/com/arcadedb/server/Issue5647SqlArithmeticHttpStatusIT.java` (4 tests) - the HTTP status
  codes, since the classification only exists at that boundary. Covers the read path, the auto-commit write path
  (where the failure arrives wrapped in a `TransactionException`, the shape that historically degraded a client
  error back to 500), and a non-overflow case that must still answer 200.

### Verification

Proven to fail first: 8 of the 11 engine tests failed against unmodified `main`, each reproducing the reported
behavior exactly (`expected 2147483648L but was -2147483648`, `java.lang.ArithmeticException: / by zero` at
`MathExpression$Operator$2.apply:140`, and "Expecting code to raise a throwable" for each silent wrap). The
three that passed are the regression guards, which is the intended split.

After the fix: 11/11 green.

Connected suites re-run green (195 tests): `MathExpressionTest`, `MultiplicationOverflowTest`,
`LetDivisionBugTest`, `SQLFunctionAbsoluteValueTest`, `TypeTest`, `Issue5163DivisionByZeroTest`,
`CypherFollowUpsIssue5602Test`.

The **full engine suite** was then run to confirm the blast radius, since these operators are shared by every
SQL arithmetic expression in the product: **10556 tests, 0 failures, 0 errors**, BUILD SUCCESS.

The server IT was **not** verified locally and is left to CI. Port 2480 on the development machine is held
permanently by a Homebrew-installed ArcadeDB service, so a `BaseGraphServerTest` server cannot bind it and the
hardcoded `127.0.0.1:248X` assertions reach that other server instead of the test's own. This is a property of
the local environment, not of the test: the IT follows the same hardcoded-port pattern as the merged
`Issue5545SqlArithmeticErrorHttpStatusIT` and ~117 other server ITs, all of which rely on CI having free ports.

## Follow-up worth filing separately

While the local IT run was contending for those ports, a request landed on an unrelated multi-server cluster and
exposed what looks like a distinct pre-existing defect on the HA command-forwarding path: when a replica forwards
a command and the leader answers 400 with an `ArithmeticErrorException`, the replica re-wraps it as a
`TransactionException` and answers **500** - the same client-error-degraded-to-server-error shape #5545 was
about, one layer further out. The reported detail also doubles the message (`long overflow -> long overflow`).
This was observed by accident rather than by design, so it needs deliberate reproduction against a real cluster
before being filed; it is out of scope here.
