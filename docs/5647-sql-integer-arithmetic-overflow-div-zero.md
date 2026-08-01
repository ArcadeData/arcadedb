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

Plus the two `Integer` overloads described in defect 3 below, rewritten to widen-then-narrow.

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

### 3. The `Integer` overloads of `+` and `-` also wrapped (found in review, cycle 1)

The issue states that "the `Integer` overload already does the right thing by widening to `long`". That premise
is only half true, and the review of this PR caught it. `PLUS.apply(Integer, Integer)` guarded
`sum < 0 && left > 0 && right > 0` and `MINUS.apply(Integer, Integer)` guarded
`result > 0 && left < 0 && right > 0`: both test the **signs of the operands** rather than whether the answer
fits, so each only ever detected overflow in one direction. Verified:

```
Integer.MAX_VALUE - Integer.MIN_VALUE  ->  -1   (should be  4294967295)
Integer.MIN_VALUE + Integer.MIN_VALUE  ->   0   (should be -4294967296)
```

Both now widen first and narrow back when the answer fits, which is the idiom `STAR.apply(Integer, Integer)`
already used a few lines above in the same enum. This is the same silent-wrap defect the PR is named for, in
the same class, so fixing it here keeps the change honest rather than shipping "SQL integer arithmetic no longer
wraps silently" with two counterexamples left in place. Unlike the `Long` overloads these have somewhere to
widen to, so the result is a correct value rather than an error.

### Deliberately unchanged

- Floating-point `/` and `%` keep IEEE 754 semantics (`±Infinity` / `NaN`). Only integer division has no
  representable answer for a zero divisor; this matches the decision taken on the Cypher side in #5163.
- The `Integer` overloads of `PLUS`/`MINUS`/`STAR` widen rather than gaining an exact-math guard: they have a
  representable answer, and `MathExpressionTest:63-64` pins that upgrade.
- `SLASH.apply(Long, Long)` keeps its lossy `(double) left / right` fallback for inexact division, raised in
  review. It predates this issue and narrowing it would change division semantics for every SQL query, which
  needs its own issue and its own regression suite.

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
SQL arithmetic expression in the product: **10556 tests, 0 failures, 0 errors**, BUILD SUCCESS. Re-run after the
review-cycle-1 `Integer` overload fix: **10557 tests, 0 failures, 0 errors**.

The cycle-1 test was proven to fail first the same way, by stashing only the main-source change and re-running
it: `expected: 4294967295L but was: -1`.

The server IT is **green in CI**, confirmed by class name in the `integration-tests` job log rather than by the
check's colour:

```
[INFO] Running com.arcadedb.server.Issue5647SqlArithmeticHttpStatusIT
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0 -- in com.arcadedb.server.Issue5647SqlArithmeticHttpStatusIT
```

That distinction matters here: `.github/workflows/mvn-test.yml` runs both `unit-tests` and `integration-tests`
with `--fail-never`, so those checks go green even when tests fail - the failures surface only through the
Tests Reporter step. `build-and-package` runs no ITs at all. The wider IT run reported zero failures and zero
errors across every module, so this change introduces no IT regressions either.

It was **not** verified locally. Port 2480 on the development machine is held
permanently by a Homebrew-installed ArcadeDB service, so a `BaseGraphServerTest` server cannot bind it and the
hardcoded `127.0.0.1:248X` assertions reach that other server instead of the test's own. This is a property of
the local environment, not of the test: the IT follows the same hardcoded-port pattern as the merged
`Issue5545SqlArithmeticErrorHttpStatusIT` and ~117 other server ITs, all of which rely on CI having free ports.

## Review cycles

- **Cycle 1** (`a607aca0d`) - approved with observations. One was a real defect in code this PR had not
  touched: the `Integer` overloads of `PLUS`/`MINUS` guarded on operand signs rather than on whether the answer
  fits, so `Integer.MAX_VALUE - Integer.MIN_VALUE` returned `-1` and `Integer.MIN_VALUE + Integer.MIN_VALUE`
  returned `0`. Verified, then fixed in `252bd7709` (defect 3 above) because the issue's premise that the
  `Integer` path "already does the right thing" was provably false and this PR's headline claim depended on it.
  Not applied: the lossy `double` fallback in `SLASH.apply(Long, Long)` (out of scope, see above); the
  suggestion that the tracking doc may not belong in the tree (checked - `docs/<issue>-*.md` is the established
  convention, 8+ prior examples including the merged #5545).
- **Cycle 2** (`252bd7709`) - approved, no blocking items. Raised the `Type.increment` parallel path, recorded
  as a follow-up below rather than fixed here.

Final state: **clean approval**, 2 of 4 cycles used.

## Follow-ups worth filing separately

### `Type.increment` has both of these defects, and it backs `sum()`

Raised in review cycle 2 and confirmed empirically against the compiled class:

```
Type.increment(Long.MAX_VALUE, 1L)                    = -9223372036854775808
Type.increment(Integer.MAX_VALUE, Integer.MIN_VALUE)  = -1
Type.increment(Integer.MIN_VALUE, Integer.MIN_VALUE)  = 0
```

`engine/src/main/java/com/arcadedb/schema/Type.java:772` is a parallel arithmetic implementation: its
`Long`+`Long` case is a bare `a.longValue() + b.longValue()`, and its `Integer`+`Integer` guard is the same
one-directional sign check removed from `MathExpression` here. `SQLFunctionSum` and `SQLFunctionAverage` both
route through it, so after this PR `select v * 2` fails cleanly on overflow while `select sum(v)` over the same
values still wraps silently.

Deliberately **not** fixed in this PR. `Type.increment` is a general-purpose utility with its own call graph,
and making it throw changes aggregation semantics for every query language, which needs its own regression
suite and its own release note. The existing `TypeTest.incrementIntegerOverflow` only asserts that
`Integer.MAX_VALUE + 1` becomes a `Long`, so it does not pin the wrap and would not obstruct the fix.

### HA command forwarding degrades a leader's 400 to a 500

Lower confidence than the one above - this one still needs deliberate reproduction before being filed.
While the local IT run was contending for those ports, a request landed on an unrelated multi-server cluster and
exposed what looks like a distinct pre-existing defect on the HA command-forwarding path: when a replica forwards
a command and the leader answers 400 with an `ArithmeticErrorException`, the replica re-wraps it as a
`TransactionException` and answers **500** - the same client-error-degraded-to-server-error shape #5545 was
about, one layer further out. The reported detail also doubles the message (`long overflow -> long overflow`).
This was observed by accident rather than by design, so it needs deliberate reproduction against a real cluster
before being filed; it is out of scope here.
