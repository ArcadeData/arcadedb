# Issue #5842: ArcadeGremlin.setTimeout() is a no-op and its backing field is static

## Summary

`ArcadeGremlin.timeout` is declared `private static Long` but assigned by the
instance method `setTimeout(long, TimeUnit)`. Nothing in the codebase ever
reads the field, so `setTimeout`/`getTimeout` are a complete no-op regardless
of the static-vs-instance question, and the static field additionally leaks
any assigned value across every `ArcadeGremlin` instance in the process.

## Root cause

- `ArcadeGremlin.java:63` declares `private static Long timeout`.
- `setTimeout(long, TimeUnit)` (instance method) assigns the static field.
- `getTimeout()` reads the static field.
- A repo-wide search (`grep -rn "setTimeout\|getTimeout"`) finds no other
  reader or writer of this field anywhere in the engine, wire protocols, or
  Studio. `executeStatement()` never applies any timeout to the script
  engine, the traversal, or the underlying query.
- The only caller of `ArcadeGremlin.setTimeout`/`getTimeout` in the whole
  repository is the characterization test
  `ArcadeGremlinEngineSelectionTest.characterizesTheProcessWideTimeoutLeak`,
  which pins the wrong (leaking) behavior on purpose.

## Fix direction chosen

The issue offered two options: wire the field into execution (per-query
Gremlin timeout), or remove the dead API (YAGNI). No code anywhere in the
engine or wire protocols consumes a Gremlin-specific timeout today (SQL has
its own, separate `Timeout`/`MultiIterator` timeout mechanism, unrelated to
this field), and there is no existing hook point in
`GremlinLangScriptEngine`/`GremlinGroovyScriptEngine`/`GraphTraversal` wired
up elsewhere to bound execution time. Inventing a new enforcement mechanism
for an API nobody calls would be speculative scope creep, not a bug fix.

**Decision: remove `setTimeout`/`getTimeout` and the backing static field
from `ArcadeGremlin`.** This eliminates both defects (no-op behavior and the
process-wide leak) without adding unverified behavior.

## Expected vs actual behavior

- **Actual (before fix):** `ArcadeGremlin.setTimeout()` is callable, appears
  to configure a per-query timeout, but silently does nothing, and its value
  leaks across all instances via the static field.
- **Expected (after fix):** the dead, misleading API is removed. There is no
  `ArcadeGremlin.setTimeout()`/`getTimeout()` to misuse.

## Affected components

- `gremlin/src/main/java/com/arcadedb/gremlin/ArcadeGremlin.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGremlinEngineSelectionTest.java`

## TDD approach

1. Removed the characterization test `characterizesTheProcessWideTimeoutLeak`
   (it exists specifically to pin the wrong behavior; per the issue's own
   instructions it must be inverted/removed once the defect is fixed) along
   with the `originalTimeout` field, `restoreProcessWideTimeout()` reflection
   helper, and its `teardown()` call — all of which exist solely to save and
   restore the static field this fix removes.
2. Added a new regression test,
   `setTimeoutAndGetTimeoutAreNotPartOfThePublicApi`, that uses reflection to
   assert `ArcadeGremlin` no longer declares `setTimeout` or `getTimeout`
   methods, or a `timeout` field. This test fails against the pre-fix code
   (the methods/field exist) and passes once they are removed — it is the
   regression guard against the API silently reappearing as dead code.
3. Removed `setTimeout`/`getTimeout` and the `private static Long timeout`
   field from `ArcadeGremlin.java`. Removed the now-unused
   `java.util.concurrent.TimeUnit` import.
4. Compiled and ran the full `gremlin` module test suite.

## Test results

TDD sequence (per issue #5842, the gremlin module's own `mvn test` always
reports `Tests are skipped` by design - Gremlin's tests run in the sibling
`arcadedb-gremlin-it` module against the shaded jar/test-jar, because the
engine's ANTLR 4.13.2 and TinkerPop's ANTLR 4.9.1 cannot coexist on one
classpath):

1. **Red:** added `setTimeoutAndGetTimeoutAreNoLongerPartOfThePublicApi` to
   `ArcadeGremlinEngineSelectionTest` while `ArcadeGremlin.setTimeout`/
   `getTimeout`/`timeout` still existed. Built with
   `mvn -pl gremlin -am install -DskipTests -o`, then ran it in isolation via
   `mvn -pl gremlin-it -o test -Dtest=ArcadeGremlinEngineSelectionTest#setTimeoutAndGetTimeoutAreNoLongerPartOfThePublicApi`:
   **1 run, 1 failure** -
   `java.lang.AssertionError: Expecting code to raise a throwable.` - proving
   the dead API was still reachable via reflection before the fix.
2. Removed `setTimeout`/`getTimeout`/the static `timeout` field from
   `ArcadeGremlin.java` and the now-uncompilable `originalTimeout` field,
   `restoreProcessWideTimeout()` helper, and the old
   `characterizesTheProcessWideTimeoutLeak` test from
   `ArcadeGremlinEngineSelectionTest.java`.
3. **Green:** `mvn -pl gremlin -am install -DskipTests -o` (compiles cleanly,
   0 errors, produces the shaded/test jars), then
   `mvn -pl gremlin-it -o test -Dtest=ArcadeGremlinEngineSelectionTest`:
   **Tests run: 7, Failures: 0, Errors: 0, Skipped: 0**.
4. **Regression sweep** of every ArcadeDB-specific Gremlin test class
   (excluding the third-party TinkerPop conformance suite, which is out of
   scope for this change and takes many minutes to run):
   `mvn -pl gremlin-it -o test -Dtest=ArcadeEdgeCountFilterStepTest,ArcadeFilterByIndexStepTest,ArcadeFilterByTypeStepTest,ArcadeGAVStepsTest,ArcadeGraphFactoryPoolTest,ArcadeGremlinAnalyzeTest,ArcadeGremlinEngineSelectionTest,GremlinClosureParseErrorTest,GremlinGAVTest,GremlinGroovyEngineTest,GremlinGroovyFallbackRCETest,GremlinHasLabelWrongKindTest,GremlinNegationPredicateTest,GremlinNextOnEmptyTest,GremlinParameterizedAnalyzeTest,GremlinTest,SQLFromGremlinTest`:
   **Tests run: 124, Failures: 0, Errors: 0, Skipped: 5** (the 5 skips are
   pre-existing and unrelated to this change).

## Impact analysis

- Pure removal of dead, unreachable code. No behavior changes for any real
  caller because there were none.
- `ArcadeQuery` (the abstract base class) never declared `setTimeout`/
  `getTimeout`, so no other subclass or interface is affected.
- No wire-protocol, Studio, or SDK surface exposed this method (verified via
  repo-wide grep), so this is not a public/remote API break.

## Recommendations

- If a genuine per-query Gremlin timeout is wanted in the future, it should
  be designed and wired into `executeStatement()` (e.g. bounding
  `GremlinLangScriptEngine`/`GremlinGroovyScriptEngine` evaluation or the
  resulting `GraphTraversal` iteration) as a new, tested feature, not
  resurrected as an inert setter.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5896

## Review cycles

- **Cycle 1** - head SHA `197cd1b3638dc2a7cdaf18917699df71e1200935` (initial
  push). `claude[bot]` posted a review comment: "Clean, well-scoped removal
  ... Overall: safe, well-tested removal of genuinely dead and misleading
  API. LGTM." One cosmetic nit was raised (a blank line left between the
  class declaration and the constructor after the field removal) and
  explicitly called "not worth blocking on" by the reviewer itself - no
  actionable items, working tree left unchanged. Codacy's automated check
  also reported "Up to standards". Loop exited on cycle 1 with a clean
  approval; no further commits were needed.

## Deferred items

None. The only reviewer note (the cosmetic blank-line nit) was explicitly
marked non-blocking by the reviewer, so nothing was deferred for developer
follow-up.

## Final state

`clean-approval` (1 review cycle).
