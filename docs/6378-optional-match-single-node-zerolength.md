# Issue #6378: OPTIONAL MATCH with a single-node named path raises ZeroLengthPathStep error

## Root cause

In `CypherExecutionPlan.buildMatchStep` (single-node pattern branch, `pathPattern.isSingleNode()`),
when the pattern carries a path variable (`p = ({k: 1})`) and the enclosing `MATCH` is `OPTIONAL`,
the code that wires up the `ZeroLengthPathStep` reassigns `matchChainStart` to the `ZeroLengthPathStep`
itself:

```java
final ZeroLengthPathStep zeroPathStep = new ZeroLengthPathStep(variable, singlePathVar, context);
zeroPathStep.setPrevious(currentStep);
currentStep = zeroPathStep;
if (isOptional && matchChainStart == matchStep)
  matchChainStart = zeroPathStep;   // BUG: overwrites the chain head
```

`OptionalMatchStep` is later constructed as
`new OptionalMatchStep(matchChainStart, currentStep, matchVariables, context)`, where
`matchChainStart` is supposed to be the *first* step of the sub-chain (the one that receives the
per-input-row feed via `setPrevious(...)`) and `matchChainEnd` (`currentStep`) is the *last* step
(the one results are pulled from).

Because `matchChainStart` was wrongly reassigned to `zeroPathStep`, `matchChainStart` and
`matchChainEnd` become the *same* step. `OptionalMatchStep.syncPull` then calls
`matchChainStart.setPrevious(...)` directly on the `ZeroLengthPathStep`, silently discarding the
link to the real `MatchNodeStep` (`matchStep`) that `zeroPathStep.setPrevious(currentStep)` had
established one line earlier. For a standalone `OPTIONAL MATCH` (no preceding clause) that call is
`matchChainStart.setPrevious(null)`, so `ZeroLengthPathStep.syncPull` hits
`checkForPrevious("ZeroLengthPathStep requires a previous step")` and throws - exactly the
reported error.

The equivalent code path for multi-node patterns with zero relationships (further down in the same
method, guarding the same kind of reassignment with `if (matchChainStart == null)`) does this
correctly - it only assigns `matchChainStart` the first time it's still unset. The single-node
branch's `matchChainStart == matchStep` condition is wrong: it fires precisely in the common case
where `matchStep` legitimately already *is* the chain start.

## Fix

Remove the erroneous reassignment. `matchChainStart` was already correctly set to `matchStep` (or
left untouched if a previous pattern in the same `MATCH` clause already set it) a few lines above;
the `ZeroLengthPathStep` should only ever become the new `currentStep` (chain end), never overwrite
`matchChainStart`.

## Test

`engine/src/test/java/com/arcadedb/query/opencypher/Issue6378OptionalMatchSingleNodeZeroLengthPathTest.java`
reproduces the exact queries from the issue report (`CREATE (:N {k: 1})`, then both the control
`MATCH p = ({k: 1}) RETURN p` and the failing `OPTIONAL MATCH p = ({k: 1}) RETURN p`), plus a
no-match case (`OPTIONAL MATCH p = ({k: 999})`) and a chained case
(`MATCH (n) OPTIONAL MATCH p = (n) RETURN p`) to cover the "matchChainStart already set from an
earlier pattern" branch.

## Verification

1. **Red**: ran the new test class against the unmodified code. As predicted, the two `OPTIONAL
   MATCH` cases failed with `java.lang.IllegalStateException: ZeroLengthPathStep requires a
   previous step` (the exact error from the issue report); the control (non-optional) and
   already-bound-variable cases passed, matching the analysis above.
2. Applied the one-line fix (removed the erroneous `matchChainStart = zeroPathStep` reassignment).
3. **Green**: re-ran the new test class - `Tests run: 4, Failures: 0, Errors: 0`.
4. Ran the broader OPTIONAL MATCH / zero-length-path / variable-length-path / GAV regression
   surface to check for side effects:
   - `Issue6378OptionalMatchSingleNodeZeroLengthPathTest`: 4/4 passed
   - `OpenCypherZeroLengthPathTest`: 5/5 passed
   - `Issue5094OptionalMatchCountStarTest`: 5/5 passed
   - `Issue5790OptionalMatchCountStarUndeclaredRelTypeTest`: 5/5 passed
   - `CypherCaseOptionalMatchRelIssue5137Test`: 4/4 passed
   - `OpenCypherVariableLengthPathTest`: 41/41 passed
   - `GAVEligibilityTest`: 42/42 passed
   - `OpenCypherOptionalMatchTest` reported "Tests run: 0" both before and after the fix, with and
     without the fully-qualified class name filter. The class compiles fine (its `.class` files
     are present in `target/test-classes`) and is unmodified by this change, so this looks like a
     pre-existing JUnit5 discovery quirk in this environment rather than something the fix
     introduced or broke; flagged for a maintainer to look into separately.

No existing tests were modified or deleted.
