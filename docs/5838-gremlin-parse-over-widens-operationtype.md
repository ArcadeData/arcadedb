# Issue #5838 - Gremlin parse() over-widens OperationType, causing MCP to deny legitimate insert operations

## Problem

`ArcadeGremlin.parse()` classifies each mutating step in a traversal by `instanceof`-checking it
against the concrete TinkerPop step classes (`AddVertexStep`, `AddVertexStartStep`, `AddEdgeStep`,
`AddEdgeStartStep`, `AddPropertyStep`). TinkerPop 3.8.1's gremlin-lang parser, however, hands
`parse()` GValue-based *placeholder* step types for `addV`/`addE`/`property`
(`AddVertexStartStepPlaceholder`, `AddEdgeStepPlaceholder`, `AddPropertyStepPlaceholder`, etc.) -
these are only resolved into the concrete classes by TinkerPop's `GValueReductionStrategy`, which
`parse()` never runs (it reads `getSteps()` straight after `eval()`).

None of the placeholder classes is a subtype of the concrete classes the `instanceof` chain
checks for, so every addV/addE/property query falls into the "unknown mutating step" catch-all,
which unions all three write `OperationType`s (`CREATE`, `UPDATE`, `DELETE`) instead of the single
one that actually applies.

`ExecuteCommandTool.execute()` (MCP) calls `GremlinQueryEngine.analyze()`, which delegates to this
`parse()`, then `checkPermission(Set<OperationType>, MCPPermissions)` rejects the whole command if
**any** `OperationType` present lacks its matching permission bit. Concrete effect: an MCP
`execute_command` call with language `"gremlin"` and command `g.addV('Person')`, against a profile
with `allowInsert=true` but `allowUpdate=false` (a realistic, supported profile), is wrongly
rejected with "Update operations are not allowed by MCP configuration" even though the query is a
pure insert.

This is fail-closed over-denial, not privilege escalation, and does not depend on HA. HA follower
routing is unaffected: the placeholders DO implement `Mutating` transitively (via `Writing`/
`Deleting`, javap-verified), so `isIdempotent()` correctly returns `false`, and
`RaftReplicatedDatabase` consults only `isIdempotent()`/`isDDL()`, never `getOperationTypes()`.

`DropStep` has no placeholder variant in gremlin-core 3.8.1, so `drop()` alone reached `parse()`
already resolved and was classified correctly before this fix.

## Fix

`gremlin/src/main/java/com/arcadedb/gremlin/ArcadeGremlin.java` - `parse()`

TinkerPop 3.8.1 introduced `AddVertexStepContract`, `AddEdgeStepContract`, and
`AddPropertyStepContract` interfaces (verified via `javap` against `gremlin-core-3.8.1.jar`) that
are implemented by **both** the placeholder step and its resolved concrete counterpart:

| Concrete class | Placeholder class | Shared contract |
| --- | --- | --- |
| `AddVertexStep`, `AddVertexStartStep` | `AddVertexStepPlaceholder`, `AddVertexStartStepPlaceholder` | `AddVertexStepContract` |
| `AddEdgeStep`, `AddEdgeStartStep` | `AddEdgeStepPlaceholder`, `AddEdgeStartStepPlaceholder` | `AddEdgeStepContract` |
| `AddPropertyStep` | `AddPropertyStepPlaceholder` | `AddPropertyStepContract` |

Changed the `instanceof` chain in `parse()` to check against these contract interfaces instead of
the concrete classes, so classification is correct regardless of whether `parse()` sees the
placeholder or the already-resolved step. `DropStep` (no placeholder variant) keeps its direct
`instanceof DropStep` check.

```java
if (step instanceof AddVertexStepContract || step instanceof AddEdgeStepContract)
  ops.add(OperationType.CREATE);
else if (step instanceof DropStep)
  ops.add(OperationType.DELETE);
else if (step instanceof AddPropertyStepContract)
  ops.add(OperationType.UPDATE);
else {
  // Unknown mutating step: assume all write types
  ...
}
```

## Tests

`gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGremlinAnalyzeTest.java`

The issue's three `@Disabled` tests pinning the correct narrow expectations were enabled (no logic
changes to the assertions themselves, since they already documented the exact expected fix):

- `addVertexOperationTypeIsExactlyCreate` - `g.addV('Person')` -> `containsExactly(CREATE)`
- `addEdgeOperationTypeIsExactlyCreate` - `addE('KNOWS')` -> `containsExactly(CREATE)`
- `addPropertyOperationTypeIsExactlyUpdate` - `property('age', 30)` -> `containsExactly(UPDATE)`

Confirmed regression-proof before committing: stashed the source fix, reinstalled
`arcadedb-gremlin`, and reran the test class through `gremlin-it` (gremlin's own tests are
`skipTests=true` and only execute against the shaded jar in the `gremlin-it` module, per its
surefire `dependenciesToScan` config) - exactly the 3 enabled tests failed
(`Tests run: 13, Failures: 3`), matching the issue's predicted symptom. Restored the fix,
reinstalled, reran - all 13 pass.

Also ran the full `arcadedb-gremlin-it` module (`mvn -pl gremlin-it test`, 344 tests including the
TinkerPop structure/process conformance suite, `GremlinParameterizedAnalyzeTest` (#5187 regression
coverage, includes a mixed `addV().property()` case asserting `.contains(CREATE)`), and all other
gremlin/server-gremlin unit tests): **344 tests, 0 failures, 0 errors, 43 skipped (pre-existing
skips, unrelated to this change)**.

## Impact analysis

- Scope: `ArcadeGremlin.parse()` only, used by MCP's `execute_command` permission check and any
  other caller of `QueryEngine.analyze()` for the gremlin engine that inspects
  `getOperationTypes()`.
- HA follower routing: unaffected (confirmed above; `isIdempotent()`/`isDDL()` are untouched by
  this change).
- No new dependency; no public API change; `OperationType` set semantics unchanged - only which
  types are added for a given step is corrected.

## Recommendations

- If TinkerPop is upgraded past 3.8.1, re-verify (via `javap`) that `*StepContract` interfaces
  still exist and are still implemented by both the placeholder and concrete step for
  addV/addE/property, and that `DropStep` still has no placeholder variant - the test
  `mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps` in `ArcadeGremlinAnalyzeTest` pins
  the current placeholder class names and will fail loudly if TinkerPop changes this shape. A
  cross-reference comment was added next to `gremlin.version` in the root `pom.xml` pointing back
  here (see cycle 1 below).

## PR

https://github.com/ArcadeData/arcadedb/pull/5903

## Review cycles

### Cycle 1 - `5046a7958`

`claude[bot]` posted a review as an issue comment confirming the root-cause analysis by tracing
`ExecuteCommandTool.checkPermission()` and verifying it really does deny on any missing
permission bit across the whole `OperationType` set. Confirmed the `*StepContract` fix is the
right shape and that HA routing (`isIdempotent()`/`isDDL()`) is untouched. Raised one actionable,
non-blocking suggestion: add a one-line cross-reference comment near the `gremlin.version`
property in the root `pom.xml` pointing at the doc/test, mirroring the existing `#5535` antlr
cross-reference comment in `gremlin/pom.xml`.

Applied: added a comment block above `<gremlin.version>3.8.1</gremlin.version>` in the root
`pom.xml` explaining the `*StepContract` requirement and cross-referencing this doc, the
regression test, and the `#5535` antlr-pairing comment. Validated with `mvn -N validate`.

No deferred items from this cycle.

### Cycle 2 - `9d0a3a7f1`

`claude[bot]` re-reviewed after the cycle 1 push. Independently re-confirmed the root cause
against `ExecuteCommandTool.checkPermission()`, confirmed the fix doesn't touch `isIdempotent()`
(HA routing unaffected), and noted it could not itself run `javap` against `gremlin-core:3.8.1`
in its sandbox but that the PR's claim is backed by a regression test
(`mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps`) that pins the exact placeholder
class names, so a future TinkerPop upgrade that changes the shape fails loudly rather than
regressing silently. Explicitly called out the new `pom.xml` cross-reference comment as a "nice
touch."

One optional, explicitly-non-blocking note: "consider trimming some of the very verbose in-code
comments if the team wants `docs/5838-*.md` to be the single source of truth." Framed as "my only
optional ask" - a style nitpick, not a defect. Skipped: the inline comments explain a genuinely
version-sensitive TinkerPop internal (placeholder vs. concrete step, contract interface, the
`DropStep` exception) at the exact point a future reader needs it, which is more discoverable
than requiring a jump to `docs/`; the first review round explicitly praised the same level of
detail. No code changes required, working tree clean. Treated as a clean approval - loop exited
after 2 cycles.

### Deferred items

None. The one optional nit from cycle 2 (trim in-code comment verbosity) was evaluated and
intentionally skipped with rationale recorded above, not deferred for developer follow-up.

### Final state

`clean-approval`
