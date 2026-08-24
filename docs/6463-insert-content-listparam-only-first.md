# Issue #6463: `INSERT INTO ... CONTENT :listParam` inserts only the first list item

## Summary

`INSERT INTO <type> CONTENT :param` where the bound parameter is a `java.util.List` silently
inserted only the **first** element of the list, with no error, unlike the equivalent JSON-array
literal form (`CONTENT [{...}, {...}, ...]`), which creates one record per item.

## Root cause

`InsertExecutionPlanner.handleCreateRecord()` decides how many empty records `CreateRecordStep`
should create (`tot`) by inspecting the parsed `InsertBody`:

```java
int tot = 1;
if (body.getValueExpressions() != null && ...) tot = body.getValueExpressions().size();
else if (body.getJsonArrayContent() != null && ...) tot = body.getJsonArrayContent().items.size();
```

There was no branch for a `CONTENT :param` **input parameter**, so `tot` stayed `1` regardless of
what the parameter resolved to. `UpdateContentStep.handleContent()` already knew how to drain a
`List`-valued parameter item by item (`parameterArray.get(arrayIndex++)`), but with `tot == 1` it
was only ever driven once per upstream record, so items `1..n-1` were silently dropped.

## Fix

Input parameters are already bound into the `CommandContext` by the time the execution plan is
built (`InsertStatement.execute()` calls `context.setInputParameters(...)` before
`createExecutionPlan(context)`), so the fix resolves the `CONTENT` parameter at plan-build time and
sizes `tot` from it when it is a non-empty `List`, mirroring the existing `getJsonArrayContent()`
branch. A `Map`/`Document`-valued parameter (single record) is unaffected.

`CreateVertexExecutionPlanner`/`CreateEdgeExecutionPlanner` extend `InsertExecutionPlanner` and
reuse `handleCreateRecord()` unchanged, so `CREATE VERTEX/EDGE ... CONTENT :param` get the fix for
free - confirmed with a dedicated test added during review (see cycle 2 below).

## Deliberately unchanged edge case

An empty-list `CONTENT :param` keeps `tot = 1` (one near-empty record created), matching the
pre-existing behavior of an empty JSON-array literal (the `items.size() > 0` guard on the
`getJsonArrayContent()` branch). This PR does not change that. Pinned by a dedicated test added
during review (cycle 2).

## PR

https://github.com/ArcadeData/arcadedb/pull/6647

## Review cycles

- **Cycle 1** - head `60fe0c0362` (initial push). Bot review (PR issue comment, no commit SHA -
  https://github.com/ArcadeData/arcadedb/pull/6647#issuecomment posted 2026-08-23T20:22:47Z):
  "No blocking issues found." One actionable-and-clear item: a positional-parameter (`?`) variant
  of the `CONTENT :param` list case wasn't covered (only the named `:people` form was). Applied -
  added `insertContentWithPositionalListParamCreatesOneRecordPerItem`. Two non-blocking notes
  skipped with rationale: (a) `Set`/other `Collection`-valued `CONTENT` params fall through to
  `tot = 1`, explicitly called out by the reviewer as pre-existing, symmetric with
  `UpdateContentStep`'s existing `List`-only special case, and not a regression from this PR; (b)
  style/consistency observations with no requested action. Response commit `9dfd443bc2`.
- **Cycle 2** - head `9dfd443bc2`. Bot review (2026-08-24T07:27:47Z): "Nice, focused fix... My only
  suggestion is the two small test additions noted above... neither blocks merging as-is." Two
  actionable-and-clear items, both verified against the code before applying: (a) the
  intentionally-unchanged empty-list edge case had no test pinning it down - applied
  `insertContentWithEmptyListParamStillCreatesOneRecord`, confirmed by tracing
  `UpdateContentStep.handleContent()` (an empty list never matches any of its branches, so the
  record is created with no properties set, count stays 1); (b) `CREATE VERTEX ... CONTENT :param`
  with a list was only covered "incidentally" via the shared planner code path, not with a
  dedicated test - applied `createVertexContentWithListParamCreatesOneVertexPerItem`, after
  verifying `CreateVertexExecutionPlanner extends InsertExecutionPlanner` and reuses
  `handleCreateRecord()` unchanged. One non-blocking note skipped with rationale: resolving the
  content parameter twice (once to size `tot`, once again in `UpdateContentStep` to drain it) is a
  small duplication the reviewer itself called "not a real performance concern" - threading the
  resolved list through would add complexity to a plan-build-time-only path for negligible gain.
  Response commit `c48e7e5d35`.
- **Cycle 3** - head `c48e7e5d35`. Bot review (2026-08-24T07:32:01Z): "No blocking issues found.
  Nice, minimal fix..." No new actionable items - both prior minor observations (Collection-typed
  params, double parameter resolution) were reduced to explicit "pre-existing limitation this fix
  doesn't need to address" / "not worth optimizing." No code changes this cycle - clean approval.

## Deferred items

None. Every actionable-and-clear review item was applied within its cycle; every skipped item had
an explicit non-blocking rationale from the reviewer itself (pre-existing/symmetric behavior, or
an admitted non-concern), so nothing was deferred to a separate notes file.

## Final state

`clean-approval` after 3 review cycles (within `--max-cycles=4`). Merge is the developer's
responsibility; this PR has not been merged.
