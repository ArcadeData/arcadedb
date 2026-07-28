# Draft issue: TRAVERSE and SELECT from a bound RID collection throw NullPointerException

File with:

```bash
gh issue create --repo ArcadeData/arcadedb --label bug \
  --title "TRAVERSE and SELECT from a bound RID collection throw NullPointerException" \
  --body-file docs/superpowers/4861-engine-traverse-npe-issue.md
```

(Delete this header block first, or pass the body inline - everything below the line is the issue body.)

---

## Problem

Passing a collection of RIDs as a query parameter target throws `NullPointerException`. Both of these
fail:

```sql
SELECT @rid AS rid FROM :seeds
SELECT @rid AS rid FROM (TRAVERSE out('LINK') FROM :seeds MAXDEPTH 2)
```

with `params = {"seeds": List<RID>}`:

```
java.lang.NullPointerException: Cannot invoke
"com.arcadedb.query.sql.parser.Expression.execute(com.arcadedb.query.sql.executor.Result,
 com.arcadedb.query.sql.executor.CommandContext)" because "this.expression" is null
  at com.arcadedb.query.sql.parser.Rid.toRecordId(Rid.java:71)
  at com.arcadedb.query.sql.executor.TraverseExecutionPlanner.handleRidsAsTarget(TraverseExecutionPlanner.java:220)
  at com.arcadedb.query.sql.executor.TraverseExecutionPlanner.handleInputParamAsTarget(TraverseExecutionPlanner.java:174)
```

A **single** bound RID works. Only a collection fails, which is what makes this easy to miss.

## Cause

Both planners build a `Rid` AST node per element and set `bucket` and `position`, but never call
`rid.setLegacy(true)`:

- `engine/src/main/java/com/arcadedb/query/sql/executor/TraverseExecutionPlanner.java:155-174`
- `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java:1435-1453`

Their own **singleton**-RID branches do call it, a few lines above in the same method:

- `TraverseExecutionPlanner.java:150`
- `SelectExecutionPlanner.java:1425`

`Rid.toRecordId` (`engine/src/main/java/com/arcadedb/query/sql/parser/Rid.java:67-71`) reads
`bucket`/`position` when `legacy` is true, and otherwise dereferences `expression`, which the
collection branch never populates:

```java
  public RID toRecordId(final Result target, final CommandContext context) {
    if (legacy) {
      return context.getDatabase().newRID(bucket.value.intValue(), position.value.longValue());
    } else {
      final Object result = expression.execute(target, context);   // expression == null here
```

Note that `Rid`'s two constructors differ: `Rid(RID)` sets `legacy = true`, while `Rid(int)` - the one
both collection branches use - leaves it false.

## Fix

Add `rid.setLegacy(true)` to both collection branches, matching the singleton branches beside them.

## Regression test

One test per planner, each binding a **collection** - a single-element bound RID passes today and
would not catch the regression:

```java
final Map<String, Object> params = Map.of("seeds", List.of(v0.getIdentity(), v1.getIdentity()));
database.query("sql", "SELECT FROM :seeds", params);
database.query("sql", "SELECT FROM (TRAVERSE out('LINK') FROM :seeds MAXDEPTH 1)", params);
```

## Impact and workaround

Any caller computing a set of RIDs and feeding it to a traversal or select must inline the RIDs as
literals (`FROM [#1:0,#1:1]`) instead of binding them. That form works and returns full BFS metadata
including `$depth` and `$path`.

## Context

Found while designing the MCP `hybrid_search` tool (#4861), whose graph expansion leg traverses from a
computed seed set. That tool works around it by inlining RID literals - safe there because the seed
RIDs originate from the engine's own retrieval legs and never from caller text - and does not depend
on this fix landing.
