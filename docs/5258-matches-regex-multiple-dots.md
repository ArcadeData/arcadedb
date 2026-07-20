# Issue #5258 - MATCHES fails when the regular expression contains multiple dots

## Problem

```sql
SELECT FROM (SELECT 'abc' AS value) WHERE value MATCHES '.*.*'
```

threw:

```
CommandSQLParsingException: Nested property access is not supported in this context: MATCHES_.*.*
```

A regex with a single dot (`.*`) worked. Supplying the regex through an input parameter did not help,
because the resolved value is still what forms the cache key.

## Root cause

`MatchesCondition.matches()` memoizes the compiled `Pattern` per command context:

```java
final String key = "MATCHES_" + regex;
Pattern p = (Pattern) context.getVariable(key);
```

`BasicCommandContext.getVariable()` treats its argument as a `$var.field` path: it splits on `.` and
throws `CommandSQLParsingException` when the key yields more than two parts. Embedding the raw regex
in the key therefore hands arbitrary user text to a namespace that assigns meaning to dots.

This was introduced by the fix for #4397 (PR #4403), which changed the key from `regex.hashCode()` to
`regex` to stop distinct regexes with colliding hash codes from sharing one compiled pattern. That
change was correct in intent - the collision bug is real - but moved the collision-free key into a
namespace that cannot hold it.

Note there is also a silent variant: a regex with exactly one dot (e.g. `a.c`) splits into two parts,
so `getVariable` performs a field lookup, always misses, and the pattern is recompiled on every row.
That degrades to a per-row `Pattern.compile` rather than an exception.

## Fix

`CommandContext` already exposes an opaque cache for precisely this situation:

```java
/**
 * Returns a value from an internal cache whose key is opaque: unlike getVariable(String), the name
 * is never interpreted as a $var.field nested path, so it can safely embed user data (e.g. a
 * full-text query string with dots).
 */
Object getCachedValue(String key);
CommandContext setCachedValue(String key, Object value);
```

It was added when `SQLFunctionSearchIndex` hit the same class of bug (a Lucene query string used as a
cache key); `MatchesCondition` was never migrated onto it. The fix is that migration:

```java
Pattern p = (Pattern) context.getCachedValue(key);
if (p == null) {
  p = Pattern.compile(regex);
  context.setCachedValue(key, p);
}
```

The key still embeds the full regex, so the #4397 collision fix is preserved. The cache lifetime is
unchanged (per command context, including the parent/child hierarchy), and the one-dot recompile
variant above is fixed as a side effect.

## Tests

New methods in `engine/src/test/java/com/arcadedb/query/sql/MatchesConditionTest.java`:

- `literalRegexWithMultipleDotsIsAccepted` - the reproduction from the issue, `MATCHES '.*.*'`.
- `parameterRegexWithMultipleDotsIsAccepted` - the same regex supplied as a named parameter,
  covering the `rightParam` branch.
- `perRowRegexesWithMultipleDotsStayDistinct` - two distinct multi-dot regexes evaluated per row
  within one command context, so the cache is exercised and must not conflate them.

All three fail on `main` with `Nested property access is not supported in this context` and pass with
the fix. The two pre-existing tests from #4397 (`collidingRegexesDoNotShareCachedPattern`,
`literalMatchesReturnCorrectRows`) are unmodified and still pass, confirming no regression of the
hash-collision fix.

## Verification

- `mvn test -Dtest=MatchesConditionTest` in `engine` - 5/5 green.
- `mvn test -Dtest='com.arcadedb.query.sql.**'` - 2266 tests, 0 failures. The 6 errors are
  pre-existing on `main` (verified by re-running with the fix reverted): GraalVM-JavaScript
  `TriggerSQLTest` / `SQLVectorHybridSearchBlogPostTest` and one benchmark, all unrelated.

## PR

https://github.com/ArcadeData/arcadedb/pull/5354

## Review cycles

Cycle 1 - `a9baf19`:

- `claude[bot]`: LGTM, no actionable items. Confirmed the fix reuses the existing opaque cache rather
  than inventing a mechanism, preserves the #4397 key semantics, and covers both `evaluate()`
  overloads through the shared `matches()` helper. Noted as non-blocking that a per-row regex column
  grows `cachedValues` for the command-context lifetime; this is pre-existing behavior from the
  `setVariable` path and not a regression, so no change was made.
- `gemini-code-assist`: suggested null-guarding `context` around the two cache calls. Declined - both
  `evaluate()` overloads already dereference `context` unconditionally before reaching `matches()`
  (`context.getInputParameters()` on the `rightParam` branch, and `expression.execute(..., context)`
  on every path), so a null context throws earlier regardless. Guarding only the cache lines would
  imply a null-safety guarantee the method does not provide. The replaced code called
  `context.getVariable(key)` with the same unconditional dereference, so null behavior is unchanged
  by this PR. Rationale posted on the review thread.

No code changes resulted from cycle 1.

## Impact

Behavior change is confined to `MATCHES`. Regexes containing two or more dots previously always threw;
they now evaluate. Regexes with one dot previously recompiled per row; they are now cached. No public
API change - `getCachedValue` / `setCachedValue` already existed on `CommandContext`.
