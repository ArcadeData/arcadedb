# Issue #4397 - MatchesCondition caches regex Pattern by regex.hashCode() collision

## Problem

`com.arcadedb.query.sql.parser.MatchesCondition.matches()` cached the compiled
`Pattern` in the command context under the key `"MATCHES_" + regex.hashCode()`.
Two distinct regex strings can share the same `String.hashCode()` (a hash
collision). When that happens within a single query/command context, the second
`MATCHES` evaluation retrieves the first regex's compiled `Pattern`, silently
returning wrong rows.

Example colliding pair: `"Aa.*"` and `"BB.*"` both hash to `2031100`.

## Root cause

`MatchesCondition.java:70`

```java
final String key = "MATCHES_" + regex.hashCode();
```

`hashCode()` is not collision-free, so it is unsafe as a cache key when the
regex value varies (parameterised or expression-derived regexes evaluated
against the same `CommandContext`).

## Fix

Use the regex string itself as the cache key. The context variable map is keyed
by `String`, so distinct regexes get distinct entries and there is no collision.

```java
final String key = "MATCHES_" + regex;
```

## Affected component

- `engine` module
- `com.arcadedb.query.sql.parser.MatchesCondition`

## Verification

- New regression test `MatchesConditionTest` reproduces the collision through
  SQL (per-row expression-derived regex) and asserts each row matches with its
  own pattern.
- Compile the `engine` module and run the new test plus existing parser tests.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/4403

## Review cycles

- **Cycle 1** - HEAD `1fbad3b9`. Gemini: COMMENTED, no actionable items. Claude:
  flagged the misleading test name `collidingRegexesWithLiteralMatches`, the
  multi-line class Javadoc, and the tracking docs file.
  - Applied: renamed the test to `literalMatchesReturnCorrectRows`; trimmed the
    class Javadoc to one line.
  - Skipped: removing this tracking doc - it is a mandated workflow artifact and
    the repo already contains many such per-issue docs.
- **Cycle 2** - HEAD `f6a46cdd`. Claude: confirmed fix and tests correct; only
  actionable item was to remove the internal `review-deferred-*.md` workflow
  metadata file. Applied: removed it (its rationale is captured here instead).
  Gemini did not re-review (known inconsistent re-review behavior).

## Final state

clean-approval - all actionable bot feedback addressed; the remaining bot notes
were non-actionable or non-applicable workflow-metadata items.
