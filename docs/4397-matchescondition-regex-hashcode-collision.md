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
