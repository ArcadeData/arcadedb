# #6359: a REBUILD `WITH` setting is evaluated and validated, where it used to be rendered and coerced

Issue: https://github.com/ArcadeData/arcadedb/issues/6359

## What changed

`REBUILD INDEX` and `REBUILD TYPE` read their `WITH` settings by EVALUATING the setting's expression and
handing the value to one of two shared readers on `DDLStatement`. Both previously read the expression a
different way, and both let bad input through silently.

**Numeric settings** (`batchSize`, `maxAttempts`) go through `parsePositiveIntSetting`. It refuses anything
that is not a whole number of at least one, naming the statement, the setting and the value.

**Boolean settings** (`statsOnly`, `repartition`) go through `parseBooleanSetting`, which accepts a `Boolean`
or the strings `true` / `false` and refuses everything else with a `CommandSQLParsingException`.

## What that means for a statement that used to work

Three shapes behave differently, and all three used to be accepted:

| statement | before | now |
|---|---|---|
| `REBUILD TYPE V WITH batchSize = 1000` | refused, `"got: null"` | runs with a batch size of 1000 |
| `REBUILD INDEX i WITH batchSize = -1` | `NumberFormatException: For input string: "0 - 1"` | refused, naming `batchSize` and `-1` |
| `REBUILD INDEX i WITH statsOnly = yes` | silently read as `false`, so the statement did a FULL rebuild | refused, naming `statsOnly` and `yes` |

The first is a plain fix: `REBUILD TYPE`'s numeric setting read `Expression.value`, which is null for every
numeric literal the parser builds, so it refused every value it was given, legal ones included.

The last is the user-visible one. `Boolean.parseBoolean` answers `false` for anything it does not recognise,
so a typo in a boolean setting did not fail - it silently selected the OTHER behaviour. For `statsOnly` that
is the difference between recomputing index statistics and rebuilding the whole index. A statement carrying
such a typo now reports it instead of quietly doing the more expensive, and different, thing.

## Bound parameters

Evaluating rather than rendering is also what makes a bound parameter work:

```sql
REBUILD INDEX `V[id]` WITH batchSize = :size
```

Rendering an expression answers with the placeholder text, which no amount of parsing turns into an integer.
Evaluation resolves literals and parameters alike, which is how `parseBooleanSetting`'s callers had always
read their values.
