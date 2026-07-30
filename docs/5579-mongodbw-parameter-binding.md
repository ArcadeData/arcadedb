# 5579 - mongodbw: bind filter values as parameters instead of escaping them

Follow-up to #5575. That PR closed the injection by escaping values into the statement text. Escaping is correct
but per-call-site: every future edit to `MongoDBToSqlTranslator` has to remember it, and forgetting is silent.
Binding removes the class of bug.

## Analysis

### Call sites that reach the translator

`MongoDBToSqlTranslator.buildExpression` has three entry points, not two:

| Entry point | Command | Statement |
| --- | --- | --- |
| `MongoDBDatabaseWrapper.appendWhere` via `deleteDocuments` | `delete` | `DELETE FROM ...` |
| `MongoDBDatabaseWrapper.appendWhere` via `executeUpdate` | `update` | `UPDATE ...` |
| `MongoDBCollectionWrapper.queryDocuments` | `find` | `SELECT FROM ...` |

The issue text says `find` does not go through the translator. That is only true of `countDocuments`, which is
served by `MongoDBCollectionWrapper.count`. `find` reaches `MongoDBDatabaseWrapper.find` ->
`handleQuery(MongoQuery)` -> `MongoDBCollectionWrapper.handleQuery(QueryParameters)` -> `queryDocuments`, which
builds its own `SELECT` and calls `buildExpression`. So the read path is in scope too.

### Defects the inlining hid

Beyond the injection surface, `buildValue`'s `else` branch spelled any non-`String` with `StringBuilder.append`,
which is `String.valueOf`:

- an `ObjectId` filter value emitted `ObjectId[...]`, which is not valid SQL
- a `java.util.Date` emitted its `toString()`, which is not a SQL literal either
- a `Double` could emit scientific notation

Binding keeps the Java value, so all three now compare against what the client actually sent.

## Change

`buildValue` no longer spells a value. It registers it in a `Map<String, Object>` under `p<n>` (where `n` is the
map's current size, so names are unique and sequential) and appends `:p<n>`. The map is threaded through
`buildExpression` (both overloads), `buildAnd`, `buildOr`, `buildCollection`, `appendWhere`,
`appendUpdateOperations` and the three call sites, and handed to `database.command` / `database.query`.

`$in` / `$nin` bind the whole collection to a single parameter: the SQL grammar accepts an input parameter
inside the `IN` parentheses (`x IN (:p0)`), so there is no need to emit one placeholder per element.

**Identifiers still go through `Identifier.quote()` / `quoteFieldPath()`.** SQL cannot bind a type or property
name, so that half of #5575 is unchanged. `quoteFieldPath` still splits on `.` and quotes each segment so
MongoDB navigation into an embedded document keeps resolving.

Two identifiers in `queryDocuments` were still being concatenated raw and are now quoted with the same helpers:
the collection name and the `orderBy` field names. The `orderBy` key is attacker-controlled text on the `find`
path with no validation in front of it, so leaving it raw next to a bound value would have left the same hole
open one line away.

## Verification

- `MongoDBToSqlTranslatorParamsTest` (new, no server): calls the `protected static` builders directly and asserts
  the generated SQL carries placeholders, the values never appear in the text, and the map holds the raw objects.
  Covers equality, comparison operators, `$in`/`$nin`, `$or`, `$and`, nested documents and dotted paths.
- `MongoDBParameterBindingTest` (new, wire level): drives a real server through the Mongo driver and asserts
  quote-bearing, backslash-bearing and non-string values round-trip through `find`, `updateMany` and `deleteMany`.
- `MongoDBSqlInjectionTest` must stay green unchanged - it is the #5575 regression suite.
- Full `mongodbw` module suite for regressions.

## Results

`mvn -pl mongodbw verify`: **45 tests, 0 failures**, plus `MongoQueryMetricsIT` run separately (1 test, green).
That covers `MongoDBSqlInjectionTest` (4), `MongoDBUpdateDeleteTest` (8) and `MongoDBFindTest` (3) unchanged, so
the #5575 contract and the ordinary read/write paths still hold.

The new tests were proven to fail before being trusted: reverting only `buildValue` to the previous escaping form
turns all 14 `MongoDBToSqlTranslatorParamsTest` cases red. The change was then restored and re-run green.

### Notes for review

- `buildValue` derives the placeholder name from `params.size()`, so it needs no separate counter and names come
  out in the order the values are met. Every builder shares one map per statement.
- Parameterising also makes the statement text stable across calls that differ only in their values, so the SQL
  statement cache now gets a hit where every filter used to compile a fresh statement.
- `$set` still goes out as ` MERGE <json>` built with `JSONObject`, which does its own escaping. That is a
  different mechanism from the `WHERE` clause and was left alone.
- `$inc` binds its operand but keeps the `(Number)` cast, so a non-numeric operand still fails the same way.
