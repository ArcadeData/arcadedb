# 5579 - mongodbw: bind filter values as parameters instead of escaping them

## Symptom

#5575 closed a SQL injection in the MongoDB wire protocol by escaping the field names and values that get embedded
into the translated statement. Escaping is correct, but it is per-call-site: every future edit to
`MongoDBToSqlTranslator` has to remember it, and forgetting is silent.

## Root cause

`MongoDBToSqlTranslator` builds one SQL string and the caller executes it with no parameters, so every value taken
off the wire is spelled into the statement text and its safety depends on `buildValue` escaping it correctly.

Inlining hid a second defect. `buildValue`'s `else` branch spelled any non-`String` with `StringBuilder.append`,
which is `String.valueOf`:

- an `ObjectId` filter value emitted `ObjectId[...]`, which is not valid SQL
- a `java.util.Date` emitted its `toString()`, which is not a SQL literal either
- a `Double` could emit scientific notation
- an empty `$in: []` emitted `IN ()`, which is not valid SQL

There are **three** entry points into the translator, not the two the issue lists:

| Entry point | Command | Statement |
| --- | --- | --- |
| `MongoDBDatabaseWrapper.appendWhere` via `deleteDocuments` | `delete` | `DELETE FROM ...` |
| `MongoDBDatabaseWrapper.appendWhere` via `executeUpdate` | `update` | `UPDATE ...` |
| `MongoDBCollectionWrapper.queryDocuments` | `find` | `SELECT FROM ...` |

The issue says `find` does not reach the translator. That is only true of `countDocuments`, which is served by
`MongoDBCollectionWrapper.count`. `find` reaches `MongoDBDatabaseWrapper.find` -> `handleQuery(MongoQuery)` ->
`MongoDBCollectionWrapper.handleQuery(QueryParameters)` -> `queryDocuments`, which builds its own `SELECT`. The
read path was in scope too.

## Fix

`buildValue` no longer spells a value. It registers it in a `Map<String, Object>` under `p<n>` (where `n` is the
map's current size, so names are unique and assigned in the order values are met) and appends `:p<n>`. The map is
threaded through `buildExpression` (both overloads), `buildAnd`, `buildOr`, `buildCollection`, `appendWhere`,
`appendUpdateOperations` and the three call sites, and handed to `database.command` / `database.query`.

`$in` / `$nin` bind the whole collection to a single parameter: the grammar accepts an input parameter inside the
`IN` parentheses (`x IN (:p0)`), so there is no need to emit one placeholder per element. This is also what makes
the empty-collection case work, where the old `IN ()` did not.

**Identifiers still go through `Identifier.quote()` / `quoteFieldPath()`.** SQL cannot bind a type or property
name, so that half of #5575 is unchanged, and `quoteFieldPath` still splits on `.` and quotes each segment so
MongoDB navigation into an embedded document keeps resolving.

Three smaller repairs in the same code:

- `queryDocuments` was concatenating the collection name and the `orderBy` field names raw; both now use the same
  quoting helpers. On reachability, to be precise: `orderBy` comes from `queryObject.remove("$orderBy")`, and the
  modern `find` command never populates that key, so the branch is reached only through the legacy `OP_QUERY`
  wrapper or a `mongo`-language query via `MongoQueryEngine`. It is untrusted query text either way and quoting it
  is correct, but it is not a straightforwardly remote-attacker-controlled path.
- an empty `$orderBy` left a dangling `order by` with nothing to sort on, which does not parse. Guarded.
- the `$nin` branch threw `"Operator $in was expecting a collection"`, a copy-paste from the `$in` branch above it.

## Verification

`mvn -pl mongodbw verify`: **52 tests, 0 failures**, plus `MongoQueryMetricsIT` run separately (green).

- `MongoDBToSqlTranslatorParamsTest` (new, no server, 14 cases) calls the `protected static` builders directly and
  asserts the SQL carries placeholders, the values never appear in the text, and the map holds the raw objects.
  Covers equality, comparison operators, `$in`/`$nin`, `$or`, `$and`, `$size`, nested documents, dotted paths,
  null, and non-`String` types.
- `MongoDBParameterBindingTest` (new, wire level, 13 cases) drives a real server through the Mongo driver:
  quote-, backslash- and number-bearing values through `find`, `updateMany` and `deleteMany`; `$in` on both the
  find and update paths; `$nin` positive match; empty `$in` / `$nin`; and `$set` / `replaceOne` values carrying
  `'`, `"` and `\`. Every case pins a positive match, so none can pass merely by matching nothing.
- `MongoDBSqlInjectionTest` (the #5575 regression suite) stays green **unchanged**.

The new unit tests were proven to fail before being trusted: reverting only `buildValue` to the previous escaping
form turns all 14 `MongoDBToSqlTranslatorParamsTest` cases red. The change was then restored and re-run green.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5581

## Follow-ups

- **#5583** - `$set` and full-replacement values still travel as an inlined JSON literal (` MERGE <json>` /
  ` CONTENT <json>`) rather than bound parameters. That path is safe because `JSONObject` does its own escaping,
  and this PR added tests proving it rather than assuming it, but the "unreachable by construction" property
  therefore covers the `WHERE` clause and `$inc`, not the update values. Binding them needs `MERGE`/`CONTENT` to
  accept a parameter in place of a JSON literal, which must be checked against the grammar first; if it cannot,
  closing #5583 as "escaping is the mechanism here" is a legitimate recorded decision.
- **Unfiled:** `{field: null}` binds `field = :p0` with a null, which does not match missing fields the way
  MongoDB does. This matches the prior `field = null` behavior, so it is a pre-existing semantic gap rather than a
  regression from this change, but it is still a gap.

## Impact

Removes the injection class from the `WHERE` clause of every `find`, `update` and `delete` the MongoDB wire
protocol translates, rather than relying on each call site remembering to escape. Fixes `ObjectId`, `Date`, large
`Double` and empty-`$in` filter values, which previously produced statements the SQL parser rejects or
misinterprets. Because the statement text is now stable across calls that differ only in their values, the SQL
statement cache gets a hit where every distinct filter used to compile a fresh statement.
