# MongoDB wire protocol defects: #6745, #6746, #6747, #6748

Four related defects in the MongoDB wire-protocol module (`mongodbw`), all in the same translator/handler layer
(`MongoDBToSqlTranslator`, `MongoDBDatabaseWrapper`, `MongoDBCollectionWrapper`). Fixed together in one branch since
they touch the same three files.

## Issues

- **#6745** - `find`/`update`/`delete` by `ObjectId _id` never matched: the filter bound the raw `ObjectId` object
  (whose `toString()` is `"ObjectId[<hex>]"`), while storage holds the bare lowercase hex string. The same gap
  existed for `$in`/`$nin` predicates, since `buildCollection` bound each element unchanged.
- **#6746** - `find().skip(n)` was silently ignored: the skip loop `continue`d before advancing the iterator, so it
  burned loop-counter iterations without consuming any documents.
- **#6747** - `find().sort(...)` was silently ignored: the `find` command handler never read the command's `sort`
  field, and even once wired through, a sort-only (no-filter) query hit the unconditional `SCAN` branch, which
  applies no ordering.
- **#6748** - three smaller defects:
  1. `$exists: false` was translated identically to `$exists: true` (`IS DEFINED` regardless of the boolean operand).
  2. The HTTP `mongo` query engine read `numberToReturn` gated on the presence of the `numberToSkip` key, so a
     `numberToReturn`-only request silently dropped the limit, and a `numberToSkip`-only request threw
     `JSONException` on the missing key.
  3. `$not` recursed into its operand without re-emitting the field it applies to, producing invalid SQL such as
     `field NOT > :p0`. A field-scoped `$not` with a multi-operator operand (e.g. a range) needed the field
     re-emitted per operator, AND-joined; a nested or empty `$not` operand is rejected explicitly rather than
     producing malformed SQL.

## Fix summary

- `MongoDBToSqlTranslator.buildExpression` (`$exists`): emit `IS DEFINED` / `IS NOT DEFINED` based on
  `Utils.isTrue(value)`.
- `MongoDBToSqlTranslator.buildAnd`: special-case `$not` so the field is re-emitted (once per operator, AND-joined)
  inside a `NOT (...)` clause; reject a nested or empty `$not` operand explicitly.
- `MongoDBToSqlTranslator.buildValue`/`buildCollection`: convert an `ObjectId` to its hex string
  (`ObjectId#getHexData()`) before binding - as a scalar, and per-element for `$in`/`$nin` - matching the hex string
  used on the storage path. The three previously-independent hex conversions (write path, `$set` payload path, this
  one) are now all `getHexData()`.
- `MongoDBToSqlTranslator.fillResultSet`: consume the iterator element before deciding to skip it.
- `MongoDBDatabaseWrapper.query`: read `numberToSkip`/`numberToReturn` via `JSONObject`'s default-value getter,
  each under its own key.
- `MongoDBDatabaseWrapper.find`: read the command's `sort` field and thread it through as `$orderBy` in the query
  payload (the only channel `MongoDBCollectionWrapper#handleQuery(QueryParameters)` reads an order-by from).
- `MongoDBCollectionWrapper.queryDocuments`: route to the SQL-building branch whenever a filter OR an order-by is
  present (previously gated on the filter alone), and only append `WHERE ...` when a filter actually exists.

Verified with TDD throughout: every new test was confirmed to fail against the pre-fix code before the corresponding
fix was written.

## Test coverage

- `MongoDBToSqlTranslatorParamsTest` (unit): `$exists` true/false, field-scoped `$not` (single-operator,
  multi-operator, nested, empty), top-level `$not`, ObjectId hex binding (scalar and `$in`/`$nin`).
- `MongoDBQueryTest` (unit): `numberToReturn`-only and `numberToSkip`-only requests via the `mongo` query engine.
- `MongoDBObjectIdFilterTest`, `MongoDBSortAndSkipTest`, `MongoDBExistsOperatorTest` (new, wire-level): find/update/
  delete by `ObjectId _id` (including `$in`), skip/sort (filtered/unfiltered, combined for pagination), `$exists` -
  all exercised through a real `MongoClient`, not just the internal translator API.

Local run: `mvn -o -pl mongodbw test` - full suite passes (new + pre-existing).

## Impact analysis

All four fixes are localized to the `mongodbw` module's translator/handler layer. No public API changes. The
`ObjectId` binding fix and the `$not` fix both flow through `MongoDBToSqlTranslator.buildValue`/`buildCollection`/
`buildAnd`, which are shared by `find`, `update`, and `delete`, so all three filter paths benefit from the same fix.

## Recommendations / known follow-ups (out of scope for this PR)

- `queryDocuments`'s SQL branch has no `LIMIT`/`OFFSET` pushed into the query; `numberToSkip`/`numberToReturn` are
  still applied in Java after the full `ResultSet` is materialized. Pre-existing, not introduced here.
- `buildExpression(Document)` does not insert `AND` between top-level sibling fields the way `buildAnd` does, so an
  implicit multi-field filter without an explicit `$and` (e.g. `{a: 1, b: 2}`) would produce two adjacent
  parenthesized clauses with no boolean operator between them. Not exercised by any existing test (all use `$and`
  explicitly); unrelated to this PR's diff.
