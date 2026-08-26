# MongoDB wire protocol defects: #6745, #6746, #6747, #6748

Four related defects in the MongoDB wire-protocol module (`mongodbw`), all in the same translator/handler layer
(`MongoDBToSqlTranslator`, `MongoDBDatabaseWrapper`, `MongoDBCollectionWrapper`). Fixed together in one branch since
they touch the same three files.

## Issues

- **#6745** - `find`/`update`/`delete` by `ObjectId _id` never matched: the filter bound the raw `ObjectId` object
  (whose `toString()` is `"ObjectId[<hex>]"`), while storage holds the bare lowercase hex string.
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
     `field NOT > :p0`.

## Steps accomplished

1. Read `MongoDBToSqlTranslator.java`, `MongoDBDatabaseWrapper.java`, `MongoDBCollectionWrapper.java` and the
   bundled `de.bwaldvogel.mongo` sources (`MongoQuery`, `QueryParameters`, `ObjectId`) to confirm each defect and how
   `$orderBy` already flows through `MongoDBCollectionWrapper#handleQuery(QueryParameters)` via the legacy wire
   convention.
2. Wrote regression tests first (TDD), confirmed each new test failed against the pre-fix code, then implemented
   the fix and confirmed it passed:
   - `MongoDBToSqlTranslatorParamsTest` (unit, existing file): `$exists:false`/`$exists:true`, `$not`, ObjectId hex
     binding.
   - `MongoDBQueryTest` (unit, existing file): `numberToReturn`-only and `numberToSkip`-only requests via the
     `mongo` query engine.
   - `MongoDBObjectIdFilterTest` (new, wire-level): find/update/delete by auto-generated `ObjectId _id`.
   - `MongoDBSortAndSkipTest` (new, wire-level): skip and sort, filtered and unfiltered, ascending/descending,
     combined for pagination.
   - `MongoDBExistsOperatorTest` (new, wire-level): `$exists` true/false over a real Mongo client.
3. Implemented the fixes:
   - `MongoDBToSqlTranslator.buildExpression` (`$exists`): emit `IS DEFINED` / `IS NOT DEFINED` based on
     `Utils.isTrue(value)`.
   - `MongoDBToSqlTranslator.buildAnd`: special-case `$not` so the field is re-emitted inside a `NOT (...)` clause,
     since the field name is only available in `buildAnd`'s scope, not `buildExpression`'s.
   - `MongoDBToSqlTranslator.buildValue`: convert an `ObjectId` to its hex string (`getHexData()`) before binding,
     matching the hex string used on the storage path.
   - `MongoDBToSqlTranslator.fillResultSet`: consume the iterator element before deciding to skip it.
   - `MongoDBDatabaseWrapper.query`: read `numberToReturn` under its own key.
   - `MongoDBDatabaseWrapper.find`: read the command's `sort` field and thread it through as `$orderBy` in the
     query payload (the only channel `MongoDBCollectionWrapper#handleQuery(QueryParameters)` reads an order-by
     from).
   - `MongoDBCollectionWrapper.queryDocuments`: route to the SQL-building branch whenever a filter OR an order-by
     is present (previously gated on the filter alone), and only append `WHERE ...` when a filter actually exists.
4. Ran the full `mongodbw` reactor test suite (`mvn -o -pl mongodbw -am test`) to confirm no regressions.

## Test results

See PR for CI status; local run: `mvn -o -pl mongodbw -am test` - all tests pass (new + pre-existing).

## Impact analysis

All four fixes are localized to the `mongodbw` module's translator/handler layer. No public API changes. The
`ObjectId` binding fix and the `$not` fix both flow through `MongoDBToSqlTranslator.buildValue`/`buildAnd`, which
are shared by `find`, `update`, and `delete`, so all three filter paths benefit from the same fix.

## Recommendations

None outstanding; all four issues are addressed by this PR.
