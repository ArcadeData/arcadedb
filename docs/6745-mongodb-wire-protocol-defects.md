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

See PR for CI status; local run: `mvn -o -pl mongodbw test` - all 106 tests pass (new + pre-existing).

## PR #6767

https://github.com/ArcadeData/arcadedb/pull/6767

### Review cycle 1

Head SHA `fcc0e31f87662bfce9a65d077612f1f782d84a99`. `claude` and `coderabbitai` both reviewed. Two real bugs
surfaced, both fixed and covered by new regression tests, plus two nitpicks applied:

- **Fixed (claude + coderabbitai, actionable):** field-scoped `$not` with a multi-operator operand (e.g. a range,
  `{field: {$not: {$gt: 1, $lt: 5}}}`) produced invalid SQL (``NOT (`field` > :p0 < :p1)``, missing `AND` and a
  dangling comparison). `buildAnd`'s `$not` branch now re-emits the field for each operator in the operand,
  AND-joined, matching the outer loop's behavior across sibling fields.
- **Fixed (coderabbitai, actionable):** `buildCollection` (backing `$in`/`$nin`) bound the whole collection as one
  parameter without normalizing elements, so an `ObjectId` inside an `$in`/`$nin` list kept comparing as
  `toString()` instead of the stored hex string even after the scalar (equality) ObjectId fix. Each collection
  element is now normalized the same way `buildValue` normalizes a scalar.
- **Fixed (claude + coderabbitai, nitpick):** `numberToReturn`/`numberToSkip` now use `JSONObject`'s default-value
  `getInt(name, 0)` getter instead of a `has()` check, matching the project's stated convention.
- **Fixed (claude, nitpick):** deduplicated three independent ObjectId-to-hex conversions
  (`MongoDBCollectionWrapper#insertDocuments`'s manual loop and `MongoDBDatabaseWrapper#toHexString`) down to
  `ObjectId#getHexData()`, the same conversion `buildValue` already uses.
- **Not actioned:** a pre-existing note that skip/limit are applied in Java after the SQL `ResultSet` is produced
  rather than pushed into the query - explicitly flagged by the reviewer as out of scope for this PR.

New regression tests: `inNormalizesEachObjectIdElementToItsHexString`, `ninNormalizesEachObjectIdElementToItsHexString`,
`notWithAMultiOperatorOperandJoinsEachComparisonWithItsOwnField` (unit), `findManyByIdUsingInFilterMatchesEveryListedId`
(wire-level). Full `mongodbw` suite (106 tests) passes after the fixes.

### Review cycle 2

Head SHA `a2e6479c851d1111b4fe8396621103afc093c907`. `claude` and `coderabbitai` both reviewed again.

- **Fixed (claude, actionable):** the multi-operator `$not` fix from cycle 1 itself had two unguarded edge cases -
  a nested `$not` operand (e.g. `{field: {$not: {$not: {$gt: 5}}}}`) fell through to the top-level `$not` branch
  with no field in scope and produced invalid SQL, and an empty `$not` operand (`{field: {$not: {}}}`) produced
  `NOT ()`. Neither is a real Mongo query shape, so both are now rejected with an explicit
  `IllegalArgumentException` instead of silently reaching the database as malformed SQL.
- **Fixed (coderabbitai, nitpick):** a markdown lint warning (MD038, nested single backticks) in this tracking
  doc's own cycle-1 section.
- CodeRabbit's `$in`/`$nin` ObjectId-normalization comment from cycle 1 reappeared verbatim on this cycle (stale
  re-post of an already-addressed comment; it says "Addressed in commit a2e6479" in its own body) - no new action.
- **Not actioned (claude, already flagged/deferred):** the skip/limit-applied-in-Java note from cycle 1, repeated
  for visibility; still out of scope for this bug-fix PR per cycle 1's disposition.

New regression tests: `nestedNotIsRejectedRatherThanProducingInvalidSql`, `emptyNotOperandIsRejectedRatherThanProducingInvalidSql`.

## Impact analysis

All four fixes are localized to the `mongodbw` module's translator/handler layer. No public API changes. The
`ObjectId` binding fix and the `$not` fix both flow through `MongoDBToSqlTranslator.buildValue`/`buildAnd`, which
are shared by `find`, `update`, and `delete`, so all three filter paths benefit from the same fix.

## Recommendations

None outstanding; all four issues are addressed by this PR.
