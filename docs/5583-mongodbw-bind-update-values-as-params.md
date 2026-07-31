# 5583 - mongodbw: bind $set and replacement values as parameters

Follow-up to #5579 / PR #5581. After #5581 every **filter** value is a bound SQL parameter, but the **update**
values (`$set` and full replacement) were still spelled into the statement as a JSON literal, relying on
`JSONObject.toString()` escaping. This closes that last gap so the "unreachable by construction" property covers
the whole statement.

## The open question the issue posed: can the grammar bind there?

**Yes.** The issue flagged this as needing checking first, and possibly being the bulk of the work. It is not -
the engine already supports both forms.

`GlobalConfiguration` line 474 records that the JavaCC parser is dead: *"Deprecated, has no effect. The
ANTLR4-based SQL parser is always used."* `StatementCache` only ever calls `SQLAntlrParser.parse`. So the
authority is `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4`:

```
    | MERGE expression
    | CONTENT (json | jsonArray | inputParameter)
```

- `MERGE expression` accepts `:p0` as an ordinary expression. `SQLASTBuilder.visitUpdateOperation` recognises
  that the expression is not a JSON literal and stores it in `UpdateOperations.expression`;
  `UpdateExecutionPlanner` chains `UpdateMergeStep(expression)`, whose `resolveExpression` resolves the value and
  accepts a `Map` with String keys directly (`checkStringKeys`).
- `CONTENT ... inputParameter` is an explicit grammar alternative. `SQLASTBuilder` stores it in
  `UpdateOperations.inputParam` and `UpdateContentStep` handles a `Map` value via `doc.fromMap(...)`.

Nothing in the engine had to change. Note the JavaCC `SqlParser.java` production still only accepts `Json()`
after MERGE/CONTENT, but that parser is unreachable, so it is left alone rather than hand-edited (there is no
javacc plugin in the build; the generated file is checked in).

## Change

`MongoDBDatabaseWrapper`:

- `documentToJson` / `toJsonValue` (BSON -> `JSONObject` / `JSONArray`) become `documentToMap` / `toMapValue`
  (BSON -> `LinkedHashMap` / `ArrayList`). The nested `Document` and `List` recursion and the `ObjectId` ->
  hex-string conversion are preserved exactly; only the container type changes. Insertion order is kept with
  `LinkedHashMap` so the replacement document reaches the record in wire order.
- `appendUpdateOperations` binds instead of spelling:

  ```java
  sql.append(" CONTENT ");                            // was: .append(documentToJson(u))
  MongoDBToSqlTranslator.buildValue(sql, params, documentToMap(u));

  case "$set" -> {                                    // was: .append(documentToJson(operand))
    sql.append(" MERGE ");
    MongoDBToSqlTranslator.buildValue(sql, params, documentToMap(operand));
  }
  ```

  It becomes package-private `static` (it never used instance state) so a unit test in `com.arcadedb.mongo` can
  read the generated SQL directly, the same way `MongoDBToSqlTranslatorParamsTest` drives the `protected static`
  translator builders.

Unchanged, deliberately: `$unset` and `$inc` **field names** stay on `quoteFieldPath()`. SQL cannot bind a
property name, so that half of #5575 is untouched, same as in #5581. `$inc` already binds its operand.

`JSONObject` / `JSONArray` stay imported: `handleQuery` and `json2Document` still use them on an unrelated path.

## Side effect: value fidelity

Same class of latent defect #5581 uncovered on the filter path. Going through `JSONObject` meant a value was
reshaped by JSON serialisation before it reached the record. Binding hands the engine the original Java object:

- `java.util.Date` was serialised by `JSONObject`'s date handling; it now arrives as a `Date`.
- a large `Double` was written in scientific notation; it now arrives as a `Double`.

Neither was a *security* hole - `JSONObject` escapes correctly, and #5581 added two round-trip tests proving it -
but both were fidelity losses, and both are now gone.

## Verification

New unit tests, `MongoDBUpdateValueBindingTest` (no server, reads the generated SQL directly):

| test | pins |
|---|---|
| `aSetOperandIsBoundRatherThanSpelledAsJson` | `MERGE :p0`, operand in the param map |
| `aReplacementDocumentIsBoundRatherThanSpelledAsJson` | `CONTENT :p0`, document in the param map |
| `aQuoteBearingSetValueNeverAppearsInTheStatementText` | payload text absent from the SQL |
| `aQuoteBearingReplacementValueNeverAppearsInTheStatementText` | same for the replacement path |
| `aNestedDocumentIsBoundAsANestedMap` | `Document` -> `Map` recursion survives |
| `aListValueIsBoundAsAList` | `List` recursion survives, nested `Document`s inside it too |
| `anObjectIdIsBoundAsItsHexString` | the `ObjectId` -> hex conversion is preserved |
| `aDateIsBoundAsADateInsteadOfBeingSerialised` | fidelity, was reshaped by `JSONObject` |
| `updateValuesAndFilterValuesShareOneParameterNumbering` | `MERGE :p0 ... WHERE ... :p1`, no collision |
| `unsetStillSpellsFieldNamesAsQuotedIdentifiers` | the deliberate #5575 carve-out is unchanged |
| `incStillBindsItsOperandAndQuotesItsFieldName` | #5581 behaviour unchanged |
| `aCraftedFieldNameInASetOperandCannotBreakOutOfTheStatement` | field names inside the bound map are data now |

New wire-level tests added to `MongoDBParameterBindingTest` (real server through the Mongo driver):

| test | pins |
|---|---|
| `aNestedDocumentSurvivesBeingSetByAnUpdate` | nested map round-trips through the bound parameter |
| `anArraySurvivesBeingSetByAnUpdate` | list round-trips |
| `aNestedDocumentSurvivesAFullReplacement` | same on the CONTENT path |

The two pre-existing round-trip tests the issue named (`aQuoteBearingValueSurvivesBeingSetByAnUpdate`,
`aQuoteBearingValueSurvivesAFullReplacement`) are unchanged and must stay green: they are the observable contract
and are indifferent to which mechanism provides it.

### Proving the tests can fail

Run in this order deliberately, so the red was recorded against the *old* implementation rather than assumed.
`appendUpdateOperations` was first made package-private `static` with its body still on `documentToJson`, and the
new unit test class was run against it:

```
Tests run: 12, Failures: 6, Errors: 4, Skipped: 0
```

10 of 12 red. The 2 that passed are `unsetStillSpellsFieldNamesAsQuotedIdentifiers` and
`incStillBindsItsOperandAndQuotesItsFieldName`, which is correct: they pin behaviour this change deliberately
leaves alone, so they must be green on both sides.

The binding was applied only after that run.

## Test results

Whole module, after the change:

```
mvn -pl mongodbw verify
Tests run: 67, Failures: 0, Errors: 0, Skipped: 0
```

67 up from 52 (+12 unit, +3 wire). No engine module was touched, so nothing outside `mongodbw` is in scope.

The wire-level tests matter more than usual here: they are what proves the engine actually accepts `MERGE :p0`
and `CONTENT :p0` at runtime, not just that the grammar file contains the alternatives.

## Note on the two existing tests' comments

`aQuoteBearingValueSurvivesBeingSetByAnUpdate` and `aQuoteBearingValueSurvivesAFullReplacement` are unchanged in
behaviour and assertions. Their **comments** were updated: each described the value as travelling "as a JSON
literal", which this change makes false. Leaving a comment asserting the opposite of what the code does is worse
than the edit. Nothing was weakened - the assertions still pin the same observable contract.

## Follow-ups not taken

- **Dotted `$set` keys do not navigate.** `{"$set": {"address.city": "Rome"}}` reaches the record as a literal
  property named `address.city` rather than setting `city` inside `address`. `documentToMap` keeps the key
  verbatim and `UpdateMergeStep.handleMerge` calls `doc.set(key, value)` per entry, so the behaviour is
  byte-identical to the old JSON-literal path (`MERGE {"address.city": "Rome"}` produced the same map). Not a
  regression and out of scope here, but worth its own issue if MongoDB dotted-path semantics are expected. Note
  this is only the **update** side: on the filter side `quoteFieldPath()` does split the path per segment, so the
  two halves disagree.
- `{field: null}` binds `field = :p0` with null, which does not match missing fields the way MongoDB does.
  Carried over from #5581, unfiled, not a regression.
- `executeUpsert` / `applyOperatorsToDocument` build a record through the API rather than through SQL, so they
  never had this exposure and are untouched. They do, however, call `record.set(key, rawBsonValue)` with no
  conversion, so unlike the update path they never turn a nested `ObjectId` into hex: the same `$set` stores a
  different representation depending on whether the row already existed. Pre-existing, worth its own issue.

## Review cycles

### Cycle 1 - `6190293b3`

`claude[bot]`: **LGTM**, no blocking issues. It independently traced the grammar/engine path and confirmed the
binding, the merge semantics, the parameter numbering, and the recursion/fidelity claims. Three non-blocking
notes:

1. `documentToMap` sized its map with `new LinkedHashMap<>(doc.size())`, which is a *capacity*, so with the
   default 0.75 load factor it rehashes once a document has ~4 or more fields. **Applied**, using the JDK 19
   factory `LinkedHashMap.newLinkedHashMap(doc.size())` rather than the suggested
   `(int) (doc.size() / 0.75f) + 1`: same sizing, no arithmetic in the source, and it matches the existing
   `HashMap.newHashMap(size)` at `QuerySession:83`.
2. Dotted `$set` keys - recorded above as a follow-up, not changed here.
3. `{field: null}` - already recorded, carried over from #5581.

### Cycle 2 - `5515db035`

`claude[bot]`: **LGTM**, no blocking issues. Three non-blocking observations:

1. *`buildValue` derives placeholder names from `params.size()`, so a future caller that pre-seeds the map would
   get a silent collision; a threaded counter would make the invariant structural.* **Not applied.** This is the
   same suggestion the cycle-4 reviewer made on #5581, declined then for the same reasons and explicitly marked
   "not for this PR" here too: all four call sites pass a fresh empty map, the contract is documented on
   `buildValue`, and changing its signature would touch every filter-path caller for a hazard that does not
   exist today. It belongs with `buildValue`, not with this change.
2. *No test covered a combined `$set` + `$inc` update.* **Applied** - a real coverage gap. The grammar takes
   `updateOperation+`, so the two chain into `MERGE :p0 SET \`count\` += :p1`, and the concern worth settling was
   whether the bound payload of `MERGE expression` would swallow the `SET` keyword that opens the next clause.
   It does not. Added `aCombinedSetAndIncUpdateChainsTwoOperationsOverOneParameterMap` (unit, pins the exact
   statement text and both placeholders) and `aCombinedSetAndIncUpdateAppliesBothOperations` (wire, seeds
   `count: 1` and asserts `count == 4` with a quote-bearing `note` intact). Both pass, so this was a missing
   test rather than a defect.
3. Dotted `$set` keys - agreed out of scope, already disclosed above.

Module total after cycle 2: **69 green** (+2).

### Cycle 3 - `4f3b0891a`

`claude[bot]`: **LGTM**, no blocking issues. It additionally verified that `quoteFieldPath()` ->
`Identifier.quote()` escapes both backtick and backslash, so the deliberately-unbindable field-name half is safe
rather than merely out of scope. Two non-blocking observations:

1. *`appendUpdateOperations` is now an independently-callable seam, so it should restate `buildValue`'s
   "params must start empty" precondition in its own Javadoc.* **Applied.** Previously the invariant was
   guaranteed only by the single caller and pinned by one test. Stated as an invariant with no issue references,
   per repo convention.
2. Dotted `$set` keys deserve their own issue - agreed, and left for the developer to file rather than opened
   unilaterally. Recorded under "Follow-ups not taken" above.

### Cycle 4 - `53b07cb6e` - found a server-crash bug

`claude[bot]`: **LGTM**, no blocking issues, three non-blocking notes. The first one turned out to matter a great
deal.

1. *Date fidelity is only unit-tested; a wire-level round trip would guard the stated behaviour change.*
   **Applied - and it failed.** Writing that test exposed a crash, described in full below.
2. *The empty-`params` contract is documented but not enforced; a cheap `assert` would make a future
   silent-overwrite loud.* **Applied** as `assert params.isEmpty()` at the top of `appendUpdateOperations`.
   `assert` is already used in this file (line 425), so this matches local convention.
3. *`executeUpsert` -> `applyOperatorsToDocument` does not apply the `ObjectId` -> hex conversion that
   `toMapValue` does, so a nested `ObjectId` lands differently depending on whether the row already exists.*
   **Verified correct** - that path calls `record.set(key, rawBsonValue)` with no conversion. Pre-existing and
   unrelated to binding; recorded as a follow-up, not changed.

#### The crash

`aDateSurvivesBeingSetByAnUpdate` failed, and not on the update - on the **read-back**:

```
SEVER [MongoWireMessageEncoder] Failed to encode {...}
java.lang.IllegalArgumentException: Unknown type: class java.time.LocalDateTime
    at de.bwaldvogel.mongo.wire.bson.BsonEncoder.determineType(BsonEncoder.java:204)
```

The write succeeded; encoding the stored value into the wire response threw, which killed the connection and
surfaced client-side as `MongoSocketReadException: Prematurely reached end of stream`.

**This is a pre-existing defect, not one this change introduced.** Confirmed with a throwaway probe on the
**insert** path, which this PR does not touch: `insertOne(new Document("when", date))` followed by `find` fails
identically. `MongoDBToSqlTranslator.convertDocumentToMongoDB` passed every stored value straight through, and a
temporal property is held as a `java.time` value, which the encoder rejects. So **any** date written through
mongodbw and read back killed the connection.

**But binding made it reachable on a path where it previously was not.** Before this change a `$set` date went
through `JSONObject` and was stored as a *string*, which encodes fine. After it, the value is stored as a real
`LocalDateTime`. So without a fix, this PR would have converted a lossy-but-working path into a crashing one -
and the unit test could never have caught it, because the failure is in the response encoder.

#### The fix

`convertDocumentToMongoDB`'s two identical overloads now share one `convertMapToMongoDB`, and values pass through
`toBsonValue`, which maps temporal types onto the single temporal type `BsonEncoder` accepts. Inspecting the
encoder's bytecode shows it handles `java.time.Instant` and **not** `java.util.Date` - the first attempt
converted to `Date` and failed the same way with `Unknown type: class java.util.Date`, so this was determined
empirically rather than assumed. `LocalDateTime`/`LocalDate` are anchored at `ZoneOffset.UTC`, matching what
`DateUtils` uses on the way in, so the round trip is exact. `toBsonValue` also recurses into embedded documents
and lists, since a date can sit inside either.

Four wire tests cover it: the `$set` path, the insert path (independent of this PR's change), a date nested in a
sub-document, and the large-`Double` case from the same fidelity claim.

## Final state

Four review cycles, all `LGTM` with no blocking issues at any point. Of the ten non-blocking notes: four applied
(map presizing, precondition Javadoc, `assert params.isEmpty()`, wire-level fidelity tests), one was a coverage
gap that was filled (combined `$set` + `$inc`), one led to the temporal-encoding fix above, one was declined with
reasoning (`params.size()` counter), and three were pre-existing gaps recorded as follow-ups.

Module total: **73 green**, up from 52.

### Needs the developer's attention before merge

The cycle-4 temporal-encoding fix (`MongoDBToSqlTranslator.convertMapToMongoDB` / `toBsonValue`) **has not been
through a bot review cycle** - the 4-cycle limit was reached when it was written. It is also the one part of this
PR that goes beyond the issue's scope. It is here because leaving it out would have shipped a regression, but it
is genuinely a separate bug fix and a reasonable thing to split into its own PR.
