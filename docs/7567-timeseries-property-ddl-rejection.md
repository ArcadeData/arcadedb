# #7567 - CREATE PROPERTY / ALTER PROPERTY CUSTOM role silently drop time-series tag and field values

Issue: https://github.com/ArcadeData/arcadedb/issues/7567

## Finding ledger

- [x] 1. `CREATE PROPERTY <tsType>.<name> <TYPE>` on an existing TIMESERIES type is accepted, the column shows up in
      the schema listing, and every write silently discards its value - **fixed**, refused at DDL time on every
      `createProperty` entry point.
- [x] 2. `ALTER PROPERTY <tsType>.<name> CUSTOM role = "FIELD"` is accepted and changes nothing: the write path never
      reads a property's CUSTOM metadata - **fixed**, refused in `LocalProperty.setCustomValue`, with `= null`
      (removal) still allowed so a database that already stored the key can clean it up.

## Root cause

A TIMESERIES type stores its columns in `LocalTimeSeriesType.tsColumns` (a `List<ColumnDefinition>`), which is
populated exactly once, by `TimeSeriesTypeBuilder.create()` (`CREATE TIMESERIES TYPE`), and re-hydrated from
`schema.json` by `LocalTimeSeriesType.fromJSON()`. The write path iterates that list and nothing else:

```java
// engine/src/main/java/com/arcadedb/query/sql/executor/SaveElementStep.java:213
private void saveToTimeSeries(final LocalTimeSeriesType tsType, final TimeSeriesEngine engine, final Document doc, ...) {
  final List<ColumnDefinition> columns = tsType.getTsColumns();
  ...
  for (int i = 0; i < columns.size(); i++) {
    final ColumnDefinition col = columns.get(i);
    final Object value = doc.get(col.getName());
```

`LocalDocumentType.createProperty()` writes to a completely different map (`properties`), which is what the schema
listing renders. So a property created after the type exists is real as far as the listing is concerned and invisible
to the engine, and the value that arrives under its name is dropped by the loop above without a word.

`ALTER PROPERTY ... CUSTOM role` lands in `LocalProperty.setCustomValue()`, a free-form metadata map. Nothing in the
time-series engine reads it - a column's role lives in `ColumnDefinition.getRole()`, fixed at creation.

`addTsColumn()` has exactly one caller outside JSON restore, so there is no supported way to add a column to a
TIMESERIES type after `CREATE TIMESERIES TYPE`:

```
$ grep -rn "addTsColumn" --include='*.java' . | grep -v '/test/'
engine/src/main/java/com/arcadedb/schema/LocalTimeSeriesType.java:248:  public void addTsColumn(final ColumnDefinition column) {
engine/src/main/java/com/arcadedb/schema/TimeSeriesTypeBuilder.java:142:      type.addTsColumn(col);
```

## Completeness

### 1. Invariant

> On a TIMESERIES type the declared schema properties are exactly the declared time-series columns: no DDL and no
> schema API may add a property the write path cannot store, drop one it does store, or claim to change a column's
> role - and a database or an export written before this rule still opens.

### 2. Every way to violate it

```
$ grep -rn "\.createProperty(\|\.getOrCreateProperty(" --include='*.java' . | grep -v '/src/test/' | wc -l
26
$ grep -rn "\.createProperty(\|\.getOrCreateProperty(" --include='*.java' . | grep -v '/src/test/' | awk -F: '{print $1}' | sort | uniq -c | sort -rn
   6 ./engine/src/main/java/com/arcadedb/query/opencypher/query/OpenCypherQueryEngine.java
   3 ./integration/src/main/java/com/arcadedb/integration/importer/format/XMLImporterFormat.java
   3 ./integration/src/main/java/com/arcadedb/integration/importer/AbstractImporter.java
   2 ./integration/src/main/java/com/arcadedb/integration/importer/OrientDBImporter.java
   2 ./integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java
   2 ./engine/src/main/java/com/arcadedb/schema/LocalSchema.java
   2 ./engine/src/main/java/com/arcadedb/schema/LocalEdgeType.java
   1 ./integration/src/main/java/com/arcadedb/integration/importer/Neo4jImporter.java
   1 ./integration/src/main/java/com/arcadedb/integration/importer/format/RDFImporterFormat.java
   1 ./integration/src/main/java/com/arcadedb/integration/importer/format/JsonlImporterFormat.java
   1 ./integration/src/main/java/com/arcadedb/integration/importer/format/JSONImporterFormat.java
   1 ./engine/src/main/java/com/arcadedb/schema/TimeSeriesTypeBuilder.java
   1 ./engine/src/main/java/com/arcadedb/query/sql/parser/CreatePropertyStatement.java
```

Every one of those reaches `LocalDocumentType.createProperty(String, Type, String)` - the other overloads, the
`JSONObject` default method on `DocumentType` and all six `getOrCreateProperty` overloads delegate to it - so the
funnel is a single method.

```
$ grep -rn "dropProperty(" --include='*.java' . | grep -v '/test/' | grep -v 'RemoteDocumentType\|MutableDocumentType'
engine/src/main/java/com/arcadedb/schema/LocalDocumentType.java:638:      dropProperty(propertyName);      <- getOrCreateProperty, on a type change
engine/src/main/java/com/arcadedb/schema/LocalProperty.java:90:  (comment only)
engine/src/main/java/com/arcadedb/schema/DocumentType.java:181:  Property dropProperty(String propertyName);
engine/src/main/java/com/arcadedb/query/opencypher/query/OpenCypherQueryEngine.java:526: schema.getType(typeName).dropProperty(propName);
engine/src/main/java/com/arcadedb/query/sql/parser/DropPropertyStatement.java:100: sourceClass.dropProperty(propertyName.getStringValue());
```

Sibling shape - who else writes `CUSTOM` metadata a reader never consults:

```
$ grep -rn "setCustomValue(" --include='*.java' . | grep -v '/src/test/' | grep -v '/e2e/'
engine/src/main/java/com/arcadedb/schema/DocumentType.java:160         (JSON restore of a property)
engine/src/main/java/com/arcadedb/schema/LocalProperty.java:291        (the implementation)
engine/src/main/java/com/arcadedb/schema/LocalDocumentType.java:...    (type-level custom, not property-level)
engine/src/main/java/com/arcadedb/query/sql/parser/AlterPropertyStatement.java:76
engine/src/main/java/com/arcadedb/query/sql/parser/CreatePropertyStatement.java:114   (inline CUSTOM, issue #5409)
integration/src/main/java/com/arcadedb/integration/importer/format/JsonlImporterFormat.java:297
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| SQL `CREATE PROPERTY <ts>.<x>` -> `CreatePropertyStatement` -> `LocalDocumentType.createProperty(String,Type,String)` | yes | yes |
| SQL `CREATE PROPERTY <ts>.<x> IF NOT EXISTS` (falls through to the same funnel when absent) | yes | yes |
| Java API `type.createProperty(...)` / `getOrCreateProperty(...)`, every overload | yes | yes |
| SQL `ALTER PROPERTY <ts>.<x> CUSTOM role = ...` -> `LocalProperty.setCustomValue` | yes | yes |
| SQL `CREATE PROPERTY <ts>.<x> ... CUSTOM role = ...` inline (#5409) | argued - unreachable on a TIMESERIES type: a new name is refused by the row above before the CUSTOM loop runs, an existing name throws "already exists", and the `IF NOT EXISTS` form returns before applying CUSTOM (`CreatePropertyStatement.java:90-119`) | no |
| SQL `ALTER PROPERTY <ts>.<x> CUSTOM role = null` (removal) | deliberately allowed - the only way to clear a key a pre-fix database already stored | yes |
| Java API `property.setCustomValue("role", ...)` on a TIMESERIES column | yes | yes |
| SQL `DROP PROPERTY <ts>.<declared column>` -> `LocalDocumentType.dropProperty` | yes | yes |
| `TimeSeriesTypeBuilder.create()` registering the declared columns as properties | allowed by name match | yes |
| `LocalSchema.readConfiguration()` opening a PRE-FIX database that already carries a stray property | tolerated + WARNING | yes |
| `JsonlImporterFormat` restoring a PRE-FIX export (stray property, or `custom.role` on a column) | tolerated + WARNING | yes |
| Remote `RemoteDocumentType.createProperty(...)` (HTTP/gRPC/Studio/console) | argued - emits `create property ...` SQL executed server-side through the same funnel (`network/src/main/java/com/arcadedb/remote/RemoteDocumentType.java:163-189`) | no (same funnel) |
| `LocalSchema.copyType()` | argued - the target type is only ever `LocalDocumentType` or `LocalVertexType`; `LocalEdgeType` is rejected and no other class is accepted (`LocalSchema.java:940-949`), so the copy target is never a TIMESERIES type | no |
| `OpenCypherQueryEngine` typed-property path | argued - same funnel (`createProperty`/`dropProperty` on the schema type) | no (same funnel) |
| `ALTER TYPE <ts> SUPERTYPE +Other` -> `addSuperType` | **filed as #7581** - a different statement reaching the same silent drop through polymorphic properties; refusing a type hierarchy on a TIMESERIES type is a behaviour decision of its own scope | no |

### 4. Tests per entry point

`engine/src/test/java/com/arcadedb/engine/timeseries/Issue7567TimeSeriesPropertyDDLTest.java` (10 tests) drives each
fixed row through its own entry point, plus three controls: the declared columns still round-trip, a DOCUMENT type is
untouched, and a pre-fix database still opens.
`integration/src/test/java/com/arcadedb/integration/importer/Issue7567PreFixTimeSeriesPropertyRestoreIT.java` covers
the JSONL restore of a pre-fix export.

Each rejection was proved able to fail: with the three guards neutralized, 6 of the 10 engine tests fail
("Expecting code to raise a throwable") and the other 4 - the controls - stay green. Neutralizing only the
`isReadingFromFile()` tolerance fails `preFixStrayPropertyStillOpensAndCanBeDropped` alone. Neutralizing the
importer's two skips fails the IT with `ImportException`.

### 5. Reachability

`LocalDocumentType.createProperty(String, Type, String)` is the method `CREATE PROPERTY` calls on every live path
(SQL, Cypher, importers, remote, HA replay). `LocalProperty.setCustomValue` is the method `ALTER PROPERTY ... CUSTOM`
calls. Both are on the hot DDL path with no feature flag, and the tests below drive them through SQL rather than
through the schema API only.

### 6. Test runs

| Suite | Result |
|---|---|
| `mvn -o -pl engine -am test -DexcludedGroups=benchmark,vector,slow` | 14920 run, **1 failure**, 22 skipped - the failure is `MultiColumnAggregationResultTest.emptySumAndCountStayZeroNotNaN`, red on `main` before this branch and unrelated to it (a pure in-memory unit test of classes this diff does not touch; it fails standalone too). Filed as **#7584** |
| `mvn -o -pl integration verify -DskipITs=false -DexcludedGroups=benchmark,vector,slow` | 475 unit + 134 IT, all green, including the pre-existing `Issue7032JsonlRoundTripIT` |
| `server` module | NOT run: port 2480 was held by another agent's server run for the whole session, and a server suite started against an occupied port reports authentication failures rather than a port conflict. It adds no new entry point - `RemoteDocumentType.createProperty` emits `create property ...` SQL that the server executes through the same `LocalDocumentType` funnel the engine suite covers |

## Adversarial pass

The skill's Phase 1.5 spawns a `general-purpose` subagent; the `Task` tool is not available in this nested session, so
the pass was run directly instead, against the tree rather than against the reasoning above. Two findings, both
verified by running code rather than by reading it:

1. **The refusal also blocked REMOVING a stale `CUSTOM role`.** `ALTER PROPERTY x CUSTOM key = null` is the
   documented removal form (`AlterPropertyExecutionTest:65`), and the first version of the guard threw on it - so a
   database that had already stored `custom.role` could never clear it, which is precisely the database the fix is
   supposed to help. Probe output before the fix:
   `PROBE custom-role-null: REFUSED: Cannot set the custom value 'role' ...`.
   **Real and in scope - fixed on this branch**, with `removingAStaleCustomRoleIsAllowed` pinning it.
2. **`ALTER TYPE <ts> SUPERTYPE +Other` reaches the same silent drop.** Probe output:
   `PROBE supertype-on-ts: ACCEPTED, polymorphic props=[s, v, humidity, ts]` - `humidity` comes from the super type,
   the write path still walks `tsColumns` only, and the value is dropped exactly as in the reported case.
   **Real and out of scope - filed as #7581**: it goes through `addSuperType`, which none of this patch's guards see,
   and whether a TIMESERIES type may have a type hierarchy at all is a separate decision.

### 7. Residual risk

- A database created **before** this fix keeps whatever stray property it already has. It opens with a WARNING naming
  the property and the type; the value is still dropped on write. Remediation is `DROP PROPERTY <ts>.<stray>`, which
  stays allowed precisely because the stray name is not a declared column. Automatically deleting it at load would be
  a silent schema mutation during open, which is worse. The same applies to a stale `custom.role`, which
  `ALTER PROPERTY ... CUSTOM role = null` clears.
- The fix does not add the ability to add a column to an existing TIMESERIES type. `addTsColumn` has one caller and
  the sealed/mutable row formats are fixed-stride, so that is a feature with a storage-format question behind it, not
  this bug.
- `ALTER TYPE <ts> SUPERTYPE` still reaches the same silent drop - **#7581**.
- `getTsColumn` reads `tsColumns` without a lock. That list is written only by `TimeSeriesTypeBuilder.create()` before
  the type is registered and by `fromJSON` on the single-threaded schema-load path, so no reader can observe it being
  mutated; this is stated as the reason the lock-free read is safe, not as a claim that the list is immutable.
