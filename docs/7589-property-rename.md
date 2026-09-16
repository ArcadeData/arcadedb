# #7589 - Property rename: no in-place rename primitive

Issue: https://github.com/ArcadeData/arcadedb/issues/7589

## What was asked

`Property.rename(String)` / `setName(String)` in the Java API, and `ALTER PROPERTY <type>.<old> NAME <new>` in SQL,
mirroring `DocumentType.rename(String)` (which already exists for types): update the schema's own record of the
name, leave every stored document untouched, no data movement, no index rebuild.

## Two things had to be checked before writing any code

### 1. Is a record keyed by property NAME or by a stable id?

A record is keyed by a stable id: `BinarySerializer` writes each field as `[nameId][value]`, where `nameId` comes
from `Dictionary.getIdByName(name, true)` (`engine/src/main/java/com/arcadedb/engine/Dictionary.java`) - a single,
database-wide table mapping identifier strings to small integers, shared by every type. `Dictionary.updateName()`
already exists and rewrites the id→name mapping in place, with **no production caller today**.

That looked like it made a true O(1) rename possible: repoint the dictionary entry, touch no document. It does not,
because **the same dictionary is also referenced by ordinary string VALUES**, not only by identifiers: `Dictionary`'s
own class Javadoc notes that a string value is looked up with `create=false` and stored as a reference "when it
happens to match an entry already present" (`BinarySerializer.serializeProperties`). So the id for `"amount"` is not
scoped to a property - it is shared with a plain STRING field, anywhere in the database, embedded documents included,
whose value happens to equal `"amount"`. Renaming that dictionary entry would also silently change what a completely
unrelated stored string decodes as. There is no cheap way to rule this out short of scanning every record in the
database (including nested/embedded ones), which is no better than a full rewrite - it just makes the cost a read
scan instead of a write.

### 2. Would an automated data-copying rename be consistent with the engine's own rules?

The natural fallback was: do what the issue's own six-statement workaround does (create new property, copy every
row, drop indexes, drop old property, rebuild indexes), just automated and transactional. Before building that, the
question is whether a schema DDL statement is allowed to touch record content as a side effect at all.

It is not, anywhere in the current engine. `LocalDocumentType.dropProperty()` only removes the entry from the
`properties` map and persists `schema.json` - it never scans a bucket, and a document that already carried the
dropped field keeps carrying it. `cascadeDeleteExternalValues` is not a schema-DDL hook: its only call site is
`LocalDatabase.deleteRecordNoLock`, i.e. it runs when a **record** is deleted, not when a **property** is dropped.
`dropType()` deletes the type's bucket **files** wholesale (the expected "drop a container, lose its contents"), not
a record-by-record rewrite. Every other property/type setter (`setMandatory`, `setReadonly`, `rename()` on a type,
`addSuperType`) is `recordFileChanges` + `saveConfiguration()`, i.e. schema.json only. `CREATE PROPERTY` does not
back-validate MANDATORY/NOT NULL against existing rows either - that is enforced only at the next write.

This is deliberate, not an oversight: ArcadeDB's schema is descriptive, not prescriptive (`Person.get("undeclared")`
comes back with whatever the document actually has - see `ImmutableDocument`/`BinarySerializer.deserializeProperty`,
which resolve a field purely from the record's own stored name-id, never from `LocalDocumentType.properties`). A
document does not need a property declared to carry it, which is exactly why `DROP PROPERTY` leaving a stray field
behind is not a bug. Wiring a bulk `UPDATE ... SET / REMOVE` into `ALTER PROPERTY` would be the first schema
statement in the engine that reaches into records as a side effect - a new category of behaviour, not a bigger
version of an existing one.

## Decision: schema-metadata-only rename

`Property.rename(String)` / `ALTER PROPERTY <type>.<old> NAME <new>` update only the type's own record of the
property's name (`LocalDocumentType.properties`, persisted through `schema.json`). They do not touch, scan or
revisit any existing record, exactly like `DROP PROPERTY` already doesn't.

**Consequence a caller has to know:** a value already written under the old name keeps reading back under the old
name after the rename. Only a write made *after* the rename lands under the new name. This is not a partial
implementation of "true" rename - it is the same schema/data decoupling every other DDL statement in the engine
already relies on, applied consistently one level down from `DocumentType.rename()`. Migrating existing values, if
that is what a caller needs, is still the explicit multi-statement recipe the issue itself documents (create new
property, `UPDATE ... SET new = old`, drop indexes on old, `UPDATE ... REMOVE old`, drop old property, rebuild
indexes) - this issue makes the *label* change free and instant; it does not make the *data* migration implicit.

### What is preserved across the rename

Every other attribute of the property - type, `ofType`, `mandatory`, `notNull`, `readonly`, `hidden`, `external`,
`compression`, `min`, `max`, `regexp`, `default`, custom values - carries over unchanged
(`LocalProperty.copyWithName`). `name` and `id` are `final` on `AbstractProperty`, so the rename constructs a new
`LocalProperty` under the new name (which gets its own dictionary id, as any newly-named property would) and swaps
it into the owning type's `properties` map; the old `Property` handle becomes stale, the same way a handle to a
dropped property already is (`LocalProperty.checkStillDeclaredIn`).

### Refusals

- **An index stands on the property.** Mirrors `dropProperty`'s own refusal: the index's definition names the
  property by its old name, and propagating a rename through every index type's file/logic naming is out of scope
  here. Drop the index, rename, recreate it on the new name.
- **The property is a declared TIMESERIES column.** Mirrors the #7567/#7581 pattern: the time-series write path
  resolves those by the fixed name in `LocalTimeSeriesType.tsColumns`, which this method does not - and must not -
  touch.
- **The new name is already used**, own or inherited (same check `createProperty` already makes).

## Files changed

- `engine/src/main/java/com/arcadedb/schema/Property.java` - `rename(String)` on the interface.
- `engine/src/main/java/com/arcadedb/schema/DocumentType.java` - `renameProperty(String, String)` on the interface.
- `engine/src/main/java/com/arcadedb/schema/LocalProperty.java` - `rename()`, `copyWithName()`.
- `engine/src/main/java/com/arcadedb/schema/LocalDocumentType.java` - `renameProperty()`.
- `engine/src/main/java/com/arcadedb/query/sql/parser/AlterPropertyStatement.java` - `NAME` setting.
- `network/src/main/java/com/arcadedb/remote/RemoteDocumentType.java`,
  `network/src/main/java/com/arcadedb/remote/RemoteProperty.java` - remote-client wiring (the parser test at
  `AlterPropertyStatementTestParserTest` shows `NAME` was already parseable; only `RemoteDocumentType` needed a new
  method, `RemoteProperty.rename()` throws `UnsupportedOperationException` like every other remote mutator).
