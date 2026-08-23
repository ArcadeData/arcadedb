# Issue #6460: JSONL import does not remap LINK-typed property values

## Root cause

`JsonlImporterFormat` rebuilds an old-RID -> new-RID map (`ridIndex`) while importing, but only consulted it for
edge endpoints (`loadEdge`'s `ridIndex.get(out)` / `ridIndex.get(in)`). A regular `LINK`-typed **property** value
(and a `LIST`/`MAP` declared `OF LINK`) was passed straight through `loadProperties` -> `json2map` ->
`convertFromJSONType` -> `MutableDocument.fromMap()`, none of which consult `ridIndex`. `Type.convert`'s LINK
branch parses the string into an `RID` using the source database's RID string verbatim (`RID.create(database,
"#<bucket>:<pos>")`). Because import recreates records at fresh positions, that RID pointed at an unrelated
record (or none) in the target database - silent data corruption, no error raised.

## Fix

`integration/src/main/java/com/arcadedb/integration/importer/format/JsonlImporterFormat.java`:

- `loadProperties` now calls a new `remapLinkProperties(DocumentType, Map)` after `json2map()` and before
  `fromMap()`. For every property present in the JSON that the schema declares as `LINK`, or as `LIST`/`MAP`
  declared `OF LINK`, the source RID string(s) are looked up in `ridIndex` and replaced with the resolved new
  RID. Only schema-declared properties are touched - without a declared type there is no reliable way to tell a
  LINK string ("#12:34") apart from an ordinary string that merely looks like one.
- A value that cannot be resolved yet - it references a record that appears **later** in the source stream (a
  forward reference) - is left as-is and its property name is recorded, keyed by the record's own new identity,
  in `pendingLinkReconciliation`.
- After the whole file has been read (every RID mapping is now known), `reconcileUnresolvedLinks()` revisits
  every such record once, re-resolves the still-outstanding LINK values through `ridIndex`, and saves the record
  again if anything changed. A value that still cannot be resolved after this (the referenced record was
  excluded from the export, or the source link was already dangling) is left unchanged, matching the documented
  pre-fix behavior for that case, and is not counted as an import error.
- `loadDocument`, `loadVertex`, and `loadEdge` were updated to capture `loadProperties`' returned unresolved-name
  set and register it against the record's identity once `save()` has assigned one.

This mirrors, for properties, the same `ridIndex` mechanism edges already use for their endpoints - with a
reconciliation pass added on top so forward references (which edges do not need to handle, since an edge always
throws if an endpoint isn't found yet) are also fixed up instead of silently left wrong.

## Test

`integration/src/test/java/com/arcadedb/integration/importer/JsonLImporterIT.java`:
`importDatabaseRemapsLinkTypedPropertyValues` - hand-crafted JSONL with a `Person` type carrying a scalar `LINK`
property (`bestFriend`) and a `LIST OF LINK` property (`friends`). Source `"r"` RIDs are deliberately NOT the
sequence a fresh import would allocate, so any unremapped (still-old) value is directly observable:

- `Person#2.bestFriend -> Person#1` is a **backward** reference (Person#1 already imported) - resolved
  immediately by `remapLinkProperties`.
- `Person#3.friends -> [Person#4]` is a **forward** reference (Person#4 imported later in the stream) - resolved
  only by `reconcileUnresolvedLinks`.

Verified TDD-style: reverted the fix (`git stash` on the production file only) and confirmed the test fails
(`expected: #1:0 but was: #6:5`), then restored the fix and confirmed it passes.

### Verification run

- `JsonLImporterIT`, `JsonlExporterIT`, `Issue6455JsonlDateTimeRoundTripIT`: 12/12 passed.
- Full `integration` module suite (`-DexcludedGroups=benchmark,slow,vector`): 157 tests, 0 failures, 9 skipped.
- `mvn -pl integration -am compile`: BUILD SUCCESS.

## Scope / limitations carried forward

- Embedded-document (`EMBEDDED`) properties are not walked recursively for nested LINK values - out of scope of
  the reported bug, which is about direct LINK/LIST-of-LINK/MAP-of-LINK properties on the record itself.
- A LINK value that never resolves (the referenced record was excluded from the export via `-includeTypes` /
  `-excludeTypes`, or was already a dangling link in the source database) is left as the original source RID
  string, same as the pre-fix behavior for that specific case - there is nothing more correct to do without
  external knowledge of intent, and it is not counted as an import error.
