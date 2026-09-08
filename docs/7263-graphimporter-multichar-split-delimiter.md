# #7263 — `GraphImporter.splitEdge` takes a `String` delimiter but uses only its first character

## Finding ledger

- [x] 1. A multi-character delimiter matches on `charAt(0)` alone, so every value after the first keeps the
      rest of the delimiter as a prefix and its edge is silently counted as unresolved.
- [x] 2. `splitEdge(a, e, t, "")` throws `StringIndexOutOfBoundsException` from inside the pass-1 row loop,
      after vertices have already been committed.
- [x] 3. `splitEdge(a, e, t, null)` throws an NPE from the same place.

All three fixed in this branch. Statuses in full:

1. **fixed** — both walkers advance by `delimiter.length()` and match with `indexOf(String, int)`.
2. **fixed** — `validateEdgeTargets()` raises `IllegalArgumentException` before pass 1 opens a file.
3. **fixed** — same check, same place.

## Root cause

`EdgeDef.delimiter` is a `String`, but both split walkers reduce it to a single `char` before walking the
field:

- `collectEdge()` (inline, non-self-referencing split): `final char delim = ed.delimiter.charAt(0);`
- `collectSplitKeys()` (deferred, self-referencing split): `collectSplitKeys(fieldVal, ed.delimiter.charAt(0), …)`

Both then walk with `indexOf(char, start)` and advance `start = end + 1` — a stride of one character, which
is the same single-character assumption expressed twice. Nothing validates the delimiter's length, so an
empty or null value reaches `charAt(0)` and throws mid-import.

## Completeness

### Invariant

> A split-field edge splits its field on the **complete** configured delimiter string, on both the inline and
> the deferred walker; and a null or empty delimiter is a configuration error raised by `validateEdgeTargets()`
> before any row is read.

### Enumeration

```
$ grep -rn "splitEdge(" --include='*.java' --include='*.json' --include='*.md' .
./integration/src/test/java/com/arcadedb/integration/importer/GraphImporterIdTypesTest.java:861,908,944   ("|")
./integration/src/test/java/com/arcadedb/integration/importer/StackOverflowImporterApiTest.java:116        ("|")
./integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:332   v.splitEdge(attr, edgeType, target, ej.getString("split"));   <- JSON entry point
./integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java:653   public void splitEdge(...)                                     <- fluent entry point
```

Two entry points, both funnelling into the same `EdgeDef`. Every existing test uses `"|"`, which is why the
single-character assumption was never exercised.

```
$ grep -rn "\.delimiter" integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java
:1033  collectSplitKeys(fieldVal, ed.delimiter.charAt(0), deferred, idx);
:1080  final char delim = ed.delimiter.charAt(0);
:1433  this.delimiter = delim;
```

Two consumers (`:1033`, `:1080`) and one writer (`:1433`).

```
$ grep -rn "isSplit" --include='*.java' integration/
:873   final boolean resolvesByName = ed.byName || ed.isSplit;
:1028  final String fieldVal = ed.isSplit ? record.get(...) : identity(record, ...);
:1032  if (ed.isSplit)
:1057  final IdIndex index = ed.byName || ed.isSplit ? ts.nameToIdx : ts.idToIdx;
:1072  if (ed.isSplit) {
:1418/:1432  field + assignment
```

Both delimiter consumers are gated on `ed.isSplit`; a non-split `EdgeDef` (`edgeIn`/`edgeOut`/`edgeInByName`/
`edgeOutByName`) is constructed with `delim = null` and never reads it.

Same-shape sibling grep — a JSON string field consumed as `charAt(0)`:

```
$ grep -rn "charAt(0)" --include='*.java' integration/src/main/java/
GraphImporter.java:415   final char delimiter = config.getString("delimiter", ",").charAt(0);   <- SAME SHAPE, also unvalidated
GraphImporter.java:1033, :1080, :1081, :1143                                                   <- the reported defect
GraphImporter.java:1544  final boolean negative = text.charAt(0) == '-';                       <- guarded by an isEmpty() test above it
JsonlRowSource.java:57   if (line.isEmpty() || line.charAt(0) != '{')                          <- guarded
ImporterSettings.java:162, format/AbstractImporterFormat.java:37                               <- guarded by length checks
format/CSVImporterFormat.java:739-740  delimiter.charAt(0)                                     <- the legacy CSV importer, out of scope (see Residual risk)
vector/TextEmbeddingsImporterLSM.java:80,84                                                    <- names, not delimiters
```

14 hits; one of them (`GraphImporter.java:415`) is the same defect shape and is unguarded: `"delimiter": ""`
in a JSON source config throws a bare `StringIndexOutOfBoundsException`, and `"delimiter": "||"` is silently
truncated. `CsvRowSource` genuinely takes a `char`, so that one is fixed by *rejecting* the bad value with a
message rather than by honouring the whole string.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| Fluent `splitEdge(…, ", ")` → `collectEdge()` inline walker | yes | yes — `multiCharacterDelimiterSplitsOnTheWholeString` |
| Fluent `splitEdge(…, ", ")` → `collectSplitKeys()` deferred self-referencing walker | yes | yes — `multiCharacterDelimiterSplitsOnTheWholeStringOnTheDeferredPath` |
| JSON `"split": ", "` → `splitEdge()` → both walkers | yes | yes — `jsonConfigHonoursAMultiCharacterSplitDelimiter` |
| Fluent `splitEdge(…, "")` | yes — `validateEdgeTargets()` | yes — `anEmptySplitDelimiterIsRejectedBeforeAnyRowIsRead` |
| Fluent `splitEdge(…, null)` | yes — `validateEdgeTargets()` | yes — `aNullSplitDelimiterIsRejectedBeforeAnyRowIsRead` |
| JSON `"split": ""` | yes — same validation | yes — `jsonConfigRejectsAnEmptySplitDelimiter` |
| JSON source `"delimiter": ""` / `"delimiter": ";;"` → `createRecordSource()` (sibling) | yes — rejected with a message | yes — `aCsvSourceDelimiterMustBeASingleCharacter` |
| A delimiter that only *wraps* the field (`"\|java\|python\|"`, the existing convention) | yes — stride is now `delimiter.length()`, which is 1 for a one-character delimiter | yes — the four pre-existing `"\|"` tests in `GraphImporterIdTypesTest` still pass unchanged |
| `edgeIn` / `edgeOut` / `edgeInByName` / `edgeOutByName` | argued — constructed with `delim = null`, `isSplit = false`; both delimiter reads and the new validation are gated on `ed.isSplit` (grep above) | n/a |
| `new CsvRowSource(path, char, int)` programmatic constructor | argued — the parameter is a `char`, so no string can be truncated on this path | n/a |

### Reachability

`collectEdge()` is called from `processVertexSource()`'s row loop (`:1021`), `collectSplitKeys()` from the
deferred loop immediately below it (`:1033`), and `validateEdgeTargets()` is the first statement of
`run()` (`:781`). All three are on the only import path there is; the four existing `splitEdge` tests plus
`StackOverflowImporterApiTest` drive them today.

### Residual risk

- The legacy `CSVImporterFormat` (`format/CSVImporterFormat.java:739-740`) has the same
  `delimiter.charAt(0)` shape on a different, older importer with its own settings parser
  (`ImporterSettings`). It is not reachable from `GraphImporter` and is untouched here.
- A split delimiter that is a *prefix of itself at an offset* (e.g. `"aa"` against `"aaa"`) resolves
  left-to-right like `String.indexOf`, which is the same semantics `String.split` gives. No test pins that;
  it is not a behaviour the importer promises.

## Changes

1. `collectEdge()` and `collectSplitKeys()` walk with the full `String` delimiter: `startsWith` for the
   optional leading wrap, `indexOf(String, int)` for each separator, and a stride of `delimiter.length()`.
2. `validateEdgeTargets()` rejects a null or empty split delimiter, naming the edge, the vertex source and
   the attribute, before pass 1 opens a file.
3. `createRecordSource()` rejects a CSV `"delimiter"` that is not exactly one character, instead of
   truncating it silently or throwing a bare `StringIndexOutOfBoundsException`.
4. `splitEdge()`'s javadoc says the delimiter is the whole string and may be more than one character.

## Test results

Before the fix (`mvn -o -pl integration test -Dtest=GraphImporterSplitDelimiterTest`):

```
Tests run: 8, Failures: 4, Errors: 4, Skipped: 0
  aWrappedMultiCharacterDelimiterIsConsumedWhole ......... expected: 2L but was: 0L
  multiCharacterDelimiterSplitsOnTheWholeStringOnTheDeferredPath ... expected: 3L but was: 2L
  jsonConfigRejectsAnEmptySplitDelimiter ................ StringIndexOutOfBoundsException: Index 0 out of bounds for length 0
  aCsvSourceDelimiterMustBeASingleCharacter ............. Expecting code to raise a throwable
```

The four `Errors` were a cascade: the `StringIndexOutOfBoundsException` fired from inside the pass-1 row
loop with a transaction open, so `drop()` in `@AfterEach` refused ("Cannot drop the database in transaction")
and leaked the instance into the next four `setup()` calls. That is the *shape* of failure the issue
describes - a configuration mistake surfacing as a crash after vertices are already committed. The teardown
now rolls back before dropping so a single failure reports itself rather than four later ones.

After the fix:

```
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
```

Whole module, no regressions (`mvn -o -pl integration test -DexcludedGroups=benchmark,vector,slow`):

```
Results:
Tests run: 244, Failures: 0, Errors: 0, Skipped: 9
```

`GraphImporterIdTypesTest` (24 tests, four of which drive `splitEdge` with `"|"`) and
`Issue6811CsvDelimiterOptionTest` are green unchanged, which is the evidence that a one-character delimiter
still behaves exactly as before.

## Impact

- A `splitEdge` delimiter longer than one character now produces the edges it names instead of one edge per
  row and a warning pointing at the data files.
- A null or empty delimiter is an `IllegalArgumentException` naming the edge, the vertex source and the
  attribute, raised before any file is opened, instead of a `StringIndexOutOfBoundsException` or an NPE
  raised after vertices have been committed.
- A JSON source `"delimiter"` that is not exactly one character is refused with a message instead of being
  silently truncated (`"||"` -> `'|'`) or throwing a bare index-out-of-bounds (`""`).
- Behaviour for a one-character delimiter, which is every delimiter in the tree today, is unchanged: the
  stride is `delimiter.length()`, which is 1.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen this document. The `Task` tool was
disabled for this session, so the pass was run by hand instead, against the diff and the tree only. That
substitution is a weaker version of the check - the reviewer had already been convinced - and is recorded
here rather than glossed over.

| Finding | Disposition |
|---|---|
| `CsvRowSource:91` splits rows with `line.split(String.valueOf(delimiter), -1)`, and `String.split`'s first argument is a **regex**. Verified by running it: `'\|'` turns every character into its own field, `'.'` annihilates the row into empty fields, `'$'` does not split at all. Reachable from the JSON `"delimiter"` and from the public `CsvRowSource(path, char, int)` constructor. | **Real, out of scope** - filed as **#7267**. Different file, different defect, and it affects the programmatic constructor this PR does not touch. `'\|'` is the delimiter every `splitEdge` example uses, so it is a plausible thing to reach for. |
| A split delimiter containing the source's own field separator (`", "` over a comma-delimited CSV) cuts the field up before the walker sees it. `validateEdgeTargets()` cannot diagnose it: `RecordSource` (`:702-704`) declares only `forEach`, and `CsvRowSource.delimiter` (`:39`) is private with no accessor - both verified by grep. This fix makes `", "` usable and therefore makes the collision *more* likely. | **Real, out of scope** - filed as **#7268**. Needs a widening of the `RecordSource` SPI, an API change rather than a validation addition. |
| The class javadoc documented the JSON `"split"` key with `"\|"` only, so a reader had no way to know a longer delimiter was now allowed. | **Real, in scope** - fixed here (`:265-266`). |
| The claim in `collectEdge`'s new comment and in `collectSplitKeys`'s javadoc that `validateEdgeTargets()` has already refused a null or empty delimiter. | **Verified, not a finding.** `run()` is the only public method that reads rows (`grep -n "^  public " GraphImporter.java`), `validateEdgeTargets()` is its first statement (`:792`), and `processVertexSource` is called at `:805`. `main()` (`:127`) reaches the walkers only through `fromJSON(...).run()`. |
| A `"delimiter"` key on a JSONL or XML source is silently ignored. | **Not a finding for this invariant.** It is an ignored key, not a truncated value, and cannot make a split-field edge resolve wrongly. Validating unknown JSON keys generally is a separate and much larger change. |
| The CSV-delimiter length check throws from `fromJSON()` (parse time) while the split-delimiter check throws from `run()`. | **Not a finding.** Both satisfy what the issue asked for - the error is raised before any row is read - and the CSV one cannot be deferred to `run()` without keeping an invalid `RecordSource` alive in between. |
