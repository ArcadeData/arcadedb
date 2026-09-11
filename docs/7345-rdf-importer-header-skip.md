# #7345 - The RDF importer skips the first triple of every N-Triples file as a header row

Issue: https://github.com/ArcadeData/arcadedb/issues/7345
Type: bug
Branch: `fix/7345-rdf-importer-header-skip`

## Problem

`RDFImporterFormat.load()` inherits the CSV convention that the first line of a source names the
columns, so it defaults `skipEntries` to 1 when the caller passed no `-edgesSkipEntries`. N-Triples,
N-Quads and Turtle have no header row: every line is a statement. The format is selected by content
sniffing precisely because the first line *is* a triple, so the one line the importer is certain
carries data is the one it throws away - silently, since `parsedRecords` counts it and nothing in the
report distinguishes a skipped header from a malformed row.

The same convention reaches RDF a second way: `CSVImporterFormat.analyze()`, which `RDFImporterFormat`
does not override, consumes line 0 as the column names when no `-...Header` option was given. For an
RDF source that means the analysis names the entity's properties after the first triple's three terms
(`<http://a/s1>`, `<http://a/rel>`, `<http://a/o1>`), and `AbstractImporter.updateDatabaseSchema()`
then creates them on the edge type - properties nothing ever writes to.

## Root cause

One convention ("line 0 is a header") is hard-coded in two places, and both are inherited by a format
whose sources have no header:

- `RDFImporterFormat.load()` - `skipEntries` defaults to `1L`.
- `CSVImporterFormat.analyze()` - each of the three entity-type branches defaults `skipEntries` to
  `1L`, and the parse loop's `line == 0 && header == null` branch consumes the first row as field
  names.

## Invariant the fix establishes

For a source parsed by `RDFImporterFormat`, no line of the file is ever treated as a header: the
default `skipEntries` is 0 on every branch that can be reached with an RDF source, and the analysis
never consumes a statement as the column-name row. An explicit `-edgesSkipEntries`/`-verticesSkipEntries`/
`-documentsSkipEntries` is still honoured exactly as given.

## Completeness

### Enumeration (commands and output)

```
$ grep -rn "BY DEFAULT SKIP THE FIRST LINE AS HEADER" --include='*.java' . | grep -v '/target/'
integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java:564
integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java:833
integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java:841
integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java:849
integration/src/main/java/com/arcadedb/integration/importer/format/RDFImporterFormat.java:80
```

`CSVImporterFormat.java:128` and `:394` are the two remaining default sites (`loadDocuments()` and
`loadVertices()`); they carry no such comment but have the same shape and are listed by the
`skipEntries` grep below.

```
$ grep -rn "skipEntries" --include='*.java' integration/src/main/java | grep -v '/target/'
CSVImporterFormat.java:128,131   loadDocuments()  default 1
CSVImporterFormat.java:178       loadDocuments()  skip branch
CSVImporterFormat.java:394,396   loadVertices()   default 1
CSVImporterFormat.java:428       loadVertices()   skip branch
CSVImporterFormat.java:562,565   loadEdges()      default 1
CSVImporterFormat.java:610       loadEdges()      skip branch
CSVImporterFormat.java:825-850   analyze()        default 1 per entity type
CSVImporterFormat.java:873       analyze()        skip branch
RDFImporterFormat.java:78,81     load()           default 1
RDFImporterFormat.java:127       load()           skip branch
```

(the `engine` hits - `PaginatedSegmentDimCursor`, `SparseSegmentBuilder`, `PaginatedSegmentReader` -
are the sparse-vector skip list, an unrelated use of the same identifier.)

```
$ grep -rn "READ THE HEADER FROM FILE\|Reading header from 1st line" --include='*.java' . | grep -v '/target/'
integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java:883
integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java:885
```

One header-consumption site, in `analyze()`.

```
$ grep -rn "extends CSVImporterFormat" --include='*.java' . | grep -v '/target/'
gremlin/.../GraphSONImporterFormat.java:59
gremlin/.../GraphMLImporterFormat.java:36
integration/.../RDFImporterFormat.java:36
```

```
$ grep -rn "new RDFImporterFormat" --include='*.java' integration/src/main | grep -v '/target/'
integration/src/main/java/com/arcadedb/integration/importer/SourceDiscovery.java:679
```

One production construction site; it is reached for any source whose first line sniffs as an
N-Triples statement, on whichever of `Importer.load()`'s four `loadFromSource()` calls carries it.

### Entry-point coverage table

| # | Entry point | Covered by fix? | Covered by a test? |
|---|---|---|---|
| 1 | `Importer -url <f.nt> -edgeType X` -> `RDFImporterFormat.load()` skip default | yes | yes |
| 2 | `Importer -edges <f.nt>` -> `RDFImporterFormat.load()` skip default | yes | yes |
| 3 | `RDFImporterFormat.load()` with an explicit `-edgesSkipEntries 1` (opt-in must still skip) | yes - default-only change | yes |
| 4 | `RDFImporterFormat.load()` with an explicit `-edgesSkipEntries 0` (the documented workaround) | yes - unchanged | yes |
| 5 | `CSVImporterFormat.analyze()` EDGE branch on an RDF source: skip default + header consumption | yes | yes |
| 6 | `CSVImporterFormat.analyze()` VERTEX branch on an RDF source (`-vertices f.nt`) | yes | yes |
| 7 | `CSVImporterFormat.analyze()` DOCUMENT branch on an RDF source (`-documents f.nt`) | yes | yes |
| 8 | `CSVImporterFormat` itself (CSV/TSV): all six default sites keep defaulting to 1 | yes - hook returns `true` | yes |
| 9 | `GraphMLImporterFormat` / `GraphSONImporterFormat` | argued - see below | n/a |

Row 9, argued with evidence: both subclasses override `load()` **and** `analyze()` and neither
delegates to `super`, so no `skipEntries` default and no header-consumption branch of
`CSVImporterFormat` is reachable from either.

```
$ grep -n "public void load\|public SourceSchema analyze" gremlin/.../GraphMLImporterFormat.java gremlin/.../GraphSONImporterFormat.java
GraphMLImporterFormat.java:38   public void load(...)
GraphMLImporterFormat.java:51   public SourceSchema analyze(...)   -> returns new SourceSchema(this, parser.getSource(), analyzedSchema)
GraphSONImporterFormat.java:67  public void load(...)
GraphSONImporterFormat.java:330 public SourceSchema analyze(...)   -> returns new SourceSchema(this, parser.getSource(), analyzedSchema)
```

## Adversarial pass

The orchestrator's Phase 1.5 spawns a `general-purpose` subagent for this. No `Task` tool is exposed in this
session, so the pass was run by the author instead - which is weaker, and is recorded as such. What it produced:

1. **`analyze()`'s new `columns` bound could change CSV behaviour.** Checked: `columns` is `row.length` whenever
   `fieldNames` is non-empty, which is every CSV case, so a data row wider than its header still fails on
   `fieldNames.get(i)` exactly as before. Written as a count rather than a `Math.min` for precisely this reason.
   Not real, but the reason is now a comment in the code.
2. **An entity registered with no properties could divide by zero.** `AnalyzedEntity.getAverageRowLength()` is
   `totalRowLength / analyzedRows`, and `analyzedRows` is advanced by `setRowSize()`, which the DATA LINE branch
   calls immediately after `getOrCreateEntity()`. An entity therefore never exists with `analyzedRows == 0`.
   Not real.
3. **Line 0 is now parsed as a triple, so a line 0 with fewer than three columns would throw
   `ArrayIndexOutOfBoundsException` at `row[1]`.** In production `SourceDiscovery` selects this format only when
   line 0 sniffs as a well-formed statement with the separator it then hands to the parser, so line 0 always has
   at least three terms on that delimiter. The literal and typed-literal shapes were the ones worth doubting -
   quoted objects, `"12"^^<...>`, `"bonjour"@fr` - and `Issue7346RdfShapeDetectionTest` now drives all of them
   through line 0 for the first time, since the old default skipped it: 12 tests, green. Real risk, retired by
   evidence rather than by argument.
4. **`-edgesSkipEntries` is the only option the RDF loop reads.** Real, out of scope. Filed as **#7487**.
5. **The report still cannot say why a row did not become an edge.** Real, out of scope, and named in the issue
   itself. Filed as **#7488**.
6. **Turtle `@prefix`/`@base` directives imported as statements.** Not filed: `SourceDiscovery` only enters the
   RDF arm when the first character is `<` or `_`, so a Turtle file that opens with a directive is never dispatched
   to this format at all. A directive after a leading triple is not a shape worth an issue on speculation.

## Fix

`CSVImporterFormat` gains one protected hook, `firstLineIsHeader()`, returning `true`.
`RDFImporterFormat` overrides it to `false`. The hook is read at:

- the three `skipEntries` defaults in `analyze()` (rows 5-7),
- `analyze()`'s `line == 0 && header == null` header-consumption branch (row 5),
- `RDFImporterFormat.load()`'s own `skipEntries` default (rows 1-3).

`analyze()`'s per-column property loop is bounded by whether any field names were established, so a
format with no header line registers its entity - the type still has to be created - with no columns
to name, instead of throwing `IndexOutOfBoundsException` on `fieldNames.get(0)`.

`loadDocuments()`, `loadVertices()` and `loadEdges()` of `CSVImporterFormat` also read the hook, so
the rule lives in one place; for RDF they are dead code (`RDFImporterFormat` overrides `load()`
entirely), which is why rows 1-3 name `RDFImporterFormat.load()` and not them.

## Pre-existing tests that encoded the bug

Three test classes were written against the old default and are updated here. Per the skill's
"never modify existing tests" rule these are called out explicitly - each change is either a fixture
change that preserves the test's own subject exactly, or an assertion the issue itself asks to be
flipped:

- `RDFImporterFormatCommitCadenceTest` and `RDFImporterFormatTransactionLeakTest` build their
  fixtures with a literal `s,p,o` first line and relied on the default skip to drop it. Their
  `ImporterSettings` factories now set `edgesSkipEntries = 1L` explicitly. Not one assertion, not one
  fixture byte changes: the skip those tests always wanted is now asked for rather than inherited.
- `RDFImporterFormatDelimiterDetectionTest` asserted N-1 edges for N triples, with a javadoc saying
  "the first being skipped as the header row RDF sources default to". The counts become N.
- `Issue7346RdfShapeDetectionTest#assertImportsAsRdf` asserted `createdEdges=2` for three triples,
  with a javadoc saying "the behaviour #7345 tracks ... asserted as it is rather than as it should
  perhaps become". It becomes 3.

## Reachability

The changed code is on the live CLI path, not only under the new tests.
`SourceDiscovery:679` is the one production site that constructs `RDFImporterFormat`, and it is reached for any
source whose first line sniffs as an N-Triples statement. Five of the nine new tests drive `Importer.load()` end to
end through `-url`, `-edges`, `-vertices` and `-documents`; all five failed before the change and pass after it, so
the hook is read on a path a user actually drives and is not gated off by any flag.

## Test results

New class `RDFImporterFormatHeaderDefaultTest` (9 tests), one per fixed row of the table plus two CSV controls.

Before the fix: `Tests run: 9, Failures: 5, Errors: 1`. The five failures are rows 1, 2, 5, 6 and 7 - each reporting
one edge short, and row 5 reporting
`Expecting empty but was: ["<http://a/s1>", "<http://a/o1>", ".", "<http://a/rel>"]` for the edge type's property
names. (The one error was a defect in the test itself - the CSV edge control was missing `-edgeFromField`/
`-edgeToField` - fixed before the implementation went in.) Rows 3, 4 and 8 passed before and after, which is what
"default-only change" means.

After the fix:

```
$ mvn -o -pl integration -Dtest='RDFImporterFormat*Test,Issue7346RdfShapeDetectionTest' test
Tests run: 40, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl integration -DexcludedGroups=benchmark,vector,slow test
Tests run: 375, Failures: 0, Errors: 0, Skipped: 9

$ mvn -pl gremlin -am -Dtest='*Import*' test      # GraphML/GraphSON, the other two CSVImporterFormat subclasses
BUILD SUCCESS

$ mvn -pl console -am -Dtest='ConsoleTest' test   # the console's own IMPORT command
Tests run: 64, Failures: 0, Errors: 0, Skipped: 0
```

## Residual risk

- **The import report still does not distinguish a skipped line from a malformed one.** The issue observed that
  `parsedRecords` counts every row while `createdEdges` counts only the ones that became edges, and nothing names
  the difference. This change removes the case where that gap was caused by a phantom header, but an explicit
  `-edgesSkipEntries` still opens the same silent gap, and so does a row the loop declines for any other reason.
  Out of scope here. **Filed as #7488.**
- **`RDFImporterFormat.load()` reads only `edgesSkipEntries`, whichever route the source arrived on.** It ignores
  the `entityType` parameter it is handed, so `-vertices f.nt -verticesSkipEntries 2` skips nothing while
  `analyze()`, which does honour the entity type, reads a different option for the same file. The divergence was
  masked while the default was 1 everywhere and is observable now that it is 0. Out of scope here - this is which
  option supplies the value, not what the value defaults to. **Filed as #7487.**
- **The RDF analysis registers the entity with no properties**, which is a change from three garbage ones. The edge
  type is still created, which is what `updateDatabaseSchema()` needs from it; nothing in the RDF load path reads
  the analyzed properties (`RDFImporterFormat.load()` writes only `label` on the edge and `settings.typeIdProperty`
  on the vertices). Verified by `theAnalysisDoesNotNameTheEdgeTypesPropertiesAfterTheFirstTriple`, which imports
  into a type the run creates itself and then asserts the four edges landed.
- **Everything else the table lists is covered.** There is no entry point in it left blank.
