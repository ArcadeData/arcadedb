# #7315 - the RDF branch of `SourceDiscovery` drops the delimiter it just detected

## Problem

A canonical, space-delimited N-Triples file cannot be imported at all:

```
com.arcadedb.integration.importer.ImportException: Error on parsing source 'file:///tmp/t.nt'
Caused by: java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1
	at com.arcadedb.integration.importer.format.RDFImporterFormat.load(RDFImporterFormat.java:88)
```

`row` carries one element because the parser split the line on `,` while the file is space-delimited.

## Root cause

`SourceDiscovery.analyzeChar()` identifies a source as RDF by collecting every character that falls outside
`<...>` on the first line and checking they are all the same. That character *is* the delimiter. The CSV
branches of the same class hand the resolved delimiter to the format they build
(`new CSVImporterFormat(userDelimiter)`, `SourceDiscovery.java:286` and `:408`); the RDF branch built
`new RDFImporterFormat()` and threw the character away.

`RDFImporterFormat extends CSVImporterFormat` and inherits both readers of that field -
`createCSVParser(settings)` (used by `load()`) and `analyze()` - each of which resolves through
`CSVImporterFormat.delimiterFor(settings)`:

```java
private String delimiterFor(final ImporterSettings settings) {
  return delimiter != null ? delimiter : settings.getValue("delimiter", ",");
}
```

The no-arg constructor leaves the field null, so the lookup fell through the generic `delimiter` option to a
comma. The stale comment on the branch claimed "the RDF importer never reads it", which is not the case: it
reads it twice over, and #6946 removed the only thing that populated either.

## Invariant the fix establishes

> When content sniffing resolves a source to `RDFImporterFormat`, the delimiter that format parses with is the
> user's own when they set one and the detected character otherwise - never the inherited comma fallback - and
> that delimiter is carried on the format, never written into the import-wide `settings.options`.

## Completeness

### Commands run

```
$ grep -rn "analyzeChar" integration/src/main/java
SourceDiscovery.java:323:    FormatImporter format = analyzeChar(parser, settings);
SourceDiscovery.java:344:      format = analyzeChar(parser, settings);
SourceDiscovery.java:355:      format = analyzeChar(parser, settings);
SourceDiscovery.java:446:  private FormatImporter analyzeChar(final Parser parser, final ImporterSettings settings)

$ grep -rn "extends CSVImporterFormat" --include='*.java' .
gremlin/.../GraphSONImporterFormat.java:59
gremlin/.../GraphMLImporterFormat.java:36
integration/.../RDFImporterFormat.java:36

$ grep -rn "createCSVParser" --include='*.java' .
CSVImporterFormat.java:110, :357, :517, :911 (the declaration)
RDFImporterFormat.java:42

$ grep -rn 'delimiterFor|getValue("delimiter"' --include='*.java' .
SourceDiscovery.java:282
CSVImporterFormat.java:85 (the declaration), :86, :780 (analyze), :912 (createCSVParser)

$ grep -rn 'new RDFImporterFormat|new CSVImporterFormat' --include='*.java' .
SourceDiscovery.java:286, :408, :485          <- the three production construction sites
integration/src/test/... 14 direct instantiations in RDFImporterFormatCommitCadenceTest,
                            RDFImporterFormatTransactionLeakTest, CSVImporterFormatLoadEdgesTransactionLeakTest

$ grep -n "delimiter|createCSVParser|super\(" gremlin/.../GraphMLImporterFormat.java gremlin/.../GraphSONImporterFormat.java
(no matches)

$ grep -rn "SourceDiscovery|RDFImporterFormat|CSVImporterFormat" --include='*.java' engine server
5 hits, all inside comments or a test's allow-list string - no call sites
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `Importer -url <space-delimited .nt>` -> `analyzeChar` site 1 -> `RDFImporterFormat.load()` | yes | yes - `aSpaceDelimitedNTriplesFileImportsThroughTheUrlRoute` |
| `Importer -edges <space-delimited .nt>` -> same branch, different `loadFromSource()` call | yes | yes - `aSpaceDelimitedNTriplesFileImportsThroughTheEdgesRoute` |
| tab-delimited source -> `createCSVParser`'s `TsvParser` branch (a second parser shape) | yes | yes - `aTabDelimitedTriplesFileImportsThroughTheUrlRoute` |
| `CSVImporterFormat.analyze()` - the schema-inference reader of the same field, called by `SourceDiscovery.getSchema()` before `load()` | yes (same constructor argument feeds both readers) | yes - implicitly by all three above: with a comma the header row is one column and `fieldNames.get(1)` would throw before `load()` ever ran |
| user's explicit `-delimiter` / `-edgesDelimiter` must still beat the detected one (#6946) | yes - via the existing `resolveDelimiter()` | yes - `anExplicitDelimiterStillOverridesTheDetectedOne` (a guard: it passed before the fix too) |
| the detected RDF delimiter must not reach the next entity of the same import (#6946) | yes - carried on the format, nothing written to `settings.options` | yes - `theDetectedRdfDelimiterDoesNotLeakIntoTheNextEntityOfTheSameImport` |
| `analyzeChar` sites 2 and 3 (the `#` and `//` comment-skip loops inside `analyzeText`) | yes - the parameter is threaded to all three sites | argued, see below |
| `GraphMLImporterFormat` / `GraphSONImporterFormat` - the other two subclasses of `CSVImporterFormat`, also built with the no-arg constructor | n/a | argued, see below |

### Rows argued with evidence

- **`analyzeChar` sites 2 and 3 are unreachable for the RDF branch today.** Both are called immediately after
  `skipLine(parser)`, which is `while (parser.isAvailable() && parser.nextChar() != '\n');` - it exits with
  `currentChar == '\n'`, and `Parser.getCurrentChar()` returns exactly that field. `analyzeChar` dispatches on
  `'<'` and `'{'`, so neither branch can be entered from those two call sites. They still receive the
  delimiter, so the two are correct rather than merely unreached, but no test can drive the RDF branch through
  them without first changing `skipLine`, which is out of scope here.
- **`GraphMLImporterFormat` and `GraphSONImporterFormat` cannot reach this defect.** The grep above finds no
  occurrence of `delimiter`, `createCSVParser` or a `super(` call in either file - they override `load` and
  `analyze` with TinkerPop's own XML/JSON readers and never consult the inherited field. They are also selected
  by file extension at `SourceDiscovery.java:290`/`:299`, before `analyzeChar` runs at all.

### Reachability

The changed line is `SourceDiscovery.java:485`, inside the production content-sniffing path that
`Importer.loadFromSource()` drives on every source with no recognised extension. All five new tests go through
`new Importer(args).load()` end to end rather than instantiating the format directly, so the assertion is that
the live CLI path works, not merely that the constructor accepts an argument. No feature flag gates it.

## Fix

- `RDFImporterFormat` gains an explicit no-arg constructor and a delimiter-carrying one delegating to
  `super(delimiter)` - the same pair `CSVImporterFormat` already has.
- The RDF branch of `SourceDiscovery.analyzeChar()` returns
  `new RDFImporterFormat(resolveDelimiter(userDelimiter, delimiter))`, reusing the existing
  `resolveDelimiter()` that the `analyzeText` CSV branch uses, so an explicit user delimiter still wins and a
  discarded guess is still logged.
- `analyzeChar` takes `userDelimiter` as a parameter, threaded from all three of its call sites.
- The stale comment claiming "the RDF importer never reads it" is replaced with what the code does hold.

Nothing is written into `settings.options`, so #6946's leak stays fixed.

## Test results

`mvn -pl integration test -Dtest=RDFImporterFormatDelimiterDetectionTest`

- Before the fix: `Tests run: 5, Failures: 0, Errors: 4` - all four with
  `java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1`, the reported stack.
  `anExplicitDelimiterStillOverridesTheDetectedOne` passed, as a #6946 guard should.
- After the fix: `Tests run: 5, Failures: 0, Errors: 0`.

`mvn -pl integration test -DexcludedGroups=benchmark,vector,slow`: `Tests run: 323, Failures: 0, Errors: 0,
Skipped: 9` - BUILD SUCCESS.

`mvn -pl gremlin-it verify -DskipITs=false`: `Tests run: 395, Failures: 0, Errors: 7`. All seven are
`AbstractGremlinServerIT` port-bind and database-delete failures - "Unable to listen to a HTTP port in the
configured port range 2480 - 2489". `lsof -nP -iTCP:2480 -sTCP:LISTEN` showed two other JVMs already holding
the port (parallel agents on this machine), which is the conflict `CLAUDE.md` documents. The diff touches no
server code. `Issue6751GraphSONMultiPropertyTest` - the one gremlin test that exercises an importer format -
passed.

## Impact

A space- or tab-delimited N-Triples file now imports without `-delimiter`. Nothing else changes shape: the
comma-delimited RDF sources the existing tests use still resolve to a comma (detected, not inherited), and
every direct `new RDFImporterFormat()` in the test sources keeps the previous fallback behaviour through the
no-arg constructor.

## Adversarial pass

No isolated subagent could be spawned - this session has no `Task` tool - so the pass was run by hand against
the tree instead, by probing the N-Triples shapes adjacent to the one the issue reported. Every result below
was produced by running `new Importer(...).load()` on the patched tree, not by reading:

```
                                             result
LF,   <s> <p> <o> .                          {parsedRecords=3, createdEdges=2}     <- the shape the fix covers
LF,   <s> <p> <o>       (no trailing dot)    {parsedRecords=3, createdEdges=2}     <- the issue's own repro
CRLF, <s> <p> <o> .                          THREW NumberFormatException: "<http://a/rel>"
LF,   <s> <p> "literal" .                    THREW NumberFormatException: "<http://a/rel>"
LF,   # comment, then triples                THREW NumberFormatException: "generated"
```

| Finding | Disposition |
|---|---|
| CRLF line endings defeat RDF detection: the `size() - 1` bound in `allDelimitersAreTheSame` tolerates exactly one trailing character, and `\r` is a second one after the `.` | real, out of scope - the branch is never taken, so the delimiter never gets a chance to be dropped. Filed as **#7346** |
| A literal object (`"hello world"`) is not inside `<...>`, so its characters join the delimiter set and the uniformity test fails at index 1 | real, out of scope - same reason. Filed as **#7346** |
| A leading `#` or `//` comment is sniffed as the first data line: `skipLine()` leaves `currentChar` on `'\n'`, so both comment loops exit after one pass and `parser.reset()` rewinds past what they consumed | real, out of scope - a general sniffing defect, not RDF-specific. Filed as **#7347**. It is also why `analyzeChar`'s other two call sites are dead, which is the row argued above |
| The first triple of every N-Triples file is dropped as a header row | real, out of scope. Filed as **#7345** |
| An exotic detected delimiter (`"`, which is also univocity's quote character) would now be handed to the parser where a comma was used before | not real as a regression: such a source produced one column and the same `ArrayIndexOutOfBoundsException` before the fix. It fails either way, and no valid RDF line has that shape |
| The new tests could pass against a fix that only covers `load()` and not `analyze()` | not real: `SourceDiscovery.getSchema()` calls `analyze()` before `load()` ever runs, and with a comma the header row is one column while the data rows are three or four, so `fieldNames.get(1)` throws first. Both readers take the delimiter from the same constructor argument |

All three filed issues are named in the PR body under **Known gaps**.

## Residual risk

Two behaviours the reporter may hit next; neither is this defect and both predate it:

- RDF sources still skip their first line as a header by default (`RDFImporterFormat.load()` sets
  `skipEntries = 1` when `-edgesSkipEntries` is unset). N-Triples has no header row, so the first triple of a
  file is silently dropped unless `-edgesSkipEntries 0` is passed. Filed as **#7345**; it is why the new
  tests assert N-1 edges for an N-triple file.
- `SourceDiscovery.analyzeChar()` reads the delimiter from the first line only; a file whose first line uses a
  different separator from the rest still misparses. That is the same one-line-sample limitation the CSV
  branch has always had.
- Three shapes of canonical N-Triples still do not reach this branch at all, so the fix does not rescue them:
  CRLF line endings and literal objects (**#7346**), and files with a leading comment block (**#7347**). The
  patch is correct for every source the RDF branch is taken on; it does not widen which sources those are.
