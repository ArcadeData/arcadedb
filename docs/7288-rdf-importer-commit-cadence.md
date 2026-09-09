# Issue #7288 - RDFImporterFormat.load never reaches its mid-loop commit

Reported: an RDF import runs as one transaction whatever `-commitEvery` says.

## Finding ledger

- [x] 1. `context.parsed` is incremented twice per row, so the `% commitEvery` boundary is always odd and never fires for the (even) default `commitEvery = 5000` - **fixed**: the second increment is gone and the boundary is measured against a loop-local `txCount`.
- [x] 2. `context.parsed` is not reset on entry to `load()`, unlike every sibling format, so an earlier import phase shifts the boundary by an arbitrary offset - **fixed**: `context.parsed.set(0)` on entry, and the boundary no longer reads that counter at all.
- [x] 3. (Issue's "Related" bullet) the trailing `database.commit()` is unconditional and commits a caller-supplied transaction on the success path - **fixed**: gated on `ownsTransaction`, matching `CSVImporterFormat.loadDocuments()`.

## Root cause

`RDFImporterFormat.load()` derives its commit boundary from `context.parsed`, an import-wide
progress counter shared by every phase of the same `Importer.load()` run, instead of from a
per-loop count of the rows this loop has processed. Two independent things then break the
boundary: the counter advances twice per row (so it is out of step with the row count by a
factor of two plus the header offset), and it is not zeroed on entry (so it carries whatever an
earlier phase left in it).

## Completeness

### Invariant

> In `RDFImporterFormat.load()`, exactly one intermediate commit runs for every
> `settings.commitEvery` edges the loop creates - independent of `commitEvery`'s parity, of the
> header rows skipped, and of what an earlier import phase left in `context.parsed` - and no
> commit at all runs when the transaction belongs to the caller.

### Enumeration

Formats that override `load()`, and whether they reset the shared counter:

```
$ for f in integration/.../format/*.java; do grep -q "public void load(" $f && \
    (grep -q "context.parsed.set(0)" $f && echo "  RESET   $f" || echo "  NO-RESET $f"); done
  RESET    CSVImporterFormat.java
  RESET    GloVeImporterFormat.java
  NO-RESET JSONImporterFormat.java
  NO-RESET JsonlImporterFormat.java
  RESET    Neo4jImporterFormat.java
  RESET    OrientDBImporterFormat.java
  NO-RESET RDFImporterFormat.java
  RESET    Word2VecImporterFormat.java
  RESET    Word2VecImporterFormatLSM.java
  NO-RESET XMLImporterFormat.java
```

Increments of the counter per file - RDF is the only one that increments more than once inside a
single row loop (CSV's 3 are one each in `loadDocuments`/`loadVertices`/`loadEdges`):

```
$ grep -c "context.parsed.incrementAndGet()" integration/.../format/*.java
CSVImporterFormat.java:3
JSONImporterFormat.java:1
XMLImporterFormat.java:1
RDFImporterFormat.java:2
JsonlImporterFormat.java:1
```

Every reader of the counter:

```
$ grep -rn "context.parsed.get()" integration/src/main/java
Neo4jImporter.java:369            % 1_000_000 == 0   -> progress log only
Neo4jImporter.java:488            % 1_000_000 == 0   -> progress log only
CSVImporterFormat.java:232/507/681                   -> log only
JsonlImporterFormat.java:185      % COMMIT_EVERY == 0 -> COMMIT BOUNDARY
XMLImporterFormat.java:189        > parsingLimitEntries -> PARSE LIMIT
RDFImporterFormat.java:110        % settings.commitEvery == 0 -> COMMIT BOUNDARY (the bug)
FormatImporter.java:55/67/78                          -> progress log only
```

The counter is shared across phases because one `ImporterContext` serves the whole run -
`Importer.load()` calls `loadFromSource()` four times (`url`, `documents`, `vertices`, `edges`)
against the same `context` (`Importer.java:78-81`), and `loadFromSource()` ends in
`format.load(sourceSchema, entityType, parser, database, context, settings)`
(`Importer.java:139`). Any phase after the first therefore enters with a non-zero counter.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `RDFImporterFormat.load()`, importer-owned tx, **even** `commitEvery` (the reported default) | yes | yes - `anEvenCommitEveryStillCommitsMidFile` |
| `RDFImporterFormat.load()`, importer-owned tx, **odd** `commitEvery` (committed at ~half the requested cadence) | yes | yes - `anOddCommitEveryCommitsOnTheRequestedCadenceNotHalfOfIt` |
| `RDFImporterFormat.load()` entered with a non-zero `context.parsed` from an earlier phase | yes | yes - `aCounterLeftBehindByAnEarlierPhaseDoesNotShiftTheCommitBoundary` |
| `RDFImporterFormat.load()` with a caller-owned transaction - no intermediate commit, and no trailing one either | yes (finding 3) | yes - `aCallerOwnedTransactionIsNeverCommittedByTheImport` |
| `Importer` CLI pipeline (`-url x.nt -commitEvery N`) -> `loadFromSource()` -> `RDFImporterFormat.load()` | yes | yes - `theCliPipelineCountsEachSourceRowExactlyOnce` |
| Sibling `JsonlImporterFormat:185` - commit boundary off the same unreset counter | no - filed as **#7313** | n/a |
| Sibling `XMLImporterFormat:189` - `parsingLimitEntries` off the same unreset counter | no - filed as **#7313** | n/a |
| Sibling `JSONImporterFormat:274` - increments, never reads | argued: it has no gate on the counter, so a stale value can only make the reported `parsedRecords` wrong, never change what the import does. Grep of every reader above shows no `JSONImporterFormat` row. | n/a |
| Sibling `Neo4jImporter:369/488`, `CSVImporterFormat:232/507/681`, `FormatImporter:55/67/78` | argued: log statements only, per the reader grep above | n/a |
| `SourceDiscovery` RDF branch drops the delimiter it detected, so a space-delimited `.nt` file never parses | no - filed as **#7315** (found by the end-to-end test below, which uses comma-delimited triples to stay off it) | n/a |

### Reachability

`RDFImporterFormat` is constructed on a live path: `SourceDiscovery.java:485` returns
`new RDFImporterFormat()` from content sniffing, and `Importer.loadFromSource()` calls
`format.load(...)` on whatever it returns. No feature flag gates it. `settings.commitEvery` is
read inside the loop on every iteration, so `-commitEvery` reaches it.

### Residual risk

The two sibling formats that read the same unreset counter for a real decision (Jsonl's commit
boundary, XML's parse limit) are not changed here - both are single-increment-per-row, so the
parity half of the defect does not apply to them, and only the stale-offset half does. Filed
rather than fixed to keep this PR to the format the issue names.

## Changes

`integration/src/main/java/com/arcadedb/integration/importer/format/RDFImporterFormat.java`

1. `context.parsed.set(0)` at the top of `load()`, the way the six sibling formats that reset it do.
2. The second `context.parsed.incrementAndGet()` (the one after the edge was created) is removed, so
   each source row is counted once.
3. A loop-local `int txCount` counts edges created since the last commit; the boundary is
   `txCount >= settings.commitEvery`, reset to `0` after each commit. This is the same shape
   `CSVImporterFormat.loadEdges()` already uses, and it decouples the cadence from the shared
   progress counter entirely - so neither the header skip, nor `commitEvery`'s parity, nor an
   inherited offset can move it.
4. The trailing `database.commit()` is gated on `ownsTransaction`, matching the periodic commit above
   it and `CSVImporterFormat.loadDocuments()`'s trailing commit.

`integration/src/test/java/com/arcadedb/integration/importer/format/RDFImporterFormatCommitCadenceTest.java` (new)

Five tests, one per fixed row of the coverage table.

## Test results

Before the fix, all five new tests failed - and for the stated reason, not incidentally:

```
[ERROR] Tests run: 5, Failures: 4, Errors: 1
  anEvenCommitEveryStillCommitsMidFile                        expected: 6L but was: 0L
  anOddCommitEveryCommitsOnTheRequestedCadenceNotHalfOfIt     expected: 3L but was: 1L
  aCounterLeftBehindByAnEarlierPhaseDoesNotShiftTheCommitBoundary  expected: 6L but was: 0L
  aCallerOwnedTransactionIsNeverCommittedByTheImport          Expecting value to be true but was false
  theCliPipelineCountsEachSourceRowExactlyOnce                expected: 5L but was: 9L
```

`9L` for a five-row source is the double increment stated exactly: one header row plus two per data
row, `1 + 2*4`.

After the fix, the whole `integration` module, unit tests and integration tests:

```
$ mvn -o -pl integration verify -DskipITs=false -DexcludedGroups=benchmark,vector
[INFO] Tests run: 305, Failures: 0, Errors: 0, Skipped: 9      (surefire)
[INFO] Tests run: 123, Failures: 0, Errors: 0, Skipped: 0      (failsafe)
[INFO] BUILD SUCCESS
```

`RDFImporterFormatTransactionLeakTest`, the four tests #7272 left on this same method, are among the
305 and stay green.

## Impact

An RDF import now honours `-commitEvery`. Before this, every `.rdf`/`.nt` source accumulated its whole
edge set in one transaction whatever the setting said, which is the memory-exhaustion failure mode
`commitEvery` exists to prevent; and a failure mid-file discarded the entire import rather than the
last incomplete batch. Callers that hand the importer their own transaction now get it back unresolved
on the success path as well as on the failure path.

## Scope of this fix

Only `RDFImporterFormat`. `RDFImporterFormat` is referenced from exactly two non-test files, both in
this module:

```
$ grep -rl RDFImporterFormat . | grep '\.java$' | grep -v /target/
integration/src/test/.../RDFImporterFormatTransactionLeakTest.java
integration/src/test/.../RDFImporterFormatCommitCadenceTest.java
integration/src/main/.../SourceDiscovery.java
integration/src/main/.../format/RDFImporterFormat.java
```

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent deliberately kept ignorant of the author's reasoning.
The `Task` tool is disabled in this session, so that could not run and the pass was made by the author
instead - which is the weaker version of it, and is recorded as such. Three findings, all fixed here:

1. **An exhaustive claim in a comment that the code does not hold.** The first draft said the value
   reaching the modulo "was always odd". It is `2N+1` only under the default one-line header skip;
   `-edgesSkipEntries 0` makes it `2N`, even, in which case the old code committed on every
   `commitEvery/2` rows rather than never. Fixed: the comment now names the default it depends on.

2. **`txCount` could grow without bound on the caller-owned path.** With `ownsTransaction == false`
   the boundary never fires, so nothing would ever have reset the counter and it would overflow on a
   source of more than 2^31 rows. Harmless - the guard is false on that path either way - but it is a
   counter with no bound. Fixed: the increment moved inside the `ownsTransaction` guard
   (`ownsTransaction && ++txCount >= settings.commitEvery`), so it only counts where it is read.

3. **The end-to-end test would have passed on an import that created nothing.** It asserted only
   `parsedRecords`, which is a row count: an import that parsed every row and created no edge at all
   would have satisfied it. Fixed: it now also asserts `createdEdges == 4` (four of the five triples;
   the first is skipped as the header row RDF sources default to).

Not found: any path that reaches the changed lines and is not in the coverage table, and any claim in
the new comments that a command in this document does not back.

## Progress-counter side effect, stated rather than claimed away

`FormatImporter.printProgress()` prints `(context.parsed.get() - context.lastParsed) / deltaInSecs` as
a rate. Zeroing `context.parsed` on entry means the first tick of a non-first phase can compute that
rate against a larger `lastParsed` and print a negative number once. This is not new here: the six
sibling formats listed above already zero the same counter at the same point, so an import whose first
phase is CSV and whose second is anything already has it. Left alone deliberately - changing it means
changing what the progress line reports for every format, which is its own change.
