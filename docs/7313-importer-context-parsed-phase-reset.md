# #7313 - `JsonlImporterFormat` and `XMLImporterFormat` take a decision off `context.parsed` without resetting it

Issue: https://github.com/ArcadeData/arcadedb/issues/7313
Branch: `fix/7313-importer-context-parsed-reset`

## Finding ledger

- [x] 1. `JsonlImporterFormat:185` - the periodic commit boundary (`context.parsed.get() % COMMIT_EVERY == 0`) is
      measured against an import-wide counter, so a phase entered with a stale offset `k` commits after
      `COMMIT_EVERY - (k % COMMIT_EVERY)` records instead of after `COMMIT_EVERY`.
- [x] 2. `XMLImporterFormat:189` - the parse limit (`context.parsed.get() > settings.parsingLimitEntries`) is
      measured against the same counter, so a stale offset `k` truncates the phase after
      `parsingLimitEntries - k` objects, and when `k >= parsingLimitEntries` the phase imports nothing while
      reporting success. **Fixed** - `context.parsed.set(0)` on entry to `load()`.

## Root cause

`ImporterContext` is created once per `Importer.load()` and shared by all four `loadFromSource()` phases
(`Importer.java:78-81`), each of which ends in `format.load(..., context, ...)` (`Importer.java:139`).
`context.parsed` is therefore a running total across phases. Seven formats zero it on entry to `load()` for
exactly that reason; the two above do not, and both take a real decision off the value.

## Completeness

### 1. The invariant

> A decision a format's `load()` takes off `context.parsed` - a commit boundary, a parse limit - counts only the
> rows that this `load()` call itself parsed, never rows an earlier `loadFromSource()` phase of the same
> `Importer.load()` left in the shared counter.

### 2. Enumerate every way to violate it

Every writer and reader of the counter, repo-wide, main code only:

```
$ grep -rn 'context\.parsed' --include='*.java' . | grep -v '/src/test/'
integration/.../Neo4jImporter.java:367:            context.parsed.incrementAndGet();
integration/.../Neo4jImporter.java:369:            if (context.parsed.get() > 0 && context.parsed.get() % 1_000_000 == 0) {
integration/.../Neo4jImporter.java:486:          context.parsed.incrementAndGet();
integration/.../Neo4jImporter.java:488:          if (context.parsed.get() > 0 && context.parsed.get() % 1_000_000 == 0) {
integration/.../format/Word2VecImporterFormat.java:47:    context.parsed.set(0);
integration/.../format/XMLImporterFormat.java:126:            context.parsed.incrementAndGet();
integration/.../format/XMLImporterFormat.java:189:        if (settings.parsingLimitEntries > 0 && context.parsed.get() > settings.parsingLimitEntries)
integration/.../format/RDFImporterFormat.java:49:    context.parsed.set(0);
integration/.../format/RDFImporterFormat.java:96:        context.parsed.incrementAndGet();
integration/.../format/Word2VecImporterFormatLSM.java:47:    context.parsed.set(0);
integration/.../format/Neo4jImporterFormat.java:39:    context.parsed.set(0);
integration/.../format/GloVeImporterFormat.java:47:    context.parsed.set(0);
integration/.../OrientDBImporter.java:410:    context.parsed.set(0);
integration/.../OrientDBImporter.java:418:        context.parsed.incrementAndGet();
integration/.../OrientDBImporter.java:784:    context.parsed.incrementAndGet();
integration/.../format/JSONImporterFormat.java:274:    context.parsed.incrementAndGet();
integration/.../format/OrientDBImporterFormat.java:39:    context.parsed.set(0);
integration/.../format/CSVImporterFormat.java:99:    context.parsed.set(0);
integration/.../format/CSVImporterFormat.java:176:        context.parsed.incrementAndGet();
integration/.../format/CSVImporterFormat.java:232:      ... log ... context.parsed.get());
integration/.../format/CSVImporterFormat.java:423:        context.parsed.incrementAndGet();
integration/.../format/CSVImporterFormat.java:507:      ... log ... context.parsed.get());
integration/.../format/CSVImporterFormat.java:605:          context.parsed.incrementAndGet();
integration/.../format/CSVImporterFormat.java:681:      ... log ... context.parsed.get());
integration/.../format/FormatImporter.java:55:            context.parsed.get(), (context.parsed.get() - context.lastParsed) / deltaInSecs, ...
integration/.../format/FormatImporter.java:67:            context.parsed.get(), (context.parsed.get() - context.lastParsed) / deltaInSecs, ...
integration/.../format/FormatImporter.java:78:      context.lastParsed = context.parsed.get();
```

Every concrete `FormatImporter`, so no format is missed by reading only the ones that mention the field:

```
$ grep -rn '^public class .*ImporterFormat' --include='*.java' integration/src/main/java gremlin/src/main/java
integration/.../CSVImporterFormat.java:67:public class CSVImporterFormat extends AbstractImporterFormat {
integration/.../Word2VecImporterFormat.java:40:public class Word2VecImporterFormat extends AbstractImporterFormat {
integration/.../Word2VecImporterFormatLSM.java:40:public class Word2VecImporterFormatLSM extends AbstractImporterFormat {
integration/.../JsonlImporterFormat.java:74:public class JsonlImporterFormat extends AbstractImporterFormat {
integration/.../XMLImporterFormat.java:44:public class XMLImporterFormat implements FormatImporter {
integration/.../Neo4jImporterFormat.java:34:public class Neo4jImporterFormat extends AbstractImporterFormat {
integration/.../JSONImporterFormat.java:65:public class JSONImporterFormat implements FormatImporter {
integration/.../RDFImporterFormat.java:36:public class RDFImporterFormat extends CSVImporterFormat {
integration/.../GloVeImporterFormat.java:40:public class GloVeImporterFormat extends AbstractImporterFormat {
integration/.../OrientDBImporterFormat.java:34:public class OrientDBImporterFormat extends AbstractImporterFormat {
gremlin/.../GraphSONImporterFormat.java:59:public class GraphSONImporterFormat extends CSVImporterFormat {
gremlin/.../GraphMLImporterFormat.java:36:public class GraphMLImporterFormat extends CSVImporterFormat {
```

Seven of the ten `integration` formats zero the counter on entry to `load()`; `OrientDBImporter.parseRecords()`
is an eighth reset site outside a format:

```
$ grep -rn 'parsed\.set(0)' --include='*.java' . | grep -v '/src/test/'
integration/.../OrientDBImporter.java:410:    context.parsed.set(0);
integration/.../format/Word2VecImporterFormat.java:47:    context.parsed.set(0);
integration/.../format/Word2VecImporterFormatLSM.java:47:    context.parsed.set(0);
integration/.../format/Neo4jImporterFormat.java:39:    context.parsed.set(0);
integration/.../format/CSVImporterFormat.java:99:    context.parsed.set(0);
integration/.../format/RDFImporterFormat.java:49:    context.parsed.set(0);
integration/.../format/OrientDBImporterFormat.java:39:    context.parsed.set(0);
integration/.../format/GloVeImporterFormat.java:47:    context.parsed.set(0);
```

The three that do not are `JsonlImporterFormat`, `XMLImporterFormat` and `JSONImporterFormat`. Both gremlin
formats override `load()` (`GraphMLImporterFormat:38`, `GraphSONImporterFormat:67`) rather than inheriting
`CSVImporterFormat`'s, so neither inherits its reset - and neither needs one:

```
$ grep -rn 'context\.parsed' --include='*.java' gremlin/
(no output)
```

The same *shape* elsewhere - a decision taken off `settings.parsingLimitEntries`:

```
$ grep -rn 'parsingLimitEntries' --include='*.java' . | grep -v '/src/test/'
integration/.../ImporterSettings.java:70:  public long    parsingLimitEntries;
integration/.../ImporterSettings.java:186:    case "parsingLimitEntries" -> parsingLimitEntries = Long.parseLong(value);
integration/.../SourceDiscovery.java:96:        ... log ...
integration/.../format/XMLImporterFormat.java:189: (the site under fix)
integration/.../vector/TextEmbeddingsImporterLSM.java:251-252: parser = parser.limit(settings.parsingLimitEntries);
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `JsonlImporterFormat.load()` -> commit boundary at `:185` | yes | yes - `JsonlImporterFormatStaleParsedCounterTest` (direct `load()` with an injected stale offset, plus the two-phase `Importer` CLI route) |
| `XMLImporterFormat.load()` -> parse limit at `:189` | yes | yes - `XMLImporterFormatStaleParsedCounterTest` (direct `load()` with an injected stale offset, plus the two-phase `Importer` CLI route) |
| `JSONImporterFormat:274` - increments, never resets | argued | n/a |
| `FormatImporter#printProgress:55/67/78` - reads the counter | argued | n/a |
| `Neo4jImporter:369/488` - `% 1_000_000` on the counter | argued | n/a |
| `OrientDBImporter:410/418/784` | argued | n/a |
| `GraphMLImporterFormat`, `GraphSONImporterFormat` (gremlin) | argued | n/a |
| `TextEmbeddingsImporterLSM:251` - the other `parsingLimitEntries` reader | argued | n/a |

Arguments, each backed by the greps above:

- **`JSONImporterFormat:274`** - the single occurrence of `context.parsed` in the file is an
  `incrementAndGet()`; the file contains no `context.parsed.get()` at all (`grep -c 'context\.parsed\.get' JSONImporterFormat.java` -> `0`), so no decision is taken off the value and a
  stale offset cannot change what the format imports. It leaves the run's reported `parsedRecords` cumulative
  rather than phase-scoped, which is a reporting inconsistency with the formats that do reset - filed as a
  follow-up rather than changed here, because it is a behaviour change to the report and not a defect in what
  gets imported.
- **`FormatImporter#printProgress`** - guarded by `settings.verboseLevel < 2` and does nothing but format a log
  line; `context.lastParsed` briefly exceeding `context.parsed` after a reset yields a negative rate in that log
  line, which is already the case for the seven formats that reset today. Cosmetic, no decision.
- **`Neo4jImporter:369/488`** - the modulo gates a `log(...)` status line and nothing else, and the format
  wrapper that reaches this code (`Neo4jImporterFormat:39`) already zeroes the counter on entry.
- **`OrientDBImporter`** - `parseRecords()` zeroes the counter at `:410` before its own loop, and neither
  `:418` nor `:784` is read back for a decision (`grep -c 'context\.parsed\.get' OrientDBImporter.java` -> `0`).
- **`GraphMLImporterFormat` / `GraphSONImporterFormat`** - the `context.parsed` grep returns zero hits in `gremlin/`,
  so neither format reads or writes the counter at all, even though both override `load()`.
- **`TextEmbeddingsImporterLSM:251`** - applies the limit through `parser.limit(...)`, a byte/entry limit on the
  `Parser` itself, never through `context.parsed`.

### 4. Reachability

`SourceDiscovery.analyzeSourceContent()` returns `new JsonlImporterFormat()` for a `jsonl` file type and
`new XMLImporterFormat()` for an `xml` file type (`SourceDiscovery.java:289-293`) under any entity type, so both
are reached by `-documents` / `-vertices` / `-url` in the same run as an earlier phase. Both fixed sites are on
the live `Importer.load()` path, and each format has a CLI-route test driving a genuine two-phase import.

### 5. Residual risk

The fix makes `context.parsed` phase-scoped for the two formats that take a decision off it, matching the seven formats
that already reset it. It does **not** make the counter phase-scoped repo-wide: `JSONImporterFormat`
still accumulates across phases (reporting only - see the argument above), which is tracked as a follow-up.

## Changes

- `integration/src/main/java/com/arcadedb/integration/importer/format/JsonlImporterFormat.java` -
  `context.parsed.set(0)` on entry to `load()`, with a comment naming the seven siblings that already do it and
  the decision it protects.
- `integration/src/main/java/com/arcadedb/integration/importer/format/XMLImporterFormat.java` - the same, for
  the `-parsingLimitEntries` check.
- `integration/src/test/java/com/arcadedb/integration/importer/format/JsonlImporterFormatStaleParsedCounterTest.java` - new.
- `integration/src/test/java/com/arcadedb/integration/importer/format/XMLImporterFormatStaleParsedCounterTest.java` - new.

The reset is a strict no-op for a single-source import: `context.parsed` is zero when the first phase's
`load()` runs, and nothing between phases advances it - `FormatImporter#analyze` does not receive an
`ImporterContext` at all (`FormatImporter.java:37`), and `AbstractImporter` never names `context.parsed`
(`grep -n 'parsed' AbstractImporter.java` returns only the unrelated `dumpSchema(..., parsedObjects)`
parameter at `:260/:262`). The only behaviour that changes is a run with more than one `loadFromSource()`
phase.

## Test results

Each fixed row has a test that fails on `main` and passes with the fix, on both the direct `load()` call and
the live `Importer.load()` CLI route:

Before the fix (`mvn -o -pl integration -Dtest='JsonlImporterFormatStaleParsedCounterTest,XMLImporterFormatStaleParsedCounterTest' test`):

```
Tests run: 6, Failures: 3, Errors: 1, Skipped: 0
  JsonlImporterFormatStaleParsedCounterTest.aCounterLeftBehindByAnEarlierPhaseDoesNotShiftTheCommitBoundary
    expected: 0L but was: 1L
  JsonlImporterFormatStaleParsedCounterTest.theTwoPhaseCliRouteScopesTheCounterToTheJsonlPhase
    expected: 2L but was: 5L
  XMLImporterFormatStaleParsedCounterTest.aCounterLeftBehindByAnEarlierPhaseDoesNotTruncateTheParseLimit
    Schema Type with name 'item' was not found   (the phase imported nothing at all)
  XMLImporterFormatStaleParsedCounterTest.theTwoPhaseCliRouteScopesTheLimitToTheXmlPhase
    expected: 3L but was: 0L
```

The two that already passed are the deliberate controls - `theBoundaryStillFiresAtCommitEveryRecords` and
`theParseLimitStillStopsTheImport` - which hold both before and after, so the fix cannot be "never commit
mid-file" or "ignore the limit".

After the fix: `Tests run: 6, Failures: 0, Errors: 0`.

Whole-module regression run, `mvn -o -pl integration -DexcludedGroups=benchmark,vector test`:

```
Tests run: 325, Failures: 0, Errors: 0, Skipped: 9
BUILD SUCCESS
```

`Issue6946AutoDetectDelimiterOptionTest` is the pre-existing multi-phase (`-documents` + `-vertices`) test in
that run; both of its phases are CSV, which already reset, so it is unaffected and green.

## Impact

- A jsonl `-documents`/`-vertices`/`-url` phase that follows another phase now commits every `COMMIT_EVERY`
  records of its own instead of at an arbitrary offset into the first batch.
- An XML phase that follows another phase now gets its own `-parsingLimitEntries` budget instead of an
  already-spent one - previously it could import nothing at all and still report success.
- `parsedRecords` in `Importer.load()`'s returned map is now the last phase's own count for jsonl and XML
  runs, as it already was for the seven formats that reset. Single-source imports are unchanged.

## Adversarial pass

No isolated `Task` subagent was available in this session (the tool is not exposed here), so the pass was run
by hand against the diff and the issue body. Findings and dispositions:

1. **`-parsingLimitEntries` imports `limit + 1` objects** - the check is a strict `>` taken after the record
   has been created, so a limit of two imports three. Real, verified by the new tests' own counts, and
   untouched by this fix. **Filed as #7341**, and both new XML tests carry a comment pointing at it so the
   `isEqualTo(3)` is not mistaken for the intended contract.
2. **`JSONImporterFormat` still never resets the counter** - real, but it takes no decision off the value
   (`grep -c 'context.parsed.get' JSONImporterFormat.java` -> `0`), so it is a reporting inconsistency rather
   than the defect class fixed here. **Filed as #7342**, together with the reporter's own "or, better" option
   of hoisting the reset into `Importer.loadFromSource()`.
3. **`FormatImporter#printProgress` prints a negative rate on the first tick after a reset** - real
   (`context.lastParsed` is not reset alongside `parsed`), cosmetic, gated behind `verboseLevel >= 2`, and
   already true for the seven formats that reset today. Folded into #7342 rather than filed on its own,
   because the two are the same decision.
4. **"The fix could be a per-phase local counter instead of a reset"** - not a defect in the patch. Both
   shapes establish the invariant; the reset is what the seven siblings and #7288's `RDFImporterFormat:49` do,
   and `JsonlImporterFormat`/`XMLImporterFormat` each increment `context.parsed` at exactly one site
   (`JsonlImporterFormat:177`, `XMLImporterFormat:126`), so after the reset the modulo and the limit are exact.
5. **"The reset could break a single-source import"** - not real. See the Changes section: the counter is
   already zero there, so the statement is a no-op. The 325-test module run is the evidence.
