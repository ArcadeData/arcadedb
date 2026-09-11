# #7341 - XMLImporterFormat imports `parsingLimitEntries + 1` objects

Issue: https://github.com/ArcadeData/arcadedb/issues/7341

## Finding ledger

- [x] 1. **Fixed.** `XMLImporterFormat.load():195` - `context.parsed.get() > settings.parsingLimitEntries`, evaluated
      after the object has already been handed to `database.async().createRecord(...)`, so
      `-parsingLimitEntries N` imports `N + 1` objects.
- [x] 2. **Fixed.** `XMLImporterFormat.analyze():339` - `parsedObjects > analyzingLimitEntries`, the same shape against a
      different setting. The issue asks for both to be settled together.
- [x] 3. **Updated.** `XMLImporterFormatStaleParsedCounterTest` pins the current `N + 1` behaviour (three assertions of
      `isEqualTo(3)` for a limit of 2). The issue names it as the test to update.

## Root cause

Both loops increment the object counter when an object *completes*, then test the counter with a strict `>`
at the bottom of the same iteration. For a limit of `N` the counter reaches `N` after the Nth object without
tripping the test, the loop runs one more time, and the `N+1`th object is created (`load()`) or fed to the
schema analyser (`analyze()`) before `N+1 > N` finally breaks.

`load()` is worse than a cosmetic off-by-one because the break is below
`database.async().createRecord(record, ...)`: the object that trips the limit has already been submitted.

## Completeness

### 1. Invariant

> With `-parsingLimitEntries N` (resp. `-analyzingLimitEntries N`) an importer route that enforces the
> setting processes **at most** N objects from the source, never N+1.

### 2. Enumeration

Every reader of the two settings, found by command:

```
$ grep -rn "parsingLimitEntries" --include="*.java" integration/src/main/java/
integration/.../ImporterSettings.java:70:  public long    parsingLimitEntries;
integration/.../ImporterSettings.java:186:    case "parsingLimitEntries" -> parsingLimitEntries = Long.parseLong(value);
integration/.../SourceDiscovery.java:96:   (log line only)
integration/.../format/XMLImporterFormat.java:53:  (comment)
integration/.../format/XMLImporterFormat.java:195: if (settings.parsingLimitEntries > 0 && context.parsed.get() > settings.parsingLimitEntries)
integration/.../vector/TextEmbeddingsImporterLSM.java:251: if (settings.parsingLimitEntries > 0)
integration/.../vector/TextEmbeddingsImporterLSM.java:252:   parser = parser.limit(settings.parsingLimitEntries);

$ grep -rn "analyzingLimitEntries" --include="*.java" .
integration/.../format/XMLImporterFormat.java:210: final int analyzingLimitEntries = settings.getIntValue("analyzingLimitEntries", 0);
integration/.../format/XMLImporterFormat.java:339: if (analyzingLimitEntries > 0 && parsedObjects > analyzingLimitEntries)
```

Sibling grep - the *shape* (a strict `>` against a `*Limit*` value), 15 hits in the importer module:

```
$ grep -rnE "> *(settings\.)?[A-Za-z]*[Ll]imit[A-Za-z]*\b" --include="*.java" integration/src/main/java/ | wc -l
15
```

The 15 break down as: 4 are `ImporterSettings` switch arms (assignment, not a comparison), 3 are unrelated
`Delimiter`/`delimiters` identifiers, 3 are `Parser.java:108/118/128` (`position.get() > limit`, a **byte**
offset, where one byte of overshoot is not an entry count), 2 are the two sites this PR fixes, 1 is
`CSVImporterFormat:876` (`analysisLimitBytes`, bytes again), 1 is `CSVImporterFormat:879`
(`analysisLimitEntries`, argued below) and 1 is the `analysisLimitEntries` switch arm.

Formats that never read `parsingLimitEntries` at all (`CSVImporterFormat`, `JSONImporterFormat`,
`JsonlImporterFormat`, `RDFImporterFormat`, `Neo4jImporterFormat`, `OrientDBImporterFormat`,
`GloVeImporterFormat`, `Word2VecImporterFormat`, `Word2VecImporterFormatLSM`) cannot violate an off-by-one
they do not implement - they violate a *different* property (the flag is silently ignored), filed separately.

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `XMLImporterFormat.load()` direct call, `-parsingLimitEntries N` | yes | yes |
| `Importer` CLI route (`-documents ... -parsingLimitEntries N`) → `load()` | yes | yes |
| `XMLImporterFormat.analyze()` direct call, `-analyzingLimitEntries N` | yes | yes |
| `Importer` CLI → `SourceDiscovery.getSchema()` → `analyze()`, `-analyzingLimitEntries N` | yes | yes (added by the adversarial pass) |
| `TextEmbeddingsImporterLSM.loadFromFile()` → `Stream.limit(N)` | n/a - **argued** | n/a |
| `CSVImporterFormat.analyze()` → `line > settings.analysisLimitEntries` | n/a - **argued** | n/a |
| `Parser` byte limit (`position.get() > limit`) | n/a - **argued** | n/a |
| 9 formats that ignore `parsingLimitEntries` entirely | no - **filed** #7482 | no |
| `parsingLimitBytes` parsed, logged, never enforced | no - **filed** #7482 | no |
| `analysisLimitEntries` (the documented flag) never reaches XML; XML analysis unbounded by default | no - **filed** #7485 | no |

**Argued rows, with evidence:**

- `TextEmbeddingsImporterLSM:252` uses `Stream.limit(n)`, whose contract is "at most `n` elements". It is
  already the cap this fix makes XML agree with - it is the *reference* behaviour the issue cites, not a
  violator.
- `CSVImporterFormat:879` compares a **0-based** `line` counter (`for (long line = 0; ...; ++line)`,
  `:871`) in a check placed **before** the row is consumed, and index 0 is spent on the header (either
  read from the file at `:882` when `header == null`, or skipped by `skipEntries` defaulting to 1 at
  `:834/:841/:850` when a header was supplied). Lines `1..L` are therefore the data rows analysed, i.e.
  exactly `L` of them. Pre-check plus 0-based plus a consumed index 0 cancels the `>`; there is no
  off-by-one here.
- `Parser:108/118/128` bound a **byte** position, not an entry count. `-parsingLimitBytes` is documented as
  a coarse cut-off and overshooting it by the tail of one record is its intended behaviour.

### 5. Reachability

- `XMLImporterFormat.load()` is reached from `Importer.loadFromSource()` via
  `SourceSchema.getContentImporter().load(...)`; the new CLI test drives that path end to end, so the
  changed line runs outside the unit test too.
- `XMLImporterFormat.analyze()` is reached from `SourceDiscovery.getSchema():94`
  (`formatImporter.analyze(entityType, parser, settings, analyzedSchema)`), which `Importer.load()` calls
  for every source. `analyzingLimitEntries` has no named arm in `ImporterSettings.parseParameter`, but the
  method ends with `options.put(name, value)` for *every* argument (`:227`), so `-analyzingLimitEntries 2`
  on the command line does reach `settings.getIntValue("analyzingLimitEntries", 0)`. The setting is
  undocumented, not dead.
- No feature flag gates either site; both are plain `if`s in the parse loop.

### 7. Residual risk

The fix makes the two XML limits caps. It does **not** make `-parsingLimitEntries` work on the nine
importer formats that never read it, and it does not make `-parsingLimitBytes` do anything anywhere; both
are tracked by #7482. Within XML, a source whose objects are nested deeper than `objectNestLevel` still
counts only objects at that level - unchanged by this PR and not part of the reported defect.

## Changes

| File | Change |
|---|---|
| `integration/src/main/java/com/arcadedb/integration/importer/format/XMLImporterFormat.java` | `load()`: `context.parsed.get() > settings.parsingLimitEntries` -> `>=`; `analyze()`: `parsedObjects > analyzingLimitEntries` -> `>=`. Both carry a comment saying why the comparison has to be non-strict. |
| `integration/src/test/java/com/arcadedb/integration/importer/format/XMLImporterFormatParsingLimitTest.java` | New. Six tests: the cap on `load()`, a limit of one, a limit at and above the object count, a zero (disabled) limit, the cap on `analyze()`, and the CLI route end to end. |
| `integration/src/test/java/com/arcadedb/integration/importer/format/XMLImporterFormatStaleParsedCounterTest.java` | The three assertions #7313 deliberately pinned at the old `N+1` boundary (`isEqualTo(3)` for a limit of 2) move to `isEqualTo(2)`, and the javadoc that pointed at #7341 as the reason for the three now records the cap. Nothing is deleted; #7313's own scenarios still assert what they asserted. |

### Note on the modified existing test

`constraints.md` forbids modifying existing tests. This is the one designated exception: #7341's own text
names `XMLImporterFormatStaleParsedCounterTest` as "the test to update when this is fixed", and its javadoc
at the time said the same. The assertions there pinned the defect on purpose so that #7313's counter reset
could not silently move the boundary; the boundary is exactly what this PR moves, so leaving them would
have made the branch red with no way to be both fixed and green. The scenarios, their names and their
coverage are untouched; only the three pinned numbers and the comment explaining them changed.

## Test results

```
$ mvn -o -pl integration test -Dtest='XMLImporterFormatParsingLimitTest,XMLImporterFormatStaleParsedCounterTest'
  (before the fix) Tests run: 6, Failures: 4   - all four bug-reproducing tests red, both regression guards green
  (after the fix)  Tests run: 9, Failures: 0

$ mvn -o -pl integration test -DexcludedGroups=benchmark,vector,slow
  Tests run: 373, Failures: 0, Errors: 0, Skipped: 9 - BUILD SUCCESS (re-run after the adversarial pass added a test)
```

No test outside `integration` touches the XML importer or either limit:

```
$ grep -rln "XMLImporter\|parsingLimitEntries" --include="*.java" --exclude-dir=integration . | grep -i test
(no output)
```

## Impact

`-parsingLimitEntries N` on an XML source now imports N objects instead of N+1, and
`-analyzingLimitEntries N` samples N objects instead of N+1. This is a behaviour change for anyone who was
relying on the old count, but the old count was never the documented one and never agreed with the vector
route that applies the same flag. A limit of 0 (the default, "no limit") and a limit the source never
reaches are both unaffected, each pinned by its own test.

## Adversarial pass

The skill asks for a `general-purpose` subagent that has not seen the reasoning. **The `Task` tool is not
available in this environment**, so the pass was run directly against the staged diff and the issue text
instead of the tracking doc. That is a weaker pass than the skill intends - it is the author re-reading his
own patch - and it is recorded as such. Three findings, all verified by reading the tree:

1. **The coverage table claimed a test it did not have.** The row
   `SourceDiscovery.getSchema()` -> `analyze()` was marked "covered by a test", but the only CLI test
   (`theCliRouteAppliesTheLimitAsACap`) passes `-parsingLimitEntries` and never sets an analyse limit, so
   nothing drove the *analyse* boundary through its live caller - exactly the hole section 4 of the
   checklist exists to catch. **Fixed here:** `theCliRouteAppliesTheAnalyzingLimitAsACap` runs
   `Importer.load()` with `-analyzingLimitEntries 2` and asserts on the schema
   `AbstractImporter.updateDatabaseSchema()` builds from the analysed properties. Reverting the `analyze()`
   half of the fix turns it red ("Expecting value to be false but was true"), so it tests the boundary and
   not the plumbing.
2. **The analyse phase reads a setting that is not the documented one.** `XMLImporterFormat.analyze()`
   reads `analyzingLimitEntries` out of the options map with a default of `0`, while `ImporterSettings` has
   `analysisLimitEntries` (default `10000`, real CLI arm) that only `CSVImporterFormat` honours. So
   `-analysisLimitEntries` does nothing to an XML import, and XML schema analysis reads the whole file by
   default where CSV stops at 10000 rows. Real, and a behaviour change too big to fold into an off-by-one.
   **Filed as #7485.**
3. **`parsedStructure` in `analyze()` is dead.** Declared at `:236`, assigned at `:303-304`, never read.
   Verified by grep (three hits, no reader). Pre-existing, invisible to users, untouched by this patch, and
   too small to be worth a tracker entry - recorded here rather than filed, so the next reader does not
   spend the same five minutes on it.

Checked and **not** real:

- *"`>=` breaks the disabled case."* No: both guards keep their `limit > 0` prefix, and
  `aZeroLimitMeansNoLimit` imports all four objects.
- *"`>=` now drops the last object when the limit equals the object count."* No: the break happens after
  the Nth object has been created, so a limit of 4 on 4 objects still imports 4. Pinned by
  `aLimitAtOrAboveTheObjectCountImportsEverything`, which also covers a limit the source never reaches.
- *"Breaking one event earlier skips `waitCompletion()` / `endParsing()`."* No: both sit after the loop,
  not inside it (`load():203`, `analyze():355`), and the 372-test module run is green.

## Review cycles

### Cycle 1 - `f09ccbd`

`claude` reviewed the commit and closed with "correct, minimal, well-tested fix [...] Nothing blocking." It
confirmed the root cause from the source (the increment and `createRecord(...)` are in the same
`if (nestLevel == objectNestLevel)` block, both ahead of the bottom-of-loop check), confirmed the disabled
(`0`) case and the limit-at-or-above-count case are unaffected, and endorsed filing #7482 and #7485 rather
than folding them in. It also noted it could not run `mvn` in its sandbox, so its pass was a code read.

One nit, **applied**: the two CLI tests built their argument array with
`("-documents file://" + path + " ...").split(" ")` against an absolute path, which would mis-tokenise on a
checkout directory containing a space. Both now pass a literal `new String[] { ... }` to `Importer`, which
removes the hazard entirely. Not manufactured into a space-containing path in the test itself: that would
be exercising URL handling rather than the limit, and a pre-existing limitation there would turn this PR
red for an unrelated reason.

Two remarks recorded and **not** acted on, with reasons:

- *"the tracking doc is 196 lines, the bulk of the diff, for a two-character fix."* Observation, not a
  request - the reviewer says it follows the repo's existing `docs/<issue>-<slug>.md` convention and
  raises no objection. The line count is the completeness sweep the workflow requires; shrinking it would
  delete the evidence for the "argued" rows.
- *"could not run mvn."* Nothing to act on in the branch. The numbers in this doc come from runs in this
  worktree, not from the reviewer's sandbox.

No inline review comments and no `pulls/7486/reviews` entries were posted on this commit; CodeRabbit's only
comment was its in-progress status notice.
