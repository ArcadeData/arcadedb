# #7267 - `CsvRowSource` splits rows with a regex, so a metacharacter delimiter shreds or annihilates every row

Issue: https://github.com/ArcadeData/arcadedb/issues/7267

## Root cause

`integration/src/main/java/com/arcadedb/integration/importer/graph/CsvRowSource.java:96`

```java
return line.split(String.valueOf(delimiter), -1);
```

`String.split(String, int)`'s first argument is a **regular expression**. The delimiter is a `char` chosen by the
operator, so any `char` that is a regex metacharacter is compiled as that metacharacter instead of as itself.

## Reproduction (measured, not recalled)

`"a<d>b<d>c".split(String.valueOf(d), -1)` on JDK 21:

| delimiter | result | family |
|---|---|---|
| `;` `,` `]` `}` `-` | `[a, b, c]` | correct |
| `\|` | `[a, \|, b, \|, c, ]` | **silent** - alternation of two empty branches, every char is its own field |
| `.` | `[, , , , , ]` | **silent** - the row is annihilated |
| `$` | `[a$b$c, ]` | **silent** - end-anchor, no split at all |
| `^` | `[a^b^c]` | **silent** - start-anchor, no split at all |
| `*` `+` `?` `{` | `PatternSyntaxException: Dangling meta character` / `Illegal repetition` | crash |
| `(` `)` `[` | `PatternSyntaxException: Unclosed group` / `Unmatched closing ')'` / `Unclosed character class` | crash |
| `\` | `PatternSyntaxException: Unescaped trailing backslash` | crash |

The silent family is the one the issue is about: the header line is split into single characters, so every
`record.get(attribute)` misses, and the import produces property-less vertices, or none at all, saying nothing.
The crashing family is not benign either - a `PatternSyntaxException` mentions a regex the operator never wrote.

## Completeness

### Invariant

**A `CsvRowSource` splits each line on the literal delimiter character it was given, for every one of the 65536
possible `char` values, and never interprets that character as a regular expression.**

### Every way to violate it

```shell
$ grep -rn "split(String.valueOf(" --include="*.java" . | grep -v /target/
integration/src/main/java/com/arcadedb/integration/importer/graph/CsvRowSource.java:96:    return line.split(String.valueOf(delimiter), -1);
```

One hit in the whole tree: the reported line. Splitting is funnelled through the private `splitLine`, which both
the header read and the per-row read call, so a single call site carries both.

```shell
$ grep -rn "\.split(" integration/src/main/java/
.../ImportSecurityValidator.java:162:    for (final String dir : allowed.split(",")) {
.../graph/GraphImporter.java:316:        final String[] parts = spec.split("=", 2);
.../graph/GraphImporter.java:408:    final String[] parts = value.split(":");
.../graph/CsvRowSource.java:96:    return line.split(String.valueOf(delimiter), -1);
.../SourceDiscovery.java:396:          final String[] fields1 = line.toString().split(" ");
.../SourceDiscovery.java:397:          final String[] fields2 = line2.toString().split(" ");
.../format/CSVImporterFormat.java:150,410,570:  ....split(",")
.../format/CSVImporterFormat.java:804:        final String[] headerColumns = header.split(",");
.../format/JSONImporterFormat.java:418:      for (String tName : typeName.split(",")) {
.../importer/OrientDBImporter.java:890:    for (final String pair : fieldTypes.split(",")) {
.../exporter/ExporterSettings.java:82,84,86:   value.split(",")
```

Every other `.split(` in the module takes a **compile-time constant** that is not a regex metacharacter
(`","`, `"="`, `":"`, `" "`). None of them can be reached by an operator-supplied character. Argued, not fixed.

Constructors and factories that can carry a metacharacter delimiter into the class:

```shell
$ grep -n "CsvRowSource(\|static CsvRowSource from(" integration/src/main/java/com/arcadedb/integration/importer/graph/CsvRowSource.java
41:  public CsvRowSource(final String filePath) {                                        // default ','
45:  public CsvRowSource(final String filePath, final char delimiter, final int skipLines)
51:  public static CsvRowSource from(final String dir, final String fileName)             // default ','
55:  public static CsvRowSource from(final String dir, final String fileName, final char delimiter)
```

Config-driven construction:

```shell
$ grep -n "new CsvRowSource" -r integration/src/main/java/
.../graph/GraphImporter.java:473:      return new CsvRowSource(filePath, delimiter.charAt(0), skipLines);
```

Sibling delimiter walkers in the same module (the `#7263` / `#7268` fixes) - already literal, `indexOf`-based:

```shell
$ grep -n "indexOf(delim" integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java
1256:        pos = fieldVal.indexOf(delim, start);
1321:      pos = fieldVal.indexOf(delimiter, start);
```

`CSVImporterFormat` hands its delimiter to Univocity as a `char` / `String` on `CsvFormat.setDelimiter`
(lines 739-740, 867), which is a literal separator, not a pattern. Argued, not fixed.

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| JSON config `"delimiter": "\|"` -> `GraphImporter.createRecordSource` -> `new CsvRowSource(path, char, skipLines)` | yes | yes - `aJsonConfiguredPipeDelimiterImportsEveryProperty` |
| `new CsvRowSource(path, delimiter, skipLines)` (public ctor) | yes | yes - `everyRegexMetacharacterSplitsOnTheLiteralCharacter`, `aPipeDelimitedImportThroughTheConstructorLoadsEveryProperty` |
| `CsvRowSource.from(dir, file, delimiter)` (public factory) | yes | yes - `theThreeArgFactorySplitsOnTheLiteralCharacter` |
| `new CsvRowSource(path)` / `from(dir, file)` - default `','` | yes (behaviour preserved) | yes - `theDefaultCommaDelimiterIsUnchanged`, plus the pre-existing `GraphImporterCSVTest` |
| header line (same `splitLine`, one call site) | yes | yes - asserted inside every metacharacter case |
| trailing / repeated empty fields (`split(..., -1)` semantics) | yes (preserved exactly) | yes - `emptyAndTrailingFieldsKeepTheirSplitMinusOneShape` |
| `GraphImporter` split-edge walkers (`collectSplitKeys`, inline walker) | n/a - argued: already literal `indexOf` walks (#7263/#7268), evidence above | n/a |
| `CSVImporterFormat` (Univocity) | n/a - argued: `CsvFormat.setDelimiter` takes a literal separator | n/a |
| other `.split(` in `integration/src/main/java` | n/a - argued: all compile-time constants, none a metacharacter | n/a |

No row is blank, and no row needed a follow-up issue.

### Reachability

`new CsvRowSource(...)` is constructed on a live path by `GraphImporter.createRecordSource`
(`GraphImporter.java:473`), which every JSON-configured CSV source goes through, and directly by callers of the
public constructor and factory - `GraphImporterCSVTest`, `GraphImporterArrayPropsTest`,
`GraphImporterSplitDelimiterTest` and the StackOverflow/UberTrips importers all build one. `splitLine` runs once
per header and once per data row of every such import. Nothing gates it behind a flag.

### Residual risk

`CsvRowSource` still has **no quoting support** - it is documented as "for quoted fields use Univocity" and a
delimiter that appears inside a field value still splits that value. That is a separate, documented limitation
of this class and not what #7267 reports; it is unchanged by this fix. Beyond that, the table above has no
uncovered row.

## Fix

`splitLine` walks the line with `indexOf(char, from)` and fills a `String[]` directly:

- **literal by construction** - a `char` compared with `==` cannot be a metacharacter;
- **cheaper than the code it replaces** - `String.split` only takes its regex-free fast path for a single
  *non-metacharacter* char; for `|`, `.`, `$` and friends it compiles a `Pattern` per call, i.e. per row of a
  bulk import. The walk allocates the result array once and no `Pattern` ever;
- **byte-identical output to `split(literal, -1)`** for every delimiter that worked before: one leading pass
  counts the separators so the array is sized exactly, and trailing empty fields are kept.

`java.util.regex.Pattern.quote` was rejected: `\Q;\E` is not a single character, so `String.split` would compile
a `Pattern` on *every* row for *every* delimiter, including the default comma - a hot-path regression for the
common case in exchange for correctness in the rare one.

`com.arcadedb.utility.CodeUtils.split(String, char, int, int)` was rejected as the reuse candidate for two
reasons: it drops the **trailing empty field** (`"a;b;"` answers `[a, b]`, where `split(";", -1)` answers
`[a, b, ""]`), which would silently change behaviour for the delimiters that work today, and it returns a
`List<String>` that would need an extra array copy per row.

## Verification

New test: `integration/src/test/java/com/arcadedb/integration/importer/Issue7267CsvRowSourceLiteralDelimiterTest.java`.

**Before the fix** - 5 of its 7 tests fail, one per bug-carrying entry point:

```text
[ERROR] Tests run: 7, Failures: 5, Errors: 0, Skipped: 0
  everyRegexMetacharacterSplitsOnTheLiteralCharacter:121 [delimiter '|' ...] Expecting map: {} to contain only: ["lastName"="Miner", "firstName"="Jay", "id"="1"]
  aMetacharacterDelimiterNeverRaisesARegexError            (PatternSyntaxException)
  aPipeDelimitedImportThroughTheConstructorLoadsEveryProperty:166 expected: "Jay" but was: null
  theThreeArgFactorySplitsOnTheLiteralCharacter:186 Expecting map: {} to contain entries: ["firstName"="Jay"]
  aJsonConfiguredPipeDelimiterImportsEveryProperty
```

The two that pass before the fix are the two that assert behaviour must **not** change -
`theDefaultCommaDelimiterIsUnchanged` and `emptyAndTrailingFieldsKeepTheirSplitMinusOneShape` - which is what a
behaviour-preservation test is supposed to do.

**After the fix:**

```text
[INFO] Tests run: 7, Failures: 0, Errors: 0, Skipped: 0 -- in Issue7267CsvRowSourceLiteralDelimiterTest
```

**No regressions** - the whole `integration` module, benchmark/vector/slow lanes excluded
(`mvn -o -pl integration test -DexcludedGroups=benchmark,vector,slow`):

```text
[INFO] Results:
[INFO] Tests run: 270, Failures: 0, Errors: 0, Skipped: 9
[INFO] BUILD SUCCESS
```

That run includes every existing CSV consumer of the changed method: `GraphImporterCSVTest`,
`GraphImporterArrayPropsTest`, `GraphImporterSplitDelimiterTest`, `GraphImporterIdTypesTest`,
`Issue6811CsvDelimiterOptionTest`, `Issue7266ImporterConfigValidationTest`.

## Impact

- Behaviour changes only for delimiters that were already broken. Every delimiter that produced correct fields
  before produces byte-identical fields now, empty and trailing fields included.
- Bulk imports get slightly cheaper across the board: no `Pattern` is compiled for any delimiter, where the old
  code compiled one per row for every non-default separator that is a metacharacter.

## Finding ledger

- [x] 1. `CsvRowSource.splitLine` splits on a regex - fixed on all three entry points (JSON config, public
      constructor, three-argument factory), 7 regression tests.

## Adversarial pass

The `Task` tool is disabled in this session, so the isolated subagent could not be spawned. The pass was run by
hand against the same three inputs (issue body, diff, tree) and is recorded here in full rather than skipped.

1. **"The rest threw" was false.** The test's `REGEX_METACHARACTERS` javadoc claimed every character except
   `|`, `.`, `$`, `^` raised a `PatternSyntaxException`. `]` and `}` did not: they produced correct fields
   before this fix. Evidence - `String.java:3691-3692` in the JDK 26 `src.zip` gates the regex-free fast path on
   `regex.length() == 1 && ".$|()[{^?*+\\".indexOf(ch) == -1`, and `]` and `}` are absent from that set, which
   matches the measured `']' -> [a, b, c]`. **Fixed here**: the javadoc now says which two are carried for
   symmetry rather than as regressions, so nobody later reads the loop as fourteen reproduced bugs.
2. **The performance sentence in `splitLine`'s javadoc was an unverified claim about the JDK.** Now verified
   against `String.java:3684-3696` (same fast-path condition), which is exactly what it asserts. **Fixed here**
   by proving it rather than by weakening it.
3. **Is any construction of `CsvRowSource` outside the covered entry points?** No.
   `grep -rn "new CsvRowSource(\|CsvRowSource.from(" --include="*.java" .` answers exactly one production site,
   `GraphImporter.java:473`, plus the class's own two factories and test code; and
   `grep -rln CsvRowSource` outside `integration/` answers nothing. **Not real** - the coverage table is complete.
4. **Does the fix change `String.split(literal, -1)`'s answer for any line shape?** Walked by hand for the four
   shapes that differ between splitters: `""` -> `[""]`, `"abc"` -> `["abc"]`, `";"` -> `["", ""]`,
   `"a;;b;"` -> `["a", "", "b", ""]`. The counting pass and the walk scan with the identical
   `indexOf(delimiter, pos + 1)` sequence, so the second loop can never see `-1`. **Not real.**
5. **Does `indexOf(int)` mis-handle a surrogate delimiter?** No. `String.indexOf(int ch, int fromIndex)` routes
   any `ch < Character.MIN_SUPPLEMENTARY_CODE_POINT` to a plain char scan, and a `char` argument always widens
   to a value in that range. **Not real.**

No finding was out of scope, so no follow-up issue was filed.

## Review cycles

### Cycle 1 - `b0a498c846`

`claude` reviewed on the PR's issue-comment surface. No blocking finding: correctness verified by hand against
the same boundary shapes listed in the adversarial pass, the `char` -> `indexOf(int, int)` widening confirmed
safe, and the three-entry-point claim confirmed against `GraphImporter.java:467-473`. Three non-blocking items:

1. **`REGEX_METACHARACTERS` overclaims** - two of its fourteen entries were never broken, so the name reads
   confusingly on first pass even though the javadoc says so. **Applied**: renamed to `REGEX_SYNTAX_DELIMITERS`,
   which is what the array actually holds, and the javadoc now says why the name avoids "metacharacter".
2. **A single-pass split with a growable `int[]` position buffer would scan each row once instead of twice.**
   **Skipped, with reasoning** - the reviewer already scoped it as "not worth doing now", and it trades the one
   thing the current shape is chosen for: a second heap allocation per row, against CLAUDE.md's
   "lightweight on garbage collector" mantra, to save one branch-predictable `indexOf` scan of a line that is
   typically a few hundred bytes. If CSV import ever measures as allocation-bound this is worth revisiting; it
   is not a defect and is not tracked as one.
3. **The tracking doc is detailed for a ~40-line fix.** **Skipped** - the reviewer answers this himself, and it
   is verified: `git ls-tree -r --name-only origin/main -- docs/` lists `docs/7264-graphimporter-tx-leak-zero-count.md`,
   `docs/7266-three-small-defects.md`, `docs/7225-unlearn-removed-peer-hosts.md` and two more, so one write-up
   per issue is current repo practice, not a one-off here.

Nothing was deferred: every item was either applied or answered with evidence, so no `review-deferred-*.md`
notes file was produced.

Re-ran after the rename: `Tests run: 7, Failures: 0, Errors: 0, Skipped: 0`.

### Cycle 2 - `1d9c7a7bf2`

`claude` re-reviewed and traced the splitter by hand again, independently confirming the boundary shapes
(leading/trailing/interior empties, delimiter as last character, zero-delimiter line) against
`String.split(literal, -1)`, and confirming the scope claim - that `CsvRowSource` was the only regex-based
splitter reachable with an operator-supplied delimiter. Verdict "looks good to merge". It also noted that the
fix removes a class of untrusted-input-controls-a-regex risk, since a config-file delimiter could previously
steer `Pattern` compilation. Two non-blocking points, neither a defect:

1. **`splitLine`'s javadoc ran ~25 lines for a 15-line method**, with the per-delimiter blow-by-blow duplicated
   from this document. **Applied**: trimmed to the three things a reader at the call site needs - that the
   argument used to be a regex and must not become one again, one example from each failure family, and why
   `Pattern.quote` is not the cheaper fix - with the full table left here and pointed to by name. This was the
   one point both review cycles raised in some form, which is why it was acted on rather than argued.
2. **The tracking doc is large relative to the diff.** Already answered in cycle 1 with the `git ls-tree`
   evidence that per-issue docs are current repo practice; the reviewer explicitly registered "no objection".

The reviewer could not run Maven in its environment and said so. That gap is covered here: the full
`integration` module was re-run after the trim - `Tests run: 270, Failures: 0, Errors: 0, Skipped: 9`.

Again nothing deferred, so no `review-deferred-*.md` notes file was produced in this cycle either.

### Cycle 3 - `c25e1547d9`

Two reviewers this cycle. `claude`: "No blocking issues found", with the correctness trace repeated
independently a third time and the `Pattern.quote` / `CodeUtils.split` rejections checked against the actual
`CodeUtils` source rather than taken on trust. `coderabbitai` posted one inline finding. Two items applied, two
answered:

1. **`coderabbitai`, MD040 on `docs/7267-csvrowsource-regex-delimiter.md`** - eight fenced blocks carried no
   language, which `markdownlint-cli2` flags. **Applied**: `shell` on the five grep/command blocks, `text` on
   the three captured-output blocks.
2. **`claude`, the `emptyAndTrailingFieldsKeepTheirSplitMinusOneShape` assertions are easy to misread** - a
   `doesNotContainKey` there is about `CsvRecordReader.get` folding empty to null, not about the splitter
   dropping a column. **Applied**: split into two assertions so each `.as()` describes one claim, and the
   fold-to-null one says which behaviour it is testing.
3. **The two-pass scan was reasoned about, not benchmarked.** **Skipped** - the reviewer's own conclusion is
   "I doubt this is measurable", and there is no CSV-import benchmark in the tree to move. The claim the fix
   actually rests on is not "two passes beat one" but "no `Pattern` is compiled per row", which is proved from
   `String.java:3691-3692` rather than measured. Recorded here so the next person can see it was weighed.
4. **The per-issue doc convention is worth confirming with the team.** **Skipped, and passed to the developer**
   rather than argued: the evidence that it is current practice is in cycle 1, but whether the team *wants* the
   convention is the maintainer's call, not this PR's.

`Tests run: 7, Failures: 0, Errors: 0, Skipped: 0` after both changes.

### Cycle 4 - `41af0b66dd`

`claude`: "No blocking issues found. This looks solid and ready to merge." No inline comments on this SHA from
any reviewer. The only two remarks are explicitly re-raised "for visibility" and were both answered in earlier
cycles - the doc's length against repo convention, and the second scan versus a per-row allocation. Nothing new
was actionable, nothing was applied, and the working tree ended the cycle empty.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7286

Commits:

| SHA | What |
|---|---|
| `b0a498c846` | the fix, the 7 regression tests, this document |
| `1d9c7a7bf2` | cycle 1: `REGEX_METACHARACTERS` -> `REGEX_SYNTAX_DELIMITERS` |
| `c25e1547d9` | cycle 2: trimmed `splitLine`'s javadoc |
| `41af0b66dd` | cycle 3: markdownlint MD040 fences; one assertion per claim in the empty-field test |

## Deferred items

None. No `review-deferred-*.md` notes file was produced in any of the four cycles: every review comment was
either applied or answered with evidence in the cycle sections above. (`docs/review-deferred-47afd7da.md` in
this directory belongs to PR #7210 / issue #6990 and was already on `main` before this branch existed.)

Two things are the maintainer's call rather than deferred work, and are named here so they are not discovered
in a diff stat:

- Whether the per-issue `docs/<issue>-<name>.md` convention should continue at this length for small fixes.
  Three reviews raised it; all three confirmed it matches current practice on `main` and none objected.
- Whether CSV import is worth a benchmark. The performance claim this fix rests on - no `Pattern` compiled per
  row - is proved from the JDK's fast-path condition, not measured; the count-then-fill pass was reasoned
  about, not benchmarked, and there is no CSV-import benchmark in the tree to extend.

## Final state

`clean-approval` at cycle 4 of a maximum of 4.

Every row of the coverage table is fixed and tested. No follow-up issue was needed, and none was filed.
