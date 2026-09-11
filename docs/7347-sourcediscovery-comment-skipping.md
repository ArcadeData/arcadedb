# #7347 - SourceDiscovery never actually skips leading `#` or `//` comment lines

/ Issue: https://github.com/ArcadeData/arcadedb/issues/7347
/ Branch: `fix/7347-sourcediscovery-comment-skip`

## Finding ledger

The issue reports one defect with two named consequences. Reading the code found four independent
defects in the same twelve lines, each of which alone makes comment skipping a no-op:

- [x] 1. `skipLine()` stops on the `'\n'` it just read, so the `analyzeChar()` call after it is
      always handed `'\n'` - a character its dispatch on `<`, `_` and `{` cannot match - fixed
- [x] 2. the `#` loop's own condition re-reads that same `'\n'`, so it runs at most once however
      many comment lines the source opens with - fixed
- [x] 3. the `//` loop runs after `parser.reset()`, which leaves `getCurrentChar()` at `0`, so it
      never runs at all, not even on a source whose first two characters are `//` - fixed
- [x] 4. that same `reset()` gives back the one line the `#` loop consumed, so the separator scan
      below it starts on the comment - the reported symptom - fixed

Items 1 and 3 are the issue's own numbered findings. Items 2 and 4 are the two the issue describes
in prose; they are listed separately because each needs its own test.

## Root cause

`SourceDiscovery.analyzeText()`:

```java
parser.mark();

// SKIP COMMENTS '#' IF ANY
while (parser.isAvailable() && parser.getCurrentChar() == '#') {
  skipLine(parser);
  format = analyzeChar(parser, settings, userDelimiter);
  if (format != null)
    return format;
}

// SKIP COMMENTS '//' IF ANY
parser.reset();

try {
  while (parser.getCurrentChar() == '/' && parser.nextChar() == '/') {
```

with

```java
private void skipLine(final Parser parser) throws IOException {
  while (parser.isAvailable() && parser.nextChar() != '\n')
    ;
}
```

`Parser.getCurrentChar()` returns the last character `nextChar()` read, so `skipLine` exits with
`currentChar == '\n'`. `Parser.reset()` sets `currentChar = 0` and rebuilds the stream from
`source.reset()` - it does not honour `mark()`, which nothing in `main/` reads:

```
$ grep -rn "parser.mark()\|parser.reset()" integration/src/main/java
integration/src/main/java/com/arcadedb/integration/importer/Importer.java:135:    parser.reset();
integration/src/main/java/com/arcadedb/integration/importer/SourceDiscovery.java:90:    parser.reset();
integration/src/main/java/com/arcadedb/integration/importer/SourceDiscovery.java:339:    parser.mark();
integration/src/main/java/com/arcadedb/integration/importer/SourceDiscovery.java:350:    parser.reset();
integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java:781:    parser.reset();
integration/src/main/java/com/arcadedb/integration/importer/format/Word2VecImporterFormat.java:62:    parser.reset();
integration/src/main/java/com/arcadedb/integration/importer/format/Word2VecImporterFormatLSM.java:62:    parser.reset();
integration/src/main/java/com/arcadedb/integration/importer/format/XMLImporterFormat.java:219:    parser.reset();
```

Line 339 is the only `mark()` call site in `main/`, and the `reset()` at line 350 is the only thing
that could have paired with it. The call was removed with the loops it belonged to.

## Invariant

> Content sniffing decides a source's format from the first line that is not a leading `#` or `//`
> comment.

## Completeness

### Call graph

```
$ grep -rn "analyzeSourceContent\|analyzeText\|analyzeChar\|skipLine" integration/src/main/java
SourceDiscovery.java:89:    final FormatImporter formatImporter = analyzeSourceContent(parser, entityType, settings, logger);
SourceDiscovery.java:237:  private FormatImporter analyzeSourceContent(...)
SourceDiscovery.java:323:    FormatImporter format = analyzeChar(parser, settings, userDelimiter);
SourceDiscovery.java:327:    return analyzeText(parser, settings, logger, userDelimiter);
SourceDiscovery.java:336:  private FormatImporter analyzeText(...)
SourceDiscovery.java:343:      skipLine(parser);
SourceDiscovery.java:344:      format = analyzeChar(parser, settings, userDelimiter);
SourceDiscovery.java:354:      skipLine(parser);
SourceDiscovery.java:355:      format = analyzeChar(parser, settings, userDelimiter);
SourceDiscovery.java:626:  private void skipLine(...)
SourceDiscovery.java:638:  private FormatImporter analyzeChar(...)
```

(the `skipLine` hits in `graph/GraphImporter.java` and `graph/CsvRowSource.java` are a `skipLines`
config option, a different thing.)

One caller of `analyzeSourceContent`, one of `analyzeText`. Three of `analyzeChar`: the live one in
`analyzeSourceContent` and the two dead ones the issue names, which this change collapses into one
live call.

### Entry points

```
$ grep -rn "loadFromSource" integration/src/main/java
Importer.java:79:      loadFromSource(settings.url, urlEntityType, analyzedSchema);
Importer.java:80:      loadFromSource(settings.documents, AnalyzedEntity.EntityType.DOCUMENT, analyzedSchema);
Importer.java:81:      loadFromSource(settings.vertices, AnalyzedEntity.EntityType.VERTEX, analyzedSchema);
Importer.java:82:      loadFromSource(settings.edges, AnalyzedEntity.EntityType.EDGE, analyzedSchema);
```

```
$ grep -rln "integration.importer" --include="*.java" engine gremlin server
engine/src/main/java/com/arcadedb/query/sql/parser/ImportDatabaseStatement.java
gremlin/.../GraphSONImporterFormat.java, GraphMLImporterFormat.java (+ their tests)
server/src/main/java/com/arcadedb/server/ServerControlPlane.java
```

`ImportDatabaseStatement` (SQL `IMPORT DATABASE`) and `ServerControlPlane` both go through
`Importer.load()`, so they reach the same four `loadFromSource` calls; they are not separate sniffing
paths. The two gremlin formats are resolved by extension in `analyzeSourceContent`'s `knownFileType`
switch, before sniffing.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `-url` / `IMPORT DATABASE` (`EntityType.DATABASE`) -> `analyzeText` | yes | yes - `everyEntityTypeSkipsTheComment[DATABASE]`, `aHashCommentedNTriplesFileImportsThroughTheUrlRouteAsIfTheCommentWereNotThere` |
| `-documents` (`EntityType.DOCUMENT`) -> `analyzeText` | yes | yes - `everyEntityTypeSkipsTheComment[DOCUMENT]`, `aHashCommentedSemicolonCsvKeepsItsOwnDelimiter` |
| `-vertices` (`EntityType.VERTEX`) -> `analyzeText` | yes | yes - `everyEntityTypeSkipsTheComment[VERTEX]` |
| `-edges` (`EntityType.EDGE`) -> `analyzeText` | yes | yes - `everyEntityTypeSkipsTheComment[EDGE]`, `aHashCommentedNTriplesFileImportsThroughTheEdgesRouteAsIfTheCommentWereNotThere` |
| `analyzeChar`'s RDF arm, reached after a comment for the first time | yes | yes - `aHashCommentIsSkippedAndTheNTriplesLineBelowItIsRecognised`, `aSlashCommentIsSkippedAndTheNTriplesLineBelowItIsRecognised` |
| `analyzeChar`'s XML arm, same | yes | yes - `aCommentedXmlSourceIsRecognisedAsXml` |
| `analyzeChar`'s JSON arm, same | yes | yes - `aCommentedJsonSourceIsRecognisedAsJson` |
| the separator scan, for a commented delimited source | yes | yes - `theSeparatorOfACommentedDelimitedSourceIsTakenFromItsFirstDataLine`, `theSeparatorOfASlashCommentedDelimitedSourceIsTakenFromItsFirstDataLine` |
| a commented source at LOAD time (the format layer): `//` for every format, and `#` for XML/JSON | no - **filed as #7490** | n/a |
| a blank line between the comment block and the data | yes | yes - `aBlankLineBetweenTheCommentBlockAndTheDataIsSkippedToo`, `aLeadingBlankLineIsSkipped` |
| a source whose only line terminator is a bare `'\r'` | yes, for the comment block | yes - `aSourceTerminatedOnlyByCarriageReturnsStillHasItsCommentBlockSkipped` |
| a STRAY bare `'\r'` in an otherwise line-feed source | yes | yes - `aStrayCarriageReturnDoesNotSwallowTheHeaderBelowIt`, `theCharacterAfterABareCarriageReturnIsNotEatenWithIt` |
| the user's `-delimiter` on the newly-live `analyzeChar` dispatch | yes | yes - `anExplicitDelimiterStillWinsOnTheNewlyLiveDispatch` |
| a `#` comment appearing after the first data line | no - **argued**, see Residual risk | n/a |
| a comment line preceded by whitespace | no - **argued**, see Residual risk | n/a |

### Sibling shapes

```
$ grep -rn "nextChar() != " integration/src/main/java
SourceDiscovery.java:364:      while (parser.isAvailable() && parser.nextChar() != '\n') {   # separator scan
SourceDiscovery.java:393:          while (parser.isAvailable() && parser.nextChar() != '\n')   # vector second-line read
SourceDiscovery.java:627:    while (parser.isAvailable() && parser.nextChar() != '\n')     # skipLine - THE BUG
SourceDiscovery.java:651:      while (parser.isAvailable() && parser.nextChar() != '\n') {   # analyzeChar line read
```

Four line-reading loops of the same shape; three of them are line READERS whose callers consume the
line as they go and do not inspect `getCurrentChar()` afterwards, so stopping on the `'\n'` is
correct for them. Only `skipLine` is a line SKIPPER whose caller's next act is to read
`getCurrentChar()`, which is what makes the shared shape a bug in exactly one place.

## The fix

`integration/src/main/java/com/arcadedb/integration/importer/SourceDiscovery.java`

- `skipLine()` now consumes the line terminator as well, so it leaves the parser on the first
  character of the next line, and returns how many characters it read. All three terminators end a
  line: `"\n"`, `"\r\n"` and a bare `"\r"`. The bare `'\r'` was added in review cycle 1 - without it
  a source that uses it as its only terminator has no line ends at all as far as this method is
  concerned, so the first comment line swallows the whole source and sniffing is left with nothing.
- `skipComments()` (new) advances past every leading `#` comment line, `//` comment line and blank
  line in one loop rather than two, and returns the character offset of the first data line.
  Blank lines are in it because skipping the comments and then stopping on the blank line below
  them hands the separator scan an empty line, which produces no candidate and so
  "Cannot determine the file type" - a HARDER failure than the wrong-delimiter one that shape used
  to get, i.e. a regression this change would otherwise have introduced. The same test also skips a
  blank FIRST line, which used to reach the scan and fail there for the same reason. Bounded at
  `MAX_COMMENT_LINES = 10_000`: without a bound a source that is comments all the way down would be
  read end to end just to decide its format. Past the bound it reports no comment prefix at all,
  which puts sniffing back where it looked before this change.
- `rewindTo()` (new) replaces the bare `parser.reset()`. Counted in characters, because that is what
  `skipComments` counts and what a multi-byte encoding makes different from bytes:
  `Parser.getPosition()` is advanced both per character by `nextChar()` and in bulk by the reader's
  own reads, so it cannot be used to seek.
- `analyzeText()` calls `skipComments()`, then `analyzeChar()` once - and only when there actually
  was a comment prefix, so a source without one is not dispatched on twice - then rewinds to the
  first data line for the separator scan.
- `parser.mark()` removed: dead, per the grep above.
- `analyzeSourceContent()` made package-private so a test can assert the format a source is
  recognised as without going on to parse it. The two questions differ for a `//`-commented source
  (#7490), and only the first one has an answer.

`integration/src/main/java/com/arcadedb/integration/importer/format/CSVImporterFormat.java`

- `getDelimiter()` added. The delimiter is the only thing that distinguishes the two possible
  answers for a commented DELIMITED source - the format class is `CSVImporterFormat` either way -
  so the test needs to read it, and reflection into a private field is worse than an accessor.

### Behaviour for a source with no comments

Bit-identical. `skipComments()` returns 0 on the first character, `analyzeChar()` is not called a
second time, and `rewindTo(parser, 0)` is `parser.reset()` with no replay. `uncommentedSourcesAreUnchanged`
and `aFirstLineOpeningWithASingleSlashIsData` are the two tests that were GREEN before the fix and
stay green after it.

## Tests

`integration/src/test/java/com/arcadedb/integration/importer/Issue7347CommentSkippingTest.java`, 25 tests.

Proved red before the fix and green after: with the pre-fix `analyzeText`/`skipLine` bodies restored
and everything else unchanged, **17 of the 19 tests that existed at that point failed**. The two that
passed are the two no-regression guards named above, which is what they are for.

```
# pre-fix bodies restored
[ERROR] Tests run: 19, Failures: 14, Errors: 3, Skipped: 0
#   theSeparatorOfACommentedDelimitedSourceIsTakenFromItsFirstDataLine: expected ";" but was " "
#   aHashCommentIsSkippedAndTheNTriplesLineBelowItIsRecognised: but was instance of CSVImporterFormat
#   aHashCommentedNTriplesFileImportsThroughTheUrlRouteAsIfTheCommentWereNotThere: ERROR

# fix in place
[INFO] Tests run: 25, Failures: 0, Errors: 0, Skipped: 0
```

Full runs:

| Suite | Result |
|---|---|
| `mvn -o -pl integration verify -DskipITs=false -DexcludedGroups=benchmark,vector` | 395 tests, 0 failures, 9 skipped (unit) + 133 tests, 0 failures (IT) |
| `mvn -o -pl gremlin-it verify -DskipITs=false -DexcludedGroups=benchmark,vector,slow` | 1926 tests, 0 failures, 544 skipped - includes `GraphMLImporterIT`, `GraphSONImporterIT`, `Issue6751GraphSONMultiPropertyTest` |

(the `gremlin` module itself skips its own tests by design - they run in `gremlin-it` against the
shaded jar.)

## Reachability

`analyzeText()` is reached from `SourceDiscovery.getSchema()`, which `Importer.loadFromSource()`
calls for every one of the four entity types, on every import that does not name a file type and
whose extension does not imply one. No flag gates it. The `aHashCommented*` tests drive the live
`Importer.load()` CLI path end to end, so the changed code runs in production and not only under a
unit test.

## Residual risk

What this does NOT cover:

1. **The comment prefix is handed on to the format intact, and only univocity skips anything there.**
   Sniffing is now right for every format; parsing is not. Measured on this branch with
   `-url <file> -documentType Doc -forceDatabaseCreate true`:

   | Source | Result |
   |---|---|
   | `# c` + `<root><row id="1"/><row id="2"/></root>` | `ImportException: Error on importing from source ...` |
   | `# c` + `{"id": 1}\n{"id": 2}` (jsonl) | `MalformedJsonException ... at line 1 column 2` |
   | `# c` + `id;name;score` CSV | imports correctly - univocity's default comment character is `#` |
   | `# c` + N-Triples | imports correctly - same reason |
   | `//` + any of the four | fails |

   Before this change those same sources were mis-sniffed as delimited text and either failed with an
   error naming the wrong thing or "succeeded" importing nothing, so this is a better error rather
   than a new failure - but it is still not an import. Filed as **#7490**, which lays out both ways
   to close it. The regression test asserts detection for every format and a completed import only
   for the one combination that works, and its Javadoc says why.
2. **Comments after the first data line are not skipped.** They never were, and sniffing only ever
   looks at the beginning of the source; for `#` the format layer skips them anyway.
3. **A comment line preceded by whitespace is not recognised as one.** `skipComments()` tests the
   first character of the line, the same way both loops it replaces did. Widening that would change
   which sources are recognised as commented, which is more than this issue asks for.
4. **The 10,000-line bound is a heuristic**, not a correctness boundary. Past it sniffing behaves as
   it did before this change - it reads line 1 - and logs a WARNING saying so.
   `commentSkippingIsBoundedAndTheBoundIsNotOffByOne` pins both sides of it.
5. **A remote source with a long comment prefix is streamed twice.** `rewindTo()` resets the source
   and replays the prefix, and `Parser.reset()` goes through `Source.reset()`, which for an HTTP
   source re-opens the connection. The reset itself is not new - the `parser.reset()` this replaces
   was unconditional at the same point - but the prefix that gets read before it is. Bounded by
   `MAX_COMMENT_LINES`.
6. **The rest of `SourceDiscovery`'s line handling is still `'\n'`-only.** `skipLine()` now ends a
   line on a bare `'\r'`, but the separator scan and `analyzeChar()` do not, so a classic-Mac source
   has its comment block skipped and then its whole remainder read as one line. Not made worse by
   this change and not fixed by it either; `aSourceTerminatedOnlyByCarriageReturnsStillHasItsCommentBlockSkipped`
   pins the half that is.
7. **`Parser.mark()` is now dead.** `grep -rn "\.mark()" integration/src` finds no call site. It was
   already inert: `Parser.reset()` rebuilds the stream from `Source.reset()` and never reads the
   mark. Left in place because it is public API on a public class; removing it is a larger change
   than this bugfix.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a `general-purpose` subagent that has NOT been shown the author's
reasoning. **No `Task` tool is available in this environment**, so the subagent could not be spawned.
Two substitutes were used instead, and both are weaker than the real thing for the reason the phase
exists - neither of them arrives unconvinced:

1. a deliberate re-read of the patch against the issue body alone, written up as the follow-up issue
   the reporter would file;
2. the `code-review` skill run at `high` over the working-tree diff. It forks into a background
   subagent, and in this environment that subagent never returned its findings into the session -
   it reported `completed` with no result, and a follow-up request left it `running` past five
   minutes. So this substitute produced NOTHING, and the findings below are all from (1).
   Recorded rather than quietly dropped: a review that produced no output is not a review that
   found nothing.

Findings and dispositions:

| # | Finding | Disposition |
|---|---|---|
| 1 | A blank line between the comment block and the data made the patch turn a SOFT failure (wrong delimiter, garbage import) into a HARD one ("Cannot determine the file type"): the comments were skipped and the scan then stopped on the empty line, which yields no separator candidate. A regression introduced by the patch itself. | **Fixed here.** `skipComments()` skips blank lines too. `aBlankLineBetweenTheCommentBlockAndTheDataIsSkippedToo`, `aLeadingBlankLineIsSkipped`. |
| 2 | The `MAX_COMMENT_LINES` check was off by one: it fired at the TOP of the iteration after the bound's last comment line had been consumed, so a source with exactly 10,000 comment lines took the give-up path. | **Fixed here.** The check moved to after the line is known to be a comment. `commentSkippingIsBoundedAndTheBoundIsNotOffByOne` pins both sides of the bound. |
| 3 | The issue's own closing note - "that path has never executed and is untested", about `analyzeChar` handing `userDelimiter` to the format on its newly-live call site - had no test. | **Fixed here.** `anExplicitDelimiterStillWinsOnTheNewlyLiveDispatch`. |
| 4 | The comment prefix is skipped by sniffing and handed to the format intact. Probing the four formats on this branch showed the gap is wider than the `//` it was first written up as: a `#`-commented XML or JSONL source is now recognised correctly and then dies in the XML/JSON parser. Only univocity's `#` is skipped downstream. | **Filed as #7490** (out of scope: the fix is in the format layer, not in `SourceDiscovery`). The issue was retitled and rewritten once the probe showed the wider scope, and the overstated "`#` works end to end" claims in the code and test Javadoc were corrected. |
| 5 | The issue's suggested fix says the loops "should not `reset()` past the lines they consumed", and the patch does reset (inside `rewindTo`). | **Not real.** The literal suggestion is wrong: `analyzeChar()` CONSUMES the line it inspects (`SourceDiscovery.java`, the `while (parser.isAvailable() && parser.nextChar() != '\n')` inside its `<`/`_` arm), so a source whose first data line starts with `<` but is neither XML nor a triple would leave the separator scan reading the SECOND data line. Rewinding to the recorded offset is the same thing the suggestion asks for - the scan sees the first data line - reached differently. |
| 6 | `Parser.mark()` is now called from nowhere: `grep -rn "\.mark()" integration/src` returns no hits at all. | **Not real** as a defect, and deliberately not removed: it is public API on a public class, and it has been inert since long before this change - `Parser.reset()` rebuilds the stream from `source.reset()` and never reads the mark. Deleting public API is a larger change than this bugfix. |

## Review cycles

### cycle 1 - `b3d5b32` (first push)

`claude` reviewed on the PR as an issue comment (its surface on this org's repos). CodeRabbit was
rate-limited on this PR - "Review limit reached ... You've used all 4 included reviews currently
available" - and left no review, no inline comment and no thread, so `claude` is the only reviewer
this cycle had.

The review traced the offset bookkeeping character by character against `Parser`'s semantics and
found no case where `rewindTo` lands one character off. Three items, dispositions:

| Item | Disposition |
|---|---|
| A source whose only line terminator is a bare `'\r'`: `skipLine()` stops only on `'\n'`, so the first comment line swallows the whole source. Raised as pre-existing and non-blocking. | **Applied.** Non-blocking as raised, but the OUTCOME does change under this patch - the old code rewound to the start and sniffed the whole file as one line, the new one rewinds past everything and throws "Cannot determine the file type". `skipLine()` now ends a line on `"\n"`, `"\r\n"` or a bare `"\r"`, and `aSourceTerminatedOnlyByCarriageReturnsStillHasItsCommentBlockSkipped` was verified to be the ONLY test that goes red when that handling is removed. |
| The inline comments narrate the historical bug at length; "worth a maintainer call on whether to trim it, not a correctness issue", and the review itself notes the file's existing `ISSUE #NNNN` comments make this consistent with local convention. | **Partly applied.** The blow-by-blow of all four defects in `analyzeText` was cut to three lines: it is duplicated verbatim in the test Javadoc, this doc and the PR body, so the code comment was the copy carrying the least. The issue reference and the reason the replacement is shaped as it is were kept - the file's own convention, which the review confirms. |
| `CSVImporterFormat.getDelimiter()` and package-private `analyzeSourceContent()` are narrowly-scoped test-only accessors with no behavioural effect - "reasonable". | No change. |

No deferred items.

### cycle 2 - `e6ee095`

`claude` again, on the same issue-comment surface; CodeRabbit still rate-limited, still zero reviews,
inline comments and threads. Verdict: **"No blocking issues found."** The review re-traced the offset
bookkeeping including the carriage-return handling added in cycle 1 and confirmed the
`MAX_COMMENT_LINES` boundary is not off by one.

Four notes, all explicitly non-blockers:

| Item | Disposition |
|---|---|
| A remote source's comment prefix is now read twice - once by `skipComments()`, again by `rewindTo()`'s replay after `Source.reset()` re-opens the connection. "Worth confirming this is an acceptable cost." | **Argued, no change.** It is: bounded by `MAX_COMMENT_LINES`, and the `reset()` itself was already unconditional at this point. Already stated in Residual risk 5, which is where the confirmation the review asks for lives. |
| `is.mark(0)` inside `Parser.resetInput()` now has no remaining reader. "Not worth a follow-up on its own, just flagging in case a future cleanup pass wants to fold it in." | **Argued, no change.** It was already inert before this PR - the anonymous `BufferedInputStream`'s `reset()` override sets `pos = 0` and never consults `markpos` - so this PR did not make it dead, it only removed the last `Parser.mark()` caller. Touching `Parser`'s internals is outside a `SourceDiscovery` bugfix, and the reviewer agrees it does not earn an issue. Residual risk 7. |
| The inline comments are dense; matches the file's convention, "not flagging it as inconsistent". | **No change.** Trimmed once already in cycle 1; a second pass would start removing the reasons rather than the narration. |
| The tracking doc is large; matches repo convention, "no concern there". | **No change.** |

**Separately**, the `code-review` subagent from Phase 1.5 - which had been sitting `running` since
before the first push, and whose findings never reached the session - turned out to be editing the
worktree while the review loop ran. It left debug `System.out.println` calls in `SourceDiscovery`, a
deleted comment block, a scratch `Probe7347b.java`, and one new test. The debris was discarded
(`git show HEAD:... > ...`, which is why the pushed commits never carried it) and the test kept, but
not as written:

- its fixture (`"\rid;name;score\n1,first,10\n2,second,20\n"`) does pin something real - a stray
  bare `'\r'` in an otherwise line-feed source, which is NOT the all-`'\r'` source cycle 1 tested -
  so it is kept, as `aStrayCarriageReturnDoesNotSwallowTheHeaderBelowIt`;
- its Javadoc claimed it caught "an earlier, still-buggy version of this test". Verified by removing
  the bare-`'\r'` early return from `skipLine()` and re-running: the test stayed GREEN, so the claim
  was wrong and the Javadoc was rewritten to say what the fixture actually separates;
- the branch it claimed to pin - "the character after a bare `'\r'` belongs to the next line" - had
  no test at all, because on a delimited source losing one character costs one character of a column
  name and the separator scan answers the same either way. `theCharacterAfterABareCarriageReturnIsNotEatenWithIt`
  was added for it, on a JSON source, because the first-character dispatch is the one reader that
  looks at exactly one character.

Mutation-tested to confirm each branch is pinned by a distinct test:

```
# bare-'\r' early return removed
[ERROR]   theCharacterAfterABareCarriageReturnIsNotEatenWithIt:377   <- and nothing else

# all bare-'\r' handling removed
[ERROR]   aStrayCarriageReturnDoesNotSwallowTheHeaderBelowIt:364
[ERROR]   aSourceTerminatedOnlyByCarriageReturnsStillHasItsCommentBlockSkipped:337
[ERROR]   theCharacterAfterABareCarriageReturnIsNotEatenWithIt:375
```

No deferred items.
