# 5629 - An explicit `null` in an optional argument propagates

Issue: https://github.com/ArcadeData/arcadedb/issues/5629

## The question

Cypher functions with an optional trailing argument were reading an **explicit** `null` in that position as
"argument omitted, use the default", while the same `null` in the first position propagated. The issue asked for
one of two settlements, and explicitly warned against settling it per-function:

1. **Propagate** - an explicit `null` optional argument makes the call answer `null`.
2. **Keep, and document** - "an explicit `null` in an optional position means the default" becomes the house rule.

## Decision: propagate

Three independent lines of evidence, all pointing the same way.

### 1. Neo4j propagates

The issue asked for Neo4j's behaviour to be checked first. From the Cypher manual source
(`neo4j/docs-cypher`, `modules/ROOT/pages/functions/`):

| function | documented consideration |
|---|---|
| `round(value[, precision, mode])` | "`round()` returns `null` if any of its input parameters are `null`." |
| `replace(original, search, replace [, limit])` | "If any argument is `null`, `null` will be returned." |
| `btrim(original [, trimCharacter])` | "`btrim("hello", null)` returns `null`." |
| `ltrim` / `rtrim` | "`ltrim("hello", null)` returns `null`." |

`round()` is the direct counterpart of the case raised in the issue, and it propagates. `replace()`'s `limit` and
`btrim()`'s `trimCharacter` are optional arguments whose omission selects a default, exactly the shape under
discussion, and both propagate.

Two of the five functions named in the issue have no Neo4j reference point at all: `format()` and
`vector_distance()` are ArcadeDB extensions. A third, `normalize()`, cannot express the case - Neo4j declares
`normalForm` as a keyword type `[NFC, NFD, NFKC, NFKD]` rather than an expression, so `normalize('x', null)` does
not parse there. `isNormalized()` is an operator in Neo4j (`IS NORMALIZED`), not a two-argument function. So
`round()` carries the weight, and it is unambiguous.

Neo4j is **not** uniformly "propagate": `substring(original, start, length)` and `left`/`right` raise an error on
a null length rather than answering null. But in no documented case does Neo4j read an explicit `null` as
"argument omitted". That is the invariant worth adopting.

### 1b. ArcadeDB had already decided this once, for `substring()`

`CypherSubstringFunction` - the implementation Cypher actually resolves `substring()` to - carries this comment,
added by issue #5193:

> `Issue #5193: an explicitly supplied null length propagates null (as Neo4j does), it must not be treated as an
> omitted argument.`

So the rule this issue asks for is not a new decision. It was made, for one function, with the same reasoning and
the same citation, and then not written anywhere the next function would find it. That is precisely the drift the
issue is about, and it is the argument for putting the rule on a shared helper rather than in a comment.

### 2. ArcadeDB already propagated, in most places

An audit of all 22 stateless Cypher functions whose `getMinArgs()` differs from `getMaxArgs()`:

- **9 already propagate**: the five temporal constructors (`date`, `datetime`, `localdatetime`, `time`,
  `localtime`), `point()`, `ltrim()`, `rtrim()`, `vector_create()`. `VectorCreateFunction` even carries an
  explicit `// Null propagation: if dimension is explicitly null, return null` comment.
- **11 silently defaulted**: the five named in the issue, plus the five `*.truncate` functions and `substring()`.
- **1 throws**: `range()`, deliberately, for issue #5477.

So propagation was already the majority reading, and the defaulting group was drift rather than a considered
house rule. Option 2 would have meant changing the 9 that were already right.

### 3. The two positions must agree

`normalize(null)` answers `null` but `normalize('x', null)` normalized as NFC. The same absent value meant two
different things depending on which position it landed in, which is the property that makes a wrong query look
like a successful one.

## The rule

> An explicit `null` in an optional argument position is never "argument omitted". Omitting the argument selects
> the function's default; writing `null` there is subject to the usual null-in/null-out rule.

It is stated once, on `CypherFunctionHelper.isExplicitNull(args, position)`, and every function with an optional
argument calls it rather than re-deciding. That is what the issue asked for: the mechanism, not a per-function
answer.

## Changes

All eleven defaulting functions now propagate.

| function | optional argument | before | after |
|---|---|---|---|
| `normalize(input, normalForm)` | normal form | NFC | `null` |
| `isNormalized(input, normalForm)` | normal form | NFC | `null` |
| `format(temporal, pattern)` | pattern | `toString()` | `null` |
| `round(value, precision, mode)` | rounding mode | HALF_UP | `null` |
| `vector_distance(a, b, metric)` | metric | EUCLIDEAN | `null` |
| `date.truncate(unit, input, fields)` | adjustment map | map ignored | `null` |
| `datetime.truncate(...)` | adjustment map | map ignored | `null` |
| `localdatetime.truncate(...)` | adjustment map | map ignored | `null` |
| `time.truncate(...)` | adjustment map | map ignored | `null` |
| `localtime.truncate(...)` | adjustment map | map ignored | `null` |
| `SubstringFunction` (see below) | length | to end of string | `null` |

Omitting the argument still selects the default in every one of them; only the explicitly-written `null`
changes.

### Two of the five named functions were not where the issue pointed

Both are dual-path cases, and finding them is the reason the audit was worth running:

- **`substring()`**: Cypher resolves it to `CypherSubstringFunction`, which already propagated (#5193).
  `com.arcadedb.function.text.SubstringFunction` is a *second* implementation of the same function that no factory
  currently registers, and it still defaulted. It is aligned here so the divergence does not surface the day it is
  wired up, and it is covered by a direct unit test since no query can reach it.
- **`vector_distance()`**: in Cypher this is a grammar rule whose metric is a keyword, so `vector_distance(a, b,
  null)` does not parse - the same situation as Neo4j's `normalize()`. `VectorDistanceFunction`, the class the
  issue points at, is reached from Cypher under the name **`vector.distance()`**, where the metric is an ordinary
  expression and the explicit null is expressible. That is the path the fix and the test use.

### Ordering is preserved in `round()`

`RoundFunction` validates every argument before null propagation decides the answer, so that an unusable mode or
an out-of-domain precision is reported even when the value is null (issue #5484). The new check sits with the
other null propagation, after validation, so `round(null, 2, 'SIDEWAYS')` still raises rather than answering
`null`.

### `substring()` diverges from Neo4j, deliberately

Neo4j raises on `substring(s, start, null)`. ArcadeDB's `substring()` already answers `null` for a null `start`,
so raising only for a null `length` would make one function disagree with itself. It propagates for both - which
is also what #5193 already chose for the Cypher-facing implementation. Whether ArcadeDB should instead follow
Neo4j and raise for *both* positions is a separate question - see follow-ups.

## Verification

New test class `CypherOptionalArgumentNullIssue5629Test` covers, for each of the eleven functions, that:

- an explicit `null` in the optional position answers `null`,
- **omitting** the argument still selects the documented default (the guard against over-applying the rule),
- and, where the function has one, the argument's validation still fires.

It also pins the nine functions that already propagated, so the convention cannot drift back.

## Existing tests changed

Two assertions encoded the behaviour this issue decided to change. Both were found by running the suite, not by
reading it, and each is a one-line change that leaves what the test was written to prove intact.

1. `CypherNumericFunctionArgumentIssue5484Test.theRoundingModeOfRoundIsNotANumericArgument` asserted
   `round(3.14159, 2, null) == 3.14`. It now expects `null`. The point of that test - that the mode position is
   not rejected as non-numeric - is unchanged, and `round(3.14159, 2, 'FLOOR')` and `'CEILING'` still pin it.
2. `TextStatelessFunctionsTest.formatFunctionNullPattern` asserted that a null pattern answers `toString()`. It
   now expects `null`, and gained an assertion that *omitting* the pattern still answers the ISO string - the
   behaviour that test was really guarding.

## PR and review history

PR: https://github.com/ArcadeData/arcadedb/pull/5699

| cycle | head | outcome | applied |
|---|---|---|---|
| 1 | `5ae1ba3b7` | LGTM, 3 non-blocking | Moved the pre-existing misplaced `CypherFunctionHelper` import into the grouped block in the five truncate files. Two other points corrected rather than applied: the "redundant post-check guards" are still load-bearing (`args.length >= 3` carries the omitted-vs-present distinction; `instanceof Map` still rejects a present non-map value), and the import was not introduced by this PR. |
| 2 | `28f6464a5` | LGTM, 3 minor | Strengthened the four truncate default-path assertions from `.isNotNull()` to the concrete truncated value, and moved `@SuppressWarnings("unchecked")` from `getMinArgs()` to `execute()` where the cast lives (in all five files, not the two reported). |

CI on `28f6464a5`: `build-and-package` SUCCESS, CodeQL all languages SUCCESS, Codacy 0 new issues, Meterian SUCCESS.

### Reviewer point not actioned

Cycle 2 asked for a changelog/release-note line, since this changes observable output for 11 functions - most visibly
`format(x, null)`, which now answers `null` instead of the ISO string. The repository has no `CHANGELOG` file, so
there is nowhere to put it here. The behavioural change is documented in the PR body and in the table above;
whoever cuts the release should carry it into the release notes.

## Follow-ups (not in this change)

1. **The `*.truncate` family raises raw JDK exceptions for client mistakes.** Two cases, both surfacing as HTTP
   500 for what is the caller's error, and both the same class of defect as #5484:
   - `args[0].toString()` on the unit selector has no null check, so a null unit is a `NullPointerException`.
     `TrimFunction` does the same for its mode.
   - An unknown unit reaches `TemporalUtil.truncateDate`, which throws `IllegalArgumentException: Unknown
     truncation unit: ...` rather than a `CommandSemanticException`. The regression test added here asserts only
     the message for that case, deliberately, so it does not encode the wrong exception class as correct.
2. **`trim(mode, trimChar, source)` defaults a null `trimChar` to whitespace**, disagreeing with `ltrim`/`rtrim`
   next to it and with Neo4j's `btrim("hello", null)` -> `null`. Its 1-or-3 arity means the argument is not
   trailing, so it is a different shape and was left alone.
3. **`range()` throws on a null step** rather than propagating, by explicit decision in #5477. Worth confirming
   that is still the intended reading now that the convention is written down.
4. **`substring()` null `start` and `length`**: propagate (current, after this change) or raise as Neo4j does?
5. **`com.arcadedb.function.text.SubstringFunction` is registered by no factory**, so Cypher's `substring()` and
   this class are two implementations of one function with only one of them reachable. Either wire it up or
   delete it; keeping both invites the divergence this change just repaired.
6. **The `*.truncate` family silently ignores a present, non-null, non-map third argument.**
   `date.truncate('year', d, 42)` answers the truncated date and drops the `42`, because the `args[2] instanceof
   Map` guard is the only thing looking at it. That is the same shape of defect this change repaired for `null`,
   one step further out: a wrong query looks like a successful one. It should be a type error.

   Deliberately **not** covered by a characterization test here. Asserting that `date.truncate('year', d, 42)`
   returns the truncated value would encode the defect as the expected behaviour and make fixing it look like a
   regression - the same trap avoided with the unknown-unit exception class above. It belongs with follow-up 1,
   as one pass over the family's argument handling.
