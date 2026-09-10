# #7321 — `/ts/{database}/latest` accepts only one tag

Issue: https://github.com/ArcadeData/arcadedb/issues/7321
Branch: `feat/7321-ts-latest-multiple-tags`
Type: enhancement (labels on the issue: `enhancement`, `server`, `timeseries`)

## Goal

`GET /api/v1/ts/{database}/latest` narrows a series with a single `tag=name:value` query
parameter and honours only the first occurrence when it repeats, so on a type with more than
one tag column a caller cannot name one series. Its sibling `POST /ts/{database}/query` takes a
whole `tags` object and conjoins every pair. Make `latest` read every `tag` occurrence and
conjoin them the same way.

## Analysis

`GetTimeSeriesLatestHandler.buildTagFilter` reads the parameter through
`AbstractServerHttpHandler.getQueryParameter(exchange, "tag")`, which is
`exchange.getQueryParameters().get(name).getFirst()` — every later occurrence in the `Deque`
is dropped before the handler ever sees it. Undertow already parses `?tag=a:1&tag=b:2` into a
two-entry `Deque`; nothing below the accessor loses it.

The conjunction machinery is already there and unused by this handler:

- `TagFilter.and(int, Object)` returns a new filter with one more ANDed condition.
- `TimeSeriesHandlerUtils.buildTagFilter(JSONObject, List<ColumnDefinition>)` already walks a
  whole map and chains `TagFilter.eq` / `TagFilter.and`. `/ts/query` and the Grafana query
  handler both go through it.
- `SpecBuilders.repeatableQueryParam` already exists, added for Prometheus `match[]`, and
  `GetPromQLSeriesHandler` already reads its whole `Deque` — the exact precedent for a
  repeatable query parameter on this server.

So the asymmetry is confined to one handler method plus the OpenAPI text that documents it.

### Design decision — one shared per-pair resolver

Rather than copy the map-walking loop into the handler, the name-to-column-position resolution
moves into `TimeSeriesHandlerUtils.andTag(...)`, and both `buildTagFilter(JSONObject, ...)` and
the new `buildTagFilterFromQueryParams(Collection<String>, ...)` call it. Two independent
copies of this loop is what let the two endpoints drift apart in the first place; one resolver
is what stops it happening again.

### Semantics chosen for an occurrence that names no tag column

`/ts/query` silently contributes no conjunct for a `tags` key that matches no TAG column. The
new `latest` behaviour matches it, because the issue asks for "exactly as the query endpoint
conjoins its `tags` object". That silent drop is a real hazard on both endpoints — see
Residual risk — but it is pre-existing, shared, and changing it is a separate contract change.

## Completeness

### Invariant

> Every `tag=name:value` occurrence a caller sends to `GET /api/v1/ts/{database}/latest`
> contributes an AND-conjunct to the `TagFilter` the engine scans with; no occurrence past the
> first is dropped by the accessor.

### Commands run

```
$ grep -rn "String getQueryParameter" server/src/main/java
AbstractServerHttpHandler.java:1351:  protected String getQueryParameter(final HttpServerExchange exchange, final String name)
AbstractServerHttpHandler.java:1355:  protected String getQueryParameter(..., final String defaultValue)
   -> 1356:  final Deque<String> par = exchange.getQueryParameters().get(name);   // returns getFirst()

$ grep -rn "buildTagFilter" --include=*.java .   # (excluding target/)
server/.../GetTimeSeriesLatestHandler.java:90,117      <- the reported site
server/.../PostTimeSeriesQueryHandler.java:105,253,256 -> TimeSeriesHandlerUtils.buildTagFilter(JSONObject)
server/.../PostGrafanaQueryHandler.java:115            -> TimeSeriesHandlerUtils.buildTagFilter(JSONObject)
server/.../TimeSeriesHandlerUtils.java:37              <- the map-walking implementation
engine/.../promql/PromQLEvaluator.java:249,305,547     <- its own List<LabelMatcher> walker
server/src/test/.../openapi/TimeSeriesApiSpecTest.java:144  <- the pinned wording

$ grep -rn "getQueryParameters()" server/src/main/java | grep -v "getQueryParameter("
   23 hits. Every one but two reads a single-valued key (database, precision, name, userName).
   The two that read a whole Deque on purpose:
     GetPromQLSeriesHandler.java:68   matchParams = ...get("match[]")   <- repeatable, done right
     GetTimeSeriesLatestHandler.java:51 databaseParam (single-valued, .getFirst())

$ grep -rn "repeatableQueryParam" server/src/main/java server/src/test/java
   SpecBuilders.java:101         the builder
   PrometheusApiSpec.java:211    match[]  <- the only production user so far
   SpecBuildersTest.java:55,56   its unit test

$ grep -rni "TimeSeriesTagFilter" .     -> 0 hits   (gRPC RPC of #7305 not implemented)
$ grep -rn  "timeSeriesLatest"    .     -> 0 hits   (RemoteDatabase method of #7307 not implemented)
$ gh issue view 7305 -> OPEN "gRPC proto has no time-series API"
$ gh issue view 7307 -> OPEN "RemoteDatabase has no client access to the /ts/* time-series API"
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `GET /ts/{db}/latest?tag=…` → `GetTimeSeriesLatestHandler.buildTagFilter` | **fixed here** | yes — 9 cases in `TimeSeriesTagFilterBuilderTest` (port-free) and 4 over a real socket in `TimeSeriesQueryHandlerIT`: `latestNarrowsOnEveryRepeatedTagOccurrence`, `latestWithASingleTagIsUnchangedByTheRepeatableParameter`, `latestAndsTwoOccurrencesOfTheSameTagRatherThanUnioningThem`, `latestKeepsTheResolvedTagsWhenAnOccurrenceNamesNoTagColumn` |
| OpenAPI `tag` parameter on `/ts/{db}/latest` (`TimeSeriesApiSpec.createLatestPath`) | **fixed here** — re-described and re-typed as a repeatable array parameter | yes — `TimeSeriesApiSpecTest.tagIsARepeatableParameterWhoseOccurrencesAreConjoined` (replaces the pin the issue asked to update) |
| `POST /ts/{db}/query` `tags` object → `TimeSeriesHandlerUtils.buildTagFilter(JSONObject,…)` | **argued** — already conjoined every pair before this change; refactored onto the shared `andTag` resolver with no behaviour change | yes — existing `queryWithTagFilter`, plus `TimeSeriesTagFilterBuilderTest.theTagsObjectPathStillAndsEveryPair` and the IT `queryConjoinsEveryTagPair`, both guarding the refactor |
| Grafana `POST /grafana/query` → the same `TimeSeriesHandlerUtils.buildTagFilter(JSONObject,…)` | **argued** — one shared helper, same refactor, same behaviour | existing Grafana handler ITs |
| PromQL `{a="x",b="y"}` → `PromQLEvaluator.buildTagFilter(List<LabelMatcher>,…)` | **argued** — a separate walker over the matcher list that already chains `TagFilter.and` for every matcher; untouched | existing PromQL tests |
| gRPC `TimeSeriesLatest` RPC | **argued** — does not exist on `main`; `grep -rni TimeSeriesTagFilter` returns 0 hits and #7305 is OPEN | n/a |
| `RemoteDatabase.timeSeriesLatest(type, tagName, tagValue)` | **argued** — does not exist on `main`; `grep -rn timeSeriesLatest` returns 0 hits and #7307 is OPEN. When #7307 lands it can take a map directly | n/a |
| A `tag` occurrence naming no TAG column, or carrying no `:`, contributes no conjunct and is not reported — on `latest` **and** on `/ts/query` | **filed** — #7334 | no |

### Reachability

`GetTimeSeriesLatestHandler` is registered on a live route:
`HttpServer` binds `/api/v1/ts/{database}/latest` to it, and `TimeSeriesQueryHandlerIT` reaches
it over a real socket against a real server, not through a unit-level stub. No feature flag
gates the handler; the `tag` parameter is read per request, not bound at startup.

### Modified existing test — deliberate, and requested by the issue

`constraints.md` forbids modifying existing tests. `TimeSeriesApiSpecTest.tagDescriptionPins
TheColonSeparatorAndFirstOccurrenceOnlySemantics` is the one exception taken here, because it
pins the *contract this issue exists to change* and the issue body names updating it as part of
the work ("`TimeSeriesApiSpecTest`'s pinned wording updated to match, which is why this was not
folded into #7305"). It is replaced by an equivalent pin over the new wording plus the
repeatable declaration, not deleted and not weakened.

## Follow-up issues filed

- #7334 — a tag name that matches no `TAG` column (and, on `latest`, an occurrence with no `:`)
  is silently dropped instead of rejected, on `/ts/query`, the Grafana query handler and `latest`.

## Residual risk

- An occurrence whose name matches no TAG column, or that carries no `:`, is silently dropped
  rather than rejected. A typo therefore widens the result set instead of erroring — the caller
  gets the newest sample of a series they did not ask for. This is pre-existing, is shared with
  `/ts/query` and the Grafana handler, and is now filed as https://github.com/ArcadeData/arcadedb/issues/7334.
- `latest` still scans `Long.MIN_VALUE..Long.MAX_VALUE` and takes the last row, so a narrower
  filter does not make it cheaper. Unchanged by this work.
- Nothing else in the coverage table is left blank.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen the author's reasoning. **No
`Task` tool is exposed in this environment**, so no such subagent could be spawned; the pass was
run by the author against the diff instead, which is weaker — it had already been convinced.
Recorded here rather than silently skipped.

| Finding | Disposition |
|---|---|
| Two occurrences of the SAME tag name (`?tag=host:a&tag=host:b`) were left unspecified. The loop ANDs them, so the request selects nothing — defensible, but accidental rather than stated, and a plausible thing for someone to later "fix" into a set-membership reading, which would silently widen every such request | **Fixed here** — pinned by `TimeSeriesTagFilterBuilderTest.twoOccurrencesOfTheSameTagAreAndedNotUnioned` and the IT `latestAndsTwoOccurrencesOfTheSameTagRatherThanUnioningThem`, and stated in `buildTagFilterFromQueryParams`'s javadoc. The OpenAPI wording "every occurrence must match" already reads correctly for it |
| A tag position is an index among the NON-timestamp columns, so an off-by-one would still look like a working filter on a single-tag type and only misbehave once a second tag column exists — exactly the shape this issue introduces | **Fixed here** — `tagPositionSkipsTheTimestampColumnOnly` pins `region` at index 1 with a FIELD column after it, and asserts the filter reads `region` and not `host` |
| An occurrence naming a FIELD column or the TIMESTAMP column resolves to nothing, same as an unknown name | **Filed** — part of #7334; the skip itself is covered by `anUnresolvableOccurrenceIsSkippedWithoutDiscardingTheOthers`, which also proves it does not discard the occurrences that did resolve |
| A committed copy of the OpenAPI document might carry the old `tag` wording and drift | **Not real** — `grep -rn "Only the first occurrence\|name:value" --include=*.json --include=*.yaml --include=*.yml --include=*.md --include=*.adoc --include=*.js --include=*.py --include=*.go .` (excluding `target/`) returns only this tracking doc. The spec is generated from `TimeSeriesApiSpec` at runtime |
| The refactor could have changed the `tags`-object path's behaviour | **Not real** — `andTag` returns where the old inner loop `break`ed with the same assignment; `theTagsObjectPathStillAndsEveryPair` and the existing `queryWithTagFilter` both hold |

## Test results

Run with `-Dmaven.repo.local=$WORKTREE/.m2repo` (parallel-build isolation).

| Suite | Result |
|---|---|
| `TimeSeriesTagFilterBuilderTest` (new, 9 cases) | 9/9 pass |
| Same, with `buildTagFilterFromQueryParams` temporarily reverted to first-occurrence-only | **3 fail** (`everyOccurrenceBecomesAnAndedCondition`, `occurrenceOrderDoesNotMatter`, `twoOccurrencesOfTheSameTagAreAndedNotUnioned`) — the tests can fail, and fail for the right reason |
| `TimeSeriesApiSpecTest` | 12/12 pass; before the spec change the 2 new/updated cases failed on the old wording and on `expected "array" but was "string"` |
| `SpecBuildersTest` | 12/12 pass |
| `arcadedb-server` full unit suite (`mvn -o -pl server test`) | 1293/1293 pass, 0 failures, 0 errors — no regression |

### Verification gap: the integration tests did not run locally

`TimeSeriesQueryHandlerIT` binds `127.0.0.1:2480`. On this machine that port is held by a
Homebrew ArcadeDB 26.9.1 service (`com.arcadedb.server.ArcadeDBServer`, up 5+ days), so the run
fails with `Error on connecting to server http://127.0.0.1:2480` before reaching any assertion.
Stopping a developer's own long-running service to run a test is not a call this workflow should
make unattended, so the 5 new IT methods are **verified by CI on the PR, not locally**. What
they assert is covered locally and exhaustively by `TimeSeriesTagFilterBuilderTest`, which needs
no port and is proven able to fail.
