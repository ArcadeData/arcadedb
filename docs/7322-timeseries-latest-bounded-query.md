# #7322 - TimeSeries `latest` scans the whole series to return one row

Issue: https://github.com/ArcadeData/arcadedb/issues/7322
Type: bug (performance)
Branch: `fix/7322-timeseries-latest-bounded-query`

## Problem

`TimeSeriesGateway.latest(engine, tagFilter)` answered "the newest sample" with:

```java
final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, tagFilter);
return rows.isEmpty() ? null : rows.get(rows.size() - 1);
```

`TimeSeriesEngine.query` merges every shard's full range into one `ArrayList` and sorts it, so a
type holding N samples costs O(N) heap and O(N log N) time to answer a question about one row.
`TimeSeriesEngine.queryDescending(from, to, cols, tagFilter, limit, metrics)` - added in #5414 /
#5416 for exactly this shape - answers it in O(shards x blocks touched).

## Root cause

`latest` was extracted verbatim from `GetTimeSeriesLatestHandler` in #7305; #7305's goal was that
HTTP and gRPC answer *identically* against a known baseline, so the code was moved, not changed.
The bounded form was left for this issue because it changes which row wins a tie at the newest
timestamp.

## Analysis

`queryDescending` accumulates shard-by-shard in shard order and calls
`TimeSeriesSealedStore.trimToDescendingLimit(merged, need)`, which is a **stable** descending sort
followed by `removeLast()`. With `need == 1` the surviving row is therefore the first row in
insertion order among those sharing the maximum timestamp - i.e. the one from the earliest shard
that holds a row at that timestamp.

`query()` is the mirror image: a stable **ascending** sort of the same shard-ordered list, then
`rows.get(size - 1)`, which is the **last** of the tied rows.

So the two disagree exactly on ties, and only on ties. Nothing specified either answer.

## Decision on the tie-break

`latest` now returns **the row the newest-first scan yields first**. The javadoc states the
contract as: the timestamp is the guarantee; among rows sharing it the descending scan's first row
wins, which for a tie spread across shards is the lowest-numbered shard's row. A caller that needs
to disambiguate tied samples narrows the selection with tags.

This is a deliberate, documented change from the pre-#7322 answer (last of the tied rows in
ascending merge order), pinned by `Issue7322LatestBoundedTest`.

## Completeness

### Invariant

> Answering "the newest sample of this selection" never materialises more of the series than the
> blocks and pages the newest-first scan has to touch.

### Enumeration

Callers of the changed method:

```
$ grep -rn "TimeSeriesGateway.latest" --include="*.java" */src/main/java
server/src/main/java/com/arcadedb/server/http/handler/GetTimeSeriesLatestHandler.java:81
grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java:3645
```

Every "newest sample" surface in the tree:

```
$ grep -rniE "ts/.*latest|timeSeriesLatest" --include="*.java" --include="*.proto" */src/main
grpc/src/main/proto/arcadedb-server.proto:197   rpc TimeSeriesLatest
server/src/main/java/com/arcadedb/server/http/HttpServer.java:261  .get("/ts/{database}/latest", ...)
grpc-client/.../RemoteGrpcDatabase.java:2437    client stub -> gRPC RPC above
network/.../RemoteDatabase.java:990             client stub -> HTTP endpoint above
```

The two client stubs are transport shells over the two server entry points; they hold no query
logic. The SQL path (`FetchFromTimeSeriesStep`) already uses `queryDescending`.

Siblings - other `engine.query(...)` call sites in main sources:

```
$ grep -rn "engine\.query(" --include="*.java" */src/main/java
engine/.../TimeSeriesGateway.java:279                     <- fixed here
server/.../PostGrafanaQueryHandler.java:141               bounded range, returns the rows
server/.../GetPromQLLabelValuesHandler.java:93            UNBOUNDED, discards the rows -> #7371
server/.../PostTimeSeriesQueryHandler.java:117            bounded range, returns the rows
server/.../PostPrometheusReadHandler.java:157             bounded range, returns the rows
server/.../GetPromQLSeriesHandler.java:103                bounded range, discards the rows -> #7371
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `GET /api/v1/ts/{database}/latest` -> `GetTimeSeriesLatestHandler` -> `TimeSeriesGateway.latest` | yes | yes |
| gRPC `TimeSeriesLatest` -> `ArcadeDbGrpcService.timeSeriesLatest` -> `TimeSeriesGateway.latest` | yes | yes |
| `RemoteDatabase.timeSeriesLatest` (HTTP client stub) | yes (transport over row 1) | yes |
| `RemoteGrpcDatabase.timeSeriesLatest` (gRPC client stub) | yes (transport over row 2) | yes |
| SQL `SELECT ... ORDER BY ts DESC LIMIT n` -> `FetchFromTimeSeriesStep` | n/a - already bounded | pre-existing |
| PromQL label/series discovery -> `engine.query` over the full range | **no** | no - filed as #7371 |
| `/ts/query`, `/ts/grafana`, Prometheus remote-read | argued: the range is caller-bounded and every row is returned, so the materialisation is the answer, not waste | n/a |

### Residual risk

The fix does not change any endpoint that legitimately returns the rows it materialises. It does
change which row `latest` returns when several samples share the newest timestamp; that case was
unspecified before and is now specified and tested.

## Change

`engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesGateway.java`

```java
final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, tagFilter, 1, null);
return newest.isEmpty() ? null : newest.getFirst();
```

The javadoc now states the cost, the tie-break and the fact that both protocols call this method and
nothing else. `GetTimeSeriesLatestHandler`'s comment, which described the old whole-range scan, was
updated to match; no other call-site logic changed.

## Reachability

Both call sites are on live paths, verified by grep rather than recalled:

- `HttpServer.java:261` registers `.get("/ts/{database}/latest", new GetTimeSeriesLatestHandler(this))`
  on the running route table.
- `arcadedb-server.proto:197` declares `rpc TimeSeriesLatest`, implemented at
  `ArcadeDbGrpcService.java:3631`.

Neither is behind a feature flag.

## Test results

`engine/src/test/java/com/arcadedb/engine/timeseries/Issue7322LatestBoundedTest.java` - 6 tests.

Against the **unfixed** tree, `aTieAtTheNewestTimestampGoesToTheRowTheNewestFirstScanYieldsFirst`
failed with `expected: [.., "in_shard_0", 1.0] but was: [.., "in_shard_1", 2.0]` - the old ascending
scan answered the last of the tied rows. The other five passed on both trees; they are the
correctness guard that the swap did not change the non-tie answer.

```
mvn -o -pl engine -am test -Dtest='TimeSeries*,Issue5414*,Issue5416*,Issue7322*'
  Tests run: 278, Failures: 0, Errors: 0, Skipped: 0     BUILD SUCCESS
```

That run includes `Issue5414LastPointTest` (22) and `Issue5416DescendingTailTest`, which own the
bounded scan this fix now depends on.

```
mvn -o test-compile -pl server,grpcw -am                 BUILD SUCCESS
mvn -o test -pl server -Dtest='TimeSeriesTagFilterBuilderTest,TimeSeriesApiSpecTest'
  Tests run: 21, Failures: 0, Errors: 0, Skipped: 0      BUILD SUCCESS
```

**Not run:** `TimeSeriesQueryHandlerIT` and `Issue7305TimeSeriesGrpcIT`, the protocol-level tests
that drive the two entry points. They bind hard-coded ports 2480/2481
(`"http://127.0.0.1:248" + serverIndex + ...`), and `lsof -nP -iTCP:2480 -sTCP:LISTEN` showed both
held by concurrent builds for the whole of this session. Running them would have answered against
someone else's server and produced a meaningless result in either direction. Their assertions
(`latestValue` pins ts 3000, `latestWithTagFilter` pins ts 2000, `Issue7305TimeSeriesGrpcIT`
cross-checks HTTP against gRPC) are all non-tie cases, which the engine-level suite covers
identically; CI runs them on free ports.

## Adversarial pass

The orchestrator's Phase 1.5 spawns an independent subagent for this. **No `Task` tool exists in
this environment**, so the pass was run inline - which is weaker, because the reviewer had already
been convinced by its own reasoning. Recorded as a known reduction in the gate's value, not as a
pass.

**1. Does any existing protocol test assert a `latest` answer where the newest timestamp is TIED?**
Real and load-bearing: those are precisely the tests the port conflict stopped me running, so a tie
in one of their fixtures would have been an undetected break. Checked every fixture that reaches
`latest`:

| Fixture | Timestamps | Tie at max? |
|---|---|---|
| `TimeSeriesQueryHandlerIT.createTypeAndIngestData` | 1000, 2000, 3000 | no |
| `Issue7305TimeSeriesGrpcIT.threeSamples` | 1000, 2000, 3000 | no |
| `Issue7305TimeSeriesGrpcIT.aNonFiniteLatestSampleIsNullOnBothProtocols` | 1000, 2000 | no |
| `Issue7305RemoteDatabaseTimeSeriesIT` | 1000, 2000, 3000 | no |
| `Issue7305TimeSeriesGrpcAclIT` / `TimeSeriesPerTypeAclIT` | 1000, 2000 | no |

Every one is a distinct-timestamp fixture, so none of them can observe the tie-break change. The
cross-protocol agreement assertions (`assertValuesAgree(latestHttp.latest(), latestGrpc.latest())`)
hold by construction either way: both protocols call this one method and nothing else.

**2. A no-match tag filter still walks every block.** Real, and deliberately not fixed. When the
filter matches nothing, no shard ever collects a row, so the running lower bound never tightens and
each shard walks its whole range - the caveat `queryDescending`'s own javadoc records from #5416.
It is a change in *which* resource is spent, not a regression: the old path walked everything AND
allocated an `Object[]` per row, which is the cost this issue is about. Fixing it needs a per-shard
tag index, which is a different piece of work. Covered for correctness by
`latestIsNullWhenTheSelectionHoldsNoRow`.

**3. Does the NaN path survive the swap?** `Issue7305TimeSeriesGrpcIT` stores a NaN sample and
asserts both protocols render it as null. The descending walk prunes on timestamps only and boxes
values through the same codec as the ascending one, so the rendered row is unchanged;
`TimeSeriesAccuracyTest` (6 tests) and the 278-test engine suite cover the codec.

## Residual risk

- The tie-break at the newest timestamp changed, deliberately and by the issue's own request. No
  existing test observes it (evidence: the table above); the new one pins it.
- A `latest` call whose tag filter matches nothing reads every block of every shard. Pre-existing
  in `queryDescending`, shared with the SQL `ORDER BY ts DESC LIMIT n` path, unchanged by this fix.
- PromQL label/series discovery still materialises whole series - filed as #7371.
- `TimeSeriesQueryHandlerIT` and `Issue7305TimeSeriesGrpcIT` were not run locally (ports held). CI
  runs them.

## Review cycle 1 - `bf280e89`

`claude` reviewed on the PR issue-comment surface. No blocking findings; the review traced the
stable-sort tie-break, the round-robin routing the tie test depends on, and the `lowerBound`
inclusivity end-to-end rather than taking the write-up at face value, and confirmed both protocols
call the gateway method and nothing else.

| Comment | Disposition |
|---|---|
| The `boundedNewest` test helper duplicates the expression under test, so those assertions are "closer to A equals A than an independent check" | **Applied.** Real, and worth fixing even though the review called it non-blocking: an assertion that cannot fail is not coverage. Replaced with `exhaustiveNewest`, the pre-#7322 ascending scan, which is a genuinely independent implementation of the same question. The helper now also asserts the newest timestamp is unique in the selection, so a fixture later edited into a tie fails loudly instead of silently comparing against the wrong row. Removed from the tie test entirely - the tie is the one case where the two implementations disagree by design, so the expected row is spelled out there. |
| Confirm the two port-blocked ITs are green in CI before merge | **Handed to the developer**, with the check names, in the PR. Nothing to change in the branch; CI runs them on free ports. |
| The new javadoc is long, if the team prefers terser | **Skipped, with reason.** The review itself judged the length appropriate for a previously-unspecified tie-break contract, and this is the only place that contract is written down. Trimming it would delete the answer to the question the next reader will have. |

Re-verified after the change: `Issue7322LatestBoundedTest` 6/6 green on the fixed tree, and still
**red on the unfixed one** - reverting `TimeSeriesGateway.latest` to `engine.query(...)` fails
`aTieAtTheNewestTimestampGoesToTheRowTheNewestFirstScanYieldsFirst` with
`expected: "in_shard_0" but was: "in_shard_1"`. The oracle swap did not weaken the pin.
Full suite re-run: 278 tests, 0 failures.

## Review cycle 2 - `ee07eac1`

`claude` re-reviewed and closed with **"No blocking issues found."** It re-traced the stable-sort
tie-break, the inclusive `lowerBound` pruning and the single-gateway claim independently, and
called the oracle replacement "exactly the kind of self-correction that should happen in response
to review feedback."

Three non-blocking notes, none of them changes to this branch:

| Comment | Disposition |
|---|---|
| A non-matching tag filter still forces a full walk | Already documented as out of scope; the review agreed it is a pre-existing `queryDescending` limitation shared with the SQL path, not a regression. |
| Confirm the two port-blocked ITs are green in CI before merge | For the developer. They are the only tests exercising the real HTTP/gRPC entry points end to end. |
| The tracking doc is long for the size of the code change | Noted by the review itself as consistent with the repo's `docs/73xx-*.md` convention. No change. |

**No deferred items.** `docs/review-deferred-47afd7da.md` in this worktree is not from this run - it
came in on `main` with #7210 (`git log -1 --format=%H -- docs/review-deferred-47afd7da.md` is an
ancestor of `main`). This branch touches exactly four files.

## Outcome

- PR: https://github.com/ArcadeData/arcadedb/pull/7376
- Final state: **clean-approval** after 2 review cycles.
- Follow-up filed: #7371 (PromQL discovery still materialises whole series).
- Merge belongs to the developer.

### Ledger

- [x] Move `TimeSeriesGateway.latest` onto `queryDescending(from, to, null, tagFilter, 1, null)` - done.
- [x] Decide and document the tie-break - done: the timestamp is the guarantee; among rows sharing
      it the newest-first scan's first row wins. Stated in the javadoc.
- [x] Add a test that pins it - `aTieAtTheNewestTimestampGoesToTheRowTheNewestFirstScanYieldsFirst`,
      verified red against the unfixed tree both before and after the review change.
