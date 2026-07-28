# MCP `hybrid_search` Tool - Design

**Issue:** [#4861](https://github.com/ArcadeData/arcadedb/issues/4861)
**Epic:** [#4859](https://github.com/ArcadeData/arcadedb/issues/4859) - MCP GraphRAG & Agent-Memory Surface
**Date:** 2026-07-28
**Status:** design approved, spec under review

## Problem

`vector.fuse` already fuses two or more ranked retrieval pipelines server-side (RRF, DBSF, LINEAR),
and `SEARCH_INDEX` already ranks full-text hits. What no single expression can do is fan out over
graph relationships from the retrieved set and have those neighbors compete for the final top-K.
Reaching that today means orchestrating several statements by hand, with no depth cap, no fan-out
cap, and no dedup of nodes reachable by more than one path.

Against epic #4859's governing principle, `hybrid_search` qualifies primarily under criterion 2
(unreachable as a single call) and secondarily under criterion 1 (ergonomic guardrail over
`vector.fuse` argument shapes).

## Dependency status

The issue records two blockers. Both are resolved:

- **#4860 (`vector_search`)** has landed. `VectorSearchTool` is on main and its validation logic is
  the basis for the shared vector leg below.
- **#5267 (per-bucket BM25)** was closed 2026-07-27 by PR #5400, which moved full-text corpus
  statistics to type-wide scope. The stated objection - that fusing per-bucket-normalized BM25
  scores with vector similarities would bake an unsoundness into a second surface - no longer
  applies. BM25 scores are now comparable across buckets, so the full-text leg can be fused
  directly.

`hybrid_search` is already present in `MCPDispatcher.RAG_TOOL_NAMES`, reserved by #4867. Registering
the tool activates that entry with no configuration change.

## Verification performed before this design

Two throwaway probes were run against the engine to test the load-bearing assumptions. Both changed
the design. The probes were deleted after recording their results.

1. **`vector.fuse` accepts bound `List<Map>` legs.** This statement, with no `FROM`, executes and
   returns `record`, the flattened record properties, `@rid`, `@type`, and `score`:

   ```sql
   SELECT expand(`vector.fuse`(:l1, :l2, :l3, :opts))
   ```

   A leg carrying **no score key at all** fuses correctly under RRF, which is what makes the
   rank-only expansion leg possible.
2. **RIDs must be bound as `RID` objects, not RID strings.** `SQLFunctionVectorFuse.extractRid`
   accepts `RID` and `Identifiable` only and silently drops anything else, so a leg built from
   `rid.toString()` fuses to an empty result with no error.
3. **`TRAVERSE ... FROM :ridCollection` throws `NullPointerException`.** So does
   `SELECT FROM :ridCollection`. `TraverseExecutionPlanner:155-174` and
   `SelectExecutionPlanner:1435-1453` build `Rid` AST nodes for each element but never call
   `rid.setLegacy(true)`, which their own singleton-RID branches do a few lines above
   (`TraverseExecutionPlanner:150`). `Rid.toRecordId` then dereferences the null `expression` field.
   Inlining RID literals (`FROM [#1:0,#1:1]`) works and returns full BFS metadata. This is a
   pre-existing engine defect, filed separately; `hybrid_search` works around it.
4. **`out(:boundEdgeList)` silently matches nothing.** Passing a bound `List<String>` as the
   edge-type argument returns only the seeds - no neighbors, no error. Edge type names must be
   inlined as validated, quoted literals.

Probe 3's inlined form returns exactly what the issue asks for:

```
TRAVERSE out('LINK') FROM [#1:0,#1:1] MAXDEPTH 2 LIMIT 50 STRATEGY BREADTH_FIRST

  rid=#1:0 depth=0 path=[#1:0]
  rid=#1:1 depth=0 path=[#1:1]
  rid=#1:2 depth=1 path=[#1:0, #1:2]      <- reachable two ways, emitted once, shallowest path
  rid=#1:3 depth=2 path=[#1:0, #1:2, #1:3]
```

`$path` is an `ArrayList<RID>` including the seed. Nodes reachable by more than one path are emitted
once at their shallowest depth, which satisfies the issue's dedup criterion without extra code.

## Design decisions

### Graph expansion is a third ranked fusion source

Vector hits and full-text hits are retrieved first. Their union seeds a breadth-first traversal, and
the expanded neighbors form a third ranked list. All available legs go into `vector.fuse`, producing
one flat ranked list in which a neighbor competes on its own merits and carries the path back to its
seed.

The alternative - fusing the two retrieval legs and then attaching neighborhoods as nested context on
each result - is the more common "retrieve then enrich" GraphRAG shape, but it does not match the
issue's flat `{rid, fusedScore, path?, properties}` output or its "dedup of nodes reached via
multiple paths **before fusion scoring**" criterion. Shipping both behind an `expandMode` parameter
was rejected: two output shapes from one tool doubles the test matrix and the reasoning burden on the
model, against the epic's explicit surface-discipline principle.

### The expansion leg is rank-only, and forces RRF

An expanded node has no natural score, only a BFS position. RRF is rank-only, so the expansion leg
enters fusion with no score field and its BFS order as its rank; no scoring math is invented at the
MCP layer.

DBSF and LINEAR require a numeric score on every row of every source and would otherwise throw
`CommandSQLParsingException` from deep inside `SQLFunctionVectorFuse.requireScores`. `hybrid_search`
therefore rejects `DBSF`/`LINEAR` combined with `expand` up front, naming both remedies. Both
strategies remain available for vector + full-text fusion without expansion.

Synthesizing a decayed score (`seedScore * decay^depth`) was rejected: `decay` would be a tuning knob
with no engine counterpart, producing a number that looks like a similarity while being nothing of
the kind. Silently downgrading DBSF to RRF was rejected as ignoring an explicit caller request.

### Java orchestrates the legs; the engine scores the fusion

Each leg runs independently from Java, which owns the caps, the ordering, and the error messages. The
three materialized rank lists are then handed to the engine's `vector.fuse` as bound parameters, so
fusion scoring has exactly one implementation in the tree.

A single generated statement using `LET` was rejected: the expansion leg must read the vector leg's
output, so it would depend on `LET` evaluation order, and per-leg caps and error messages become much
coarser. Reimplementing RRF in the MCP layer was rejected as forking the fusion implementation on the
surface the epic wants to be canonical.

### `vector.hybridScore` is deliberately not used

The engine also exposes `vector.hybridScore`, an alpha blend
`(vector_score * alpha) + (keyword_score * (1 - alpha))`. It takes exactly two numeric scores, so it
can neither accept a third leg nor accept the rank-only expansion leg. `vector.fuse` is the correct
primitive.

### The vector leg reaches full parity with `vector_search`

`vector_search`'s validation is extracted into a shared helper so `hybrid_search` accepts `sparse`,
`queryIndices`, `efSearch`, and `filter` exactly as `vector_search` does. The issue's schema is a
subset, not a ceiling: rejecting a sparse index would exclude precisely the case `vector.fuse` was
built for, and divergent validation between two sibling retrieval tools produces two error-message
vocabularies for the same mistake.

## Architecture

### Files

| File | Change |
|---|---|
| `server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java` | new |
| `server/src/main/java/com/arcadedb/server/mcp/tools/MCPVectorLeg.java` | new - vector-leg validation extracted from `VectorSearchTool` |
| `server/src/main/java/com/arcadedb/server/mcp/tools/VectorSearchTool.java` | delegates its private statics to `MCPVectorLeg`; no behavior change |
| `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java` | `TOOLS_LIST`, `toolsCall` switch, `formatResult` summary |

Server module only. No engine changes.

`MCPVectorLeg` carries the logic currently private to `VectorSearchTool`: index resolution and
dense/sparse type checking, dimension validation, `readFloatArray` with its finite-value guard, the
non-zero dense vector guard, `buildSparseQuery` including `queryIndices` validation, and filter
normalization with its `MAX_FILTER_EXPRESSION` bound. `VectorSearchTool` keeps its own response
shaping, truncation semantics, and tool definition.

### Data flow

```
0  isAllowReads() -> resolveDatabase(READ)
1  validate args; if expand present && strategy != RRF -> reject
2  leg1 vector    MCPVectorLeg -> parameterized SQL (dense|sparse, efSearch, filter)
                  -> List<Map>{@rid: RID, distance|score}       rank = list order
3  leg2 fulltext  FullTextSearch.resolveFullTextIndex + search(idx, q, legLimit)
                  sort score desc, RID asc  -> List<Map>{@rid, score}
4  seeds          leg1 order, then leg2 hits not already present, dedup, cap at MAX_SEEDS
5  leg3 expand    SELECT @rid, $depth, $path
                  FROM (TRAVERSE <dir>('E1','E2') FROM [#1:0,#1:4,...]
                        MAXDEPTH d LIMIT seedCount + MAX_EXPANSION STRATEGY BREADTH_FIRST)
                  WHERE $depth > 0
                  -> List<Map>{@rid}  (no score key)  + side map RID -> {depth, path}
6  fuse           SELECT expand(`vector.fuse`(:l1,:l2,:l3,:opts))
7  shape          {rid, fusedScore, sources, properties} + {depth, path} when expansion-sourced
```

Details that are not obvious from the sketch:

- **`WHERE $depth > 0` drops the seeds from leg 3.** A seed is already ranked by leg 1 and/or leg 2;
  letting it also appear in leg 3 would double-count its own presence. `TRAVERSE` emits each RID once
  at its shallowest depth, so a seed reachable from another seed is still correctly excluded.
- **BFS order is the rank, unsorted.** `STRATEGY BREADTH_FIRST` emits `(depth asc, seed order,
  discovery order)`, which is the desired ordering, so leg 3 needs no re-sort.
- **Fusion is skipped when only one leg exists.** `vector.fuse` requires at least two sources, so a
  vector-only call (no `fulltextQuery`, no `expand`) returns the vector leg directly and never
  reaches the fusion statement.
- **`depth` and `path` are rejoined after fusion.** `vector.fuse` returns `record`, `@rid`, `@type`,
  and `score` and drops every other field, so leg 3's metadata is carried in a Java-side side map
  keyed by RID.
- **`MAXDEPTH` takes a grammar literal, not a parameter.** It is inlined as a server-clamped integer.
- **Seed RIDs are inlined as literals**, per probe finding 3. They originate from the engine's own
  legs and never from caller-supplied text, so there is no injection surface.

## Input schema

```jsonc
{
  "database":        "string",              // required
  "vectorIndexName": "string",              // required - LSM_VECTOR or LSM_SPARSE_VECTOR
  "queryVector":     [number],              // required
  "k":               10,                    // required (default 10), 1..1000 - final result count
  "sparse":          false,
  "queryIndices":    [integer],             // sparse only
  "efSearch":        integer,               // dense only
  "filter":          "string",              // read-only WHERE, applied to the vector leg
  "fulltextIndexName": "string",            // e.g. "Article[content]"
  "fulltextQuery":     "string",            // both must be present, or neither
  "expand": {
    "edgeTypes": ["string"],                // omit = every edge type
    "direction": "out" | "in" | "both",     // default "out"
    "maxDepth":  1                          // default 1, hard cap 3
  },
  "fusionStrategy": "RRF" | "DBSF" | "LINEAR",   // default RRF
  "weights": { "vector": 1.0, "fulltext": 1.0, "expand": 0.5 }
}
```

`required` is `database`, `vectorIndexName`, `queryVector`, `k` - identical to `vector_search`, which
also lists `k` as required while defaulting it.

**`weights` is keyed by leg name, not positional.** `vector.fuse` takes a positional float array whose
length must equal the source count, so its meaning shifts depending on which optional legs are
present. That is the most error-prone part of the raw syntax for a model, and hiding it is squarely
governing principle 1.

**The default weight for `expand` is 0.5, not 1.0.** Under RRF a rank-1 expanded node scores
`1/(60+1)`, exactly what the single nearest neighbor scores. At uniform weight an arbitrary one-hop
neighbor ties the best semantic match, so the expansion leg must be worth less per rank slot than the
retrieval legs.

**The full-text leg takes `fulltextIndexName` only**, not `full_text_search`'s `typeName` +
`properties` addressing. That would be two more fields to save one round trip on a tool whose input
schema is already the largest in the server. A caller who knows only the type can get the index name
from `get_schema`, the schema Resource, or a `full_text_search` error message.

## Output schema

```jsonc
{
  "vectorIndexName": "Article[embedding]",
  "sparse": false,
  "scoring": "distance_lower_is_better:COSINE",
  "fulltextIndexName": "Article[content]",   // fused responses only
  "fusionStrategy": "RRF",                   // fused responses only
  "legs": {
    "vector":   { "count": 40 },
    "fulltext": { "count": 27, "indexName": "Article[content]", "similarity": "BM25" },
    "expand":   { "count": 133, "direction": "out", "edgeTypes": ["CITES"],
                  "maxDepth": 2, "truncated": false }
  },
  "count": 10,
  "truncated": true,
  "fused": true,
  "results": [
    { "rid": "#12:3", "fusedScore": 0.0325, "sources": ["vector","fulltext"],
      "properties": { } },
    { "rid": "#12:9", "fusedScore": 0.0161, "sources": ["expand"],
      "depth": 1, "path": ["#12:3","#12:9"], "properties": { } }
  ]
}
```

`sources` names the legs that contributed to each row. Leg membership is already in hand, and without
it a fused score is an uninterpretable float - an agent cannot distinguish "top semantic match" from
"two hops from something relevant". `depth` and `path` appear only on rows the expansion leg
contributed.

The two `truncated` flags mean different things and both follow `vector_search`'s convention that
truncation describes a filled window, never index cardinality:

- top-level `truncated` is `count >= k` - the result window was filled, so more matches may exist and
  `k` should be raised;
- `legs.expand.truncated` is `expandedRowCount >= MAX_EXPANSION` - the traversal hit its fan-out cap,
  so the neighborhood was explored only partially and narrowing `edgeTypes` or `maxDepth` will give a
  more complete picture of the part that matters.

`fused` reports whether fusion actually ran. `vector.fuse` requires at least two sources, so a call
naming neither `fulltextQuery` nor `expand` cannot be fused: that response sets `fused` to false and
its rows carry the vector leg's native `distance` (dense) or `score` (sparse) instead of `fusedScore`.
Every other call sets `fused` to true and carries `fusedScore`.

`fulltextIndexName` and `fusionStrategy` appear only on a fused response. The full-text index name and
its similarity are always reported inside `legs.fulltext` whenever that leg ran, including when it
matched nothing, so an unfused response still says which index was searched.

**Each leg's score reaches `vector.fuse` under the key matching its direction.** The engine reads a
value under `score` as a similarity and sign-flips only the value it reads under `distance`, so a
dense vector leg - whose native value is a distance, lower being better - must be emitted under
`distance`. Emitting it under `score` leaves `RRF` correct, because RRF is rank-only, while silently
inverting the vector ranking under `DBSF` and `LINEAR`: the nearest neighbor normalizes to the bottom
of the range and the farthest to the top. The sparse vector leg and the full-text leg are both
similarities and are emitted under `score`.

## Caps

Every cap is enforced in code, not by trusting the declared JSON Schema.

| Constant | Value | Rationale |
|---|---|---|
| `MAX_K` | 1000 | matches `vector_search` |
| `LEG_OVERFETCH` | 4 | each retrieval leg fetches `min(k*4, MAX_LEG_CANDIDATES)`; fusing top-k lists to return k needs headroom or the fusion is decorative |
| `MAX_LEG_CANDIDATES` | 1000 | absolute ceiling per retrieval leg |
| `MAX_SEEDS` | 256 | seeds taken in rank order from the union of legs 1 and 2 |
| `MAX_EXPANSION` | 2000 | expanded rows beyond depth 0 |
| `MAX_DEPTH` | 3 | the issue's hard cap |

**`TRAVERSE ... LIMIT` applies inside the subquery, before the outer `WHERE $depth > 0`**, so the
seeds consume limit slots. The emitted limit is `seedCount + MAX_EXPANSION`; without that term a
256-seed search at `MAX_EXPANSION` would silently lose its last 256 expanded rows.

With a `filter` on the vector leg, `vector_search`'s existing candidate over-fetch
(`min(legLimit * 8, 8000)`) applies on top of `legLimit`, unchanged.

`maxDepth > 3`, out-of-range `k`, and an over-long `filter` are **rejected, not clamped**.
`vector_search` already rejects out-of-range `k` rather than clamping, and a clamp that changes what
the caller asked for without saying so is the harder behavior to debug.

## Error handling

All argument faults raise `IllegalArgumentException`, following the house style of naming what is
wrong, how to fix it, and what is available.

| Condition | Behavior |
|---|---|
| `DBSF`/`LINEAR` with `expand` | rejected, naming the rank-only leg and both remedies |
| `fulltextIndexName` without `fulltextQuery`, or the reverse | rejected as an incomplete leg rather than silently ignored |
| `expand` against a non-vertex type | rejected, naming the type; validated once against the index's type, not per seed |
| unknown edge type in `edgeTypes` | rejected, listing the available edge types |
| edge type name containing a quote or backtick | rejected before reaching the inlined SQL |
| `maxDepth` above `MAX_DEPTH` | rejected |
| engine failure from a generated statement | wrapped as `Invalid hybrid search ...`, as `VectorSearchTool.invalidExpression` does |

**Edge-type validation is a correctness guard, not defensive polish.** Per probe finding 4, `out()`
with a name matching nothing returns an empty neighborhood and no error, so an unvalidated typo
silently degrades hybrid search to plain two-way fusion with the caller never learning why.

Stale and concurrently deleted RIDs need no handling: `vector.fuse` already skips
`RecordNotFoundException` per row.

## Permissions

`config.isAllowReads()` gate, then `MCPToolUtils.resolveDatabase(..., RequiredAccess.READ)`. Profile
gating is already provided by the reserved `RAG_TOOL_NAMES` entry, so the tool appears in `all` and
`rag` and is absent from `admin` the moment it is registered.

Both generated statements - the TRAVERSE and the fusion - pass through `analyze()` with an
`isIdempotent()` assertion before execution, the invariant `vector_search` already holds. This
matters most for the TRAVERSE statement, the only one carrying inlined text.

## Testing

`MCPPermissionsTest:200` currently asserts `isToolAllowed(ALL, "hybrid_search")` is **false** - it is
that test's example of a name present in a profile set but not registered. Registering the tool flips
it to true. The line must move to a genuinely unregistered name and gain positive assertions for
`hybrid_search` under `ALL` and `RAG`, and a false assertion under `ADMIN`.

Tool registration is asserted by presence rather than by a hardcoded count
(`MCPServerPluginTest:258-291`), so no count assertion needs bumping and there is no merge conflict
with sibling MCP PRs.

| Test | Where | Covers |
|---|---|---|
| vector-only, no fulltext or expand | `MCPServerPluginTest` | fusion skipped, vector leg returned directly |
| vector + fulltext | `MCPServerPluginTest` | two-way fusion, `sources` naming both legs |
| vector + expand | `MCPServerPluginTest` | `depth`/`path` present, seeds excluded from leg 3 |
| vector + fulltext + expand | `MCPServerPluginTest` | the issue's three-way case |
| `maxDepth: 4` rejected | `MCPServerPluginTest` | depth cap enforced server-side |
| depth cap honored at 3 | `MCPServerPluginTest` | a four-hop chain yields no depth-4 node |
| diamond graph, node reachable two ways | `MCPServerPluginTest` | dedup before fusion, shallowest path wins |
| `DBSF` with `expand` rejected | `MCPServerPluginTest` | rank-only leg guard |
| unknown edge type rejected | `MCPServerPluginTest` | silent-empty-neighborhood guard |
| `expand` on a document type rejected | `MCPServerPluginTest` | vertex precondition |
| `fulltextQuery` without `fulltextIndexName` | `MCPServerPluginTest` | incomplete leg |
| profile gating | `MCPPermissionsTest`, `MCPStdioServerTest` | `rag` yes, `admin` no |
| registered in both transports | `MCPServerPluginTest`, `MCPStdioServerTest` | HTTP and stdio |
| leg-limit and seed-cap arithmetic | unit test alongside `MCPToolUtilsTest` | caps without a running server |

Fixtures need a vertex type carrying both a vector index and a full-text index, with edges between
those records. `MCPServerPluginTest` already builds vector and full-text fixtures for the sibling
tools; this adds edges to them. No containers are required.

## Documentation

MCP documentation lives in the separate `ArcadeData/arcadedb-docs` repository at
`src/main/asciidoc/reference/mcp/mcp.adoc`, not in this tree, so the docs acceptance item is a
companion PR there rather than a file in this PR. It adds a `hybrid_search` section with a worked
three-leg example and a note that `vector.fuse` via the `query` tool remains available for pure
two-way fusion without graph expansion.

That page currently documents 13 tools and is already behind main - `sample_records`, `vector_search`,
and the `arcadedb://{database}/schema` Resource are undocumented. Repairing that pre-existing drift is
out of scope here.

## Follow-up work filed separately

**Engine: `TRAVERSE`/`SELECT` from a bound RID collection throws NPE.**
`TraverseExecutionPlanner:155-174` and `SelectExecutionPlanner:1435-1453` omit the
`rid.setLegacy(true)` their own singleton-RID branches perform, so `Rid.toRecordId` dereferences a
null `expression`. Reproduced by probe 3. Confirmed still present on main as of 2026-07-28. The issue
body is drafted at `docs/superpowers/4861-engine-traverse-npe-issue.md` and is not yet filed;
`hybrid_search` does not depend on the fix landing.

## Non-goals

- No embedding generation. Callers supply pre-computed vectors, per epic #4859's non-goals.
- No `expandMode` alternative output shape.
- No dense + sparse dual vector leg in v1; that would push the input schema to four legs and make the
  weights story considerably harder to use correctly.
- No repair of the pre-existing `arcadedb-docs` MCP drift.
