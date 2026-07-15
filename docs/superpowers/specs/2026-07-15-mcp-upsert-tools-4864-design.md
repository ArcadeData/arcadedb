# MCP `upsert_entity` / `upsert_relationship` tools (#4864)

**Issue:** [#4864](https://github.com/ArcadeData/arcadedb/issues/4864)
**Epic:** [#4859 - MCP GraphRAG & Agent-Memory Surface](https://github.com/ArcadeData/arcadedb/issues/4859) (Wave 1)
**Date:** 2026-07-15
**Status:** approved

## Problem

ArcadeDB's MCP server exposes operator/DBA-oriented tools plus `query`/`execute_command`, which are already Turing-complete over SQL/Cypher. Agents can already upsert through `execute_command`, so a dedicated tool is justified only against the epic's governing principle. Two criteria apply here:

1. **Ergonomic guardrail (epic criterion #1).** Models reliably build match-key logic wrong: string-concatenated `WHERE` clauses, missing key normalization, and as a result they duplicate nodes. Duplicate-entity-on-repeat is the single most common agent-memory failure mode. Wrapping match-or-create-by-key behind a parameterized tool is a concrete correctness win.
2. **Different permissioning (epic criterion #3).** An upsert composes `allowInsert` (create path) **and** `allowUpdate` (update path) as a single unit - a combination `ExecuteCommandTool` does not need to express on its own today.

The injection dimension sharpens this: the safe implementation binds every value as a Cypher parameter. A model hand-writing `execute_command` will interpolate values into the query string instead, which is both an injection vector and a correctness hazard when a value contains quotes.

## Governing constraint

The MCP layer stays **schema-agnostic**. Per the maintainer's ruling on the epic thread, MCP is "just another protocol/API, like the HTTP API or gRPC or Bolt." These tools impose no governance or memory vocabulary; they upsert records addressed by caller-supplied type names and match keys, and return the resulting record. Nothing more.

## Design

### Architecture: server layer only

Unlike `full_text_search` (#4862), no engine-layer extraction is needed. Both tools live entirely in `com.arcadedb.server.mcp.tools` and reuse existing engine and MCP machinery:

- `UpsertEntityTool` - `getDefinition()` + `static execute(server, user, args, config)`
- `UpsertRelationshipTool` - same shape

Each `execute` method:

1. Resolves the database via `MCPToolUtils.resolveDatabase(...)` (LLM-friendly "database not found" error, house pattern).
2. Validates arguments (required fields present, `matchKeys` non-empty, identifiers safe).
3. Builds a parameterized Cypher `MERGE` statement (text) and a `Map<String,Object>` of bound parameters.
4. Delegates to a shared helper that analyzes, permission-checks, executes in a transaction, and serializes.

The shared step (3→4 handoff) is factored into `MCPToolUtils.executeParameterizedWrite(database, cypher, params, config)` so the analyze + `checkPermission` + `database.transaction(...)` + `JsonSerializer` block is written once, not copied into both tools. `ExecuteCommandTool` is left unchanged to keep the Wave-1 diff minimal; adopting the helper there is a possible later cleanup, not part of this issue.

### Query construction: `upsert_entity`

```
MERGE (n:`<TypeName>` {`k1`: $m0, `k2`: $m1})
SET n.`p1` = $s0, n.`p2` = $s1
RETURN n
```

- `matchKeys` entries become the `MERGE` key map; their values bind to `$m0..$mN`.
- `setProperties` entries become individual `SET n.\`key\` = $sI` assignments; their values bind to `$s0..$sN`. Individual assignments are used rather than a `SET n += $map` map-parameter form, so the generated statement stays explicit and does not depend on map-parameter support inside `SET`.
- If `setProperties` is absent or empty, the `SET` line is omitted entirely: the statement is a pure match-or-create.
- `RETURN n` yields the resolved record for serialization.

`matchKeys` MUST be a non-empty object. An empty match key would make `MERGE` match or create an arbitrary node of the type, which is never the caller's intent; reject it with a clear error.

### Query construction: `upsert_relationship`

```
MERGE (a:`<FromType>` {`fk1`: $f0})
MERGE (b:`<ToType>`   {`tk1`: $t0})
MERGE (a)-[r:`<RelType>`]->(b)
SET r.`rp1` = $r0
RETURN r
```

- Endpoints are resolved with `MERGE`, not `MATCH`: a missing endpoint is auto-created as a thin node carrying only its match keys. This mirrors `upsert_entity` semantics and supports incremental graph construction, the primary agent-memory use case.
- Relationship properties go in the trailing `SET`, **not** inside the `MERGE (a)-[r {…}]->(b)` pattern. Edge identity is therefore endpoints + `relType` only. Repeated calls between the same resolved endpoints with different `relProperties` update the single edge rather than creating a second one - the acceptance criterion for no-duplicate edges.
- `fromMatchKeys` and `toMatchKeys` MUST each be non-empty, for the same reason as `matchKeys`.

### Identifier safety and injection

Cypher can bind **values** as parameters but cannot parameterize **identifiers** - type/label names, relationship types, and property keys. The split:

| Element | Handling |
|---|---|
| `matchKeys` / `setProperties` / `relProperties` **values** | Bound parameters (`$m*`, `$s*`, `$f*`, `$t*`, `$r*`). Never interpolated. |
| type name, relType, property **keys** | Backtick-quoted in the generated text: `` `Person` ``, `` n.`email` ``. |

Any identifier that is null, blank, or contains a backtick is rejected up front with `IllegalArgumentException` naming the offending value. Because a validated identifier cannot contain a backtick, backtick-quoting is closed - no identifier can terminate its own quoting and inject clauses. Backtick-quoting (rather than a strict `[A-Za-z_][A-Za-z0-9_]*` allowlist) is chosen so legitimate type/property names containing spaces or dashes still work; a backtick inside an ArcadeDB identifier is vanishingly rare and rejecting it is an acceptable v1 limitation.

**Injection regression test (acceptance criterion).** Call `upsert_entity` with a `matchKeys` value such as `x'}) DETACH DELETE n //`. It must be stored as a literal string property and create exactly one node; nothing is deleted, and no second node appears on a repeat call with the same value.

### Permissions

The generated statement is analyzed and gated through the **existing** path, not new bespoke logic:

```java
final QueryEngine engine = database.getQueryEngine("cypher");
final QueryEngine.AnalyzedQuery analyzed = engine.analyze(cypher);
ExecuteCommandTool.checkPermission(analyzed.getOperationTypes(), config);
```

Any `MERGE` analyzes to `{CREATE, UPDATE}` in ArcadeDB's OpenCypher engine: `OpenCypherQueryEngine.getOperationTypes()` adds both `CREATE` and `UPDATE` whenever `hasMerge()` is true, independent of whether a `SET` clause is present. So every upsert - with or without `setProperties`/`relProperties` - requires both `allowInsert` and `allowUpdate` enabled; `checkPermission` throws a `SecurityException` naming whichever flag is missing. This is exactly the composition the pre-existing `MCPPermissionsTest.upsertRequiresBothCreateAndUpdate` stub asserts, and it matches the issue's "require both as a single unit" intent - reusing the analyzer lands on that behavior rather than having to hard-code it.

**Type auto-creation.** ArcadeDB's Cypher `MERGE` auto-creates a missing vertex/edge type (`Labels.ensureCompositeType` / `getOrCreateEdgeType` in `MergeStep`), and that create analyzes as part of `CREATE`, not `SCHEMA`. So an agent with `allowInsert` can grow the schema through these tools without `allowSchemaChange`. This is an accepted, deliberate choice: incremental knowledge-graph construction is the use case, and requiring `allowSchemaChange` for a first-write of a new type would break it. It is documented as such, and safe-by-default is unaffected because all write flags remain `false` out of the box.

### Input schema

`upsert_entity`:

```json
{
  "type": "object",
  "properties": {
    "database":      {"type": "string"},
    "typeName":      {"type": "string"},
    "matchKeys":     {"type": "object", "description": "property:value pairs used as the match/merge key"},
    "setProperties": {"type": "object"}
  },
  "required": ["database", "typeName", "matchKeys"]
}
```

`upsert_relationship`:

```json
{
  "type": "object",
  "properties": {
    "database":      {"type": "string"},
    "fromType":      {"type": "string"}, "fromMatchKeys": {"type": "object"},
    "toType":        {"type": "string"}, "toMatchKeys":   {"type": "object"},
    "relType":       {"type": "string"},
    "relProperties": {"type": "object"}
  },
  "required": ["database", "fromType", "fromMatchKeys", "toType", "toMatchKeys", "relType"]
}
```

**Deviation from the issue text.** #4864 names the entity-type field `type`. A property literally named `type` inside a JSON Schema collides with the schema's own `type` keyword. Following the precedent set for `full_text_search` (#4862), the field is renamed `typeName`. The relationship fields `fromType`/`toType`/`relType` do not collide and are kept verbatim. **Issue #4864's schema is to be amended to match.**

### Output schema

Matches `execute_command` (full record, nothing more - consistent with the schema-agnostic constraint and the rest of the server's serialization):

```json
{
  "records": [
    {"@rid": "#12:0", "@type": "Person", "email": "a@b.com", "name": "Ada"}
  ],
  "count": 1
}
```

Records are serialized with the same `JsonSerializer` configuration `ExecuteCommandTool` uses (`setIncludeVertexEdges(false)`, `setUseCollectionSize(false)`, `setUseCollectionSizeForEdges(false)`), which emits `@rid`, `@type`, and all properties. No separate `created` vs `matched` flag is returned; the record and its `@rid` are what agents chain on.

### Registration

Both transports, mirroring the existing tools. Keep the diff minimal and consistently placed - these are the files every Wave-1 issue touches.

- `MCPHttpHandler`: add both `getDefinition()` calls to the static `TOOLS_LIST`, add `case "upsert_entity"` / `case "upsert_relationship"` to the `tools/call` dispatch switch, and add a `formatResult` arm (`case "upsert_entity", "upsert_relationship" -> result.getInt("count", 0) + " record(s)"`; the existing `default -> "ok"` makes this optional but the arm keeps the log line informative).
- `MCPStdioServer`: add both to the static `TOOLS_LIST` and the dispatch switch.

## Testing

| Suite | Coverage |
|---|---|
| `MCPPermissionsTest` | `upsertRequiresBothCreateAndUpdate` stub already present - keep green. The `{CREATE, UPDATE}` composition is the contract these tools rely on. |
| `UpsertToolsTest` (new, or new methods in `MCPServerPluginTest`) | Idempotency: two `upsert_entity` calls with identical `matchKeys` create exactly one node; changed `setProperties` update it in place. Edge no-dup: two `upsert_relationship` calls between the same resolved endpoints create exactly one edge; changed `relProperties` update it. Endpoint auto-creation: `upsert_relationship` against non-existent endpoints creates the thin nodes. Injection: malicious `matchKeys` value stored literally, one node, nothing deleted. Identifier rejection: a backtick in `typeName`/`relType`/a key raises `IllegalArgumentException`. Empty `matchKeys` rejected. |
| `MCPServerPluginTest` (tools/list) | Both tools present in the HTTP `tools/list`; increment the tool-count assertion by 2. |
| `MCPStdioServerTest` | Both tools present in the stdio `tools/list`. |

**TDD spike first.** Before building the tools out, write two probe tests that confirm the two engine behaviors the whole design rests on: (a) a parameterized relationship `MERGE` between the same endpoints does not duplicate the edge, and (b) backtick-quoted identifiers plus bound parameters parse and execute inside a `MERGE`. If either behaves unexpectedly, the statement-construction strategy is revisited before writing production code.

## Risks and known quirks

- **No-duplicate / idempotency depends on a UNIQUE index on the match keys.** `MergeStep` matches by index only when one covers the match keys; otherwise it falls back to a full type scan (`iterateType(label, true)`). Two consequences when no such index exists: (1) each upsert is O(n) in the type's size, so bulk-building via repeated upserts is O(n^2) - counter to the performance mantra and the agent-memory hot path; (2) `MERGE` is match-then-create and **not** atomic across concurrent transactions - two overlapping upserts with identical `matchKeys` can both create, producing the very duplicate the tool exists to prevent. The retry-on-collision path in `MergeStep` (catch `DuplicatedKeyException` -> re-match) only fires when a UNIQUE index actually raises the collision. The tool descriptions state this; operators building agent memory should create a UNIQUE index on the match-key properties. **Auto-creating that index on first use is a candidate follow-up** (it interacts with `allowSchemaChange` and with match-key-set variability across calls, so it is a separate decision, not folded into this PR).
- **Relationship `MERGE` semantics.** The no-duplicate-edge guarantee depends on ArcadeDB's Cypher `MERGE (a)-[r:R]->(b)` matching an existing edge by endpoints + type. Verified by the spike and the no-dup test, not assumed. Related engine issues around directed/undirected relationship reuse (#4095/#4096) concern `MATCH` traversal, not `MERGE` creation, but the test is the guard.
- **Backtick-quoted identifiers + parameters in `MERGE`.** Confirmed by the spike before production code, since the entire injection-safety story rests on it.
- **Merge conflicts.** The `TOOLS_LIST` static blocks, dispatch switches, and tool-count assertion in `MCPHttpHandler.java` / `MCPStdioServer.java` / `MCPServerPluginTest.java` are touched by every Wave-1 issue (#4860, #4862, #4863, #4865). Rebase-and-reapply the small, consistently-placed additions.
- **Tool-count coupling.** The exact target of the `tools/list` count assertion depends on which sibling Wave-1 tools have already merged. The change is always "+2 for these two tools"; reconcile the absolute number at merge time.

## Out of scope

- **Documentation.** The user-facing MCP docs live in the out-of-tree `arcadedb-docs` repository and are tracked as a separate follow-up, exactly as for #4862.
- **Temporal / bi-temporal versioning.** Optional `validFrom`/`validTo` versioned upserts are a plausible future primitive. This schema does not preclude adding them later, but they are not built here.
- **Embedding generation.** Out of scope for the whole epic; these tools accept caller-supplied values only.
- **`ExecuteCommandTool` refactor.** Adopting the new `executeParameterizedWrite` helper in `ExecuteCommandTool` is a possible cleanup, deliberately deferred to keep the Wave-1 diff minimal.
