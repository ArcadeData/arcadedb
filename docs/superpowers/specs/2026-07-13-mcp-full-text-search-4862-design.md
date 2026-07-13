# MCP `full_text_search` tool (#4862)

**Issue:** [#4862](https://github.com/ArcadeData/arcadedb/issues/4862)
**Epic:** [#4859 - MCP GraphRAG & Agent-Memory Surface](https://github.com/ArcadeData/arcadedb/issues/4859) (Wave 1)
**Date:** 2026-07-13
**Status:** approved

## Problem

ArcadeDB's MCP server exposes 10 operator/DBA-oriented tools. It has no retrieval surface. `full_text_search` is the lexical leg of the retrieval trio (`vector_search` #4860, `hybrid_search` #4861, `full_text_search` #4862).

An agent can already reach full-text search through the generic `query` tool, so this tool is justified solely as an **ergonomic guardrail** (epic criterion #1): the raw syntax has quirks a model will not reliably reproduce.

Concretely, the raw SQL form is:

```sql
SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java') = true ORDER BY $score DESC
```

Three things a model gets wrong here:

1. The index name is a bracket literal, `Article[content]`, auto-derived by `LocalDocumentType` as `typeName + Arrays.toString(propertyNames).replace(" ", "")`. It is not a free-form identifier.
2. The BM25 relevance is exposed only as the context variable `$score`, which must be explicitly projected. Omit it and you get rank order with no scores.
3. On a miss, `Schema.getIndexByName` throws a bare `SchemaException("Index with name 'X' was not found")` with no recovery hint.

## Governing constraint

The MCP layer stays **schema-agnostic**. Per the maintainer's ruling on the epic thread, MCP is "just another protocol/API, like the HTTP API or gRPC or Bolt." It does not impose a governance or memory vocabulary; agents filter on domain properties the same way they filter on any other field. This tool therefore returns records and scores, nothing more.

## Design

### Architecture: two layers

**Engine layer.** New stateless utility `com.arcadedb.index.fulltext.FullTextSearch`, holding logic extracted from the currently-private `SQLFunctionSearchIndex.performSearch(...)`:

```java
public static Map<RID, Float> search(Database db, String indexName, String queryText);
public static TypeIndex       resolveFullTextIndex(Database db, String indexName);
public static String          getSimilarity(TypeIndex index);      // "BM25" | "CLASSIC"
public static List<String>    listFullTextIndexes(Database db);
```

`SQLFunctionSearchIndex.performSearch` is reduced to a delegate.

The extracted logic is the existing traversal: resolve the index by name, assert it is a `TypeIndex` whose bucket indexes are `LSMTreeFullTextIndex`, run a `FullTextQueryExecutor` per bucket index, and merge `RID -> float score`. It carries a subtle invariant (a RID lives in exactly one bucket, so the `Float::sum` merge is effectively an insert rather than a real sum) that must exist in exactly one place. Extracting rather than copying is what keeps that true.

**Exception-compatibility requirement.** The helper preserves today's exception *types* exactly:

- missing index name -> `SchemaException` (propagated from `Schema.getIndexByName`)
- index exists but is not a type index -> `CommandExecutionException("Index 'X' is not a type index")`
- type index whose bucket indexes are not full-text -> `CommandExecutionException("Index 'X' is not a full-text index")`

This makes the SQL path byte-identical after the refactor, so the existing `SQLFunctionSearchIndexTest`, `FullTextScoreTest`, `FullTextQuerySyntaxTest`, `FullTextMultiPropertyTest`, and `FullTextPolymorphicScoreTest` suites serve as the regression net **without being edited**. Any edit needed to those suites is a signal the refactor changed behavior.

**Server layer.** New `com.arcadedb.server.mcp.tools.FullTextSearchTool`, following the `QueryTool` shape (`getDefinition()` + `static execute(server, user, args, config)`).

It performs its *own* pre-validation before calling the helper, so it can raise an LLM-friendly `IllegalArgumentException` that enumerates the database's real full-text indexes. This mirrors the established house pattern in `MCPToolUtils.resolveDatabase`, which lists available databases so the model self-corrects without a round-trip. SQL semantics stay frozen; MCP gets the good error.

### Why not synthesize SQL

The rejected alternative was to build `SELECT *, $score FROM <Type> WHERE SEARCH_INDEX(:idx, :q) ORDER BY $score DESC LIMIT n` and bind the arguments. It was rejected because `queryText` legitimately contains quote characters (phrase queries such as `"java programming"`, or an apostrophe in `don't`), so it *must* be a bound parameter rather than interpolated. But `SEARCH_INDEX` is an `IndexableSQLFunction` that the planner may consume at plan time via `searchFromTarget`, and **no test in the repository binds its arguments as parameters**. Staking the tool on an unproven path, where the failure mode is SQL injection, is not warranted when the programmatic path is available and exact.

### Input schema

```json
{
  "type": "object",
  "properties": {
    "database":   {"type": "string"},
    "indexName":  {"type": "string"},
    "typeName":   {"type": "string"},
    "properties": {"type": "array", "items": {"type": "string"}},
    "queryText":  {"type": "string"},
    "limit":      {"type": "integer", "default": 10}
  },
  "required": ["database", "queryText"]
}
```

The type-addressing argument is named `typeName`, not `type`. A property literally named `type` inside a JSON Schema collides with the schema's own `type` keyword, which is legal but invites a model to second-guess which one it is looking at. `typeName` removes the ambiguity at no real cost.

Index addressing accepts **both forms**:

| Input | Behavior |
|---|---|
| `indexName` | Used directly. |
| `typeName` + `properties` | Derives `Type[p1,p2]` (comma-joined, no spaces). |
| `typeName` alone | Resolves the sole full-text index on that type. Zero or several -> error listing candidates. |
| both `indexName` and `typeName` | `indexName` wins. Documented in the tool description. |
| neither | Error. |

**Supertype quirk.** A full-text index declared on a supertype is named for the *supertype* (`Searchable[searchable_text]`), and querying it correctly returns subtype records. Passing `typeName: "Decision"` for a subtype therefore resolves nothing. The enumerating error message is what rescues this case, which is a concrete reason the error must list real index names rather than merely reporting absence.

### Output schema

```json
{
  "indexName": "Article[content]",
  "similarity": "BM25",
  "count": 2,
  "results": [
    {"rid": "#3:0", "score": 1.84, "properties": {"@rid": "#3:0", "@type": "Article", "title": "Java 21"}},
    {"rid": "#3:7", "score": 0.91, "properties": {"...": "..."}}
  ]
}
```

Results are sorted by score descending, then truncated to `limit`. Records are loaded with `database.lookupByRID(rid, true)` and serialized with `JsonSerializer.serializeDocument(Document)`, which emits `@rid`, `@type`, and all properties, consistent with the rest of the server's serialization.

**Deviation from the issue text.** #4862 specifies a field named `bm25Score`. That name is wrong for this engine: an ArcadeDB full-text index is not necessarily BM25. Indexes persisted before BM25 support load as `CLASSIC` similarity, and a user can pin `METADATA {"similarity": "CLASSIC"}` explicitly. On those, `$score` is a term-coordination count, not a BM25 score. Labeling it `bm25Score` would be a lie, and would silently corrupt score fusion in `hybrid_search` (#4861).

The field is therefore `score`, with the index's actual similarity reported once at the top level via `LSMTreeFullTextIndex.isBM25()` / `FullTextIndexMetadata.getSimilarity()`. This keeps the tool usable against legacy CLASSIC indexes (which still work fine via SQL) while giving #4861 the metadata it needs to decide whether a score is fusable. **Issue #4862's text is to be amended to match.**

### Permissions

Gated on `config.isAllowReads()`, throwing `SecurityException("Read operations are not allowed by MCP configuration")`, exactly as `QueryTool` does. The tool is pure-read, so the `OperationType` -> `isAllow*()` gating pattern from `ExecuteCommandTool` does not apply. No new configuration flag is introduced, so safe-by-default (`enabled=false`) is unchanged.

### The guardrail itself

The ergonomic payload lives in the tool **description**, which teaches the query syntax the model would otherwise botch. Supported by the Lucene `QueryParser` front-end and translated to LSM lookups by `FullTextQueryExecutor`:

| Form | Meaning |
|---|---|
| `+java +db` | both terms required |
| `java -python` | exclude |
| `java db` | either (default operator, switchable to AND via index metadata) |
| `"java programming"` | all terms in the same document. **Order is not enforced** in this implementation. |
| `data*` | prefix |
| `databse~` | fuzzy (Levenshtein) |
| `title:java` | field-scoped, meaningful on multi-property indexes |
| `java^3.0` | boost |

Unsupported clause types (for example range queries) are silently ignored by the executor. Invalid syntax raises `IndexException("Invalid search query: ...")`.

## Registration

Both transports, mirroring the existing 10 tools:

- `MCPHttpHandler`: add to the static `TOOLS_LIST`, add a `case "full_text_search"` to the `tools/call` dispatch switch, and add a `formatResult` arm.
- `MCPStdioServer`: add to the static `TOOLS_LIST` and the dispatch switch.

## Testing

| Suite | Coverage |
|---|---|
| `FullTextSearchTest` (new, engine) | Helper resolves indexes, returns scored RIDs, reports BM25 vs CLASSIC similarity, enumerates full-text indexes, and preserves the three exception types. |
| existing engine full-text suites | Regression net for the `performSearch` extraction. Must stay green **unedited**. |
| `MCPServerPluginTest` | Happy path over HTTP against a seeded full-text index; error cases: unknown index, non-full-text index, subtype `typeName`, ambiguous `typeName`, neither addressing form. |
| `MCPStdioServerTest` | Tool present in stdio `tools/list`. |
| `MCPPermissionsTest` | `allowReads=false` denies. |

`MCPServerPluginTest` currently asserts `assertThat(tools.length()).isEqualTo(10)`; this becomes 11.

## Risks and known quirks

- **Merge conflicts.** The `TOOLS_LIST` static blocks and dispatch switches in `MCPHttpHandler.java` and `MCPStdioServer.java`, plus the tool-count assertion, are touched by *every* Wave-1 issue (#4860, #4863, #4864, #4865). The epic flags this explicitly. Keep the diff minimal and consistently placed.
- **Cold start.** A BM25 index whose corpus counters are invalid runs a full type scan on its first query (`ensureCounters()` -> `computeCorpusCounters`). A first `full_text_search` call against a large, freshly-loaded index can therefore stall. Remedy is `REBUILD INDEX <name> WITH statsOnly = true`. Worth a docs note, not a code change.
- **Terminology.** ArcadeDB's full-text index is its own LSM-tree structure with BM25 scoring; Lucene is used only for tokenization/analysis and query parsing, not as the storage or retrieval engine. Docs must say "BM25 full-text index", never "Lucene index".

## Out of scope

- `SEARCH_INDEX_MORE` (more-like-this) and `SEARCH_FIELDS`. Both exist in the engine and are plausible future tools, but neither is needed to complete the retrieval trio, and the epic enforces tool-count discipline.
- Documentation. The MCP docs live in the out-of-tree `arcadedb-docs` repository and are tracked as a separate follow-up.
- Any relaxation of default permissions.
