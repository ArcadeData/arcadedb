# MCP `hybrid_search` Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an MCP `hybrid_search` tool that fuses a vector leg, a full-text leg, and a depth-capped graph-expansion leg into a single ranked result list.

**Architecture:** Java orchestrates each retrieval leg independently, owning all caps, ordering, and error messages, then hands the materialized rank lists to the engine's `vector.fuse` SQL function as bound parameters so fusion scoring keeps exactly one implementation. The graph leg is a rank-only third source produced by a breadth-first `TRAVERSE` seeded from the union of the other two legs.

**Tech Stack:** Java 21, Maven, JUnit 5 + AssertJ, ArcadeDB SQL (`vector.fuse`, `TRAVERSE`), ArcadeDB MCP server (`server` module).

**Spec:** `docs/superpowers/specs/2026-07-28-mcp-hybrid-search-design.md`
**Issue:** [#4861](https://github.com/ArcadeData/arcadedb/issues/4861), epic [#4859](https://github.com/ArcadeData/arcadedb/issues/4859)

## Global Constraints

- Java 21+. Maven multi-module; all work is in the `server` module except Task 7, which files an issue and writes no code.
- Use `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, never another JSON library.
- Import classes; never use fully-qualified names inline.
- `final` on variables and parameters wherever possible.
- Single-statement `if` bodies take no braces.
- Tests use `assertThat(x).isTrue()` style (AssertJ), JUnit 5.
- Comments state behavioral invariants only - no issue numbers, no fix context, no "Claude" attribution anywhere.
- Do not commit to git unless a step explicitly says to; the plan's commit steps are expected and fine.
- After every Java change: compile and fix until it passes.
- Prefer primitive arrays over boxed collections on hot paths.
- Run at minimum `mvn -pl server test -Dtest='MCP*'` before declaring any task done.
- Separate multiple `-Dtest` selectors with `,` and never with `+`. Under this repo's Surefire, a `+` runs **zero** tests and reports no failure, so any verification step written with `+` proves nothing.
- Invoke Maven as `/opt/homebrew/bin/mvn`; a shell alias for `mvn` silently produces no output in this worktree.
- MCP documentation lives in the separate `ArcadeData/arcadedb-docs` repository, not this tree.

---

## File Structure

| File | Responsibility |
|---|---|
| `server/src/main/java/com/arcadedb/server/mcp/tools/MCPVectorLeg.java` | **new.** Vector-index resolution, query-vector validation, sparse-query construction, filter normalization, and generation of the vector-leg SQL. Shared by `vector_search` and `hybrid_search`. |
| `server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java` | **new.** Tool definition, leg orchestration, seed selection, expansion, fusion, response shaping. |
| `server/src/main/java/com/arcadedb/server/mcp/tools/VectorSearchTool.java` | **modify.** Delegates its private validation statics to `MCPVectorLeg`. No behavior change. |
| `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java` | **modify.** Register in `TOOLS_LIST`, dispatch in `toolsCall`, summarize in `formatResult`. |
| `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java` | **modify.** Hybrid fixture and the end-to-end HTTP tests. |
| `server/src/test/java/com/arcadedb/server/mcp/MCPPermissionsTest.java` | **modify.** Profile assertions; repair the assertion that only passes while the tool is unregistered. |
| `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java` | **modify.** Stdio transport registration and `rag` profile visibility. |
| `server/src/test/java/com/arcadedb/server/mcp/HybridSearchCapsTest.java` | **new.** Pure unit test for cap arithmetic, no running server. |

---

### Task 1: Extract the shared vector leg into `MCPVectorLeg`

Pure refactor. `vector_search`'s observable behavior must not change; the existing `vectorSearch*` tests are the proof.

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/mcp/tools/MCPVectorLeg.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/VectorSearchTool.java`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java` (existing tests, unchanged)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `MCPVectorLeg.DEFAULT_K` = 10, `MAX_K` = 1000, `FILTER_OVERFETCH` = 8, `MAX_FILTER_CANDIDATES` = 8000, `MAX_FILTER_EXPRESSION` = 4096 (all `public static final int`)
  - `record MCPVectorLeg.ResolvedVectorIndex(TypeIndex typeIndex, int dimensions, String scoring)`
  - `record MCPVectorLeg.VectorLegQuery(ResolvedVectorIndex index, boolean sparse, int candidateLimit, String sql, Map<String, Object> parameters)`
  - `static void MCPVectorLeg.validateArguments(JSONObject args, String indexNameField)` - the checks needing no `Database`, called before `resolveDatabase`
  - `static VectorLegQuery MCPVectorLeg.build(Database database, JSONObject args, String indexNameField, int limit)`
  - `static RID MCPVectorLeg.toRID(Object raw)`

`build` generates `SELECT FROM (SELECT expand(<fn>)) [WHERE (<filter>)] LIMIT :legLimit` and binds `indexName`, `k`, `options`, `candidateLimit`, `legLimit`, and either `queryVector` or `queryIndices` + `queryVector`.

The parameter is named `:legLimit`, not `:limit`: `LIMIT` is a grammar keyword and a parameter spelled `:limit` risks a lexer collision.

- [ ] **Step 1: Confirm the baseline is green before touching anything**

Run: `mvn -pl server test -Dtest='MCPServerPluginTest#vectorSearch*'`
Expected: PASS. If it does not pass on a clean tree, stop and report - the refactor has no baseline.

- [ ] **Step 2: Create `MCPVectorLeg` with the logic moved verbatim from `VectorSearchTool`**

Create `server/src/main/java/com/arcadedb/server/mcp/tools/MCPVectorLeg.java`. Use the standard ArcadeDB Apache-2.0 header (copy the 18-line header from `VectorSearchTool.java`).

```java
package com.arcadedb.server.mcp.tools;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Shared vector-retrieval leg. Resolves and validates a dense or sparse vector index against
 * caller-supplied arguments and produces the read-only SQL statement plus bound parameters that
 * fetch its ranked neighbors. Both the standalone vector search tool and the hybrid search tool
 * build their vector leg here, so a given malformed argument produces one error message rather
 * than two divergent ones.
 */
public final class MCPVectorLeg {
  public static final int DEFAULT_K             = 10;
  public static final int MAX_K                 = 1_000;
  public static final int FILTER_OVERFETCH      = 8;
  public static final int MAX_FILTER_CANDIDATES = 8_000;
  public static final int MAX_FILTER_EXPRESSION = 4_096;

  public record ResolvedVectorIndex(TypeIndex typeIndex, int dimensions, String scoring) {
  }

  public record VectorLegQuery(ResolvedVectorIndex index, boolean sparse, int candidateLimit, String sql,
      Map<String, Object> parameters) {
  }

  private record SparseQuery(int[] indices, float[] values) {
  }

  private MCPVectorLeg() {
  }

  /**
   * Validates every vector argument and assembles the leg statement. Validation order is deliberate:
   * argument-shape faults are reported before any schema lookup, so a caller that got both the flags
   * and the index name wrong learns about the flags first and does not chase a schema error.
   *
   * @param indexNameField the argument key holding the index name, which differs between tools
   * @param limit          rows the leg should return, before any fusion
   */
  public static VectorLegQuery build(final Database database, final JSONObject args, final String indexNameField,
      final int limit) {
    final String indexName = MCPToolUtils.requireString(args, indexNameField);
    final boolean sparse = args.getBoolean("sparse", false);
    final Integer efSearch = args.has("efSearch") ? args.getInt("efSearch") : null;
    if (efSearch != null && efSearch < 1)
      throw new IllegalArgumentException("'efSearch' must be at least 1");
    if (sparse && efSearch != null)
      throw new IllegalArgumentException("'efSearch' applies only to dense LSM_VECTOR indexes");
    if (!sparse && args.has("queryIndices"))
      throw new IllegalArgumentException("'queryIndices' requires sparse=true");

    final String filter = normalizeFilter(args.getString("filter", null));
    final ResolvedVectorIndex resolved = resolveIndex(database, indexName, sparse);
    final float[] queryVector = readFloatArray(args.getJSONArray("queryVector", null), "queryVector");

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("indexName", resolved.typeIndex().getName());
    parameters.put("legLimit", limit);

    final Map<String, Object> options = new LinkedHashMap<>();
    if (efSearch != null)
      options.put("efSearch", efSearch);
    parameters.put("options", options);

    final String functionCall;
    if (sparse) {
      final SparseQuery sparseQuery = buildSparseQuery(args, queryVector, resolved.dimensions());
      parameters.put("queryIndices", sparseQuery.indices());
      parameters.put("queryVector", sparseQuery.values());
      functionCall = "`vector.sparseNeighbors`(:indexName, :queryIndices, :queryVector, :candidateLimit, :options)";
    } else {
      if (queryVector.length != resolved.dimensions())
        throw new IllegalArgumentException(
            "'queryVector' has " + queryVector.length + " dimensions, but index '" + indexName + "' requires "
                + resolved.dimensions());
      requireNonZeroDenseVector(queryVector);
      parameters.put("queryVector", queryVector);
      functionCall = "`vector.neighbors`(:indexName, :queryVector, :candidateLimit, :options)";
    }

    final int candidateLimit =
        filter == null ? limit : (int) Math.min((long) limit * FILTER_OVERFETCH, MAX_FILTER_CANDIDATES);
    parameters.put("candidateLimit", candidateLimit);

    final StringBuilder sql = new StringBuilder("SELECT FROM (SELECT expand(").append(functionCall).append("))");
    if (filter != null)
      sql.append(" WHERE (").append(filter).append(')');
    sql.append(" LIMIT :legLimit");

    return new VectorLegQuery(resolved, sparse, candidateLimit, sql.toString(), parameters);
  }

  /**
   * Coerces the several shapes a RID reaches a projection in - a RID, a record reference, or the
   * string form a projected {@code @rid} can carry - into a RID. Returns null for anything else,
   * which callers treat as a row to skip.
   */
  public static RID toRID(final Object raw) {
    if (raw instanceof final RID rid)
      return rid;
    if (raw instanceof final Identifiable identifiable)
      return identifiable.getIdentity();
    if (raw instanceof final String value && RID.is(value))
      return new RID(value);
    return null;
  }

  private static String normalizeFilter(final String raw) {
    if (raw == null)
      return null;
    final String filter = raw.trim();
    if (filter.isEmpty())
      return null;
    if (filter.length() > MAX_FILTER_EXPRESSION)
      throw new IllegalArgumentException("'filter' must not exceed " + MAX_FILTER_EXPRESSION + " characters");
    return filter;
  }

  private static ResolvedVectorIndex resolveIndex(final Database database, final String indexName,
      final boolean sparse) {
    final Index rawIndex;
    try {
      rawIndex = database.getSchema().getIndexByName(indexName);
    } catch (final SchemaException e) {
      throw new IllegalArgumentException(
          "Vector index '" + indexName + "' does not exist. " + describeAvailableIndexes(database, sparse), e);
    }

    if (!(rawIndex instanceof final TypeIndex typeIndex))
      throw new IllegalArgumentException(
          "Index '" + indexName + "' is not a type index. " + describeAvailableIndexes(database, sparse));

    final Schema.INDEX_TYPE expectedType =
        sparse ? Schema.INDEX_TYPE.LSM_SPARSE_VECTOR : Schema.INDEX_TYPE.LSM_VECTOR;
    final Schema.INDEX_TYPE actualType = typeIndex.getType();
    if (actualType != expectedType) {
      final String hint = actualType == Schema.INDEX_TYPE.LSM_SPARSE_VECTOR
          ? " Set sparse=true for this index."
          : actualType == Schema.INDEX_TYPE.LSM_VECTOR ? " Set sparse=false for this index." : "";
      throw new IllegalArgumentException(
          "Index '" + indexName + "' is " + actualType + ", not " + expectedType + "." + hint + " "
              + describeAvailableIndexes(database, sparse));
    }

    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof final LSMVectorIndex denseIndex)
        return new ResolvedVectorIndex(typeIndex, denseIndex.getDimensions(),
            "distance_lower_is_better:" + denseIndex.getSimilarityFunction().name());

      if (bucketIndex instanceof final LSMSparseVectorIndex sparseIndex) {
        final LSMSparseVectorIndexMetadata metadata = sparseIndex.getSparseMetadata();
        final int dimensions = metadata != null ? metadata.dimensions : 0;
        final String modifier = metadata != null ? metadata.modifier : LSMSparseVectorIndexMetadata.MODIFIER_NONE;
        final String scoring = LSMSparseVectorIndexMetadata.MODIFIER_IDF.equals(modifier)
            ? "score_higher_is_better:idf_weighted_dot_product"
            : "score_higher_is_better:dot_product";
        return new ResolvedVectorIndex(typeIndex, dimensions, scoring);
      }
    }

    throw new IllegalArgumentException("Vector index '" + indexName + "' has no searchable bucket indexes");
  }

  private static String describeAvailableIndexes(final Database database, final boolean sparse) {
    final Schema.INDEX_TYPE expectedType =
        sparse ? Schema.INDEX_TYPE.LSM_SPARSE_VECTOR : Schema.INDEX_TYPE.LSM_VECTOR;
    final Set<String> names = new TreeSet<>();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof TypeIndex && index.getType() == expectedType)
        names.add(index.getName());
    return "Available " + expectedType + " indexes: " + names;
  }

  private static float[] readFloatArray(final JSONArray array, final String field) {
    if (array == null || array.length() == 0)
      throw new IllegalArgumentException("'" + field + "' is required and must not be empty");

    final float[] result = new float[array.length()];
    for (int i = 0; i < array.length(); i++) {
      final Object value = array.get(i);
      if (!(value instanceof final Number number))
        throw new IllegalArgumentException("'" + field + "' must contain only numbers");
      final double asDouble = number.doubleValue();
      if (!Double.isFinite(asDouble) || Math.abs(asDouble) > Float.MAX_VALUE)
        throw new IllegalArgumentException("'" + field + "' must contain only finite float values");
      result[i] = (float) asDouble;
    }
    return result;
  }

  private static void requireNonZeroDenseVector(final float[] queryVector) {
    for (final float value : queryVector)
      if (value != 0.0f)
        return;
    throw new IllegalArgumentException("Dense 'queryVector' must contain at least one non-zero value");
  }

  private static SparseQuery buildSparseQuery(final JSONObject args, final float[] queryVector, final int dimensions) {
    final JSONArray queryIndices = args.getJSONArray("queryIndices", null);
    if (queryIndices == null) {
      if (dimensions > 0 && queryVector.length != dimensions)
        throw new IllegalArgumentException(
            "Sparse 'queryVector' has " + queryVector.length + " dimensions, but the index requires " + dimensions
                + ". Pass queryIndices with compact sparse weights instead.");

      final List<Integer> indices = new ArrayList<>();
      final List<Float> values = new ArrayList<>();
      for (int i = 0; i < queryVector.length; i++)
        if (queryVector[i] != 0.0f) {
          indices.add(i);
          values.add(queryVector[i]);
        }
      if (indices.isEmpty())
        throw new IllegalArgumentException("Sparse 'queryVector' must contain at least one non-zero weight");

      final int[] compactIndices = new int[indices.size()];
      final float[] compactValues = new float[values.size()];
      for (int i = 0; i < indices.size(); i++) {
        compactIndices[i] = indices.get(i);
        compactValues[i] = values.get(i);
      }
      return new SparseQuery(compactIndices, compactValues);
    }

    if (queryIndices.length() != queryVector.length)
      throw new IllegalArgumentException(
          "'queryIndices' and 'queryVector' must have the same length (got " + queryIndices.length() + " and "
              + queryVector.length + ")");

    final int[] indices = new int[queryIndices.length()];
    final Set<Integer> seen = new HashSet<>();
    boolean hasNonZeroWeight = false;
    for (int i = 0; i < queryIndices.length(); i++) {
      final Object raw = queryIndices.get(i);
      if (!(raw instanceof final Number number) || !Double.isFinite(number.doubleValue())
          || number.doubleValue() != Math.rint(number.doubleValue())
          || number.doubleValue() < 0 || number.doubleValue() > Integer.MAX_VALUE)
        throw new IllegalArgumentException("'queryIndices' must contain only non-negative integers");

      final int index = number.intValue();
      if (!seen.add(index))
        throw new IllegalArgumentException("'queryIndices' contains duplicate dimension " + index);
      if (dimensions > 0 && index >= dimensions)
        throw new IllegalArgumentException(
            "Sparse query dimension " + index + " is outside index dimensions 0-" + (dimensions - 1));
      indices[i] = index;
      hasNonZeroWeight |= queryVector[i] != 0.0f;
    }
    if (!hasNonZeroWeight)
      throw new IllegalArgumentException("Sparse 'queryVector' must contain at least one non-zero weight");

    return new SparseQuery(indices, queryVector);
  }
}
```

- [ ] **Step 3: Rewrite `VectorSearchTool.execute` to delegate, and delete the moved statics**

In `VectorSearchTool.java`, keep `getDefinition()` exactly as it is but change its two constant references from the local fields to `MCPVectorLeg.MAX_K` and `MCPVectorLeg.DEFAULT_K`. Delete the five private constants, `normalizeFilter`, `resolveIndex`, `describeAvailableIndexes`, `readFloatArray`, `requireNonZeroDenseVector`, `buildSparseQuery`, `resolveRID`, and both private records. Keep `appendResult` and `invalidExpression`.

Replace `execute` with:

```java
  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = MCPToolUtils.requireString(args, "database");
    final int k = args.getInt("k", MCPVectorLeg.DEFAULT_K);
    if (k < 1 || k > MCPVectorLeg.MAX_K)
      throw new IllegalArgumentException("'k' must be between 1 and " + MCPVectorLeg.MAX_K);

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    final MCPVectorLeg.VectorLegQuery leg = MCPVectorLeg.build(database, args, "indexName", k);

    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(leg.sql());
    } catch (final RuntimeException e) {
      throw invalidExpression(e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated vector search is not read-only");

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray results = new JSONArray();
    try {
      final ResultSet analyzedResultSet = analyzed.execute(leg.parameters());
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", leg.sql(), leg.parameters())) {
        // A stale/deleted hit or malformed row is skipped and cannot be backfilled because the vector candidate
        // window is already fixed. The response therefore reports possible truncation for filtered short results.
        while (resultSet.hasNext() && results.length() < k)
          appendResult(database, resultSet.next(), leg.sparse(), serializer, results);
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression(e);
    }

    // Truncation describes the result window, not the index. A filled window is the only state in which further
    // matches may exist; a short result set means the search ran out of candidates that satisfy the request, so
    // reporting truncation there would tell the caller to widen a search that cannot yield more. Index cardinality
    // is deliberately not consulted: it is almost always larger than the window, which would pin the flag to true
    // and strip it of meaning, and reading it costs a full scan of the index locations on the dense path.
    return new JSONObject()
        .put("indexName", leg.index().typeIndex().getName())
        .put("sparse", leg.sparse())
        .put("scoring", leg.index().scoring())
        .put("candidateLimit", leg.candidateLimit())
        .put("truncated", results.length() >= k)
        .put("count", results.length())
        .put("results", results);
  }
```

In `appendResult`, replace the `resolveRID(row)` call with:

```java
    RID rid = MCPVectorLeg.toRID(row.getProperty("@rid"));
    if (rid == null)
      rid = row.getIdentity().orElse(null);
    if (rid == null)
      return;
```

Remove now-unused imports (`Identifiable` is not imported today; `SchemaException`, `Index`, `IndexInternal`, `LSMSparseVectorIndex`, `LSMVectorIndex`, `LSMSparseVectorIndexMetadata`, `Schema`, `ArrayList`, `HashSet`, `LinkedHashMap`, `List`, `Map`, `Set`, `TreeSet` all become unused).

- [ ] **Step 4: Compile**

Run: `mvn -pl server -am -q -DskipTests install`
Expected: BUILD SUCCESS. Fix any unused-import or missing-import errors before continuing.

- [ ] **Step 5: Run the full existing vector-search suite to prove behavior is unchanged**

Run: `mvn -pl server test -Dtest='MCPServerPluginTest#vectorSearch*'`
Expected: PASS, same test count as Step 1. A single failure here means the refactor changed behavior; do not proceed until it matches.

- [ ] **Step 6: Run every MCP test**

Run: `mvn -pl server test -Dtest='MCP*'`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/MCPVectorLeg.java \
        server/src/main/java/com/arcadedb/server/mcp/tools/VectorSearchTool.java
git commit -m "refactor(#4861): extract the shared vector retrieval leg into MCPVectorLeg"
```

---

### Task 2: `hybrid_search` tool definition, registration, and the vector-only path

Ends with a registered, callable tool that performs a vector-only search. No fusion yet.

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java`
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPPermissionsTest.java`
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`

**Interfaces:**
- Consumes: `MCPVectorLeg.validateArguments`, `MCPVectorLeg.build`, `MCPVectorLeg.toRID`, `MCPVectorLeg.DEFAULT_K`, `MCPVectorLeg.MAX_K` from Task 1. `validateArguments(JSONObject args, String indexNameField)` runs the checks needing no `Database` and MUST be called before `resolveDatabase`, so an argument fault reads the same whether or not the database resolves.
- Produces:
  - `static JSONObject HybridSearchTool.getDefinition()`
  - `static JSONObject HybridSearchTool.execute(ArcadeDBServer, ServerSecurityUser, JSONObject, MCPConfiguration)`
  - `HybridSearchTool.LEG_OVERFETCH` = 4, `MAX_LEG_CANDIDATES` = 1000, `MAX_SEEDS` = 256, `MAX_EXPANSION` = 2000, `MAX_DEPTH` = 3 (all `public static final int`)
  - `static int HybridSearchTool.legLimit(int k)`
  - `record HybridSearchTool.LegRow(RID rid, Double score)` - `score` null means the leg is rank-only
  - Response contract: `fused` (boolean). When `fused` is false the rows carry the leg's native `distance` or `score` key; when true they carry `fusedScore`.

**Response shape note.** The spec shows the fused shape. `vector.fuse` requires at least two sources, so a call with neither `fulltextQuery` nor `expand` cannot be fused at all. Rather than fabricate a `fusedScore` from a single leg, the response reports `"fused": false` and returns the leg's native score. Update the spec's output section to document both shapes as part of this task.

- [ ] **Step 1: Write the failing tests**

Add to `MCPServerPluginTest.java`. Place the fixture helper next to `seedVectorIndexes`, and the tests next to the existing `vectorSearch*` tests.

```java
  private void seedHybridGraph() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("McpHybridDoc"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE VERTEX TYPE McpHybridDoc BUCKETS 1");
      db.command("sql", "CREATE PROPERTY McpHybridDoc.title STRING");
      db.command("sql", "CREATE PROPERTY McpHybridDoc.content STRING");
      db.command("sql", "CREATE PROPERTY McpHybridDoc.embedding ARRAY_OF_FLOATS");
      db.command("sql", """
          CREATE INDEX ON McpHybridDoc (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE' }
          """);
      db.command("sql", "CREATE INDEX ON McpHybridDoc (content) FULL_TEXT");
      db.command("sql", "CREATE EDGE TYPE McpHybridCites");
      db.command("sql", "CREATE EDGE TYPE McpHybridMentions");

      // h0 is the nearest neighbor of the probe vector and the root of the citation chain.
      // h5 is the strongest full-text match for 'gearbox' and is not reachable from h0 at all,
      // so the full-text leg and the expansion leg contribute disjoint rows.
      final MutableVertex h0 = db.newVertex("McpHybridDoc").set("title", "h0")
          .set("content", "graph traversal over connected documents")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      final MutableVertex h1 = db.newVertex("McpHybridDoc").set("title", "h1")
          .set("content", "vector similarity ranking")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      final MutableVertex h2 = db.newVertex("McpHybridDoc").set("title", "h2")
          .set("content", "reciprocal rank fusion")
          .set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save();
      final MutableVertex h3 = db.newVertex("McpHybridDoc").set("title", "h3")
          .set("content", "breadth first expansion")
          .set("embedding", new float[] { 0.5f, 0.5f, 0.0f }).save();
      final MutableVertex h4 = db.newVertex("McpHybridDoc").set("title", "h4")
          .set("content", "unrelated mention target")
          .set("embedding", new float[] { 0.0f, 0.5f, 0.5f }).save();
      final MutableVertex h5 = db.newVertex("McpHybridDoc").set("title", "h5")
          .set("content", "gearbox gearbox gearbox")
          .set("embedding", new float[] { 0.1f, 0.1f, 0.9f }).save();

      // Chain h0 -> h1 -> h2 -> h3, plus a shortcut h0 -> h2 so h2 is reachable two ways,
      // plus h0 -> h4 on a different edge type so edge filtering is observable.
      h0.newEdge("McpHybridCites", h1).save();
      h1.newEdge("McpHybridCites", h2).save();
      h2.newEdge("McpHybridCites", h3).save();
      h0.newEdge("McpHybridCites", h2).save();
      h0.newEdge("McpHybridMentions", h4).save();
      // h5 is deliberately left unconnected.
      assertThat(h5.getIdentity()).isNotNull();
    });
  }

  private static JSONArray probeVector() {
    return new JSONArray().put(1.0).put(0.0).put(0.0);
  }

  private static JSONObject payloadOf(final JSONObject response) {
    assertThat(response.getBoolean("isError", true)).isFalse();
    return new JSONObject(response.getJSONArray("content").getJSONObject(0).getString("text"));
  }

  @Test
  void hybridSearchIsRegisteredInHttpTransport() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 90)
        .put("method", "tools/list")
        .put("params", new JSONObject()));

    assertThat(toolNames(response)).contains("hybrid_search");
  }

  @Test
  void hybridSearchVectorOnlyReturnsTheVectorLegUnfused() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("k", 3)));

    assertThat(payload.getString("vectorIndexName")).isEqualTo("McpHybridDoc[embedding]");
    assertThat(payload.getBoolean("fused")).isFalse();
    assertThat(payload.getBoolean("sparse")).isFalse();
    assertThat(payload.getString("scoring")).startsWith("distance_lower_is_better");
    assertThat(payload.getInt("count")).isEqualTo(3);
    assertThat(payload.getJSONObject("legs").getJSONObject("vector").getInt("count")).isGreaterThanOrEqualTo(3);

    final JSONObject first = payload.getJSONArray("results").getJSONObject(0);
    assertThat(first.getJSONObject("properties").getString("title")).isEqualTo("h0");
    assertThat(first.getDouble("distance")).isGreaterThanOrEqualTo(0.0);
    assertThat(first.has("fusedScore")).isFalse();
    assertThat(first.getJSONArray("sources").getString(0)).isEqualTo("vector");
  }

  @Test
  void hybridSearchRejectsOutOfRangeK() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("k", 0));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'k' must be between 1 and 1000");
  }
```

Add the import `com.arcadedb.graph.MutableVertex` to `MCPServerPluginTest.java` if it is not already present.

In `MCPPermissionsTest.java`, replace line 200 and extend the block:

```java
    assertThat(MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.ALL, "hybrid_search")).isTrue();
    assertThat(MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.RAG, "hybrid_search")).isTrue();
    assertThat(MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.ADMIN, "hybrid_search")).isFalse();
    assertThat(MCPDispatcher.isToolAllowed(MCPConfiguration.ToolProfile.ALL, "unknown")).isFalse();
```

The old `isToolAllowed(ALL, "hybrid_search")).isFalse()` assertion passed only because the tool was declared in `RAG_TOOL_NAMES` but never registered. `isToolAllowed` returns false for any name absent from `REGISTERED_TOOL_NAMES`, and the following line already covers that case with `"unknown"`, so nothing is lost by replacing it.

In `MCPStdioServerTest.java`, add `"hybrid_search"` to the `contains(...)` list at line 83-84 and to the `rag` profile `contains(...)` list at line 112.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl server test -Dtest='MCPServerPluginTest#hybridSearch*,MCPPermissionsTest#toolProfilesAreAllowlists'`
Expected: FAIL. `hybridSearchIsRegisteredInHttpTransport` fails because `hybrid_search` is not in `tools/list`; the two call tests fail with `Unknown tool: hybrid_search`; the permissions test fails on the new `isTrue()`.

- [ ] **Step 3: Create `HybridSearchTool` with the definition and the vector-only path**

Create `server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java` with the standard Apache-2.0 header.

```java
package com.arcadedb.server.mcp.tools;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Fuses a vector retrieval leg, a full-text retrieval leg, and a depth-limited graph expansion leg
 * into one ranked result list.
 * <p>
 * The expansion leg is seeded from the union of the retrieval legs and carries no score of its own:
 * its breadth-first discovery order is its rank. Fusion scoring is performed by the engine's
 * {@code vector.fuse}, never re-implemented here.
 * <p>
 * A request naming only the vector leg cannot be fused, because fusion needs at least two sources.
 * Such a response reports {@code fused=false} and returns the leg's native distance or score rather
 * than a fabricated fused score.
 */
public class HybridSearchTool {
  public static final int LEG_OVERFETCH      = 4;
  public static final int MAX_LEG_CANDIDATES = 1_000;
  public static final int MAX_SEEDS          = 256;
  public static final int MAX_EXPANSION      = 2_000;
  public static final int MAX_DEPTH          = 3;

  /**
   * One row of one retrieval leg. A null score marks a rank-only leg, whose position in the list is
   * its only ranking signal.
   */
  public record LegRow(RID rid, Double score) {
  }

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "hybrid_search")
        .put("description",
            """
            Retrieve records by fusing a vector search, an optional full-text search, and an optional \
            graph expansion into one ranked list. The expansion walks outward from the records the other \
            legs found, so a neighbor of a strong match can itself rank. Supply 'fulltextIndexName' with \
            'fulltextQuery' to add the full-text leg, and 'expand' to add the graph leg; with neither, \
            this returns a plain vector search and reports fused=false. Graph expansion is rank-only, so \
            it requires fusionStrategy RRF. Depth is capped at 3 hops server-side. Each result names the \
            legs it came from in 'sources', and expansion-sourced results carry 'depth' and the 'path' \
            back to their seed. Embedding generation is not performed by ArcadeDB.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to search"))
                .put("vectorIndexName", new JSONObject()
                    .put("type", "string")
                    .put("description", "Name of an LSM_VECTOR or LSM_SPARSE_VECTOR index"))
                .put("queryVector", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "number"))
                    .put("description",
                        "Dense query vector, or sparse weights corresponding to queryIndices when sparse=true"))
                .put("queryIndices", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "integer").put("minimum", 0))
                    .put("description",
                        "Sparse dimension ids corresponding to queryVector weights; omit to use queryVector positions"))
                .put("k", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("maximum", MCPVectorLeg.MAX_K)
                    .put("default", MCPVectorLeg.DEFAULT_K)
                    .put("description", "Maximum number of results to return after fusion"))
                .put("efSearch", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("description", "Dense-index search beam width; higher values improve recall at higher cost"))
                .put("filter", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Optional read-only SQL WHERE predicate applied to the vector leg's bounded candidate set"))
                .put("sparse", new JSONObject()
                    .put("type", "boolean")
                    .put("default", false)
                    .put("description", "Use vector.sparseNeighbors against an LSM_SPARSE_VECTOR index"))
                .put("fulltextIndexName", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Name of the full-text index, e.g. 'Article[content]'. Must be given together with fulltextQuery"))
                .put("fulltextQuery", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Full-text query for the second leg. Must be given together with fulltextIndexName"))
                .put("expand", new JSONObject()
                    .put("type", "object")
                    .put("description", "Graph expansion leg, seeded from the records the other legs found")
                    .put("properties", new JSONObject()
                        .put("edgeTypes", new JSONObject()
                            .put("type", "array")
                            .put("items", new JSONObject().put("type", "string"))
                            .put("description", "Edge types to traverse; omit to traverse every edge type"))
                        .put("direction", new JSONObject()
                            .put("type", "string")
                            .put("enum", new JSONArray().put("out").put("in").put("both"))
                            .put("default", "out")
                            .put("description", "Traversal direction"))
                        .put("maxDepth", new JSONObject()
                            .put("type", "integer")
                            .put("minimum", 1)
                            .put("maximum", MAX_DEPTH)
                            .put("default", 1)
                            .put("description", "Hops to walk; capped at " + MAX_DEPTH + " server-side"))))
                .put("fusionStrategy", new JSONObject()
                    .put("type", "string")
                    .put("enum", new JSONArray().put("RRF").put("DBSF").put("LINEAR"))
                    .put("default", "RRF")
                    .put("description",
                        "Fusion strategy. DBSF and LINEAR need a score on every row, so neither can be combined "
                            + "with the rank-only expansion leg"))
                .put("weights", new JSONObject()
                    .put("type", "object")
                    .put("description",
                        "Per-leg fusion weights. The expansion leg defaults to 0.5 because an arbitrary neighbor "
                            + "should not outrank a direct match")
                    .put("properties", new JSONObject()
                        .put("vector", new JSONObject().put("type", "number").put("default", 1.0))
                        .put("fulltext", new JSONObject().put("type", "number").put("default", 1.0))
                        .put("expand", new JSONObject().put("type", "number").put("default", 0.5)))))
            .put("required", new JSONArray().put("database").put("vectorIndexName").put("queryVector").put("k")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = MCPToolUtils.requireString(args, "database");
    final int k = args.getInt("k", MCPVectorLeg.DEFAULT_K);
    if (k < 1 || k > MCPVectorLeg.MAX_K)
      throw new IllegalArgumentException("'k' must be between 1 and " + MCPVectorLeg.MAX_K);

    // Argument faults are reported before the database is resolved, so a malformed request reads the
    // same whether or not the database also resolves.
    MCPVectorLeg.validateArguments(args, "vectorIndexName");

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    final MCPVectorLeg.VectorLegQuery vectorQuery = MCPVectorLeg.build(database, args, "vectorIndexName", legLimit(k));
    final List<LegRow> vectorLeg = runVectorLeg(database, vectorQuery);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONObject legs = new JSONObject()
        .put("vector", new JSONObject().put("count", vectorLeg.size()));

    final JSONArray results = new JSONArray();
    for (final LegRow row : vectorLeg) {
      if (results.length() >= k)
        break;
      final Document document = lookup(database, row.rid());
      if (document == null)
        continue;
      results.put(new JSONObject()
          .put("rid", row.rid().toString())
          .put(vectorQuery.sparse() ? "score" : "distance", row.score())
          .put("sources", new JSONArray().put("vector"))
          .put("properties", serializer.serializeDocument(document)));
    }

    return new JSONObject()
        .put("vectorIndexName", vectorQuery.index().typeIndex().getName())
        .put("sparse", vectorQuery.sparse())
        .put("scoring", vectorQuery.index().scoring())
        .put("fused", false)
        .put("legs", legs)
        .put("truncated", results.length() >= k)
        .put("count", results.length())
        .put("results", results);
  }

  /**
   * Rows each retrieval leg fetches before fusion. Fusing lists that are only as long as the requested
   * result count would leave fusion nothing to reorder, so each leg over-fetches.
   */
  public static int legLimit(final int k) {
    return (int) Math.min((long) k * LEG_OVERFETCH, MAX_LEG_CANDIDATES);
  }

  private static List<LegRow> runVectorLeg(final Database database, final MCPVectorLeg.VectorLegQuery query) {
    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(query.sql());
    } catch (final RuntimeException e) {
      throw invalidExpression("vector leg", e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated vector leg is not read-only");

    final List<LegRow> rows = new ArrayList<>();
    final Set<RID> seen = new LinkedHashSet<>();
    try {
      final ResultSet analyzedResultSet = analyzed.execute(query.parameters());
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", query.sql(), query.parameters())) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          RID rid = MCPVectorLeg.toRID(row.getProperty("@rid"));
          if (rid == null)
            rid = row.getIdentity().orElse(null);
          if (rid == null || !seen.add(rid))
            continue;
          final Object raw = row.getProperty(query.sparse() ? "score" : "distance");
          if (!(raw instanceof final Number score))
            continue;
          rows.add(new LegRow(rid, score.doubleValue()));
        }
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression("vector leg", e);
    }
    return rows;
  }

  private static Document lookup(final Database database, final RID rid) {
    try {
      return database.lookupByRID(rid, true) instanceof final Document document ? document : null;
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  private static IllegalArgumentException invalidExpression(final String stage, final RuntimeException cause) {
    final String detail = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    return new IllegalArgumentException("Invalid hybrid search " + stage + ": " + detail, cause);
  }
}
```

- [ ] **Step 4: Register the tool in `MCPDispatcher`**

Three edits in `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java`:

After the `FullTextSearchTool.getDefinition()` line in the static block (line 115):

```java
    TOOLS_LIST.put(HybridSearchTool.getDefinition());
```

After the `full_text_search` case in the `toolsCall` switch (line 288):

```java
        case "hybrid_search" -> HybridSearchTool.execute(server, user, args, config);
```

After the `full_text_search` case in `formatResult` (line 418):

```java
      case "hybrid_search" -> result.getInt("count", 0) + " fused hit(s)";
```

No edit to `RAG_TOOL_NAMES` is needed - `"hybrid_search"` is already there at line 90.

- [ ] **Step 5: Compile and run the new tests**

Run: `mvn -pl server -am -q -DskipTests install && mvn -pl server test -Dtest='MCPServerPluginTest#hybridSearch*,MCPPermissionsTest#toolProfilesAreAllowlists,MCPStdioServerTest'`
Expected: PASS.

- [ ] **Step 6: Run every MCP test**

Run: `mvn -pl server test -Dtest='MCP*'`
Expected: PASS.

- [ ] **Step 7: Update the spec's output section to document both response shapes**

In `docs/superpowers/specs/2026-07-28-mcp-hybrid-search-design.md`, in the "Output schema" section, add `"fused": true` to the example object and append this paragraph after the `truncated` bullets:

```markdown
`fused` reports whether fusion actually ran. `vector.fuse` requires at least two sources, so a call
naming neither `fulltextQuery` nor `expand` cannot be fused: that response sets `fused` to false and
its rows carry the vector leg's native `distance` (dense) or `score` (sparse) instead of `fusedScore`.
Every other call sets `fused` to true and carries `fusedScore`.
```

- [ ] **Step 8: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPPermissionsTest.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java \
        docs/superpowers/specs/2026-07-28-mcp-hybrid-search-design.md
git commit -m "feat(#4861): register the hybrid_search tool with its vector-only path"
```

---

### Task 3: Full-text leg and two-way fusion

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java`
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`

**Interfaces:**
- Consumes: `LegRow`, `legLimit`, `lookup`, `invalidExpression` from Task 2.
- Produces:
  - `record HybridSearchTool.Leg(String name, List<LegRow> rows, float weight)`
  - `static JSONObject HybridSearchTool.fuse(Database, List<Leg>, String strategy, int k, JsonSerializer, Map<RID, ExpansionInfo>)` - `ExpansionInfo` arrives in Task 4; pass `Map.of()` here
  - `record HybridSearchTool.ExpansionInfo(int depth, List<RID> path)` - declared now, populated in Task 4

- [ ] **Step 1: Write the failing tests**

Add to `MCPServerPluginTest.java`:

```java
  @Test
  void hybridSearchFusesVectorAndFullText() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        .put("k", 6)));

    assertThat(payload.getBoolean("fused")).isTrue();
    assertThat(payload.getString("fusionStrategy")).isEqualTo("RRF");
    assertThat(payload.getString("fulltextIndexName")).isEqualTo("McpHybridDoc[content]");
    assertThat(payload.getJSONObject("legs").getJSONObject("fulltext").getInt("count")).isEqualTo(1);

    final JSONArray results = payload.getJSONArray("results");
    boolean sawFullTextSource = false;
    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      assertThat(row.getDouble("fusedScore")).isGreaterThan(0.0);
      assertThat(row.has("distance")).isFalse();
      final JSONArray sources = row.getJSONArray("sources");
      for (int s = 0; s < sources.length(); s++)
        if ("fulltext".equals(sources.getString(s)))
          sawFullTextSource = true;
    }
    // h5 matches 'gearbox' and is far from the probe vector, so it can only arrive via the full-text leg.
    assertThat(sawFullTextSource).isTrue();
  }

  @Test
  void hybridSearchWeightsShiftTheFusedOrder() throws Exception {
    seedHybridGraph();

    final JSONObject fullTextHeavy = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        .put("weights", new JSONObject().put("vector", 0.01).put("fulltext", 100.0))
        .put("k", 6)));

    assertThat(fullTextHeavy.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("title")).isEqualTo("h5");
  }

  @Test
  void hybridSearchRejectsAnIncompleteFullTextLeg() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextQuery", "gearbox")
        .put("k", 3));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("fulltextIndexName").contains("fulltextQuery");
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl server test -Dtest='MCPServerPluginTest#hybridSearchFuses*,MCPServerPluginTest#hybridSearchWeights*,MCPServerPluginTest#hybridSearchRejectsAnIncomplete*'`
Expected: FAIL - `fused` is false, `fusionStrategy` is absent, and the incomplete-leg call succeeds instead of erroring.

- [ ] **Step 3: Add the full-text leg and the fusion call**

Add these imports to `HybridSearchTool.java`:

```java
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
```

Add the records and methods:

```java
  /**
   * One fusion source: its display name, its rows in rank order, and the weight applied to every
   * rank contribution it makes.
   */
  public record Leg(String name, List<LegRow> rows, float weight) {
  }

  /**
   * Provenance for a row the expansion leg contributed: how many hops from its seed, and the RID
   * path back to that seed including the seed itself.
   */
  public record ExpansionInfo(int depth, List<RID> path) {
  }

  private static List<LegRow> runFullTextLeg(final Database database, final JSONObject args, final int limit,
      final JSONObject legs) {
    final String indexName = args.getString("fulltextIndexName", null);
    final String queryText = args.getString("fulltextQuery", null);
    final boolean hasIndex = indexName != null && !indexName.isBlank();
    final boolean hasQuery = queryText != null && !queryText.isBlank();

    if (!hasIndex && !hasQuery)
      return List.of();
    // Half a leg is always a mistake, and silently dropping it would return a plausible-looking result
    // set that quietly ignored what the caller asked for.
    if (hasIndex != hasQuery)
      throw new IllegalArgumentException(
          "'fulltextIndexName' and 'fulltextQuery' must be supplied together. Give both to add the full-text leg, "
              + "or neither to search without it.");

    final TypeIndex typeIndex;
    try {
      typeIndex = FullTextSearch.resolveFullTextIndex(database, indexName);
    } catch (final SchemaException e) {
      throw new IllegalArgumentException("Full-text index '" + indexName + "' does not exist. Available full-text "
          + "indexes in '" + database.getName() + "': " + FullTextSearch.listFullTextIndexes(database), e);
    } catch (final CommandExecutionException e) {
      throw new IllegalArgumentException("Index '" + indexName + "' is not a full-text index. Available full-text "
          + "indexes in '" + database.getName() + "': " + FullTextSearch.listFullTextIndexes(database), e);
    }

    final Map<RID, Float> hits = FullTextSearch.search(typeIndex, queryText, limit);
    final List<Map.Entry<RID, Float>> ranked = new ArrayList<>(hits.entrySet());
    // Score descending, tie-broken by RID so tied hits rank deterministically rather than by hash order.
    ranked.sort(Map.Entry.<RID, Float>comparingByValue().reversed().thenComparing(Map.Entry::getKey));

    final List<LegRow> rows = new ArrayList<>(ranked.size());
    for (final Map.Entry<RID, Float> hit : ranked)
      rows.add(new LegRow(hit.getKey(), hit.getValue().doubleValue()));

    legs.put("fulltext", new JSONObject()
        .put("indexName", typeIndex.getName())
        .put("similarity", FullTextSearch.getSimilarity(typeIndex))
        .put("count", rows.size()));
    return rows;
  }

  private static float weightOf(final JSONObject args, final String legName, final float fallback) {
    final JSONObject weights = args.getJSONObject("weights", null);
    if (weights == null)
      return fallback;
    final double value = weights.getDouble(legName, fallback);
    if (!Double.isFinite(value) || value < 0.0)
      throw new IllegalArgumentException("weights." + legName + " must be a finite number that is not negative");
    return (float) value;
  }

  /**
   * Hands the materialized legs to the engine's fusion function and shapes its output. Fusion scoring
   * lives entirely in {@code vector.fuse}; nothing here re-derives a rank contribution.
   */
  private static JSONArray fuse(final Database database, final List<Leg> legList, final String strategy, final int k,
      final JsonSerializer serializer, final Map<RID, ExpansionInfo> expansion) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    final StringBuilder sql = new StringBuilder("SELECT expand(`vector.fuse`(");
    final List<Float> weights = new ArrayList<>(legList.size());

    for (int i = 0; i < legList.size(); i++) {
      final Leg leg = legList.get(i);
      final List<Map<String, Object>> rows = new ArrayList<>(leg.rows().size());
      for (final LegRow row : leg.rows()) {
        final Map<String, Object> entry = new LinkedHashMap<>(2);
        // The RID must go in as a RID: the fusion function reads @rid as a RID or a record reference
        // and silently drops any row whose @rid is a string.
        entry.put("@rid", row.rid());
        if (row.score() != null)
          entry.put("score", row.score());
        rows.add(entry);
      }
      final String name = "l" + i;
      parameters.put(name, rows);
      weights.add(leg.weight());
      if (i > 0)
        sql.append(", ");
      sql.append(':').append(name);
    }

    final Map<String, Object> options = new LinkedHashMap<>();
    options.put("fusion", strategy);
    options.put("weights", weights);
    options.put("limit", k);
    parameters.put("opts", options);
    sql.append(", :opts))");

    final Map<RID, Set<String>> sources = new HashMap<>();
    for (final Leg leg : legList)
      for (final LegRow row : leg.rows())
        sources.computeIfAbsent(row.rid(), r -> new LinkedHashSet<>()).add(leg.name());

    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(sql.toString());
    } catch (final RuntimeException e) {
      throw invalidExpression("fusion", e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated hybrid fusion is not read-only");

    final JSONArray results = new JSONArray();
    try {
      final ResultSet analyzedResultSet = analyzed.execute(parameters);
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", sql.toString(), parameters)) {
        while (resultSet.hasNext() && results.length() < k) {
          final Result row = resultSet.next();
          final RID rid = MCPVectorLeg.toRID(row.getProperty("@rid"));
          if (rid == null)
            continue;
          if (!(row.getProperty("score") instanceof final Number score))
            continue;
          final Object record = row.getProperty("record");
          final Document document = record instanceof final Document candidate ? candidate : lookup(database, rid);
          if (document == null)
            continue;

          final JSONArray sourceNames = new JSONArray();
          for (final String name : sources.getOrDefault(rid, Set.of()))
            sourceNames.put(name);

          final JSONObject result = new JSONObject()
              .put("rid", rid.toString())
              .put("fusedScore", score)
              .put("sources", sourceNames)
              .put("properties", serializer.serializeDocument(document));

          final ExpansionInfo info = expansion.get(rid);
          if (info != null) {
            result.put("depth", info.depth());
            final JSONArray path = new JSONArray();
            for (final RID step : info.path())
              path.put(step.toString());
            result.put("path", path);
          }
          results.put(result);
        }
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression("fusion", e);
    }
    return results;
  }
```

Then replace the body of `execute` after the `runVectorLeg` call, from the `legs` declaration to the `return`, with:

```java
    final JSONObject legs = new JSONObject()
        .put("vector", new JSONObject().put("count", vectorLeg.size()));

    final String strategy = strategyOf(args);
    final List<LegRow> fullTextLeg = runFullTextLeg(database, args, legLimit(k), legs);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final List<Leg> legList = new ArrayList<>(3);
    legList.add(new Leg("vector", vectorLeg, weightOf(args, "vector", 1.0f)));
    if (!fullTextLeg.isEmpty())
      legList.add(new Leg("fulltext", fullTextLeg, weightOf(args, "fulltext", 1.0f)));

    final JSONObject response = new JSONObject()
        .put("vectorIndexName", vectorQuery.index().typeIndex().getName())
        .put("sparse", vectorQuery.sparse())
        .put("scoring", vectorQuery.index().scoring())
        .put("legs", legs);

    final JSONArray results;
    if (legList.size() < 2) {
      // Fusion needs at least two sources. Rather than fabricate a fused score from one leg, report the
      // leg's own score under its native key and say plainly that no fusion happened.
      results = new JSONArray();
      for (final LegRow row : vectorLeg) {
        if (results.length() >= k)
          break;
        final Document document = lookup(database, row.rid());
        if (document == null)
          continue;
        results.put(new JSONObject()
            .put("rid", row.rid().toString())
            .put(vectorQuery.sparse() ? "score" : "distance", row.score())
            .put("sources", new JSONArray().put("vector"))
            .put("properties", serializer.serializeDocument(document)));
      }
      response.put("fused", false);
    } else {
      results = fuse(database, legList, strategy, k, serializer, Map.of());
      response.put("fused", true).put("fusionStrategy", strategy);
      if (legs.has("fulltext"))
        response.put("fulltextIndexName", legs.getJSONObject("fulltext").getString("indexName"));
    }

    return response
        .put("truncated", results.length() >= k)
        .put("count", results.length())
        .put("results", results);
```

Add the strategy parser:

```java
  private static String strategyOf(final JSONObject args) {
    final String raw = args.getString("fusionStrategy", "RRF");
    final String strategy = raw.toUpperCase(Locale.ROOT);
    if (!"RRF".equals(strategy) && !"DBSF".equals(strategy) && !"LINEAR".equals(strategy))
      throw new IllegalArgumentException(
          "Unknown fusionStrategy '" + raw + "'. Allowed: RRF, DBSF, LINEAR");
    return strategy;
  }
```

Add `import java.util.Locale;`.

- [ ] **Step 4: Compile and run the new tests**

Run: `mvn -pl server -am -q -DskipTests install && mvn -pl server test -Dtest='MCPServerPluginTest#hybridSearch*'`
Expected: PASS, including the Task 2 tests which must stay green.

- [ ] **Step 5: Run every MCP test**

Run: `mvn -pl server test -Dtest='MCP*'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java
git commit -m "feat(#4861): add the full-text leg and two-way fusion to hybrid_search"
```

---

### Task 4: Graph expansion leg and three-way fusion

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java`
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`

**Interfaces:**
- Consumes: `Leg`, `ExpansionInfo`, `LegRow`, `fuse`, `weightOf`, `legLimit` from Tasks 2 and 3.
- Produces:
  - `record HybridSearchTool.Expansion(List<LegRow> rows, Map<RID, ExpansionInfo> info, boolean truncated)`
  - `static Expansion HybridSearchTool.runExpansionLeg(Database, JSONObject expandArgs, List<RID> seeds)`

**Two engine behaviors this task depends on**, both established by probe before the design was written. Neither is negotiable:

1. **Seed RIDs are inlined as literals, never bound.** `TRAVERSE ... FROM :ridCollection` throws `NullPointerException` inside the planner. The seeds come from the engine's own legs and never from caller text, so there is no injection surface. Do not "improve" this into a bound parameter.
2. **Edge type names are inlined as quoted literals, never bound.** `out(:boundEdgeList)` returns only the seeds and raises no error, so a bound list silently produces an empty neighborhood.

- [ ] **Step 1: Write the failing tests**

Add to `MCPServerPluginTest.java`:

```java
  @Test
  void hybridSearchExpandsAlongTheGraphAndReportsPaths() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        // The fixture is small enough that an unfiltered vector leg retrieves every record, which would
        // make every record a seed and leave the expansion leg with nothing new to contribute. Narrowing
        // the vector leg to h0 is what makes the expanded rows observable.
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("direction", "out")
            .put("maxDepth", 2))
        .put("k", 10)));

    assertThat(payload.getBoolean("fused")).isTrue();
    final JSONObject expandLeg = payload.getJSONObject("legs").getJSONObject("expand");
    assertThat(expandLeg.getString("direction")).isEqualTo("out");
    assertThat(expandLeg.getInt("maxDepth")).isEqualTo(2);
    assertThat(expandLeg.getBoolean("truncated")).isFalse();
    assertThat(expandLeg.getInt("count")).isGreaterThan(0);

    JSONObject expanded = null;
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      final JSONArray sources = row.getJSONArray("sources");
      for (int s = 0; s < sources.length(); s++)
        if ("expand".equals(sources.getString(s)) && sources.length() == 1)
          expanded = row;
    }

    assertThat(expanded).isNotNull();
    assertThat(expanded.getInt("depth")).isBetween(1, 2);
    // The path starts at the seed and ends at the row itself, so it is one longer than the depth.
    assertThat(expanded.getJSONArray("path").length()).isEqualTo(expanded.getInt("depth") + 1);
    assertThat(expanded.getJSONArray("path").getString(expanded.getJSONArray("path").length() - 1))
        .isEqualTo(expanded.getString("rid"));
  }

  @Test
  void hybridSearchDedupsNodesReachableBySeveralPaths() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        // h2 must reach the result set through the expansion leg, not as a seed, or its depth is never set.
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 3))
        .put("k", 10)));

    // h2 is reachable from h0 both directly and through h1. It must appear once, at its shallowest depth.
    final Set<String> rids = new HashSet<>();
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++)
      assertThat(rids.add(results.getJSONObject(i).getString("rid"))).isTrue();

    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      if ("h2".equals(row.getJSONObject("properties").getString("title")) && row.has("depth"))
        assertThat(row.getInt("depth")).isEqualTo(1);
    }
  }

  @Test
  void hybridSearchRestrictsExpansionToTheRequestedEdgeTypes() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 1))
        .put("k", 10)));

    // h4 hangs off h0 by McpHybridMentions only, so restricting to McpHybridCites must not reach it.
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++)
      assertThat(results.getJSONObject(i).getJSONObject("properties").getString("title")).isNotEqualTo("h4");
  }

  @Test
  void hybridSearchFusesAllThreeLegs() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        // Seeds become h0 (vector) and h5 (full-text). h5 is unconnected, so every expanded row comes
        // from h0's citation chain and none of them is already a seed.
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 2))
        .put("k", 10)));

    assertThat(payload.getBoolean("fused")).isTrue();
    assertThat(payload.getJSONObject("legs").has("vector")).isTrue();
    assertThat(payload.getJSONObject("legs").has("fulltext")).isTrue();
    assertThat(payload.getJSONObject("legs").has("expand")).isTrue();

    final Set<String> allSources = new HashSet<>();
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++) {
      final JSONArray sources = results.getJSONObject(i).getJSONArray("sources");
      for (int s = 0; s < sources.length(); s++)
        allSources.add(sources.getString(s));
    }
    assertThat(allSources).contains("vector", "fulltext", "expand");
  }
```

Add `import java.util.HashSet;` and `import java.util.Set;` to the test class if not already present.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl server test -Dtest='MCPServerPluginTest#hybridSearchExpands*,MCPServerPluginTest#hybridSearchDedups*,MCPServerPluginTest#hybridSearchRestricts*,MCPServerPluginTest#hybridSearchFusesAll*'`
Expected: FAIL - `legs.expand` is absent because the `expand` argument is ignored.

- [ ] **Step 3: Implement the expansion leg**

Add to `HybridSearchTool.java`:

```java
  /**
   * Result of the graph expansion leg: the ranked rows in breadth-first discovery order, the depth
   * and path for each of them, and whether the fan-out cap cut the walk short.
   */
  public record Expansion(List<LegRow> rows, Map<RID, ExpansionInfo> info, boolean truncated) {
  }

  /**
   * Walks outward from the seeds and returns the nodes found, ranked by breadth-first discovery order.
   * <p>
   * The rows carry no score. Breadth-first order already encodes "closer to a retrieved record ranks
   * higher", which is the only ranking signal a traversal produces, and inventing a numeric score for
   * it would put a second scoring model next to the engine's.
   * <p>
   * Seeds are dropped from the result: each is already ranked by the leg that found it, and letting it
   * rank again here would count its own presence twice.
   */
  private static Expansion runExpansionLeg(final Database database, final JSONObject expandArgs,
      final List<RID> seeds) {
    final int maxDepth = expandArgs.getInt("maxDepth", 1);
    if (maxDepth < 1 || maxDepth > MAX_DEPTH)
      throw new IllegalArgumentException(
          "expand.maxDepth must be between 1 and " + MAX_DEPTH + ", got " + maxDepth);

    final String direction = expandArgs.getString("direction", "out");
    if (!"out".equals(direction) && !"in".equals(direction) && !"both".equals(direction))
      throw new IllegalArgumentException(
          "expand.direction must be one of out, in, both, got '" + direction + "'");

    final List<String> edgeTypes = validatedEdgeTypes(database, expandArgs.getJSONArray("edgeTypes", null));

    if (seeds.isEmpty())
      return new Expansion(List.of(), Map.of(), false);

    final StringBuilder sql = new StringBuilder(
        "SELECT @rid AS rid, $depth AS depth, $path AS path FROM (TRAVERSE ");
    sql.append(direction).append('(');
    for (int i = 0; i < edgeTypes.size(); i++) {
      if (i > 0)
        sql.append(", ");
      sql.append('\'').append(edgeTypes.get(i)).append('\'');
    }
    sql.append(") FROM [");
    for (int i = 0; i < seeds.size(); i++) {
      if (i > 0)
        sql.append(',');
      sql.append(seeds.get(i).toString());
    }
    // The LIMIT applies inside the traversal, before the outer depth filter removes the seeds, so the
    // seeds consume slots and the budget must cover them or the last expanded rows are lost.
    sql.append("] MAXDEPTH ").append(maxDepth)
        .append(" LIMIT ").append(seeds.size() + MAX_EXPANSION)
        .append(" STRATEGY BREADTH_FIRST) WHERE $depth > 0");

    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(sql.toString());
    } catch (final RuntimeException e) {
      throw invalidExpression("expansion leg", e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated graph expansion is not read-only");

    final List<LegRow> rows = new ArrayList<>();
    final Map<RID, ExpansionInfo> info = new HashMap<>();
    final Set<RID> seedSet = new HashSet<>(seeds);
    try {
      final ResultSet analyzedResultSet = analyzed.execute(Map.of());
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", sql.toString())) {
        while (resultSet.hasNext() && rows.size() < MAX_EXPANSION) {
          final Result row = resultSet.next();
          final RID rid = MCPVectorLeg.toRID(row.getProperty("rid"));
          if (rid == null || seedSet.contains(rid) || info.containsKey(rid))
            continue;
          if (!(row.getProperty("depth") instanceof final Number depth))
            continue;
          rows.add(new LegRow(rid, null));
          info.put(rid, new ExpansionInfo(depth.intValue(), readPath(row.getProperty("path"), rid)));
        }
        return new Expansion(rows, info, rows.size() >= MAX_EXPANSION && resultSet.hasNext());
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression("expansion leg", e);
    }
  }

  /**
   * Reads the traversal's path metadata. Falls back to the row's own RID when the path is missing, so
   * a row always reports where it is even if the engine did not carry how it was reached.
   */
  private static List<RID> readPath(final Object raw, final RID rid) {
    if (!(raw instanceof final List<?> steps))
      return List.of(rid);
    final List<RID> path = new ArrayList<>(steps.size());
    for (final Object step : steps) {
      final RID stepRid = MCPVectorLeg.toRID(step);
      if (stepRid != null)
        path.add(stepRid);
    }
    return path.isEmpty() ? List.of(rid) : path;
  }

  /**
   * Validates every requested edge type against the schema. An unknown name is rejected rather than
   * passed through, because a traversal over a name that matches nothing returns an empty neighborhood
   * and raises no error: an unvalidated typo would silently downgrade the search with no way for the
   * caller to notice.
   */
  private static List<String> validatedEdgeTypes(final Database database, final JSONArray requested) {
    if (requested == null || requested.length() == 0)
      return List.of();

    final List<String> names = new ArrayList<>(requested.length());
    for (int i = 0; i < requested.length(); i++) {
      final Object raw = requested.get(i);
      if (!(raw instanceof final String name) || name.isBlank())
        throw new IllegalArgumentException("expand.edgeTypes must contain only non-blank type names");
      if (name.indexOf('\'') >= 0 || name.indexOf('`') >= 0 || name.indexOf('\\') >= 0)
        throw new IllegalArgumentException(
            "expand.edgeTypes entry '" + name + "' contains a quote or backslash, which is not supported");
      if (!database.getSchema().existsType(name) || !(database.getSchema().getType(name) instanceof EdgeType))
        throw new IllegalArgumentException("Edge type '" + name + "' does not exist. "
            + describeAvailableEdgeTypes(database));
      names.add(name);
    }
    return names;
  }

  private static String describeAvailableEdgeTypes(final Database database) {
    final Set<String> names = new TreeSet<>();
    for (final DocumentType type : database.getSchema().getTypes())   // Collection<? extends DocumentType>
      if (type instanceof EdgeType)
        names.add(type.getName());
    return "Available edge types: " + names;
  }

  /**
   * Picks the records the expansion walks out from: every retrieval hit, in rank order, vector leg
   * first, capped so a wide retrieval cannot turn into an unbounded traversal.
   */
  private static List<RID> collectSeeds(final List<LegRow> vectorLeg, final List<LegRow> fullTextLeg) {
    final Set<RID> seeds = new LinkedHashSet<>();
    for (final LegRow row : vectorLeg) {
      if (seeds.size() >= MAX_SEEDS)
        break;
      seeds.add(row.rid());
    }
    for (final LegRow row : fullTextLeg) {
      if (seeds.size() >= MAX_SEEDS)
        break;
      seeds.add(row.rid());
    }
    return new ArrayList<>(seeds);
  }
```

Add imports:

```java
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.HashSet;
import java.util.TreeSet;
```

Then wire it into `execute`. Immediately after the `if (!fullTextLeg.isEmpty())` line that appends the full-text leg, insert:

```java
    final JSONObject expandArgs = args.getJSONObject("expand", null);
    Map<RID, ExpansionInfo> expansionInfo = Map.of();
    if (expandArgs != null) {
      requireVertexType(database, vectorQuery.index().typeIndex().getTypeName());
      final Expansion expansion = runExpansionLeg(database, expandArgs, collectSeeds(vectorLeg, fullTextLeg));
      expansionInfo = expansion.info();
      legs.put("expand", new JSONObject()
          .put("direction", expandArgs.getString("direction", "out"))
          .put("edgeTypes", expandArgs.getJSONArray("edgeTypes", new JSONArray()))
          .put("maxDepth", expandArgs.getInt("maxDepth", 1))
          .put("truncated", expansion.truncated())
          .put("count", expansion.rows().size()));
      if (!expansion.rows().isEmpty())
        legList.add(new Leg("expand", expansion.rows(), weightOf(args, "expand", 0.5f)));
    }
```

and change the `fuse(...)` call in the `else` branch from `Map.of()` to `expansionInfo`.

Add the vertex precondition:

```java
  /**
   * Graph expansion is only meaningful over vertices. Checking the index's type once gives one clear
   * error instead of a silently empty neighborhood.
   */
  private static void requireVertexType(final Database database, final String typeName) {
    if (!database.getSchema().existsType(typeName)
        || !(database.getSchema().getType(typeName) instanceof VertexType))
      throw new IllegalArgumentException("Graph expansion requires a vertex type, but '" + typeName
          + "' is not one. Drop 'expand', or search an index declared on a vertex type.");
  }
```

Add `import com.arcadedb.schema.VertexType;`.

- [ ] **Step 4: Compile and run the new tests**

Run: `mvn -pl server -am -q -DskipTests install && mvn -pl server test -Dtest='MCPServerPluginTest#hybridSearch*'`
Expected: PASS.

If `hybridSearchExpandsAlongTheGraphAndReportsPaths` fails on a null `path`, the outer `WHERE $depth > 0` is stripping the traversal metadata. In that case, drop the outer `WHERE` from the generated SQL and filter `depth > 0` in the Java loop instead - the depth is already read there. Do not remove the path assertion.

- [ ] **Step 5: Run every MCP test**

Run: `mvn -pl server test -Dtest='MCP*'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java
git commit -m "feat(#4861): add the depth-capped graph expansion leg to hybrid_search"
```

---

### Task 5: Guards, caps, and the cap-arithmetic unit test

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java`
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`
- Create: `server/src/test/java/com/arcadedb/server/mcp/HybridSearchCapsTest.java`

**Interfaces:**
- Consumes: everything from Tasks 2-4.
- Produces: no new public API; adds the rank-only-leg strategy guard to `execute`.

- [ ] **Step 1: Write the failing tests**

Add to `MCPServerPluginTest.java`:

```java
  @Test
  void hybridSearchRejectsScoreBasedFusionWithExpansion() throws Exception {
    seedHybridGraph();

    for (final String strategy : new String[] { "DBSF", "LINEAR" }) {
      final JSONObject response = callTool("hybrid_search", new JSONObject()
          .put("database", getDatabaseName())
          .put("vectorIndexName", "McpHybridDoc[embedding]")
          .put("queryVector", probeVector())
          .put("fusionStrategy", strategy)
          .put("expand", new JSONObject().put("maxDepth", 1))
          .put("k", 5));

      assertThat(response.getBoolean("isError", false)).isTrue();
      assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
          .contains(strategy).contains("RRF").contains("expand");
    }
  }

  @Test
  void hybridSearchAllowsScoreBasedFusionWithoutExpansion() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        .put("fusionStrategy", "LINEAR")
        .put("k", 5)));

    assertThat(payload.getString("fusionStrategy")).isEqualTo("LINEAR");
    assertThat(payload.getBoolean("fused")).isTrue();
  }

  @Test
  void hybridSearchRejectsDepthAboveTheServerCap() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONObject().put("maxDepth", 4))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("maxDepth").contains("1 and 3");
  }

  @Test
  void hybridSearchHonorsTheDepthCapWhenTheGraphIsDeeper() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 1))
        .put("k", 10)));

    // From h0 at one hop only h1 and h2 are reachable; h3 sits two hops out and must not appear.
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      assertThat(row.getJSONObject("properties").getString("title")).isNotEqualTo("h3");
      if (row.has("depth"))
        assertThat(row.getInt("depth")).isEqualTo(1);
    }
  }

  @Test
  void hybridSearchRejectsAnUnknownEdgeType() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridNotAnEdge")))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("McpHybridNotAnEdge").contains("Available edge types");
  }

  @Test
  void hybridSearchRejectsExpansionOverADocumentType() throws Exception {
    seedVectorIndexes();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpVectorRecord[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONObject().put("maxDepth", 1))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("vertex type").contains("McpVectorRecord");
  }
```

Create `server/src/test/java/com/arcadedb/server/mcp/HybridSearchCapsTest.java` with the standard Apache-2.0 header:

```java
package com.arcadedb.server.mcp;

import com.arcadedb.server.mcp.tools.HybridSearchTool;
import com.arcadedb.server.mcp.tools.MCPVectorLeg;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cap arithmetic, verified without a running server. These bounds are the only thing standing
 * between a wide request and an unbounded traversal, so they are pinned independently of the
 * end-to-end tests that exercise them.
 */
class HybridSearchCapsTest {

  @Test
  void legLimitOverFetchesRelativeToK() {
    assertThat(HybridSearchTool.legLimit(1)).isEqualTo(4);
    assertThat(HybridSearchTool.legLimit(10)).isEqualTo(40);
  }

  @Test
  void legLimitIsBoundedRegardlessOfK() {
    assertThat(HybridSearchTool.legLimit(MCPVectorLeg.MAX_K))
        .isEqualTo(HybridSearchTool.MAX_LEG_CANDIDATES);
    // The multiplication must not overflow into a negative or tiny limit at the top of the range.
    assertThat(HybridSearchTool.legLimit(Integer.MAX_VALUE))
        .isEqualTo(HybridSearchTool.MAX_LEG_CANDIDATES);
  }

  @Test
  void capsAreOrderedSoTheExpansionBudgetExceedsTheSeedBudget() {
    assertThat(HybridSearchTool.MAX_SEEDS).isLessThan(HybridSearchTool.MAX_EXPANSION);
    assertThat(HybridSearchTool.MAX_DEPTH).isEqualTo(3);
  }

  @Test
  void toolDefinitionDeclaresTheDepthCap() {
    final var expand = HybridSearchTool.getDefinition()
        .getJSONObject("inputSchema").getJSONObject("properties").getJSONObject("expand")
        .getJSONObject("properties").getJSONObject("maxDepth");
    assertThat(expand.getInt("maximum")).isEqualTo(HybridSearchTool.MAX_DEPTH);
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl server test -Dtest='HybridSearchCapsTest,MCPServerPluginTest#hybridSearchRejects*,MCPServerPluginTest#hybridSearchAllows*,MCPServerPluginTest#hybridSearchHonors*'`
Expected: FAIL on `hybridSearchRejectsScoreBasedFusionWithExpansion` only - the `DBSF`/`LINEAR` + `expand` calls currently reach `vector.fuse` and surface the engine's own parse-level message instead of a self-correcting one. `HybridSearchCapsTest` and the other tests in this step should already pass, since the caps and guards they cover landed in Tasks 2-4; if any of them fails, an earlier task is incomplete.

- [ ] **Step 3: Add the rank-only-leg strategy guard**

In `HybridSearchTool.execute`, immediately after `final String strategy = strategyOf(args);` insert:

```java
    // The expansion leg ranks by traversal order and carries no score, which the score-normalizing
    // strategies cannot consume. Rejecting here names the conflict; letting it through would surface
    // as a parse-level complaint about a source the caller never wrote.
    if (args.getJSONObject("expand", null) != null && !"RRF".equals(strategy))
      throw new IllegalArgumentException("fusionStrategy " + strategy + " needs a score on every row, but the graph "
          + "expansion leg is ranked by traversal order and has none. Use RRF, or drop 'expand'.");
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -pl server -am -q -DskipTests install && mvn -pl server test -Dtest='HybridSearchCapsTest,MCPServerPluginTest#hybridSearch*'`
Expected: PASS.

- [ ] **Step 5: Run every MCP test plus the vector and full-text suites**

Run: `mvn -pl server test -Dtest='MCP*,HybridSearchCapsTest'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/HybridSearchTool.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java \
        server/src/test/java/com/arcadedb/server/mcp/HybridSearchCapsTest.java
git commit -m "feat(#4861): guard hybrid_search caps, edge types, and rank-only fusion"
```

---

### Task 6: Full-module verification and pull request

**Files:** none created or modified beyond the PR description.

- [ ] **Step 1: Run the whole server module test suite**

Run: `mvn -pl server -am install -DskipTests && mvn -pl server test`
Expected: PASS. The `hybrid_search` work touches `VectorSearchTool` and `MCPDispatcher`, both of which many tests reach, so a targeted run is not sufficient evidence here.

- [ ] **Step 2: Run the engine module tests that cover the functions this tool calls**

Run: `mvn -pl engine test -Dtest='*VectorFuse*,*FullText*,SQLTraverse*'`
Expected: PASS. No engine code changed, so any failure is pre-existing; record it rather than fixing it here.

- [ ] **Step 3: Confirm no debug output survived**

Run: `git diff main --stat && git diff main | grep -n 'System.out' || echo "no System.out"`
Expected: `no System.out`.

- [ ] **Step 4: Open the pull request**

```bash
git push -u origin HEAD
gh pr create --title "feat(#4861): add the MCP hybrid_search tool" --body "$(cat <<'EOF'
Closes #4861. Part of epic #4859.

Adds `hybrid_search`, which fuses a vector leg, an optional full-text leg, and an optional
depth-capped graph expansion leg into one ranked list.

Graph expansion is a third **rank-only** fusion source: it is seeded from the union of the
retrieval legs, ranked by breadth-first discovery order, and carries no score of its own. That
makes RRF mandatory whenever `expand` is present, since DBSF and LINEAR require a score on every
row; both remain available for vector + full-text fusion.

Java orchestrates the legs and owns every cap; the engine's `vector.fuse` performs all fusion
scoring, so there is no second RRF in the tree.

Also extracts `MCPVectorLeg` from `VectorSearchTool` so both retrieval tools validate vector
arguments identically. That refactor is behavior-preserving and is covered by the existing
`vectorSearch*` tests.

Three engine behaviors constrain the implementation and were established by probe:

- `vector.fuse` drops any row whose `@rid` is a string, so RIDs are bound as `RID` objects.
- `TRAVERSE ... FROM :ridCollection` throws NPE, so seed RIDs are inlined as literals. Filed
  separately; this PR works around it and does not depend on the fix.
- `out(:boundEdgeList)` silently matches nothing, so edge types are inlined as validated quoted
  literals and an unknown name is rejected rather than producing an empty neighborhood.

`MCPPermissionsTest` previously asserted `isToolAllowed(ALL, "hybrid_search")` is false - it was
the test's example of a name declared in a profile but never registered. That assertion is
replaced with positive `ALL`/`RAG` assertions and a false `ADMIN` assertion; the adjacent
`"unknown"` case still covers unregistered names.

Docs are a companion PR against ArcadeData/arcadedb-docs.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

### Task 7: File the engine planner defect

Not code. The spec records this as follow-up work that must not be lost.

- [ ] **Step 1: Confirm the defect still reproduces on current main**

Run: `git log --oneline -1 engine/src/main/java/com/arcadedb/query/sql/executor/TraverseExecutionPlanner.java`
Then read `TraverseExecutionPlanner.java:150` and `:155-174`, and `SelectExecutionPlanner.java:1420-1453`. Confirm the singleton branch calls `rid.setLegacy(true)` and the `Iterable` branch does not. If someone has fixed it in the meantime, skip to Step 3 and note that instead.

- [ ] **Step 2: File the issue**

```bash
gh issue create --title "TRAVERSE and SELECT from a bound RID collection throw NullPointerException" --body "$(cat <<'EOF'
## Problem

Passing a collection of RIDs as a query parameter target throws `NullPointerException`. Both of
these fail:

```sql
SELECT @rid AS rid FROM :seeds
SELECT @rid AS rid FROM (TRAVERSE out('LINK') FROM :seeds MAXDEPTH 2)
```

with `params = {"seeds": List<RID>}`:

```
java.lang.NullPointerException: Cannot invoke
"com.arcadedb.query.sql.parser.Expression.execute(...)" because "this.expression" is null
  at com.arcadedb.query.sql.parser.Rid.toRecordId(Rid.java:71)
  at com.arcadedb.query.sql.executor.TraverseExecutionPlanner.handleRidsAsTarget(TraverseExecutionPlanner.java:220)
  at com.arcadedb.query.sql.executor.TraverseExecutionPlanner.handleInputParamAsTarget(TraverseExecutionPlanner.java:174)
```

## Cause

Both planners build `Rid` AST nodes for each element of the collection and set `bucket` and
`position`, but never call `rid.setLegacy(true)`:

- `engine/src/main/java/com/arcadedb/query/sql/executor/TraverseExecutionPlanner.java:155-174`
- `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java:1435-1453`

Their own **singleton**-RID branches do call it - `TraverseExecutionPlanner.java:150` and
`SelectExecutionPlanner.java:1425` - which is why a single bound RID works and a collection does
not. `Rid.toRecordId` (`Rid.java:67-71`) reads `bucket`/`position` when `legacy` is true and
otherwise dereferences the null `expression` field.

## Fix

Add `rid.setLegacy(true)` to both collection branches, matching the singleton branches.

## Regression test

One test per planner, asserting that a bound `List<RID>` target returns the expected records:

```java
final Map<String, Object> params = Map.of("seeds", List.of(v0.getIdentity(), v1.getIdentity()));
database.query("sql", "SELECT FROM :seeds", params);
database.query("sql", "SELECT FROM (TRAVERSE out('LINK') FROM :seeds MAXDEPTH 1)", params);
```

Note that a single-element bound RID passes today, so the test must bind a **collection**.

## Context

Found while designing #4861 (MCP `hybrid_search`), whose graph expansion leg needs to traverse from
a computed seed set. That tool works around it by inlining RID literals and does not depend on this
fix.
EOF
)" --label bug`
```

- [ ] **Step 3: Record the issue number in the spec**

In `docs/superpowers/specs/2026-07-28-mcp-hybrid-search-design.md`, in the "Follow-up work filed separately" section, replace `Filed as its own bug` with `Filed as #NNNN` using the number `gh` returned.

```bash
git add docs/superpowers/specs/2026-07-28-mcp-hybrid-search-design.md
git commit -m "docs(#4861): record the filed engine planner defect in the spec"
```

---

## Self-Review

**Spec coverage.**

| Spec section | Task |
|---|---|
| Extract shared vector leg, full parity | 1 |
| Tool definition, registration in both transports, permission gating | 2 |
| Vector-only path | 2 |
| Full-text leg, `fulltextIndexName` only | 3 |
| Two-way fusion via `vector.fuse`, `weights` by leg name, `expand` default 0.5 | 3 |
| Expansion as third rank-only source, BFS rank, seeds excluded via depth filter | 4 |
| `depth`, `path`, `sources` in output | 3 (sources), 4 (depth/path) |
| Dedup of nodes reachable several ways | 4 |
| RRF forced when expanding | 5 |
| Caps: `LEG_OVERFETCH`, `MAX_SEEDS`, `MAX_EXPANSION`, `MAX_DEPTH`, seed-inclusive TRAVERSE limit | 4 (limit), 5 (unit test) |
| Edge-type validation, vertex precondition, incomplete full-text leg | 3 (incomplete leg), 4 (edge types, vertex), 5 (tests) |
| `analyze()` + `isIdempotent()` on both generated statements | 2, 3, 4 |
| Engine NPE filed separately | 7 |
| Docs companion PR | 6 (PR body notes it) |

**Two deliberate deviations from the spec**, both recorded in the plan where they occur:

- The vector-only response adds a `fused: false` flag and returns the leg's native `distance`/`score` rather than a fabricated `fusedScore`. Task 2 Step 7 updates the spec to match.
- `legs.fulltext` carries `indexName` and `similarity` in addition to `count`, since both were already computed and the top-level `fulltextIndexName` is read from it.

**One spec item deliberately not given its own task:** the arcadedb-docs companion PR. It lives in a different repository, so it cannot be a step in this plan's commit sequence; Task 6's PR body records it as outstanding.

**A property of the design that the tests must respect.** Because seeds are excluded from the expansion leg, an expansion test only observes anything when the retrieval legs do *not* already return the whole fixture. The six-record fixture is smaller than `legLimit(10)`, so every expansion test narrows the vector leg with `filter` before asserting on expanded rows. Without that, `legs.expand.count` would be zero and the tests would pass vacuously or fail for the wrong reason. Do not remove those filters.

**Type consistency.** `LegRow(RID, Double)` is introduced in Task 2 and used unchanged in 3 and 4. `Leg(String, List<LegRow>, float)` and `ExpansionInfo(int, List<RID>)` are declared in Task 3 and consumed in Task 4. `fuse(...)` takes `Map<RID, ExpansionInfo>` from its first appearance in Task 3, called with `Map.of()` there and with real data in Task 4, so its signature never changes. `MCPVectorLeg.toRID` is used by all three legs and by the fusion reader. `legLimit(int)` is public from Task 2 because `HybridSearchCapsTest` calls it in Task 5.
