# MCP `full_text_search` Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `full_text_search` MCP tool that lets an agent run BM25 full-text retrieval against ArcadeDB without hand-writing the `SEARCH_INDEX('Type[prop]', ...)` / `$score` SQL syntax.

**Architecture:** Two layers. A new stateless engine helper `com.arcadedb.index.fulltext.FullTextSearch` holds search logic extracted from the currently-private `SQLFunctionSearchIndex.performSearch`, preserving its exception types so the existing full-text test suites act as an unedited regression net. A new server-side `FullTextSearchTool` (shaped like `QueryTool`) pre-validates the index, calls the helper, and shapes `{indexName, similarity, count, results:[{rid, score, properties}]}`.

**Tech Stack:** Java 21, Maven, JUnit 5 + AssertJ. Engine module (`engine/`) and server module (`server/`). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-13-mcp-full-text-search-4862-design.md`
**Issue:** [#4862](https://github.com/ArcadeData/arcadedb/issues/4862), epic [#4859](https://github.com/ArcadeData/arcadedb/issues/4859) Wave 1

## Global Constraints

- Java 21+. Use `final` on variables and parameters. Import classes; do not use fully-qualified names inline.
- Single-statement `if` bodies take no curly braces (match surrounding code).
- JSON: use `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, and prefer the two-argument getters (`getString(key, default)`) over presence checks.
- Tests: JUnit 5 + AssertJ, in the style `assertThat(x.isFoo()).isTrue();`.
- Every source file carries the existing Apache-2.0 license header (copy verbatim from a neighboring file in the same package).
- Do not add Claude as an author. Do not reference issue numbers in Javadoc or comments; comments state behavioral invariants only.
- **Commit per task** on the feature branch `feat/4862-mcp-full-text-search` (worktree at `.worktrees/feat/4862-mcp-full-text-search`). Never commit to `main`, and never merge without the operator's review. CLAUDE.md's "do not commit" governs `main` and merges, not per-task commits on an isolated branch.
- Terminology in all prose: "BM25 full-text index", never "Lucene index". Lucene is used only for tokenization/analysis and query parsing.
- The full-text index name is auto-derived as `typeName + Arrays.toString(propertyNames).replace(" ", "")`, i.e. `Article[content]`, `Article[title,body]`.
- **Merge-conflict hotspot:** `MCPHttpHandler.java` and `MCPStdioServer.java` (`TOOLS_LIST` static blocks + dispatch switches) and the tool-count assertion in `MCPServerPluginTest` are touched by every Wave-1 issue (#4860, #4863, #4864, #4865). Keep those diffs minimal.

---

## File Structure

| File | Responsibility |
|---|---|
| `engine/src/main/java/com/arcadedb/index/fulltext/FullTextSearch.java` (create) | Stateless helper: resolve a full-text `TypeIndex`, run the search, report similarity, enumerate full-text indexes. |
| `engine/src/main/java/com/arcadedb/function/sql/text/SQLFunctionSearchIndex.java` (modify) | `performSearch` reduced to a delegate. Behavior unchanged. |
| `engine/src/test/java/com/arcadedb/index/fulltext/FullTextSearchTest.java` (create) | Unit tests for the helper. |
| `server/src/main/java/com/arcadedb/server/mcp/tools/FullTextSearchTool.java` (create) | MCP tool: definition, index addressing, permission gate, output shaping. |
| `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java` (modify) | Register tool in HTTP transport. |
| `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java` (modify) | Register tool in stdio transport. |
| `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java` (modify) | End-to-end HTTP coverage + tool count 10 -> 11. |
| `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java` (modify) | Tool present in stdio `tools/list`. |

---

## Task 1: Extract the `FullTextSearch` engine helper

Pure refactor plus new public surface. The SQL path must come out byte-identical, which the existing suites prove.

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/fulltext/FullTextSearch.java`
- Create: `engine/src/test/java/com/arcadedb/index/fulltext/FullTextSearchTest.java`
- Modify: `engine/src/main/java/com/arcadedb/function/sql/text/SQLFunctionSearchIndex.java:259-293` (the private `performSearch`)

**Interfaces:**
- Consumes: `Schema.getIndexByName(String)` (throws `SchemaException` when missing), `TypeIndex.getIndexesOnBuckets()`, `TypeIndex.getType()`, `LSMTreeFullTextIndex.isBM25()`, `FullTextQueryExecutor.search(String, int)`, `IndexCursor.getFloatScore()`.
- Produces, relied on by Task 2:
  - `static TypeIndex FullTextSearch.resolveFullTextIndex(Database db, String indexName)`
  - `static Map<RID, Float> FullTextSearch.search(Database db, String indexName, String queryText)`
  - `static String FullTextSearch.getSimilarity(TypeIndex index)` returning `"BM25"` or `"CLASSIC"`
  - `static List<String> FullTextSearch.listFullTextIndexes(Database db)` returning sorted index names

- [ ] **Step 1: Write the failing test**

Create `engine/src/test/java/com/arcadedb/index/fulltext/FullTextSearchTest.java` (with the standard Apache-2.0 header copied from `FullTextScoreTest.java`):

```java
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FullTextSearchTest extends TestHelper {

  private void createArticles() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting'");
    });
  }

  @Test
  void searchReturnsScoredRids() {
    createArticles();

    database.transaction(() -> {
      final Map<RID, Float> hits = FullTextSearch.search(database, "Article[content]", "java");

      assertThat(hits).hasSize(1);
      assertThat(hits.values().iterator().next()).isGreaterThan(0f);
    });
  }

  @Test
  void resolveReturnsTypeIndex() {
    createArticles();

    database.transaction(() -> {
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Article[content]");

      assertThat(index.getName()).isEqualTo("Article[content]");
      assertThat(index.getTypeName()).isEqualTo("Article");
    });
  }

  @Test
  void similarityDefaultsToBM25() {
    createArticles();

    database.transaction(() -> {
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Article[content]");

      assertThat(FullTextSearch.getSimilarity(index)).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
    });
  }

  @Test
  void similarityReportsClassicWhenPinned() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Legacy");
      database.command("sql", "CREATE PROPERTY Legacy.txt STRING");
      database.command("sql", "CREATE INDEX ON Legacy (txt) FULL_TEXT METADATA {\"similarity\": \"CLASSIC\"}");
      database.command("sql", "INSERT INTO Legacy SET txt = 'hello world'");
    });

    database.transaction(() -> {
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Legacy[txt]");

      assertThat(FullTextSearch.getSimilarity(index)).isEqualTo(FullTextIndexMetadata.SIMILARITY_CLASSIC);
    });
  }

  @Test
  void listsOnlyFullTextIndexes() {
    createArticles();

    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Article (title) UNIQUE");

      final List<String> indexes = FullTextSearch.listFullTextIndexes(database);

      assertThat(indexes).containsExactly("Article[content]");
    });
  }

  @Test
  void missingIndexThrowsSchemaException() {
    createArticles();

    database.transaction(() -> assertThatThrownBy(() -> FullTextSearch.resolveFullTextIndex(database, "Article[nope]"))
        .isInstanceOf(SchemaException.class));
  }

  @Test
  void nonFullTextIndexThrowsCommandExecutionException() {
    createArticles();

    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Article (title) UNIQUE");

      assertThatThrownBy(() -> FullTextSearch.resolveFullTextIndex(database, "Article[title]"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("is not a full-text index");
    });
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl engine test -Dtest=FullTextSearchTest`
Expected: FAIL to compile, `cannot find symbol: class FullTextSearch`.

- [ ] **Step 3: Create the helper**

Create `engine/src/main/java/com/arcadedb/index/fulltext/FullTextSearch.java`, license header first:

```java
package com.arcadedb.index.fulltext;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared entry point for BM25 full-text search over a type's full-text index. Used by the SEARCH_INDEX SQL function and by
 * callers that need scored results without going through SQL.
 */
public class FullTextSearch {

  private FullTextSearch() {
  }

  /**
   * Resolves a full-text index by name.
   *
   * @throws com.arcadedb.exception.SchemaException if no index carries that name
   * @throws CommandExecutionException              if the index exists but is not a full-text type index
   */
  public static TypeIndex resolveFullTextIndex(final Database database, final String indexName) {
    final Index index = database.getSchema().getIndexByName(indexName);

    if (!(index instanceof final TypeIndex typeIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

    final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

    return typeIndex;
  }

  /**
   * Searches every bucket index behind the named full-text index and returns the matching RIDs with their scores.
   */
  public static Map<RID, Float> search(final Database database, final String indexName, final String queryText) {
    final TypeIndex typeIndex = resolveFullTextIndex(database, indexName);

    final Map<RID, Float> allResults = new HashMap<>();

    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex) {
        final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);
        final IndexCursor cursor = executor.search(queryText, -1);

        while (cursor.hasNext()) {
          final Identifiable match = cursor.next();
          final float score = cursor.getFloatScore();
          // Float::sum across buckets: a given RID lives in exactly one bucket, so a RID is produced by at most one bucket index
          // and the merge is effectively an insert (no real summing). For CLASSIC the additive semantics would also be correct;
          // for BM25 the per-bucket scoping relies on this one-bucket-per-RID invariant - if it ever broke, scores would be
          // double-counted here rather than failing loudly.
          allResults.merge(match.getIdentity(), score, Float::sum);
        }
      }
    }

    return allResults;
  }

  /**
   * Returns the similarity the index scores with: {@link FullTextIndexMetadata#SIMILARITY_BM25} or
   * {@link FullTextIndexMetadata#SIMILARITY_CLASSIC}. Indexes persisted before BM25 support load as CLASSIC.
   */
  public static String getSimilarity(final TypeIndex typeIndex) {
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex)
        return ftIndex.isBM25() ? FullTextIndexMetadata.SIMILARITY_BM25 : FullTextIndexMetadata.SIMILARITY_CLASSIC;

    return FullTextIndexMetadata.SIMILARITY_CLASSIC;
  }

  /**
   * Returns the sorted names of every full-text index in the database.
   */
  public static List<String> listFullTextIndexes(final Database database) {
    final List<String> names = new ArrayList<>();

    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof final TypeIndex typeIndex && typeIndex.getType() == Schema.INDEX_TYPE.FULL_TEXT)
        names.add(typeIndex.getName());

    Collections.sort(names);
    return names;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl engine test -Dtest=FullTextSearchTest`
Expected: PASS, 7 tests.

- [ ] **Step 5: Reduce `SQLFunctionSearchIndex.performSearch` to a delegate**

In `engine/src/main/java/com/arcadedb/function/sql/text/SQLFunctionSearchIndex.java`, replace the whole private `performSearch` method body (currently lines 259-293) with:

```java
  private Map<RID, Float> performSearch(final String indexName, final String queryString, final Database database) {
    return FullTextSearch.search(database, indexName, queryString);
  }
```

Then add the import `com.arcadedb.index.fulltext.FullTextSearch` and remove the imports that are now unused by this file: `com.arcadedb.database.Identifiable`, `com.arcadedb.index.Index`, `com.arcadedb.index.IndexCursor`, `com.arcadedb.index.TypeIndex`, `com.arcadedb.index.fulltext.FullTextQueryExecutor`, `com.arcadedb.index.fulltext.LSMTreeFullTextIndex`, `java.util.HashMap`.

**Verify before deleting each import:** the file also contains `searchFromTarget`, `estimate`, `canExecuteInline`, and `getScoringExplain`, which may still reference some of these types. Run `grep -n "Identifiable\|IndexCursor\|TypeIndex\|LSMTreeFullTextIndex\|FullTextQueryExecutor\|HashMap\|\bIndex\b" engine/src/main/java/com/arcadedb/function/sql/text/SQLFunctionSearchIndex.java` and keep every import that still has a usage. Removing a still-used import breaks the build; leaving an unused one is harmless but untidy.

- [ ] **Step 6: Prove the SQL path is unchanged**

The existing suites are the regression net and **must pass without being edited**. If any of them needs a change, the refactor altered behavior: stop and fix the helper instead.

Run:
```bash
mvn -q -pl engine test -Dtest='SQLFunctionSearchIndex*Test,FullText*Test,LSMTreeFullTextIndex*Test,TypeFullTextIndexBuilderTest,FullTextSearchTest'
```
Expected: PASS, all suites green, zero files edited under `engine/src/test`.

- [ ] **Step 7: Commit (operator runs this after review)**

```bash
git add engine/src/main/java/com/arcadedb/index/fulltext/FullTextSearch.java \
        engine/src/main/java/com/arcadedb/function/sql/text/SQLFunctionSearchIndex.java \
        engine/src/test/java/com/arcadedb/index/fulltext/FullTextSearchTest.java
git commit -m "refactor(engine): extract shared FullTextSearch helper from SEARCH_INDEX"
```

---

## Task 2: `FullTextSearchTool` with `indexName` addressing, registered in both transports

Delivers a working tool end-to-end for the simple addressing form. Task 3 adds `typeName` addressing on top.

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/mcp/tools/FullTextSearchTool.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java` (static `TOOLS_LIST` ~line 52, `handleToolsCall` switch ~line 158, `formatResult` switch ~line 219)
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java` (static `TOOLS_LIST` ~line 55, `tools/call` dispatch switch)
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`

**Interfaces:**
- Consumes: `FullTextSearch.{search, resolveFullTextIndex, getSimilarity, listFullTextIndexes}` from Task 1; `MCPToolUtils.resolveDatabase(server, user, name)`; `MCPConfiguration.isAllowReads()`; `JsonSerializer.createJsonSerializer().serializeDocument(Document)`; `BasicDatabase.lookupByRID(RID, boolean)`.
- Produces, relied on by Task 3: `FullTextSearchTool.getDefinition()`, `FullTextSearchTool.execute(ArcadeDBServer, ServerSecurityUser, JSONObject, MCPConfiguration)`, and the private `resolveIndexName(Database, JSONObject)` that Task 3 extends.

- [ ] **Step 1: Write the failing tests**

In `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`:

First, seed a full-text index. Add this method and call it from the existing `@BeforeEach enableMCP()` (append `seedFullTextIndex();` as its last line):

```java
  private void seedFullTextIndex() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("Article"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE Article");
      db.command("sql", "CREATE PROPERTY Article.title STRING");
      db.command("sql", "CREATE PROPERTY Article.content STRING");
      db.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");
      db.command("sql", "CREATE INDEX ON Article (title) UNIQUE");

      db.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      db.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting language'");
    });
  }
```

Add the import `com.arcadedb.database.Database` to that test file.

Then update the existing tool-count assertion in `toolsList()` from `isEqualTo(10)` to:

```java
    assertThat(tools.length()).isEqualTo(11);
```

And add these tests:

```java
  @Test
  void fullTextSearchByIndexName() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
    assertThat(payload.getString("similarity")).isEqualTo("BM25");
    assertThat(payload.getInt("count")).isEqualTo(1);

    final JSONObject hit = payload.getJSONArray("results").getJSONObject(0);
    assertThat(hit.getString("rid")).startsWith("#");
    assertThat(hit.getFloat("score")).isGreaterThan(0f);
    assertThat(hit.getJSONObject("properties").getString("title")).isEqualTo("Doc1");
  }

  @Test
  void fullTextSearchRanksAndLimits() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "language")
        .put("limit", 1));

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    // Both articles contain "language"; limit must cut the result set to the single best-scoring hit.
    assertThat(payload.getInt("count")).isEqualTo(1);
  }

  @Test
  void fullTextSearchDeniedWhenReadsDisabled() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("not allowed");
  }
```

In `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`, find the existing `tools/list` test (it asserts the tool count and/or names) and add an assertion that the new tool is present. If that test collects names into a list, assert `contains("full_text_search")`; if it asserts a count, bump it by one. Read the file first and match its existing style exactly.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -pl server test -Dtest=MCPServerPluginTest`
Expected: FAIL. `toolsList` fails with `expected 11 but was 10`, and the `full_text_search` tests fail with an error payload containing `Unknown tool: full_text_search`.

- [ ] **Step 3: Create `FullTextSearchTool`**

Create `server/src/main/java/com/arcadedb/server/mcp/tools/FullTextSearchTool.java`, license header first:

```java
package com.arcadedb.server.mcp.tools;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FullTextSearchTool {

  private static final int DEFAULT_LIMIT = 10;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "full_text_search")
        .put("description",
            """
            Search a BM25 full-text index and return the matching records ranked by relevance score. \
            Address the index either by 'indexName' (e.g. 'Article[content]') or by 'typeName' plus optional 'properties'. \
            If both are given, 'indexName' wins. Query syntax: '+a +b' requires both terms, 'a -b' excludes b, 'a b' matches \
            either, '"exact phrase"' requires all terms in the same record (term order is NOT enforced), 'pre*' matches a \
            prefix, 'term~' is a fuzzy match, 'field:term' restricts to one property of a multi-property index, and 'term^2' \
            boosts a term. The returned 'similarity' says whether scores are BM25 or legacy CLASSIC coordination counts.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to search"))
                .put("indexName", new JSONObject()
                    .put("type", "string")
                    .put("description", "Name of the full-text index, e.g. 'Article[content]' or 'Article[title,body]'"))
                .put("typeName", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Type carrying the full-text index, e.g. 'Article'. An index declared on a supertype is named for the supertype"))
                .put("properties", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "string"))
                    .put("description", "Indexed properties, used with 'typeName' to identify the index, e.g. ['content']"))
                .put("queryText", new JSONObject()
                    .put("type", "string")
                    .put("description", "The full-text query"))
                .put("limit", new JSONObject()
                    .put("type", "integer")
                    .put("description", "Maximum number of results to return (default: 10)")))
            .put("required", new JSONArray().put("database").put("queryText")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = args.getString("database");
    final String queryText = args.getString("queryText");
    final int limit = args.getInt("limit", DEFAULT_LIMIT);

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    final String indexName = resolveIndexName(database, args);
    final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(database, indexName);

    final Map<RID, Float> hits = FullTextSearch.search(database, indexName, queryText);

    final List<Map.Entry<RID, Float>> ranked = new ArrayList<>(hits.entrySet());
    ranked.sort(Map.Entry.<RID, Float>comparingByValue().reversed());

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray results = new JSONArray();
    for (final Map.Entry<RID, Float> hit : ranked) {
      if (results.length() >= limit)
        break;

      // lookupByRID returns Record, whose interface has no asDocument(); pattern-match instead. This also skips any
      // non-document record rather than failing the whole search.
      final Record record = database.lookupByRID(hit.getKey(), true);
      if (!(record instanceof final Document document))
        continue;

      results.put(new JSONObject()
          .put("rid", hit.getKey().toString())
          .put("score", hit.getValue())
          .put("properties", serializer.serializeDocument(document)));
    }

    return new JSONObject()
        .put("indexName", indexName)
        .put("similarity", FullTextSearch.getSimilarity(typeIndex))
        .put("count", results.length())
        .put("results", results);
  }

  /**
   * Resolves the target index from the 'indexName' argument. Task 3 extends this with 'typeName' addressing.
   */
  private static String resolveIndexName(final Database database, final JSONObject args) {
    final String indexName = args.getString("indexName", null);

    if (indexName == null || indexName.isBlank())
      throw new IllegalArgumentException("Provide 'indexName'. " + describeAvailable(database));

    if (!database.getSchema().existsIndex(indexName))
      throw new IllegalArgumentException("Full-text index '" + indexName + "' does not exist. " + describeAvailable(database));

    if (!FullTextSearch.listFullTextIndexes(database).contains(indexName))
      throw new IllegalArgumentException("Index '" + indexName + "' is not a full-text index. " + describeAvailable(database));

    return indexName;
  }

  /**
   * Builds the recovery hint appended to every addressing error, so the caller can self-correct without a further round-trip.
   */
  private static String describeAvailable(final Database database) {
    final List<String> indexes = FullTextSearch.listFullTextIndexes(database);

    if (indexes.isEmpty())
      return "Database '" + database.getName() + "' has no full-text indexes. Create one with: "
          + "CREATE INDEX ON <Type> (<property>) FULL_TEXT";

    return "Available full-text indexes in '" + database.getName() + "': " + indexes
        + ". An index declared on a supertype is named for the supertype.";
  }
}
```

- [ ] **Step 4: Register in the HTTP transport**

In `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java`:

Add the import `com.arcadedb.server.mcp.tools.FullTextSearchTool`. Append to the static `TOOLS_LIST` block, after `TOOLS_LIST.put(ExecuteCommandTool.getDefinition());`:

```java
    TOOLS_LIST.put(FullTextSearchTool.getDefinition());
```

Add to the `handleToolsCall` switch, after the `execute_command` arm:

```java
        case "full_text_search" -> FullTextSearchTool.execute(server, user, args, config);
```

Add to the `formatResult` switch, after the `query`/`execute_command` arm:

```java
      case "full_text_search" -> result.getInt("count", 0) + " hit(s)";
```

- [ ] **Step 5: Register in the stdio transport**

In `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java`, add the same import, append the same `TOOLS_LIST.put(FullTextSearchTool.getDefinition());` line to the static block after the `ExecuteCommandTool` entry, and add the same `case "full_text_search" -> FullTextSearchTool.execute(server, user, args, config);` arm to its `tools/call` dispatch switch. Read the file's switch first (it mirrors `MCPHttpHandler`'s) and match its formatting.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `mvn -q -pl server test -Dtest='MCPServerPluginTest,MCPStdioServerTest,MCPPermissionsTest,MCPConfigurationTest'`
Expected: PASS, all four suites green.

If `fullTextSearchByIndexName` fails on `payload.getInt("count")` being 2 rather than 1, the query matched both articles: check the seeded content strings, not the tool.

- [ ] **Step 7: Commit (operator runs this after review)**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/FullTextSearchTool.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java
git commit -m "feat(mcp): add full_text_search tool"
```

---

## Task 3: `typeName` addressing and enumerating errors

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/FullTextSearchTool.java` (the private `resolveIndexName`)
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`

**Interfaces:**
- Consumes: `FullTextSearchTool.resolveIndexName(Database, JSONObject)` and `describeAvailable(Database)` from Task 2; `FullTextSearch.listFullTextIndexes(Database)` from Task 1.
- Produces: no new public surface. Behavior only.

- [ ] **Step 1: Write the failing tests**

Add to `MCPServerPluginTest`. These rely on a second full-text index and a supertype, so first extend `seedFullTextIndex()` from Task 2 by appending these statements inside its existing `db.transaction(() -> { ... })` block:

```java
      db.command("sql", "CREATE INDEX ON Article (title, content) FULL_TEXT");

      db.command("sql", "CREATE DOCUMENT TYPE Searchable");
      db.command("sql", "CREATE PROPERTY Searchable.text STRING");
      db.command("sql", "CREATE INDEX ON Searchable (text) FULL_TEXT");
      db.command("sql", "CREATE DOCUMENT TYPE Decision EXTENDS Searchable");
      db.command("sql", "INSERT INTO Decision SET text = 'approved the java migration'");
```

Note this gives `Article` **two** full-text indexes (`Article[content]` and `Article[title,content]`), which is what the ambiguity test needs. It does not affect Task 2's tests, which address `Article[content]` explicitly.

Now the tests:

```java
  @Test
  void fullTextSearchByTypeNameAndProperties() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Article")
        .put("properties", new JSONArray().put("content"))
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
    assertThat(payload.getInt("count")).isEqualTo(1);
  }

  @Test
  void fullTextSearchByTypeNameAloneWhenUnambiguous() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Searchable")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    // The index lives on the supertype; the hit is a Decision, a subtype record.
    assertThat(payload.getString("indexName")).isEqualTo("Searchable[text]");
    assertThat(payload.getInt("count")).isEqualTo(1);
    assertThat(payload.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("@type")).isEqualTo("Decision");
  }

  @Test
  void fullTextSearchAmbiguousTypeNameListsCandidates() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Article")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Article[content]");
    assertThat(errorText).contains("Article[title,content]");
  }

  @Test
  void fullTextSearchOnSubtypeNameGuidesToSupertypeIndex() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Decision")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Decision");
    assertThat(errorText).contains("Searchable[text]");
    assertThat(errorText).containsIgnoringCase("supertype");
  }

  @Test
  void fullTextSearchUnknownIndexListsAvailable() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Artcle[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Artcle[content]");
    assertThat(errorText).containsIgnoringCase("available full-text indexes");
    assertThat(errorText).contains("Article[content]");
  }

  @Test
  void fullTextSearchOnNonFullTextIndexIsRejected() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[title]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("is not a full-text index");
  }

  @Test
  void fullTextSearchWithoutAddressingIsRejected() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("indexName");
    assertThat(errorText).contains("typeName");
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -pl server test -Dtest=MCPServerPluginTest`
Expected: FAIL. The `typeName` tests error with `Provide 'indexName'.` because Task 2's `resolveIndexName` only understands `indexName`.

- [ ] **Step 3: Extend `resolveIndexName`**

Replace the `resolveIndexName` method in `FullTextSearchTool` with:

```java
  private static String resolveIndexName(final Database database, final JSONObject args) {
    final String indexName = args.getString("indexName", null);

    // 'indexName' wins when both addressing forms are supplied.
    if (indexName != null && !indexName.isBlank())
      return validateFullTextIndex(database, indexName);

    final String typeName = args.getString("typeName", null);
    if (typeName == null || typeName.isBlank())
      throw new IllegalArgumentException(
          "Provide either 'indexName', or 'typeName' with optional 'properties'. " + describeAvailable(database));

    final JSONArray properties = args.getJSONArray("properties", null);
    if (properties != null && properties.length() > 0) {
      final StringBuilder derived = new StringBuilder(typeName).append('[');
      for (int i = 0; i < properties.length(); i++) {
        if (i > 0)
          derived.append(',');
        derived.append(properties.getString(i));
      }
      return validateFullTextIndex(database, derived.append(']').toString());
    }

    // 'typeName' alone: usable only when the type carries exactly one full-text index.
    final String prefix = typeName + "[";
    final List<String> candidates = new ArrayList<>();
    for (final String name : FullTextSearch.listFullTextIndexes(database))
      if (name.startsWith(prefix))
        candidates.add(name);

    if (candidates.isEmpty())
      throw new IllegalArgumentException(
          "No full-text index found on type '" + typeName + "'. " + describeAvailable(database));

    if (candidates.size() > 1)
      throw new IllegalArgumentException("Type '" + typeName + "' has several full-text indexes: " + candidates
          + ". Pass 'indexName', or narrow with 'properties'.");

    return candidates.get(0);
  }

  private static String validateFullTextIndex(final Database database, final String indexName) {
    if (!database.getSchema().existsIndex(indexName))
      throw new IllegalArgumentException("Full-text index '" + indexName + "' does not exist. " + describeAvailable(database));

    if (!FullTextSearch.listFullTextIndexes(database).contains(indexName))
      throw new IllegalArgumentException("Index '" + indexName + "' is not a full-text index. " + describeAvailable(database));

    return indexName;
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -pl server test -Dtest=MCPServerPluginTest`
Expected: PASS, all tests including the seven added here and the three from Task 2.

- [ ] **Step 5: Run the full affected surface**

```bash
mvn -q -pl engine test -Dtest='SQLFunctionSearchIndex*Test,FullText*Test,TypeFullTextIndexBuilderTest'
mvn -q -pl server test -Dtest='MCP*Test'
```
Expected: PASS, both green. These are the suites the change can plausibly break.

- [ ] **Step 6: Commit (operator runs this after review)**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/FullTextSearchTool.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java
git commit -m "feat(mcp): address full_text_search index by typeName and properties"
```

---

## Task 4: Record the two deviations on issue #4862

The implementation intentionally departs from the issue's written schema in two places. Both are decided and approved, but the issue text still says otherwise, and #4861 (`hybrid_search`) will be written against whatever the issue says.

**Files:** none (GitHub only).

- [ ] **Step 1: Post a comment on issue #4862**

Post, via `gh issue comment 4862` or the GitHub MCP tools:

> Two deviations from the design in this issue, settled during implementation:
>
> **1. `bm25Score` -> `score` plus a top-level `similarity` field.** An ArcadeDB full-text index is not necessarily BM25: indexes persisted before BM25 support load as `CLASSIC` similarity, and `METADATA {"similarity": "CLASSIC"}` pins it explicitly. On those, the score is a term-coordination count. Naming the field `bm25Score` would mislabel them and would silently corrupt score fusion in #4861. The output is now:
>
> ```json
> {"indexName": "Article[content]", "similarity": "BM25", "count": 1,
>  "results": [{"rid": "#3:0", "score": 1.84, "properties": {...}}]}
> ```
>
> **2. Index addressing accepts `typeName` + `properties` as well as `indexName`.** The index name is a bracket literal (`Article[content]`) that a model will not reliably reproduce, which would defeat the ergonomic-guardrail justification for this tool. Addressing errors now enumerate the database's real full-text indexes, mirroring the `MCPToolUtils.resolveDatabase` pattern. The argument is `typeName`, not `type`, to avoid colliding with JSON Schema's own `type` keyword.

- [ ] **Step 2: Open a docs follow-up**

The MCP docs live in the out-of-tree `arcadedb-docs` repository, so they cannot be updated in this PR. Open an issue there (or a checklist item on epic #4859) covering: the `full_text_search` tool reference, the query-syntax table, the BM25-vs-CLASSIC `similarity` field, and the cold-start note (a BM25 index whose corpus counters are invalid runs a full type scan on its first query; remedy is `REBUILD INDEX <name> WITH statsOnly = true`).

Docs must say "BM25 full-text index", never "Lucene index".

---

## Verification Checklist

Before opening the PR:

- [ ] `mvn -q -pl engine test -Dtest='SQLFunctionSearchIndex*Test,FullText*Test,TypeFullTextIndexBuilderTest'` passes.
- [ ] `mvn -q -pl server test -Dtest='MCP*Test'` passes.
- [ ] No file under `engine/src/test/` was edited except the newly created `FullTextSearchTest.java`. An edit anywhere else there means the Task 1 refactor changed SQL behavior.
- [ ] `git diff --stat` on `MCPHttpHandler.java` and `MCPStdioServer.java` shows only the added import, the added `TOOLS_LIST` line, and the added switch arms. These files are contested by four sibling Wave-1 issues.
- [ ] No `System.out` left behind.
