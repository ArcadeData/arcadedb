/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.search;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

public class SearchIndexesTest {

  /**
   * Regression for #11137: the logical name of a multi-label full-text or vector index is resolved by the search services.
   * <p>
   * Issue #11137: the logical name of a Cypher {@code CREATE FULLTEXT INDEX} / {@code CREATE VECTOR INDEX} over several labels is
   * resolved by {@code SEARCH_INDEX_MORE}, by the shared full-text, vector and hybrid search services (HTTP, gRPC and MCP tools) and
   * is reported by {@code schema:indexes}.
   */
  @Nested
  class LogicalMultiTypeIndexName extends TestHelper {

    @BeforeEach
    void createIndexes() {
      sql("CREATE VERTEX TYPE Doc");
      sql("CREATE VERTEX TYPE Article EXTENDS Doc");
      sql("CREATE VERTEX TYPE Post EXTENDS Doc");
      cypher("CREATE FULLTEXT INDEX docs FOR (n:Article|Post) ON EACH [n.title, n.body]");
      cypher("CREATE VECTOR INDEX emb FOR (n:Article|Post) ON n.e OPTIONS {indexConfig: "
          + "{`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}");
      cypher("CREATE (:Article {id: 'a1', title: 'graph databases', body: 'fast traversal of a graph', e: [1.0, 0.0]})");
      cypher("CREATE (:Post {id: 'p1', title: 'graph recipes', body: 'a graph of recipes', e: [0.9, 0.1]})");
      cypher("CREATE (:Post {id: 'p2', title: 'travel', body: 'mountains', e: [0.0, 1.0]})");
    }

    @Test
    void searchIndexMoreByTheLogicalNameFindsSimilarRecordsOfEveryType() {
      // the source is an Article, and the Post member is searched with its terms too
      final List<String> ids = moreLike("Article", "a1");
      assertThat(ids).contains("p1").doesNotContain("p2");
    }

    @Test
    void searchIndexMoreByASourceOfTheOtherTypeFindsTheFirstType() {
      final List<String> ids = moreLike("Post", "p1");
      assertThat(ids).contains("a1").doesNotContain("p2");
    }

    /**
     * The refusal is the function's own error whether the scan of {@code Doc} runs sequentially or in parallel, and in parallel
     * whichever thread scans the bucket the function first runs on: a producer of the pool, or the consumer itself when no producer
     * had taken that bucket yet (#8594). It used to be wrapped in "Parallel scan failed" only when a producer had, so the shape of
     * the error depended on thread scheduling.
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void searchIndexMoreByAVectorLogicalNameIsRefused(final boolean parallelScan) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallelScan);
      assertThatThrownBy(() -> ids("SELECT id FROM Doc WHERE SEARCH_INDEX_MORE('emb', [#1:0]) = true"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not a full-text index");
    }

    @Test
    void fullTextQueryByTheLogicalNameMergesEveryType() {
      final JSONObject response = FullTextQuery.search(database, new JSONObject().put("indexName", "docs").put("queryText", "graph"));
      assertThat(response.getString("indexName")).isEqualTo("docs");
      assertThat(response.getInt("count")).isEqualTo(2);
      assertThat(propertyIds(response.getJSONArray("results"))).containsExactlyInAnyOrder("a1", "p1");

      final JSONObject limited = FullTextQuery.search(database,
          new JSONObject().put("indexName", "docs").put("queryText", "graph").put("limit", 1));
      assertThat(limited.getInt("count")).isEqualTo(1);
    }

    @Test
    void fullTextQueryByAVectorLogicalNameIsRefused() {
      assertThatThrownBy(() -> FullTextQuery.search(database, new JSONObject().put("indexName", "emb").put("queryText", "graph")))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a full-text index");
    }

    @Test
    void fullTextQueryByAnUnknownNameStillFails() {
      assertThatThrownBy(() -> FullTextQuery.search(database, new JSONObject().put("indexName", "nope").put("queryText", "graph")))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not exist");
    }

    @Test
    void vectorSearchByTheLogicalNameMergesTheNearestOfEveryType() {
      final JSONObject response = VectorSearch.search(database,
          new JSONObject().put("indexName", "emb").put("queryVector", new JSONArray(List.of(1.0, 0.0))).put("k", 2));
      assertThat(response.getString("indexName")).isEqualTo("emb");
      assertThat(propertyIds(response.getJSONArray("results"))).containsExactly("a1", "p1");
    }

    @Test
    void vectorSearchBySparseFlagOnALogicalNameIsRefused() {
      assertThatThrownBy(() -> VectorSearch.search(database, new JSONObject().put("indexName", "emb")
          .put("queryVector", new JSONArray(List.of(1.0, 0.0))).put("sparse", true))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void hybridSearchByTheLogicalNamesFusesBothLegs() {
      final JSONObject response = HybridSearch.search(database, new JSONObject().put("vectorIndexName", "emb")
          .put("queryVector", new JSONArray(List.of(1.0, 0.0))).put("fulltextIndexName", "docs").put("fulltextQuery", "recipes")
          .put("k", 3));
      assertThat(response.getString("vectorIndexName")).isEqualTo("emb");
      assertThat(response.getString("fulltextIndexName")).isEqualTo("docs");
      assertThat(propertyIds(response.getJSONArray("results"))).contains("a1", "p1");
    }

    @Test
    void hybridSearchWithAnExpansionAcceptsLogicalIndexesOverVertexTypes() {
      final JSONObject response = HybridSearch.search(database, new JSONObject().put("vectorIndexName", "emb")
          .put("queryVector", new JSONArray(List.of(1.0, 0.0))).put("fulltextIndexName", "docs").put("fulltextQuery", "recipes")
          .put("expand", new JSONObject().put("depth", 1)).put("k", 3));
      assertThat(response.getString("fulltextIndexName")).isEqualTo("docs");
      assertThat(propertyIds(response.getJSONArray("results"))).contains("a1", "p1");
    }

    @Test
    void schemaIndexesNamesTheLogicalIndexOfEveryMember() {
      final List<String> logicalNames = new ArrayList<>();
      final List<String> plain = new ArrayList<>();
      sql("CREATE PROPERTY Doc.key STRING");
      sql("CREATE INDEX ON Doc (key) NOTUNIQUE");
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final String logical = row.getProperty("multiTypeIndex");
          if (logical != null)
            logicalNames.add(row.getProperty("typeName") + "=" + logical);
          else
            plain.add(row.getProperty("name"));
        }
      }
      assertThat(logicalNames).contains("Article=docs", "Post=docs", "Article=emb", "Post=emb");
      assertThat(plain).isNotEmpty().noneMatch(n -> n.startsWith("Article[title") || n.startsWith("Post[title"));
    }

    private List<String> propertyIds(final JSONArray results) {
      final List<String> ids = new ArrayList<>();
      for (int i = 0; i < results.length(); i++) {
        final JSONObject hit = results.getJSONObject(i);
        final JSONObject properties = hit.has("properties") ? hit.getJSONObject("properties") : hit;
        ids.add(properties.getString("id"));
      }
      return ids;
    }

    private List<String> moreLike(final String type, final String id) {
      final RID source;
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + type + " WHERE id = ?", id)) {
        source = rs.next().getProperty("rid");
      }
      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT id FROM Doc WHERE SEARCH_INDEX_MORE('docs', :rids, {minTermFreq: 1, minDocFreq: 1}) = true", Map.of("rids", List.of(source)))) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      return ids;
    }

    private List<String> ids(final String sql) {
      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      return ids;
    }

    private void sql(final String sql) {
      try (final ResultSet rs = database.command("sql", sql)) {
        while (rs.hasNext())
          rs.next();
      }
    }

    private void cypher(final String cypher) {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
      }
    }
  }

  /**
   * Regression for #11197: refusals name only the types the caller may read.
   * <p>
   * Issue #11197: the census over the compiled classes of every module, the engine included, found two refusals that named
   * types the caller may not read - the hybrid search's "Available edge types" list and the algorithm procedures' hint that a
   * name is a property of an edge type - and the second also told a denied type apart from a missing one ("is not a
   * relationship type, it is an edge type"). A type the caller may not read is now answered as one that does not exist.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RefusalsNameOnlyReadableTypes extends TestHelper {

    @BeforeEach
    void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("City");
        database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
        database.getSchema().createEdgeType("SECRET_LINK").createProperty("secretWeight", Type.DOUBLE);
      });
    }

    @AfterEach
    void unbind() {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    }

    @Test
    void theHybridSearchListsOnlyTheReadableEdgeTypes() {
      // control: unbound, every edge type is listed
      assertThat(catchThrowable(() -> HybridSearch.validatedEdgeTypes(database, new JSONArray().put("NOPE"))))
          .hasMessageContaining("SECRET_LINK").hasMessageContaining("ROAD");

      bindUserDenying(Set.of("SECRET_LINK"));

      final Throwable unknown = catchThrowable(() -> HybridSearch.validatedEdgeTypes(database, new JSONArray().put("NOPE")));
      assertThat(unknown).hasMessageContaining("ROAD").hasMessageNotContaining("SECRET_LINK");

      // the denied type is answered exactly as a missing one
      assertThatThrownBy(() -> HybridSearch.validatedEdgeTypes(database, new JSONArray().put("SECRET_LINK")))
          .hasMessageContaining("Edge type 'SECRET_LINK' does not exist").hasMessageNotContaining("SECRET_LINK]");
      assertThat(HybridSearch.validatedEdgeTypes(database, new JSONArray().put("ROAD"))).containsExactly("ROAD");
    }

    @Test
    void theAlgorithmRefusalNamesNoEdgeTypeTheCallerMayNotRead() {
      // control: unbound, the hint points at the edge type declaring the property
      assertThatThrownBy(() -> rows("CALL algo.mst('secretWeight') YIELD source RETURN source"))
          .hasStackTraceContaining("'secretWeight' is a property of the edge type 'SECRET_LINK'");

      bindUserDenying(Set.of("SECRET_LINK"));

      final Throwable hint = catchThrowable(() -> rows("CALL algo.mst('secretWeight') YIELD source RETURN source"));
      assertThat(hint).hasStackTraceContaining("relationship type 'secretWeight' does not exist");
      assertThat(stackTraceOf(hint)).doesNotContain("SECRET_LINK");

      // the denied type itself is answered as one that does not exist
      assertThatThrownBy(() -> rows("CALL algo.mst('SECRET_LINK') YIELD source RETURN source"))
          .hasStackTraceContaining("relationship type 'SECRET_LINK' does not exist");

      // a readable type still passes, and a readable property still gets its hint
      rows("CALL algo.mst('ROAD') YIELD source RETURN source");
      assertThatThrownBy(() -> rows("CALL algo.mst('w') YIELD source RETURN source"))
          .hasStackTraceContaining("'w' is a property of the edge type 'ROAD'");
    }

    private int rows(final String cypher) {
      int n = 0;
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext()) {
          rs.next();
          ++n;
        }
      }
      return n;
    }

    private static String stackTraceOf(final Throwable e) {
      final StringBuilder out = new StringBuilder();
      for (Throwable t = e; t != null; t = t.getCause())
        out.append(t).append('\n');
      return out.toString();
    }

    private void bindUserDenying(final Set<String> deniedTypes) {
      final Set<Integer> deniedBucketIds = new HashSet<>();
      for (final String typeName : deniedTypes)
        deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "scoped";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return !deniedBucketIds.contains(fileId);
        }

        @Override
        public boolean requestAccessOnType(final String typeName, final ACCESS access) {
          return !deniedTypes.contains(typeName);
        }

        @Override
        public long getResultSetLimit() {
          return -1;
        }

        @Override
        public long getReadTimeout() {
          return -1;
        }
      });
    }
  }

  /**
   * Regression for #7312: schema:types describes the dimensions and scoring direction of vector indexes.
   * <p>
   * Issue #7312: Studio's vector search panel lists the searchable indexes from {@code schema:types}, and it can only
   * catch a query vector of the wrong length before the round trip if that listing carries the dimension count. The
   * scoring direction is listed alongside, so the panel can say "lower is better" before the first search is run.
   * <p>
   * Both values must be the ones the search itself reports, not a second derivation of them: each test compares the
   * listing with the {@code scoring} field of a real search response over the same index.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7312">issue #7312</a>
   */
  @Nested
  class SchemaTypesDescribesVectorIndexes extends TestHelper {

    @Test
    void aDenseIndexIsListedWithItsDimensionsAndTheScoringTheSearchReports() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc7312");
        database.command("sql", "CREATE PROPERTY Doc7312.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc7312 (embedding) LSM_VECTOR METADATA { dimensions: 3, similarity: 'EUCLIDEAN' }");
        database.newVertex("Doc7312").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      });

      final Result listed = indexEntry("Doc7312", "Doc7312[embedding]");
      assertThat(listed.<Object>getProperty("dimensions")).isEqualTo(3);

      final JSONObject response = VectorSearch.search(database, new JSONObject()
          .put("indexName", "Doc7312[embedding]")
          .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
          .put("k", 1));
      assertThat(listed.<String>getProperty("scoring"))
          .isEqualTo(response.getString("scoring"))
          .isEqualTo("distance_lower_is_better:EUCLIDEAN");
    }

    @Test
    void aSparseIndexIsListedWithItsDimensionsAndTheScoringTheSearchReports() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Sparse7312");
        database.command("sql", "CREATE PROPERTY Sparse7312.dims ARRAY_OF_INTEGERS");
        database.command("sql", "CREATE PROPERTY Sparse7312.weights ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Sparse7312 (dims, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 128, modifier: 'IDF' }");
        database.command("sql", "INSERT INTO Sparse7312 SET dims = [1, 5], weights = [0.5, 0.25]");
      });

      final Result listed = indexEntry("Sparse7312", "Sparse7312[dims,weights]");
      assertThat(listed.<Object>getProperty("dimensions")).isEqualTo(128);

      final JSONObject response = VectorSearch.search(database, new JSONObject()
          .put("indexName", "Sparse7312[dims,weights]")
          .put("sparse", true)
          .put("queryIndices", new JSONArray(new Object[] { 1 }))
          .put("queryVector", new JSONArray(new Object[] { 1.0f }))
          .put("k", 1));
      assertThat(listed.<String>getProperty("scoring"))
          .isEqualTo(response.getString("scoring"))
          .isEqualTo("score_higher_is_better:idf_weighted_dot_product");
    }

    /** Only a vector index has a dimension count or a scoring direction; every other index keeps its old shape. */
    @Test
    void aNonVectorIndexCarriesNeitherField() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Plain7312");
        database.command("sql", "CREATE PROPERTY Plain7312.name STRING");
        database.command("sql", "CREATE PROPERTY Plain7312.body STRING");
        database.command("sql", "CREATE INDEX ON Plain7312 (name) NOTUNIQUE");
        database.command("sql", "CREATE INDEX ON Plain7312 (body) FULL_TEXT");
      });

      for (final String indexName : List.of("Plain7312[name]", "Plain7312[body]")) {
        final Result listed = indexEntry("Plain7312", indexName);
        assertThat(listed.hasProperty("dimensions")).as(indexName).isFalse();
        assertThat(listed.hasProperty("scoring")).as(indexName).isFalse();
      }
    }

    private Result indexEntry(final String typeName, final String indexName) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = ?", typeName)) {
        assertThat(rs.hasNext()).as("type " + typeName + " listed").isTrue();
        final List<Result> indexes = rs.next().getProperty("indexes");
        for (final Result index : indexes)
          if (indexName.equals(index.getProperty("name")))
            return index;
      }
      throw new AssertionError("index " + indexName + " not listed under " + typeName);
    }
  }

  /**
   * Regression for #7394: the expand type check of the hybrid search runs before the searches.
   * <p>
   * Issue #7394 item 5: with {@code expand}, a full-text index declared on a non-vertex type used to be
   * rejected only once the text query happened to match something.
   * <p>
   * The check sat behind {@code if (!fullTextLeg.rows().isEmpty())}, so the same request - the same index, the
   * same {@code expand} - reported success or failure depending on the corpus. A configuration error that
   * passes whenever the query matches nothing is a configuration error that ships.
   * <p>
   * Both type checks now run before the searches they used to follow: the vector index's type is checked
   * before the vector leg spends a search, and the full-text index's type as soon as the index resolves and
   * before the text search runs. A rejected request does no retrieval I/O at all.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
   */
  @Nested
  class ExpandTypeCheckIsUpFront extends TestHelper {

    private static final String VERTEX_TYPE   = "Doc7394";
    private static final String DOCUMENT_TYPE = "Note7394";
    private static final String VECTOR_INDEX  = VERTEX_TYPE + "[embedding]";
    private static final String TEXT_INDEX    = DOCUMENT_TYPE + "[body]";

    /**
     * A vertex type carrying the vector index, and a plain document type carrying the full-text index - the
     * shape the issue describes. The corpus is seeded so that "zebra" matches nothing and "alpha" matches.
     */
    private void buildSchema() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE);
        database.command("sql", "CREATE PROPERTY " + VERTEX_TYPE + ".embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON " + VERTEX_TYPE + " (embedding) LSM_VECTOR "
            + "METADATA { dimensions: 3, similarity: 'COSINE' }");

        database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);
        database.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".body STRING");
        database.command("sql", "CREATE INDEX ON " + DOCUMENT_TYPE + " (body) FULL_TEXT");

        database.newVertex(VERTEX_TYPE).set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
        database.newVertex(VERTEX_TYPE).set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
        database.newDocument(DOCUMENT_TYPE).set("body", "alpha beta").save();
      });
    }

    private JSONObject argsWithExpand(final String fulltextQuery) {
      return new JSONObject()
          .put("vectorIndexName", VECTOR_INDEX)
          .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
          .put("k", 5)
          .put("fulltextIndexName", TEXT_INDEX)
          .put("fulltextQuery", fulltextQuery)
          .put("expand", new JSONObject().put("maxDepth", 1));
    }

    /** The regression: the text query matches nothing, so the invalid configuration used to pass. */
    @Test
    void aNonVertexFullTextIndexWithExpandIsRefusedEvenWhenTheTextQueryMatchesNothing() {
      buildSchema();

      assertThatThrownBy(() -> HybridSearch.search(database, argsWithExpand("zebra")))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Graph expansion requires a vertex type")
          .hasMessageContaining(DOCUMENT_TYPE);
    }

    /** The case that was already caught, pinned so the fix does not trade one gate for the other. */
    @Test
    void aNonVertexFullTextIndexWithExpandIsStillRefusedWhenTheTextQueryMatches() {
      buildSchema();

      assertThatThrownBy(() -> HybridSearch.search(database, argsWithExpand("alpha")))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Graph expansion requires a vertex type")
          .hasMessageContaining(DOCUMENT_TYPE);
    }

    /**
     * The vector index's own type is checked before the vector leg runs, not after. A document-typed vector
     * index with {@code expand} is refused without a search being spent on it.
     */
    @Test
    void aNonVertexVectorIndexWithExpandIsRefused() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);
        database.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON " + DOCUMENT_TYPE + " (embedding) LSM_VECTOR "
            + "METADATA { dimensions: 3, similarity: 'COSINE' }");
        database.newDocument(DOCUMENT_TYPE).set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      });

      final JSONObject args = new JSONObject()
          .put("vectorIndexName", DOCUMENT_TYPE + "[embedding]")
          .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
          .put("k", 5)
          .put("expand", new JSONObject().put("maxDepth", 1));

      assertThatThrownBy(() -> HybridSearch.search(database, args))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Graph expansion requires a vertex type")
          .hasMessageContaining(DOCUMENT_TYPE);
    }

    /**
     * Without {@code expand} the same document-typed full-text index is a perfectly ordinary request: the type
     * check must be conditional on the expansion leg, not on the index.
     */
    @Test
    void aNonVertexFullTextIndexWithoutExpandIsFine() {
      buildSchema();

      final JSONObject args = new JSONObject()
          .put("vectorIndexName", VECTOR_INDEX)
          .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
          .put("k", 5)
          .put("fulltextIndexName", TEXT_INDEX)
          .put("fulltextQuery", "alpha");

      final JSONObject result = HybridSearch.search(database, args);
      assertThat(result.getJSONObject("legs").has("fulltext")).isTrue();
    }
  }
}
