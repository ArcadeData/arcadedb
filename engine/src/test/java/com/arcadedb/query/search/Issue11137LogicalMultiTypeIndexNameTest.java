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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11137: the logical name of a Cypher {@code CREATE FULLTEXT INDEX} / {@code CREATE VECTOR INDEX} over several labels is
 * resolved by {@code SEARCH_INDEX_MORE}, by the shared full-text, vector and hybrid search services (HTTP, gRPC and MCP tools) and
 * is reported by {@code schema:indexes}.
 */
class Issue11137LogicalMultiTypeIndexNameTest extends TestHelper {

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
