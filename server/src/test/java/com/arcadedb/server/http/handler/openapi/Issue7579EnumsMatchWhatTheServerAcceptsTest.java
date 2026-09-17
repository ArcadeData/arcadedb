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
package com.arcadedb.server.http.handler.openapi;

import com.arcadedb.TestHelper;
import com.arcadedb.query.search.HybridSearch;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7579, the behaviour half. {@link Issue7579VectorEnumsComeFromTheEnforcementTest} asserts that the
 * document reads its value sets from the server's own constants; this drives the engine and checks that those
 * constants really are what the server accepts and refuses.
 * <p>
 * Without it the two could agree and both be wrong: a constant nothing checks against is a list, not a contract.
 * The expectations are read out of {@link VectorApiSpec}'s document, so - as in
 * {@code Issue7568VectorResponseContractMatchesBehaviourTest} - the assertion is derived from the thing it is
 * checking and cannot drift from it.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7579">issue #7579</a>
 */
class Issue7579EnumsMatchWhatTheServerAcceptsTest extends TestHelper {
  private static final String VERTEX_TYPE  = "Doc7579";
  private static final String EDGE_TYPE    = "Link7579";
  private static final String VECTOR_INDEX = VERTEX_TYPE + "[embedding]";
  private static final String TEXT_INDEX   = VERTEX_TYPE + "[body]";

  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contributeAndSeed() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new VectorApiSpec().contribute(openAPI);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE);
      database.command("sql", "CREATE PROPERTY " + VERTEX_TYPE + ".embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY " + VERTEX_TYPE + ".body STRING");
      database.command("sql", "CREATE INDEX ON " + VERTEX_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");
      database.command("sql", "CREATE INDEX ON " + VERTEX_TYPE + " (body) FULL_TEXT");
      database.command("sql", "CREATE EDGE TYPE " + EDGE_TYPE);

      final var first = database.newVertex(VERTEX_TYPE)
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).set("body", "alpha beta").save();
      final var second = database.newVertex(VERTEX_TYPE)
          .set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).set("body", "alpha gamma").save();
      first.newEdge(EDGE_TYPE, second).save();
    });
  }

  private Schema<?> property(final String component, final String name) {
    return (Schema<?>) openAPI.getComponents().getSchemas().get(component).getProperties().get(name);
  }

  private JSONObject hybridArgs() {
    return new JSONObject()
        .put("vectorIndexName", VECTOR_INDEX)
        .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
        .put("k", 5)
        .put("fulltextIndexName", TEXT_INDEX)
        .put("fulltextQuery", "alpha");
  }

  /**
   * Every value the document declares is accepted. The full-text leg is present so fusion actually runs, which
   * is what makes the strategy reach the engine rather than being short-circuited by the unfused branch.
   */
  @Test
  void everyDeclaredFusionStrategyIsAcceptedAndEchoed() {
    for (final Object declared : property("HybridSearchRequest", "fusionStrategy").getEnum()) {
      final String strategy = (String) declared;
      final JSONObject response = HybridSearch.search(database, hybridArgs().put("fusionStrategy", strategy));

      assertThat(response.getBoolean("fused")).as("strategy %s", strategy).isTrue();
      assertThat(response.getString("fusionStrategy"))
          .as("the response echoes the strategy upper-cased, which is the value the enum declares")
          .isEqualTo(strategy);
    }
  }

  /** And one the document does not declare is refused, naming the allowed set rather than failing obscurely. */
  @Test
  void anUndeclaredFusionStrategyIsRefused() {
    assertThatThrownBy(() -> HybridSearch.search(database, hybridArgs().put("fusionStrategy", "WEIGHTED")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown fusionStrategy")
        .as("the refusal has to list the accepted values, not merely reject")
        .hasMessageContaining(String.join(", ", HybridSearch.FUSION_STRATEGIES));
  }

  /**
   * Lower case is accepted although the enum declares only the upper-case spelling. Asserted so the subset
   * relation the document relies on is a checked fact rather than a comment: the enum is narrower than the
   * server, never wider.
   */
  @Test
  void aLowerCasedStrategyIsStillAcceptedEvenThoughTheEnumDeclaresOnlyTheUpperCase() {
    final JSONObject response = HybridSearch.search(database, hybridArgs().put("fusionStrategy", "rrf"));

    assertThat(response.getString("fusionStrategy")).isEqualTo(HybridSearch.STRATEGY_RRF);
  }

  @Test
  void everyDeclaredExpandDirectionIsAcceptedAndAnUndeclaredOneIsRefused() {
    final Schema<?> direction = (Schema<?>) property("HybridSearchRequest", "expand")
        .getProperties().get("direction");

    for (final Object declared : direction.getEnum()) {
      final JSONObject expand = new JSONObject().put("maxDepth", 1).put("direction", declared);
      assertThatCode(() -> HybridSearch.search(database, hybridArgs().put("expand", expand)))
          .as("direction %s", declared)
          .doesNotThrowAnyException();
    }

    assertThatThrownBy(() -> HybridSearch.search(database,
        hybridArgs().put("expand", new JSONObject().put("maxDepth", 1).put("direction", "outward"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expand.direction must be one of "
            + String.join(", ", HybridSearch.EXPAND_DIRECTIONS));
  }

  /**
   * The values a fused hit's {@code sources} really carries. Checked against the document's own enum rather than
   * against a literal list, so a leg added to the engine that the document does not declare fails here.
   */
  @Test
  void everySourceAFusedHitReportsIsOneTheDocumentDeclares() {
    final JSONObject response = HybridSearch.search(database, hybridArgs()
        .put("expand", new JSONObject().put("maxDepth", 1)));

    final Schema<?> sources = ((Schema<?>) openAPI.getComponents().getSchemas().get("HybridSearchResponse")
        .getProperties().get("results")).getItems().getProperties().get("sources");
    final List<Object> declared = new ArrayList<>(sources.getItems().getEnum());

    final JSONArray results = response.getJSONArray("results");
    assertThat(results.length()).as("the assertion needs at least one hit to mean anything").isGreaterThan(0);
    for (int i = 0; i < results.length(); i++) {
      final JSONArray hitSources = results.getJSONObject(i).getJSONArray("sources");
      assertThat(hitSources.length()).as("a hit is in the list because some leg produced it").isGreaterThan(0);
      for (int j = 0; j < hitSources.length(); j++)
        assertThat(declared).as("hit %d source", i).contains(hitSources.getString(j));
    }
  }

  /** The similarity the full-text leg reports is one of the two the document declares. */
  @Test
  void theReportedSimilarityIsOneTheDocumentDeclares() {
    final JSONObject response = HybridSearch.search(database, hybridArgs());

    final List<Object> declared = new ArrayList<>(
        ((Schema<?>) ((Schema<?>) openAPI.getComponents().getSchemas().get("HybridSearchResponse")
            .getProperties().get("legs")).getProperties().get("fulltext")).getProperties().get("similarity")
            .getEnum());

    assertThat(declared).contains(response.getJSONObject("legs").getJSONObject("fulltext").getString("similarity"));
  }
}
