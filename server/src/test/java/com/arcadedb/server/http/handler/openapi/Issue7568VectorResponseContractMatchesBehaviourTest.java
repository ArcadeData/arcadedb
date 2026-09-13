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
import com.arcadedb.query.search.FullTextQuery;
import com.arcadedb.query.search.HybridSearch;
import com.arcadedb.query.search.VectorSearch;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7568: the behaviour half of the contract. The {@code required} list a generated client trusts is only
 * worth trusting if the search code really does send those fields on every path, so this drives the three engine
 * entry points against a live database and reads the expectation straight out of {@link VectorApiSpec} - the
 * assertion cannot drift from the document because it is derived from it.
 * <p>
 * The handlers ({@code PostVectorSearchHandler}, {@code PostVectorHybridSearchHandler},
 * {@code PostVectorFullTextSearchHandler}) return these objects unaltered, so the response body the route emits
 * is the object built here.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7568">issue #7568</a>
 */
class Issue7568VectorResponseContractMatchesBehaviourTest extends TestHelper {
  private static final String VERTEX_TYPE   = "Doc7568";
  private static final String EDGE_TYPE     = "Link7568";
  private static final String VECTOR_INDEX  = VERTEX_TYPE + "[embedding]";
  private static final String TEXT_INDEX    = VERTEX_TYPE + "[body]";

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
      final var third = database.newVertex(VERTEX_TYPE)
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).set("body", "delta").save();

      first.newEdge(EDGE_TYPE, second).save();
      second.newEdge(EDGE_TYPE, third).save();
    });
  }

  private Schema<?> schema(final String name) {
    return openAPI.getComponents().getSchemas().get(name);
  }

  /**
   * Asserts the document's promise against one real response: every name in {@code required} is a key the
   * response actually carries.
   */
  private void assertDocumentedRequiredFieldsArePresent(final String schemaName, final JSONObject response) {
    final List<String> required = schema(schemaName).getRequired();
    assertThat(required)
        .as(schemaName + " must declare a required list, or a generated client types every field as optional")
        .isNotNull().isNotEmpty();

    for (final String field : required)
      assertThat(response.has(field)).as(schemaName + " promises '" + field + "', response was " + response).isTrue();
  }

  /** The same check for the hits, which is where a caller spends most of its null-checking. */
  private void assertDocumentedRequiredHitFieldsArePresent(final String schemaName, final JSONObject response) {
    final List<String> required =
        ((Schema<?>) schema(schemaName).getProperties().get("results")).getItems().getRequired();
    assertThat(required).as(schemaName + ".results[] required list").isNotNull().isNotEmpty();

    final JSONArray results = response.getJSONArray("results");
    assertThat(results.length()).as(schemaName + " needs at least one hit for this assertion to mean anything")
        .isGreaterThan(0);
    for (int i = 0; i < results.length(); i++) {
      final JSONObject hit = results.getJSONObject(i);
      for (final String field : required)
        assertThat(hit.has(field)).as(schemaName + " hit promises '" + field + "', hit was " + hit).isTrue();
    }
  }

  private JSONObject vectorArgs() {
    return new JSONObject()
        .put("indexName", VECTOR_INDEX)
        .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
        .put("k", 5);
  }

  private JSONObject hybridArgs() {
    return new JSONObject()
        .put("vectorIndexName", VECTOR_INDEX)
        .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
        .put("k", 5);
  }

  @Test
  void theVectorSearchResponseCarriesEveryFieldItsSchemaRequires() {
    final JSONObject response = VectorSearch.search(database, vectorArgs());

    assertDocumentedRequiredFieldsArePresent("VectorSearchResponse", response);
    assertDocumentedRequiredHitFieldsArePresent("VectorSearchResponse", response);
  }

  /**
   * {@code properties} is declared an open map, and the schema description says what a caller finds in it. The
   * record's own properties are only part of the answer: {@code JsonSerializer.serializeDocument} writes
   * {@code @rid} and {@code @type} into every document it serializes, so the map's keys are not the type's
   * property names alone - which is why it cannot be given a closed set of {@code properties} instead.
   */
  @Test
  void aHitsPropertiesMapCarriesTheRecordMarkersAlongsideTheTypesOwnProperties() {
    final JSONObject hit = VectorSearch.search(database, vectorArgs()).getJSONArray("results").getJSONObject(0);

    assertThat(hit.getJSONObject("properties").keySet())
        .contains("@rid", "@type")
        .contains("embedding", "body");
  }

  @Test
  void theFullTextSearchResponseCarriesEveryFieldItsSchemaRequires() {
    final JSONObject response = FullTextQuery.search(database,
        new JSONObject().put("indexName", TEXT_INDEX).put("queryText", "alpha").put("limit", 5));

    assertDocumentedRequiredFieldsArePresent("FullTextSearchResponse", response);
    assertDocumentedRequiredHitFieldsArePresent("FullTextSearchResponse", response);
  }

  /**
   * One leg, so no fusion. This is the path that proves {@code fusionStrategy} is rightly left out of the
   * required list: declaring it required would make the document lie on exactly this response.
   */
  @Test
  void theUnfusedHybridResponseCarriesEveryFieldItsSchemaRequiresAndNoFusionStrategy() {
    final JSONObject response = HybridSearch.search(database, hybridArgs());

    assertDocumentedRequiredFieldsArePresent("HybridSearchResponse", response);
    assertDocumentedRequiredHitFieldsArePresent("HybridSearchResponse", response);

    assertThat(response.getBoolean("fused")).isFalse();
    assertThat(response.has("fusionStrategy")).isFalse();
    assertThat(response.has("fulltextIndexName")).isFalse();
  }

  /** Two legs, so fusion runs and the two conditional fields do appear - both still legitimately optional. */
  @Test
  void theFusedHybridResponseCarriesEveryFieldItsSchemaRequires() {
    final JSONObject response = HybridSearch.search(database,
        hybridArgs().put("fulltextIndexName", TEXT_INDEX).put("fulltextQuery", "alpha"));

    assertDocumentedRequiredFieldsArePresent("HybridSearchResponse", response);
    assertDocumentedRequiredHitFieldsArePresent("HybridSearchResponse", response);

    assertThat(response.getBoolean("fused")).isTrue();
    assertThat(response.has("fusionStrategy")).isTrue();
    assertThat(response.has("fulltextIndexName")).isTrue();
  }

  /**
   * The per-leg accounting is no longer an untyped map, so what the document spells out under {@code legs} has
   * to be exactly what the engine puts there - for each of the three legs, on the request that runs it.
   */
  @Test
  void theLegsAccountingMatchesTheDocumentedSubObjects() {
    final JSONObject response = HybridSearch.search(database,
        hybridArgs()
            .put("fulltextIndexName", TEXT_INDEX)
            .put("fulltextQuery", "alpha")
            .put("expand", new JSONObject().put("maxDepth", 1).put("direction", "out")
                .put("edgeTypes", new JSONArray(new Object[] { EDGE_TYPE }))));

    final Schema<?> legsSchema = (Schema<?>) schema("HybridSearchResponse").getProperties().get("legs");
    final JSONObject legs = response.getJSONObject("legs");

    assertThat(legs.keySet()).containsExactlyInAnyOrder("vector", "fulltext", "expand");
    for (final String leg : List.of("vector", "fulltext", "expand")) {
      final Schema<?> documented = (Schema<?>) legsSchema.getProperties().get(leg);
      assertThat(documented).as("legs." + leg + " must be documented").isNotNull();
      assertThat(legs.getJSONObject(leg).keySet())
          .as("legs." + leg + " keys must be exactly the ones the document names")
          .containsExactlyInAnyOrderElementsOf(documented.getProperties().keySet());
    }
  }

  /**
   * {@code weights} is documented as a closed map of three numbers. The closure is the server's own rule, so it
   * is checked here rather than only asserted on the schema.
   */
  @Test
  void theServerRefusesExactlyTheWeightsKeysTheSchemaRefuses() {
    final Schema<?> weights =
        (Schema<?>) schema("HybridSearchRequest").getProperties().get("weights");

    // Every documented key is accepted on a request that owns the matching leg.
    final JSONObject accepted = HybridSearch.search(database,
        hybridArgs()
            .put("fulltextIndexName", TEXT_INDEX)
            .put("fulltextQuery", "alpha")
            .put("expand", new JSONObject().put("maxDepth", 1))
            .put("weights", new JSONObject().put("vector", 1.0).put("fulltext", 2.0).put("expand", 0.25)));
    assertThat(accepted.getBoolean("fused")).isTrue();

    // ... and a key the document does not declare is refused, which is what additionalProperties:false says.
    assertThat(weights.getAdditionalProperties()).isEqualTo(Boolean.FALSE);
    assertThatThrownBy(() -> HybridSearch.search(database,
        hybridArgs().put("weights", new JSONObject().put("graph", 1.0))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown weights key");
  }
}
