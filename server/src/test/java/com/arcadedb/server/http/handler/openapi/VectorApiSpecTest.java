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

import com.arcadedb.server.http.handler.OpenApiSpecGenerator;
import com.arcadedb.server.vector.FullTextQuery;
import com.arcadedb.server.vector.HybridSearch;
import com.arcadedb.server.vector.VectorLeg;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The vector, hybrid and full-text routes added by issue #7306, checked against the document that describes
 * them.
 * <p>
 * {@code OpenApiSpecGenerationIT} asserts the same inventory against the <i>served</i> document, but it fetches
 * it over a hard-coded {@code localhost:2480} and therefore cannot run on a machine where anything else already
 * holds that port - the same reason {@code OpenApiSpecGeneratorTest} carries a unit copy of the version
 * assertion. This is the unit lane for the same contract.
 */
class VectorApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new VectorApiSpec().contribute(openAPI);
  }

  @Test
  void theThreeRoutesAreDocumentedAndReachTheGeneratedDocument() {
    assertThat(openAPI.getPaths().keySet()).containsExactlyInAnyOrder(
        "/api/v1/vector/{database}/search",
        "/api/v1/vector/{database}/hybrid",
        "/api/v1/vector/{database}/fulltext");

    // ... and the contributor is actually registered, which contributing in isolation cannot show.
    final OpenAPI generated = new OpenApiSpecGenerator(null).generateSpec();
    assertThat(generated.getPaths().keySet()).contains(
        "/api/v1/vector/{database}/search",
        "/api/v1/vector/{database}/hybrid",
        "/api/v1/vector/{database}/fulltext");
  }

  /**
   * A generated client derives its API class from the first tag, and the root tag vocabulary is what the
   * document declares. A tag used but not declared fails validation.
   */
  @Test
  void everyOperationCarriesTheDeclaredVectorTag() {
    openAPI.getPaths().values().forEach(item -> item.readOperations()
        .forEach(op -> assertThat(op.getTags()).containsExactly("Vector")));

    assertThat(new OpenApiSpecGenerator(null).generateSpec().getTags())
        .as("the Vector tag must be in the root vocabulary, or the document does not validate")
        .anyMatch(tag -> "Vector".equals(tag.getName()));
  }

  /**
   * The bounds in the document are the bounds the server enforces, read from the implementation constants rather
   * than written out. This is what stops a generated client from accepting locally what the server refuses
   * remotely - the divergence issue #7306 asked to be designed out.
   */
  @Test
  void theDocumentedBoundsAreTheEnforcedBounds() {
    final Schema<?> search = openAPI.getComponents().getSchemas().get("VectorSearchRequest");
    assertThat(bound(search, "k", true)).isEqualTo(BigDecimal.valueOf(VectorLeg.MAX_K));
    assertThat(bound(search, "k", false)).isEqualTo(BigDecimal.ONE);
    assertThat(((Schema<?>) search.getProperties().get("k")).getDefault())
        .isEqualTo(VectorLeg.DEFAULT_K);
    assertThat(bound(search, "efSearch", true)).isEqualTo(BigDecimal.valueOf(VectorLeg.MAX_EF_SEARCH));

    final Schema<?> fullText = openAPI.getComponents().getSchemas().get("FullTextSearchRequest");
    assertThat(bound(fullText, "limit", true)).isEqualTo(BigDecimal.valueOf(FullTextQuery.MAX_LIMIT));
    assertThat(((Schema<?>) fullText.getProperties().get("limit")).getDefault())
        .isEqualTo(FullTextQuery.DEFAULT_LIMIT);

    final Schema<?> hybrid = openAPI.getComponents().getSchemas().get("HybridSearchRequest");
    assertThat(bound(hybrid, "k", true)).isEqualTo(BigDecimal.valueOf(VectorLeg.MAX_K));
    final Schema<?> expand = (Schema<?>) hybrid.getProperties().get("expand");
    assertThat(((Schema<?>) expand.getProperties().get("maxDepth")).getMaximum())
        .isEqualTo(BigDecimal.valueOf(HybridSearch.MAX_DEPTH));
  }

  /**
   * A vector component and a similarity score are floating point. Documented as {@code integer} they would tell
   * a generated client to round the value it is about to send, which for a query vector silently changes the
   * query.
   */
  @Test
  void vectorsAndScoresAreDocumentedAsNumbersNotIntegers() {
    final Schema<?> search = openAPI.getComponents().getSchemas().get("VectorSearchRequest");
    assertThat(((Schema<?>) search.getProperties().get("queryVector")).getItems().getType()).isEqualTo("number");
    assertThat(((Schema<?>) search.getProperties().get("queryIndices")).getItems().getType()).isEqualTo("integer");

    final Schema<?> response = openAPI.getComponents().getSchemas().get("VectorSearchResponse");
    final Schema<?> hit = ((Schema<?>) response.getProperties().get("results")).getItems();
    assertThat(((Schema<?>) hit.getProperties().get("distance")).getType()).isEqualTo("number");
    assertThat(((Schema<?>) hit.getProperties().get("score")).getType()).isEqualTo("number");
  }

  /**
   * The hybrid full-text leg is addressed by {@code fulltextQuery} plus {@code fulltextIndexName}, not by the
   * standalone route's {@code queryText}/{@code indexName}. Documenting the wrong names would send a caller
   * after a leg the server never runs, and the server refuses half a leg rather than dropping it.
   */
  @Test
  void theHybridFullTextLegIsDocumentedUnderTheNamesTheServerReads() {
    final Schema<?> hybrid = openAPI.getComponents().getSchemas().get("HybridSearchRequest");
    assertThat(hybrid.getProperties().keySet())
        .contains("fulltextQuery", "fulltextIndexName")
        .doesNotContain("queryText", "indexName");
  }

  @Test
  void everyRouteDeclaresTheClientErrorTheBoundsProduce() {
    openAPI.getPaths().values().forEach(item -> item.readOperations().forEach(op -> {
      assertThat(op.getResponses()).as(op.getOperationId()).containsKeys("200", "400", "401", "403", "404", "500");
      assertThat(op.getRequestBody().getRequired()).as(op.getOperationId()).isTrue();
    }));
  }

  @Test
  void everyOperationIdIsDistinct() {
    final Operation search = openAPI.getPaths().get("/api/v1/vector/{database}/search").getPost();
    final Operation hybrid = openAPI.getPaths().get("/api/v1/vector/{database}/hybrid").getPost();
    final Operation fullText = openAPI.getPaths().get("/api/v1/vector/{database}/fulltext").getPost();

    assertThat(List.of(search.getOperationId(), hybrid.getOperationId(), fullText.getOperationId()))
        .containsExactlyInAnyOrder("vectorSearch", "hybridSearch", "fullTextSearch");
  }

  private static BigDecimal bound(final Schema<?> request, final String property, final boolean maximum) {
    final Schema<?> schema = (Schema<?>) request.getProperties().get(property);
    return maximum ? schema.getMaximum() : schema.getMinimum();
  }
}
