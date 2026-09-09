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

import com.arcadedb.query.search.FullTextSearchOperation;
import com.arcadedb.query.search.HybridSearchOperation;
import com.arcadedb.query.search.VectorSearchLeg;
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
 * Issue #7306. The point of most of these assertions is not that the document exists but that the numbers in it
 * are the ones the server enforces: a published bound the server disagrees with is worse than an undocumented
 * one, because a client that honours it still gets rejected and has nowhere to look.
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
  void theThreeSearchRoutesAreDocumentedAsPostOperations() {
    assertThat(openAPI.getPaths().keySet()).containsExactlyInAnyOrder(
        "/api/v1/vector/{database}/search",
        "/api/v1/vector/{database}/hybrid",
        "/api/v1/vector/{database}/fulltext");

    for (final String path : openAPI.getPaths().keySet()) {
      final Operation post = openAPI.getPaths().get(path).getPost();
      assertThat(post).as("%s must declare a POST", path).isNotNull();
      assertThat(post.getTags()).containsExactly("Vector");
      assertThat(post.getRequestBody().getRequired()).isTrue();
      assertThat(post.getResponses().keySet()).contains("200", "400", "401", "403", "404", "500");
    }
  }

  @Test
  void everyOperationIdIsDistinct() {
    assertThat(openAPI.getPaths().values().stream().map(item -> item.getPost().getOperationId()).toList())
        .containsExactlyInAnyOrder("vectorSearch", "hybridSearch", "fullTextSearch");
  }

  @Test
  void theDeclaredKWindowIsTheOneTheSearchEnforces() {
    final Schema<?> k = (Schema<?>) openAPI.getComponents().getSchemas().get("VectorSearchRequest")
        .getProperties().get("k");
    assertThat(k.getMinimum()).isEqualTo(BigDecimal.ONE);
    assertThat(k.getMaximum()).isEqualTo(BigDecimal.valueOf(VectorSearchLeg.MAX_K));
  }

  @Test
  void theDeclaredEfSearchWindowIsTheOneTheSearchEnforces() {
    final Schema<?> efSearch = (Schema<?>) openAPI.getComponents().getSchemas().get("VectorSearchRequest")
        .getProperties().get("efSearch");
    assertThat(efSearch.getMinimum()).isEqualTo(BigDecimal.ONE);
    assertThat(efSearch.getMaximum()).isEqualTo(BigDecimal.valueOf(VectorSearchLeg.MAX_EF_SEARCH));
  }

  @Test
  void theDeclaredFullTextLimitIsTheOneTheSearchEnforces() {
    final Schema<?> limit = (Schema<?>) openAPI.getComponents().getSchemas().get("FullTextSearchRequest")
        .getProperties().get("limit");
    assertThat(limit.getMinimum()).isEqualTo(BigDecimal.ONE);
    assertThat(limit.getMaximum()).isEqualTo(BigDecimal.valueOf(FullTextSearchOperation.MAX_LIMIT));
  }

  @Test
  void theDeclaredExpansionDepthIsTheOneTheSearchEnforces() {
    final Schema<?> expand = (Schema<?>) openAPI.getComponents().getSchemas().get("HybridSearchRequest")
        .getProperties().get("expand");
    final Schema<?> maxDepth = (Schema<?>) expand.getProperties().get("maxDepth");
    assertThat(maxDepth.getMaximum()).isEqualTo(BigDecimal.valueOf(HybridSearchOperation.MAX_DEPTH));
  }

  @Test
  void theRequiredFieldsMatchWhatEachSearchDemands() {
    assertThat(openAPI.getComponents().getSchemas().get("VectorSearchRequest").getRequired())
        .containsExactlyInAnyOrder("indexName", "queryVector", "k");
    assertThat(openAPI.getComponents().getSchemas().get("HybridSearchRequest").getRequired())
        .containsExactlyInAnyOrder("vectorIndexName", "queryVector", "k");
    assertThat(openAPI.getComponents().getSchemas().get("FullTextSearchRequest").getRequired())
        .containsExactly("queryText");
  }

  /**
   * A dense hit carries a distance and a sparse one a score. Documenting only one of them would leave a generated
   * client with no field to read for half the index types.
   */
  @Test
  void theVectorHitDocumentsBothRankingDirections() {
    final Schema<?> response = openAPI.getComponents().getSchemas().get("VectorSearchResponse");
    final Schema<?> hit = ((Schema<?>) response.getProperties().get("results")).getItems();
    assertThat(hit.getProperties().keySet()).contains("rid", "distance", "score", "properties");
  }

  @Test
  void theHybridResponseDocumentsTheExpansionProvenance() {
    final Schema<?> response = openAPI.getComponents().getSchemas().get("HybridSearchResponse");
    final Schema<?> hit = ((Schema<?>) response.getProperties().get("results")).getItems();
    assertThat(hit.getProperties().keySet()).contains("fusedScore", "sources", "depth", "path");
  }

  @Test
  void theFusionStrategyEnumIsClosed() {
    final Schema<?> strategy = (Schema<?>) openAPI.getComponents().getSchemas().get("HybridSearchRequest")
        .getProperties().get("fusionStrategy");
    assertThat(strategy.getEnum().stream().map(String::valueOf).toList())
        .containsExactly("RRF", "DBSF", "LINEAR");
  }
}
