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

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7568: the shape half of the contract, asserted against the document {@link VectorApiSpec} builds.
 * <p>
 * The three request schemas declared a {@code required} list and the three response schemas declared none, so a
 * generated client typed {@code count}, {@code results} and {@code indexName} as optional even though the server
 * always sends them - every caller in a statically typed language then null-checks a field that cannot be absent.
 * Three further properties were {@code {"type": "object"}} with neither {@code properties} nor
 * {@code additionalProperties}, which a strict generator emits as an empty model whose real content is
 * unreachable through typed access.
 * <p>
 * {@link Issue7568VectorResponseContractMatchesBehaviourTest} is the other half: it drives the engine and checks
 * that what is declared required here is what the search code actually emits.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7568">issue #7568</a>
 */
class Issue7568VectorResponseSchemaShapeTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new VectorApiSpec().contribute(openAPI);
  }

  private Schema<?> schema(final String name) {
    return openAPI.getComponents().getSchemas().get(name);
  }

  private static Schema<?> property(final Schema<?> owner, final String name) {
    return (Schema<?>) owner.getProperties().get(name);
  }

  /** The hit schema of a response, reached through {@code results[]}. */
  private static Schema<?> hitOf(final Schema<?> response) {
    return property(response, "results").getItems();
  }

  @Test
  void theVectorSearchResponseRequiresEveryFieldTheSearchAlwaysSends() {
    final Schema<?> response = schema("VectorSearchResponse");

    assertThat(response.getRequired())
        .as("VectorSearch.search puts all seven unconditionally, so none of them is optional to a client")
        .containsExactlyInAnyOrder("indexName", "sparse", "scoring", "candidateLimit", "truncated", "count",
            "results");
    // Nothing may be required that the schema does not even declare, or the document does not validate.
    assertThat(response.getProperties().keySet()).containsAll(response.getRequired());
  }

  @Test
  void theFullTextSearchResponseRequiresEveryFieldTheSearchAlwaysSends() {
    final Schema<?> response = schema("FullTextSearchResponse");

    assertThat(response.getRequired())
        .containsExactlyInAnyOrder("indexName", "similarity", "count", "results");
    assertThat(response.getProperties().keySet()).containsAll(response.getRequired());
  }

  /**
   * The hybrid response is the one where {@code required} has to discriminate: two of its ten fields are
   * genuinely conditional, and marking either of them required would make the contract lie in the other
   * direction.
   */
  @Test
  void theHybridSearchResponseRequiresTheUnconditionalFieldsAndOnlyThose() {
    final Schema<?> response = schema("HybridSearchResponse");

    assertThat(response.getRequired())
        .containsExactlyInAnyOrder("vectorIndexName", "sparse", "scoring", "legs", "fused", "truncated", "count",
            "results");
    assertThat(response.getRequired())
        .as("'fulltextIndexName' is sent only when the full-text leg ran, 'fusionStrategy' only when fused is true")
        .doesNotContain("fulltextIndexName", "fusionStrategy");
    assertThat(response.getProperties().keySet()).containsAll(response.getRequired());
  }

  /**
   * The hits are what a caller actually reads. A hit always carries its record id and its properties; which of
   * {@code distance} / {@code score} / {@code fusedScore} it carries depends on the index and on whether fusion
   * ran, so none of those three can be required.
   */
  @Test
  void everyHitRequiresTheFieldsPresentOnEveryHit() {
    final Schema<?> vectorHit = hitOf(schema("VectorSearchResponse"));
    assertThat(vectorHit.getRequired()).containsExactlyInAnyOrder("rid", "properties");
    assertThat(vectorHit.getRequired()).doesNotContain("distance", "score");

    final Schema<?> fullTextHit = hitOf(schema("FullTextSearchResponse"));
    assertThat(fullTextHit.getRequired()).containsExactlyInAnyOrder("rid", "properties");

    final Schema<?> fusedHit = hitOf(schema("HybridSearchResponse"));
    assertThat(fusedHit.getRequired())
        .as("both the fused and the unfused branch name the legs a hit came from")
        .containsExactlyInAnyOrder("rid", "sources", "properties");
    assertThat(fusedHit.getRequired())
        .as("'depth' and 'path' belong to an expansion hit only")
        .doesNotContain("fusedScore", "score", "distance", "depth", "path");
  }

  /**
   * {@code weights} is a closed map: {@code HybridSearch.validateWeights} refuses any other key outright, so the
   * document says so rather than leaving a client to discover it as a 400.
   */
  @Test
  void theWeightsMapDeclaresItsThreeLegsAndRefusesAnythingElse() {
    final Schema<?> weights = property(schema("HybridSearchRequest"), "weights");

    assertThat(weights.getProperties().keySet()).containsExactlyInAnyOrder("vector", "fulltext", "expand");
    assertThat(weights.getAdditionalProperties())
        .as("an unknown weights key is rejected by the server, so the schema must reject it too")
        .isEqualTo(Boolean.FALSE);

    for (final String leg : List.of("vector", "fulltext", "expand")) {
      final Schema<?> weight = property(weights, leg);
      assertThat(weight.getType()).as(leg).isEqualTo("number");
      assertThat(weight.getMinimum()).as(leg + " must be a finite number that is not negative")
          .isEqualTo(BigDecimal.ZERO);
    }
    // The defaults are the fallbacks HybridSearch.weightOf actually applies.
    assertThat(property(weights, "vector").getDefault()).isEqualTo(1.0f);
    assertThat(property(weights, "fulltext").getDefault()).isEqualTo(1.0f);
    assertThat(property(weights, "expand").getDefault()).isEqualTo(0.5f);
  }

  /**
   * {@code legs} is the opposite case: a fixed set of per-leg counters, so its sub-objects are spelled out
   * instead of being collapsed into an untyped map.
   */
  @Test
  void theLegsAccountingDeclaresOneSubObjectPerLeg() {
    final Schema<?> legs = property(schema("HybridSearchResponse"), "legs");

    assertThat(legs.getProperties().keySet()).containsExactlyInAnyOrder("vector", "fulltext", "expand");
    assertThat(legs.getRequired())
        .as("the vector leg always runs; the other two run only when the request asks for them")
        .containsExactly("vector");

    assertThat(property(legs, "vector").getProperties().keySet()).containsExactly("count");
    assertThat(property(legs, "fulltext").getProperties().keySet())
        .containsExactlyInAnyOrder("indexName", "similarity", "count");
    assertThat(property(legs, "expand").getProperties().keySet())
        .containsExactlyInAnyOrder("direction", "edgeTypes", "maxDepth", "truncated", "seedCount", "seedsTruncated",
            "count");
  }

  /**
   * A record's properties are arbitrary, so this one stays a map - but an open map, declared as such, rather
   * than a bare object a generator turns into an empty model.
   */
  @Test
  void aHitsPropertiesAreDeclaredAnOpenMapRatherThanAnEmptyModel() {
    for (final String responseName : List.of("VectorSearchResponse", "HybridSearchResponse",
        "FullTextSearchResponse")) {
      final Schema<?> properties = property(hitOf(schema(responseName)), "properties");
      assertThat(properties.getAdditionalProperties()).as(responseName).isEqualTo(Boolean.TRUE);
    }
  }

  /**
   * The sweep that keeps the two fixes from decaying one property at a time: every object anywhere in the six
   * components this contributor registers says what it holds.
   */
  @Test
  void noSchemaInThisContributorIsABareObject() {
    final List<String> bare = new ArrayList<>();
    for (final Map.Entry<String, Schema> entry : openAPI.getComponents().getSchemas().entrySet())
      collectBareObjects(entry.getKey(), entry.getValue(), bare);

    assertThat(bare)
        .as("a 'type: object' with neither 'properties' nor 'additionalProperties' generates an empty model")
        .isEmpty();
  }

  private static void collectBareObjects(final String path, final Schema<?> schema, final List<String> bare) {
    if (schema == null || schema.get$ref() != null)
      return;

    if ("object".equals(schema.getType())
        && (schema.getProperties() == null || schema.getProperties().isEmpty())
        && schema.getAdditionalProperties() == null)
      bare.add(path);

    if (schema.getProperties() != null)
      for (final Map.Entry<String, Schema> entry : schema.getProperties().entrySet())
        collectBareObjects(path + "." + entry.getKey(), entry.getValue(), bare);

    if (schema.getItems() != null)
      collectBareObjects(path + "[]", schema.getItems(), bare);

    if (schema.getAdditionalProperties() instanceof final Schema<?> values)
      collectBareObjects(path + ".*", values, bare);
  }
}
