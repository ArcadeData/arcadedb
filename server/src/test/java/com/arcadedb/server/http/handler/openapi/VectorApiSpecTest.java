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

import com.arcadedb.function.sql.vector.SQLFunctionVectorFuse;
import com.arcadedb.query.search.FullTextQuery;
import com.arcadedb.query.search.HybridSearch;
import com.arcadedb.query.search.VectorLeg;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.server.http.handler.OpenApiSpecGenerator;
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

  /*
   * Issue #7568: the shape half of the contract, asserted against the document {@link VectorApiSpec} builds.
   * <p>
   * The three request schemas declared a {@code required} list and the three response schemas declared none, so a
   * generated client typed {@code count}, {@code results} and {@code indexName} as optional even though the server
   * always sends them - every caller in a statically typed language then null-checks a field that cannot be absent.
   * Three further properties were {@code {"type": "object"}} with neither {@code properties} nor
   * {@code additionalProperties}, which a strict generator emits as an empty model whose real content is
   * unreachable through typed access.
   * <p>
   * {@link VectorApiContractMatchesBehaviourTest.VectorRequiredFieldsAreSent} is the other half: it drives the engine and checks
   * that what is declared required here is what the search code actually emits.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7568">issue #7568</a>
   */
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

  // Regression: #7568 the vector response schemas declare required and no bare objects
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

  // Regression: #7568 the vector response schemas declare required and no bare objects
  @Test
  void theFullTextSearchResponseRequiresEveryFieldTheSearchAlwaysSends() {
    final Schema<?> response = schema("FullTextSearchResponse");

    assertThat(response.getRequired())
        .containsExactlyInAnyOrder("indexName", "similarity", "count", "results");
    assertThat(response.getProperties().keySet()).containsAll(response.getRequired());
  }

  // Regression: #7568 the vector response schemas declare required and no bare objects
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

  // Regression: #7568 the vector response schemas declare required and no bare objects
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

  // Regression: #7568 the vector response schemas declare required and no bare objects
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

  // Regression: #7568 the vector response schemas declare required and no bare objects
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

  // Regression: #7568 the vector response schemas declare required and no bare objects
  /**
   * Whether a leg is present is conditional; what a present leg holds is not. Each sub-object is written as a
   * single expression in {@code HybridSearch}, so a leg that appears appears whole - there is no state in which
   * {@code legs.fulltext} exists without its index name. Leaving these lists off would have reproduced, one level
   * down, the very defect this fix is about.
   */
  @Test
  void eachLegSubObjectRequiresEverythingItCarriesWhenItIsPresentAtAll() {
    final Schema<?> legs = property(schema("HybridSearchResponse"), "legs");

    for (final String leg : List.of("vector", "fulltext", "expand")) {
      final Schema<?> subObject = property(legs, leg);
      assertThat(subObject.getRequired())
          .as("legs." + leg + " is written whole or not at all")
          .containsExactlyInAnyOrderElementsOf(subObject.getProperties().keySet());
    }
  }

  // Regression: #7568 the vector response schemas declare required and no bare objects
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

  /*
   * Issue #7579: five properties of the vector, hybrid and full-text schemas are typed {@code string}, which is
   * correct as far as it goes, while their accepted values are a closed set the server enforces and the document
   * names only inside the description text. A generated client gets a bare {@code String} where it could have an
   * enum, so a typo is a 400 from the server instead of a compile error in the caller.
   * <p>
   * The values are read from the constants the enforcement uses - {@link HybridSearch#FUSION_STRATEGIES},
   * {@link HybridSearch#EXPAND_DIRECTIONS}, {@link HybridSearch#LEG_NAMES} and
   * {@link FullTextIndexMetadata#SIMILARITIES} - exactly as {@code VectorApiSpec} already reads its numeric bounds
   * out of {@code VectorLeg.MAX_K} and friends. The assertions below are written against those constants rather
   * than against literal lists, so a value added to the server appears in the document without anyone editing this
   * file, and a value added to the document that the server does not accept fails here.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7579">issue #7579</a>
   */
  // Regression: #7579 the vector enums are read from the constants the server enforces
  @Test
  void theFusionStrategyIsTheSetTheServerEnforces() {
    assertThat(property(schema("HybridSearchRequest"), "fusionStrategy").getEnum())
        .as("HybridSearch.strategyOf accepts exactly these, so a client can reject a typo locally")
        .isEqualTo(HybridSearch.FUSION_STRATEGIES);

    assertThat(property(schema("HybridSearchResponse"), "fusionStrategy").getEnum())
        .as("the response echoes the request's value upper-cased, so it is the same set")
        .isEqualTo(HybridSearch.FUSION_STRATEGIES);
  }

  // Regression: #7579 the vector enums are read from the constants the server enforces
  /**
   * The request is matched case-insensitively while the enum lists only the canonical spellings, and that is the
   * safe direction: every value the document allows is a value the server accepts. Pinned because the opposite
   * - listing the case variants, or listing a value the server would refuse - is the mistake that turns a
   * generated client's local validation into a source of false rejections.
   */
  @Test
  void theDeclaredStrategiesAreASubsetOfWhatTheServerAcceptsNotASuperset() {
    for (final Object declared : property(schema("HybridSearchRequest"), "fusionStrategy").getEnum())
      assertThat(HybridSearch.FUSION_STRATEGIES).as("declared value '%s'", declared).contains((String) declared);

    assertThat(property(schema("HybridSearchRequest"), "fusionStrategy").getDescription())
        .as("and the case-insensitivity has to be stated, or a client that lower-cases its own values thinks "
            + "they are invalid")
        .contains("case-insensitively");
  }

  // Regression: #7579 the vector enums are read from the constants the server enforces
  @Test
  void theExpandDirectionIsTheSetValidateExpandAccepts() {
    final Schema<?> requestExpand = property(schema("HybridSearchRequest"), "expand");
    assertThat(property(requestExpand, "direction").getEnum())
        .isEqualTo(HybridSearch.EXPAND_DIRECTIONS);

    final Schema<?> responseExpand = property(property(schema("HybridSearchResponse"), "legs"), "expand");
    assertThat(property(responseExpand, "direction").getEnum())
        .as("the response echoes the request's value, so the two must declare one set")
        .isEqualTo(HybridSearch.EXPAND_DIRECTIONS);
  }

  // Regression: #7579 the vector enums are read from the constants the server enforces
  /**
   * A fused hit names the legs it came from, and there are exactly three legs {@code HybridSearch.search} can
   * construct. The enum sits on the array's ITEMS, not on the array, which is the easy thing to get wrong.
   */
  @Test
  void aFusedHitsSourcesAreTheThreeLegNames() {
    final Schema<?> sources = property(
        property(schema("HybridSearchResponse"), "results").getItems(), "sources");

    assertThat(sources.getType()).isEqualTo("array");
    assertThat(sources.getItems().getEnum())
        .as("the constraint belongs on the element, not on the array")
        .isEqualTo(HybridSearch.LEG_NAMES);
  }

  // Regression: #7579 the vector enums are read from the constants the server enforces
  @Test
  void theFullTextSimilarityIsTheSetTheIndexMetadataEnforces() {
    assertThat(property(schema("FullTextSearchResponse"), "similarity").getEnum())
        .as("FullTextSearch.getSimilarity returns one of these two and nothing else")
        .isEqualTo(FullTextIndexMetadata.SIMILARITIES);

    final Schema<?> fullTextLeg = property(property(schema("HybridSearchResponse"), "legs"), "fulltext");
    assertThat(property(fullTextLeg, "similarity").getEnum())
        .as("the hybrid response reports the same value through the same method")
        .isEqualTo(FullTextIndexMetadata.SIMILARITIES);
  }

  // Regression: #7579 the vector enums are read from the constants the server enforces
  /**
   * {@code scoring} is deliberately NOT an enum, and saying so is worth a test: it is a composed string
   * ("distance_lower_is_better:" plus the similarity function's own name), not a closed vocabulary, and giving
   * it an enum would make a generated client reject values the server legitimately sends.
   */
  @Test
  void theComposedScoringStringIsLeftAsAPlainString() {
    for (final String response : List.of("VectorSearchResponse", "HybridSearchResponse")) {
      final Schema<?> scoring = property(schema(response), "scoring");
      assertThat(scoring.getType()).as(response).isEqualTo("string");
      assertThat(scoring.getEnum())
          .as(response + ".scoring is 'direction:function', so its value set is the similarity functions, not a "
              + "vocabulary this document can close")
          .isNull();
    }
  }

  // Regression: #7579 the vector enums are read from the constants the server enforces
  /**
   * The point of reading the values from the constants: the engine's own checks are driven by the same lists, so
   * the document cannot come to describe a set the server does not enforce. Asserted against the constants'
   * members rather than against literals, which is what keeps this test from becoming the second place a new
   * value has to be added.
   */
  @Test
  void theConstantsTheDocumentReadsAreTheOnesTheServerChecksAgainst() {
    assertThat(HybridSearch.FUSION_STRATEGIES)
        .as("the strategies come from vector.fuse itself, which is where fusion scoring happens, so a strategy "
            + "the request check accepted and the SQL function did not cannot exist")
        .isSameAs(SQLFunctionVectorFuse.STRATEGIES)
        .contains(HybridSearch.STRATEGY_RRF, HybridSearch.STRATEGY_DBSF, HybridSearch.STRATEGY_LINEAR);
    assertThat(HybridSearch.EXPAND_DIRECTIONS)
        .contains(HybridSearch.DIRECTION_OUT, HybridSearch.DIRECTION_IN, HybridSearch.DIRECTION_BOTH);
    assertThat(HybridSearch.LEG_NAMES)
        .containsExactly(HybridSearch.LEG_VECTOR, HybridSearch.LEG_FULLTEXT, HybridSearch.LEG_EXPAND);
    assertThat(FullTextIndexMetadata.SIMILARITIES)
        .containsExactly(FullTextIndexMetadata.SIMILARITY_BM25, FullTextIndexMetadata.SIMILARITY_CLASSIC);
  }
}
