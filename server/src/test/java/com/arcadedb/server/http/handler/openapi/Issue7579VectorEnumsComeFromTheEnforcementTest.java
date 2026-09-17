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
import com.arcadedb.query.search.HybridSearch;
import com.arcadedb.schema.FullTextIndexMetadata;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7579VectorEnumsComeFromTheEnforcementTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new io.swagger.v3.oas.models.Paths());
    openAPI.setComponents(new io.swagger.v3.oas.models.Components());
    new VectorApiSpec().contribute(openAPI);
  }

  private Schema<?> schema(final String name) {
    return openAPI.getComponents().getSchemas().get(name);
  }

  private static Schema<?> property(final Schema<?> owner, final String name) {
    return (Schema<?>) owner.getProperties().get(name);
  }

  @Test
  void theFusionStrategyIsTheSetTheServerEnforces() {
    assertThat(property(schema("HybridSearchRequest"), "fusionStrategy").getEnum())
        .as("HybridSearch.strategyOf accepts exactly these, so a client can reject a typo locally")
        .isEqualTo(HybridSearch.FUSION_STRATEGIES);

    assertThat(property(schema("HybridSearchResponse"), "fusionStrategy").getEnum())
        .as("the response echoes the request's value upper-cased, so it is the same set")
        .isEqualTo(HybridSearch.FUSION_STRATEGIES);
  }

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
