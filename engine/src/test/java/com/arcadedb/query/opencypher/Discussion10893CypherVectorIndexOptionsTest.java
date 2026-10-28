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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Discussion #10893, the follow-up of #10678: Neo4j's {@code CREATE VECTOR INDEX} names the quantization with
 * {@code vector.quantization.type} since 2026.06 ({@code vector.quantization.enabled} is deprecated), the settings only ArcadeDB
 * has could not be given from Cypher at all, and the statement {@code SHOW INDEXES} lists dropped every setting but the
 * dimensions and the similarity, so it re-created a different index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Discussion10893CypherVectorIndexOptionsTest extends TestHelper {

  @Test
  void quantizationTypeMapsNeo4jNamesOntoTheLsmVectorQuantization() {
    assertThat(quantizationOf("'none'")).isEqualTo(VectorQuantizationType.NONE);
    assertThat(quantizationOf("'scalar'")).isEqualTo(VectorQuantizationType.INT8);
    assertThat(quantizationOf("'BINARY'")).isEqualTo(VectorQuantizationType.BINARY);
  }

  @Test
  void anAbsentQuantizationKeepsTheExactVectors() {
    // Neo4j quantizes by default (binary since 2026.08): ArcadeDB keeps its own default, an exact index, so recall never drops
    // behind the back of a script that did not ask for it. SHOW INDEXES then names it, so the statement means the same in both
    command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3}}");
    assertThat(vectorMetadata("v").quantizationType).isEqualTo(VectorQuantizationType.NONE);
    assertThat(createStatement("v")).contains("`vector.quantization.type`: 'none'");
  }

  @Test
  void theDeprecatedEnabledFlagMustAgreeWithTheType() {
    command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.quantization.enabled`: true, `vector.quantization.type`: 'binary'}}");
    assertThat(vectorMetadata("v").quantizationType).isEqualTo(VectorQuantizationType.BINARY);

    assertThat(refusal("CREATE VECTOR INDEX w FOR (n:L) ON n.f OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.quantization.enabled`: false, `vector.quantization.type`: 'scalar'}}"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("vector.quantization.enabled")
        .hasMessageContaining("vector.quantization.type");
    assertThat(refusal("CREATE VECTOR INDEX w FOR (n:L) ON n.f OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.quantization.type`: 'product'}}")).hasMessageContaining("'none', 'scalar' or 'binary'");
    assertThat(database.getSchema().existsIndex("w")).isFalse();
  }

  @Test
  void arcadeDbSettingsAreGivenInIndexConfigUnderTheirOwnNames() {
    command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 4, efSearch: 64, "
        + "neighborOverflowFactor: 1.5, addHierarchy: true, idPropertyName: 'uid'}}");

    final LSMVectorIndexMetadata metadata = vectorMetadata("v");
    assertThat(metadata.dimensions).isEqualTo(4);
    assertThat(metadata.efSearch).isEqualTo(64);
    assertThat(metadata.neighborOverflowFactor).isEqualTo(1.5f);
    assertThat(metadata.addHierarchy).isTrue();
    assertThat(metadata.idPropertyName).isEqualTo("uid");

    // the native names alone are enough, as in SQL's METADATA
    command("CREATE VECTOR INDEX p FOR (n:P) ON n.e OPTIONS {indexConfig: {dimensions: 4, similarity: 'EUCLIDEAN', "
        + "quantization: 'PRODUCT', pqSubspaces: 2}}");
    assertThat(vectorMetadata("p").quantizationType).isEqualTo(VectorQuantizationType.PRODUCT);
    assertThat(vectorMetadata("p").pqSubspaces).isEqualTo(2);
  }

  @Test
  void aSettingGivenTwiceOrUnknownIsRefusedAndLeavesNothingBehind() {
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, dimensions: 3}}"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("vector.dimensions").hasMessageContaining("dimensions");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.hnsw.m`: 8, maxConnections: 16}}")).hasMessageContaining("maxConnections");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.quantization.type`: 'scalar', quantization: 'INT8'}}")).hasMessageContaining("quantization");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, efSerch: 64}}"))
        .hasMessageContaining("efSerch").hasMessageContaining("efSearch");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, efSearch: 0}}"))
        .hasMessageContaining("efSearch");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.default_search_expansion_factor`: 2.0}}")).hasMessageContaining("vector.default_search_expansion_factor")
        .hasMessageContaining("efSearch");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {efSearch: 64}}"))
        .hasMessageContaining("vector.dimensions");

    assertThat(database.getSchema().existsType("L")).isFalse();
    assertThat(database.getSchema().existsIndex("v")).isFalse();
  }

  @Test
  void showIndexesListsEverySettingSoTheStatementRecreatesTheSameIndex() {
    command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 4, "
        + "`vector.similarity_function`: 'euclidean', `vector.quantization.type`: 'scalar', `vector.hnsw.m`: 8, "
        + "`vector.hnsw.ef_construction`: 50, efSearch: 64, neighborOverflowFactor: 1.5}}");
    // an odd degree has no Neo4j spelling (vector.hnsw.m is doubled), and 100 is efSearch's default but asked for (#6494)
    command("CREATE VECTOR INDEX o FOR ()-[r:R]-() ON r.e OPTIONS {indexConfig: {dimensions: 2, maxConnections: 7, "
        + "efSearch: 100}}");
    command("CREATE VECTOR INDEX p FOR (n:P) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 4, quantization: 'PRODUCT', "
        + "pqSubspaces: 2}}");

    final String vector = createStatement("v");
    final String odd = createStatement("o");
    final String product = createStatement("p");
    assertThat(vector).isEqualTo("CREATE VECTOR INDEX `v` FOR (n:`L`) ON n.`e` OPTIONS {indexConfig: {`vector.dimensions`: 4, "
        + "`vector.similarity_function`: 'euclidean', `vector.quantization.type`: 'scalar', `vector.hnsw.m`: 8, "
        + "`vector.hnsw.ef_construction`: 50, `efSearch`: 64, `neighborOverflowFactor`: 1.5}}");
    assertThat(odd).isEqualTo("CREATE VECTOR INDEX `o` FOR ()-[r:`R`]-() ON r.`e` OPTIONS {indexConfig: {`vector.dimensions`: 2, "
        + "`vector.similarity_function`: 'cosine', `vector.quantization.type`: 'none', `efSearch`: 100, `maxConnections`: 7}}");
    assertThat(product).isEqualTo("CREATE VECTOR INDEX `p` FOR (n:`P`) ON n.`e` OPTIONS {indexConfig: {`vector.dimensions`: 4, "
        + "`vector.similarity_function`: 'cosine', `quantization`: 'PRODUCT', `pqSubspaces`: 2}}");

    // the options column carries the same settings, as Neo4j's does
    assertThat(options("v")).isEqualTo(Map.of("indexProvider", "vector-2.0", "indexConfig",
        Map.of("vector.dimensions", 4, "vector.similarity_function", "euclidean", "vector.quantization.type", "scalar",
            "vector.hnsw.m", 8, "vector.hnsw.ef_construction", 50, "efSearch", 64, "neighborOverflowFactor", 1.5f)));

    final LSMVectorIndexMetadata before = vectorMetadata("v");
    command("DROP INDEX v");
    command("DROP INDEX o");
    command("DROP INDEX p");
    command(vector);
    command(odd);
    command(product);
    assertThat(createStatement("v")).isEqualTo(vector);
    assertThat(createStatement("o")).isEqualTo(odd);
    assertThat(createStatement("p")).isEqualTo(product);

    final LSMVectorIndexMetadata after = vectorMetadata("v");
    assertThat(after.maxConnections).isEqualTo(before.maxConnections).isEqualTo(16);
    assertThat(after.beamWidth).isEqualTo(before.beamWidth).isEqualTo(50);
    assertThat(after.quantizationType).isEqualTo(before.quantizationType);
    assertThat(after.efSearch).isEqualTo(before.efSearch);
    assertThat(vectorMetadata("o").efSearchConfigured).isTrue();
  }

  @Test
  void fullTextIndexTakesArcadeDbSettingsAndListsThem() {
    command("CREATE FULLTEXT INDEX f FOR (n:Doc) ON EACH [n.title, n.body] OPTIONS {indexConfig: {`fulltext.analyzer`: 'english', "
        + "defaultOperator: 'AND', bm25_k1: 1.5, title_boost: 2.0}}");

    final FullTextIndexMetadata metadata = ((LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName("f"))
        .getIndexesOnBuckets()[0]).getFullTextMetadata();
    assertThat(metadata.getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    assertThat(metadata.getDefaultOperator()).isEqualTo("AND");
    assertThat(metadata.getBm25K1()).isEqualTo(1.5f);
    assertThat(metadata.getFieldBoost("title")).isEqualTo(2.0f);

    final String fulltext = createStatement("f");
    assertThat(fulltext).isEqualTo("CREATE FULLTEXT INDEX `f` FOR (n:`Doc`) ON EACH [n.`title`, n.`body`] OPTIONS {indexConfig: "
        + "{`fulltext.analyzer`: 'english', `bm25_k1`: 1.5, `defaultOperator`: 'AND', `title_boost`: 2.0}}");
    command("DROP INDEX f");
    command(fulltext);
    assertThat(createStatement("f")).isEqualTo(fulltext);

    assertThat(refusal("CREATE FULLTEXT INDEX g FOR (n:Doc) ON EACH [n.x] OPTIONS {indexConfig: {`fulltext.analyzer`: 'english', "
        + "analyzer: 'org.apache.lucene.analysis.en.EnglishAnalyzer'}}")).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("fulltext.analyzer").hasMessageContaining("analyzer");
    assertThat(refusal("CREATE FULLTEXT INDEX g FOR (n:Doc) ON EACH [n.x] OPTIONS {indexConfig: {titel_boost: 2.0}}"))
        .hasMessageContaining("titel");
    assertThat(database.getSchema().existsIndex("g")).isFalse();
  }

  @Test
  void aPerFieldSettingOfAPropertyWithABacktickRoundTrips() {
    // the per-field key carries the property name, so it is quoted as a property name is
    command("CREATE FULLTEXT INDEX f FOR (n:Doc) ON EACH [n.`ti``tle`] OPTIONS {indexConfig: {`ti``tle_boost`: 3.0}}");
    assertThat(((LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName("f")).getIndexesOnBuckets()[0])
        .getFullTextMetadata().getFieldBoost("ti`tle")).isEqualTo(3.0f);

    final String fulltext = createStatement("f");
    assertThat(fulltext).isEqualTo("CREATE FULLTEXT INDEX `f` FOR (n:`Doc`) ON EACH [n.`ti``tle`] OPTIONS {indexConfig: "
        + "{`ti``tle_boost`: 3.0}}");
    command("DROP INDEX f");
    command(fulltext);
    assertThat(createStatement("f")).isEqualTo(fulltext);
  }

  private VectorQuantizationType quantizationOf(final String type) {
    command("DROP INDEX q IF EXISTS");
    command("CREATE VECTOR INDEX q FOR (n:Q) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.quantization.type`: "
        + type + "}}");
    return vectorMetadata("q").quantizationType;
  }

  private LSMVectorIndexMetadata vectorMetadata(final String name) {
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(name)).getIndexesOnBuckets())
      if (bucketIndex instanceof LSMVectorIndex vector)
        return vector.getMetadata();
    throw new AssertionError("no vector index behind " + name);
  }

  private String createStatement(final String name) {
    return showIndex(name).getProperty("createStatement");
  }

  private Map<String, Object> options(final String name) {
    return showIndex(name).getProperty("options");
  }

  private Result showIndex(final String name) {
    try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES YIELD * WHERE name = $name", "name", name)) {
      return rs.next();
    }
  }

  /** What the statement is refused with, unwrapped down to the cause the engine raised. */
  private Throwable refusal(final String cypher) {
    try {
      command(cypher);
    } catch (final RuntimeException e) {
      Throwable cause = e;
      while (cause.getCause() != null)
        cause = cause.getCause();
      return cause;
    }
    throw new AssertionError("Not refused: " + cypher);
  }

  private void command(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
