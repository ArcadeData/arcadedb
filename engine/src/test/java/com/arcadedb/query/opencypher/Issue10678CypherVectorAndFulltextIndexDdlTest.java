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
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10678: Neo4j 5's {@code CREATE VECTOR INDEX} and {@code CREATE FULLTEXT INDEX} were refused, so a Neo4j client could
 * not create the indexes {@code db.index.vector.queryNodes} and {@code db.index.fulltext.queryNodes} already query.
 */
class Issue10678CypherVectorAndFulltextIndexDdlTest extends TestHelper {

  private static final String VECTOR_DDL = "CREATE VECTOR INDEX v IF NOT EXISTS FOR (n:`L`) ON n.`e` OPTIONS { indexConfig: { "
      + "`vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }";

  @Test
  void createVectorIndexBuildsANamedLsmVectorIndexOnAnArrayOfFloatsProperty() {
    assertThat(stats(VECTOR_DDL).getIndexesAdded()).isEqualTo(1);

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("v");
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_VECTOR);
    final LSMVectorIndex vector = vectorIndex(index);
    assertThat(vector.getDimensions()).isEqualTo(3);
    assertThat(vector.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.COSINE);
    assertThat(database.getSchema().getType("L").getProperty("e").getType()).isEqualTo(Type.ARRAY_OF_FLOATS);

    // the connector's write path, then the read side the issue names
    command("MERGE (n:L {id: 'a'}) SET n += {e: [1.0, 0.0, 0.0]}");
    command("MERGE (n:L {id: 'b'}) SET n += {e: [0.0, 1.0, 0.0]}");
    command("MERGE (n:L {id: 'c'}) SET n += {e: [0.9, 0.1, 0.0]}");
    assertThat(nearestIds("v", 2, List.of(1.0, 0.0, 0.0))).containsExactly("a", "c");

    // idempotent, and DROP INDEX <name> IF EXISTS removes it
    assertThat(stats(VECTOR_DDL).getIndexesAdded()).isZero();
    assertThat(stats("DROP INDEX v IF EXISTS").getIndexesRemoved()).isEqualTo(1);
    assertThat(database.getSchema().existsIndex("v")).isFalse();
  }

  @Test
  void createVectorIndexOverVectorsAlreadyWrittenAsLists() {
    // the order the reporter could not use: data first, written by MERGE as a list of floats, then the index
    command("MERGE (n:L {id: 'a'}) SET n += {e: [1.0, 0.0, 0.0]}");
    command("MERGE (n:L {id: 'b'}) SET n += {e: [0.0, 1.0, 0.0]}");

    assertThat(stats(VECTOR_DDL).getIndexesAdded()).isEqualTo(1);
    assertThat(nearestIds("v", 1, List.of(0.0, 1.0, 0.0))).containsExactly("b");
  }

  @Test
  void createVectorIndexTranslatesTheNeo4jSettings() {
    command("CREATE VECTOR INDEX v FOR (n:L) ON (n.e) OPTIONS {indexProvider: 'vector-2.0', indexConfig: {`vector.dimensions`: 4, "
        + "`vector.similarity_function`: 'EUCLIDEAN', `vector.hnsw.m`: 8, `vector.hnsw.ef_construction`: 50, "
        + "`vector.quantization.enabled`: false}}");

    final LSMVectorIndex vector = vectorIndex((TypeIndex) database.getSchema().getIndexByName("v"));
    assertThat(vector.getDimensions()).isEqualTo(4);
    assertThat(vector.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
    // JVector does not double the degree at the base layer as Lucene's HNSW does
    assertThat(vector.getMetadata().maxConnections).isEqualTo(16);
    assertThat(vector.getMetadata().beamWidth).isEqualTo(50);
  }

  @Test
  void createVectorIndexTakesItsOptionsFromAParameter() {
    try (final ResultSet rs = database.command("opencypher", "CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS $options", "options",
        Map.of("indexConfig", Map.of("vector.dimensions", 5)))) {
      rs.stream().count();
    }
    assertThat(vectorIndex((TypeIndex) database.getSchema().getIndexByName("v")).getDimensions()).isEqualTo(5);
  }

  @Test
  void createVectorIndexOnARelationshipType() {
    command("CREATE VECTOR INDEX rv FOR ()-[r:REL]-() ON r.e OPTIONS {indexConfig: {`vector.dimensions`: 2}}");
    assertThat(database.getSchema().getType("REL").getPolymorphicIndexByProperties("e").getName()).isEqualTo("rv");
  }

  @Test
  void aRefusedVectorIndexLeavesNothingBehind() {
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("vector.dimensions");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.similarity_function`: 'manhattan'}}")).hasMessageContaining("manhattan");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.dimension`: 3}}")).hasMessageContaining("setting `vector.dimension`");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 0}}"))
        .hasMessageContaining("positive integer");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.hnsw.m`: 2000000000}}")).hasMessageContaining("too large");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {config: {}}"))
        .hasMessageContaining("config");
    assertThat(refusal("CREATE VECTOR INDEX v FOR (n:A|B) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3}}"))
        .hasMessageContaining("several labels");

    assertThat(database.getSchema().existsType("L")).isFalse();
    assertThat(database.getSchema().existsIndex("v")).isFalse();
  }

  @Test
  void createFulltextIndexBuildsAFullTextIndexQueryNodesReads() {
    assertThat(stats("CREATE FULLTEXT INDEX f IF NOT EXISTS FOR (n:`L`) ON EACH [n.`text`]").getIndexesAdded()).isEqualTo(1);

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("f");
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);
    // the keys are tokens, so the property keeps the type of what is written
    assertThat(database.getSchema().getType("L").existsProperty("text")).isFalse();

    command("MERGE (n:L {id: 'a'}) SET n += {text: 'the quick brown fox'}");
    command("MERGE (n:L {id: 'b'}) SET n += {text: 'a lazy dog'}");
    assertThat(fulltextIds("f", "fox")).containsExactly("a");

    assertThat(stats("CREATE FULLTEXT INDEX f IF NOT EXISTS FOR (n:`L`) ON EACH [n.`text`]").getIndexesAdded()).isZero();
    assertThat(stats("DROP INDEX f IF EXISTS").getIndexesRemoved()).isEqualTo(1);
    assertThat(database.getSchema().existsIndex("f")).isFalse();
  }

  @Test
  void createFulltextIndexOverSeveralPropertiesWithAnAnalyzer() {
    command("CREATE (:Doc {title: 'Running', body: 'nothing'}), (:Doc {title: 'other', body: 'runs'})");
    command("CREATE FULLTEXT INDEX f FOR (n:Doc) ON EACH [n.title, n.body] OPTIONS {indexConfig: {`fulltext.analyzer`: 'english', "
        + "`fulltext.eventually_consistent`: false}}");

    final LSMTreeFullTextIndex fullText = (LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName("f"))
        .getIndexesOnBuckets()[0];
    assertThat(fullText.getFullTextMetadata().getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.en.EnglishAnalyzer");
    // the English analyzer stems both 'running' and 'runs' to 'run'
    assertThat(fulltextIds("f", "run")).hasSize(2);
  }

  @Test
  void aRefusedFulltextIndexLeavesNothingBehind() {
    assertThat(refusal("CREATE FULLTEXT INDEX f FOR (n:L) ON EACH [n.t] OPTIONS {indexConfig: {`fulltext.analyzer`: "
        + "'klingon'}}")).isInstanceOf(CommandParsingException.class).hasMessageContaining("klingon");
    assertThat(refusal("CREATE FULLTEXT INDEX f FOR (n:L) ON EACH [n.t] OPTIONS {indexConfig: {`fulltext.analyzer`: "
        + "'java.lang.String'}}")).hasMessageContaining("java.lang.String");
    assertThat(refusal("CREATE FULLTEXT INDEX f FOR (n:L) ON EACH [n.t] OPTIONS {indexConfig: {`fulltext.analyzer`: "
        + "'org.example.NoSuchAnalyzer'}}")).hasMessageContaining("org.example.NoSuchAnalyzer");
    assertThat(refusal("CREATE FULLTEXT INDEX f FOR (n:L|M) ON EACH [n.t]"))
        .hasMessageContaining("several labels");
    assertThat(database.getSchema().existsType("L")).isFalse();
  }

  @Test
  void showIndexesListsAStatementThatRecreatesTheIndex() {
    command(VECTOR_DDL);
    command("CREATE FULLTEXT INDEX f FOR (n:L) ON EACH [n.a, n.b] OPTIONS {indexConfig: {`fulltext.analyzer`: 'whitespace'}}");
    command("CREATE FULLTEXT INDEX g FOR ()-[r:R]-() ON EACH [r.c]");

    final String vector = createStatement("v");
    final String fulltext = createStatement("f");
    final String relFulltext = createStatement("g");
    assertThat(vector).isEqualTo("CREATE VECTOR INDEX `v` FOR (n:`L`) ON n.`e` OPTIONS {indexConfig: {`vector.dimensions`: 3, "
        + "`vector.similarity_function`: 'cosine', `vector.quantization.type`: 'none'}}");
    assertThat(fulltext).isEqualTo(
        "CREATE FULLTEXT INDEX `f` FOR (n:`L`) ON EACH [n.`a`, n.`b`] OPTIONS {indexConfig: {`fulltext.analyzer`: 'whitespace'}}");
    assertThat(relFulltext).isEqualTo("CREATE FULLTEXT INDEX `g` FOR ()-[r:`R`]-() ON EACH [r.`c`]");

    // each runs as it is listed
    command("DROP INDEX v");
    command("DROP INDEX f");
    command("DROP INDEX g");
    command(vector);
    command(fulltext);
    command(relFulltext);
    assertThat(createStatement("v")).isEqualTo(vector);
    assertThat(createStatement("f")).isEqualTo(fulltext);
    assertThat(createStatement("g")).isEqualTo(relFulltext);
  }

  private static LSMVectorIndex vectorIndex(final TypeIndex index) {
    for (final IndexInternal bucketIndex : index.getIndexesOnBuckets())
      if (bucketIndex instanceof LSMVectorIndex vector)
        return vector;
    throw new AssertionError("no vector index behind " + index.getName());
  }

  private List<String> nearestIds(final String index, final int k, final List<Double> vector) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.vector.queryNodes($index, $k, $vector) YIELD node, score RETURN node.id AS id ORDER BY score DESC", "index",
        index, "k", k, "vector", vector)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private List<String> fulltextIds(final String index, final String query) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL db.index.fulltext.queryNodes($index, $query) YIELD node RETURN node.id AS id", "index", index, "query", query)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private String createStatement(final String name) {
    try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES YIELD name, createStatement WHERE name = $name", "name",
        name)) {
      final Result row = rs.next();
      return row.getProperty("createStatement");
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

  private QueryStatistics stats(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
      return rs.getStatistics().orElseThrow();
    }
  }
}
