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
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.fulltext.StandardEnglishStopWordsAnalyzer;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MultiTypeIndexes;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cypher index DDL: CREATE/DROP INDEX for range, vector and full-text indexes, index names and SQL interoperability.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherIndexDdlTest {
  /**
   * Issue #9705: {@code CREATE INDEX <name>} ignored the name, so {@code DROP INDEX <name>} dropped nothing, and an unknown name
   * reported a removal without {@code IF EXISTS}. Index and constraint names share one namespace, as in Neo4j.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9705CypherIndexName extends TestHelper {

    @Test
    void createIndexNamesTheIndexAndDropIndexDropsIt() {
      final QueryStatistics created = stats("CREATE INDEX person_name FOR (n:Person) ON (n.name)");
      assertThat(created.getIndexesAdded()).isEqualTo(1);
      assertThat(database.getSchema().existsIndex("person_name")).isTrue();
      assertThat(database.getSchema().existsIndex("Person[name]")).isFalse();
      assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name").getName()).isEqualTo("person_name");

      // the index serves the queries under its name too
      command("CREATE (:Person {name: 'Ann'})");
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person) WHERE n.name = 'Ann' RETURN count(n) AS c")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
      }

      final QueryStatistics dropped = stats("DROP INDEX person_name");
      assertThat(dropped.getIndexesRemoved()).isEqualTo(1);
      assertThat(database.getSchema().existsIndex("person_name")).isFalse();
      assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name")).isNull();
    }

    @Test
    void theNameSurvivesAReopen() {
      command("CREATE INDEX person_name FOR (n:Person) ON (n.name)");
      reopenDatabase();
      assertThat(database.getSchema().existsIndex("person_name")).isTrue();
      assertThat(stats("DROP INDEX person_name").getIndexesRemoved()).isEqualTo(1);
      assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name")).isNull();
    }

    @Test
    void unnamedIndexKeepsTheGeneratedName() {
      command("CREATE INDEX FOR (n:Person) ON (n.name)");
      assertThat(database.getSchema().existsIndex("Person[name]")).isTrue();
      assertThat(stats("DROP INDEX `Person[name]`").getIndexesRemoved()).isEqualTo(1);
      assertThat(database.getSchema().existsIndex("Person[name]")).isFalse();
    }

    @Test
    void dropOfAnUnknownIndexRaisesUnlessIfExists() {
      assertThatThrownBy(() -> command("DROP INDEX nosuchindex")).rootCause().isInstanceOf(SchemaException.class).hasMessageContaining("nosuchindex");
      assertThat(stats("DROP INDEX nosuchindex IF EXISTS").getIndexesRemoved()).isZero();
    }

    @Test
    void aTakenNameRaisesUnlessIfNotExists() {
      command("CREATE INDEX idx FOR (n:Person) ON (n.name)");

      // the same name on other properties
      assertThatThrownBy(() -> command("CREATE INDEX idx FOR (n:Person) ON (n.age)")).rootCause().isInstanceOf(SchemaException.class)
          .hasMessageContaining("idx");
      assertThat(database.getSchema().getType("Person").existsProperty("age")).isFalse();
      assertThat(stats("CREATE INDEX idx IF NOT EXISTS FOR (n:Person) ON (n.age)").getIndexesAdded()).isZero();
      assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("age")).isNull();

      // an equivalent index under another name: refused, or a no-op with IF NOT EXISTS
      assertThatThrownBy(() -> command("CREATE INDEX other FOR (n:Person) ON (n.name)")).isInstanceOf(RuntimeException.class);
      assertThat(stats("CREATE INDEX other IF NOT EXISTS FOR (n:Person) ON (n.name)").getIndexesAdded()).isZero();
      assertThat(database.getSchema().existsIndex("other")).isFalse();
      assertThat(database.getSchema().existsIndex("idx")).isTrue();
    }

    @Test
    void indexAndConstraintNamesShareOneNamespace() {
      command("CREATE CONSTRAINT c_email FOR (n:Person) REQUIRE n.email IS UNIQUE");
      assertThatThrownBy(() -> command("CREATE INDEX c_email FOR (n:Person) ON (n.name)")).rootCause().isInstanceOf(SchemaException.class)
          .hasMessageContaining("constraint");
      assertThat(database.getSchema().getType("Person").getPolymorphicIndexByProperties("name")).isNull();

      command("CREATE INDEX i_name FOR (n:Person) ON (n.name)");
      assertThatThrownBy(() -> command("CREATE CONSTRAINT i_name FOR (n:Person) REQUIRE n.age IS UNIQUE"))
          .rootCause().isInstanceOf(SchemaException.class).hasMessageContaining("index");

      // a plain index is no constraint: DROP CONSTRAINT does not reach it
      assertThatThrownBy(() -> command("DROP CONSTRAINT i_name")).rootCause().isInstanceOf(SchemaException.class);
      assertThat(database.getSchema().existsIndex("i_name")).isTrue();
    }

    @Test
    void theIndexOfAConstraintIsDroppedThroughTheConstraint() {
      command("CREATE CONSTRAINT c_email FOR (n:Person) REQUIRE n.email IS UNIQUE");
      final String indexName = database.getSchema().getType("Person").getPolymorphicIndexByProperties("email").getName();

      assertThatThrownBy(() -> command("DROP INDEX `" + indexName + "`")).rootCause().isInstanceOf(SchemaException.class)
          .hasMessageContaining("c_email");
      assertThat(database.getSchema().existsIndex(indexName)).isTrue();

      command("DROP CONSTRAINT c_email");
      assertThat(database.getSchema().existsIndex(indexName)).isFalse();
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

  /**
   * Issue #10678: Neo4j 5's {@code CREATE VECTOR INDEX} and {@code CREATE FULLTEXT INDEX} were refused, so a Neo4j client could
   * not create the indexes {@code db.index.vector.queryNodes} and {@code db.index.fulltext.queryNodes} already query.
   */
  @Nested
  class Issue10678CypherVectorAndFulltextIndexDdl extends TestHelper {

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

  /**
   * Issue #10863: forms of Neo4j's {@code CREATE VECTOR INDEX} and {@code CREATE FULLTEXT INDEX} that #10678 still refused - the
   * additional filtering properties of a vector index ({@code WITH [n.a, n.b]}), a vector index without
   * {@code vector.dimensions}, and the {@code standard} analyzer (the standard analyzer with English stop words).
   */
  @Nested
  class Issue10863CypherIndexDdlFollowUps extends TestHelper {

    @Test
    void vectorIndexWithFilteringPropertiesIsCreatedAndTheSearchClauseFiltersOnThem() {
      command("CREATE VECTOR INDEX v FOR (n:Doc) ON n.e WITH [n.lang, n.year] OPTIONS {indexConfig: {`vector.dimensions`: 2}}");

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("v");
      // the filtering properties are not part of the vector index: it is still on the vector alone
      assertThat(index.getPropertyNames()).containsExactly("e");
      assertThat(vectorIndex(index).getDimensions()).isEqualTo(2);

      command("CREATE (:Doc {id: 'a', lang: 'en', year: 2020, e: [1.0, 0.0]})");
      command("CREATE (:Doc {id: 'b', lang: 'fr', year: 2021, e: [0.99, 0.01]})");
      command("CREATE (:Doc {id: 'c', lang: 'en', year: 2022, e: [0.0, 1.0]})");

      assertThat(ids("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX v FOR [1.0, 0.0] WHERE d.lang = 'en' LIMIT 2) SCORE AS s "
          + "RETURN d.id AS id ORDER BY s DESC")).containsExactly("a", "c");
      assertThat(ids("MATCH (d:Doc) SEARCH d IN (VECTOR INDEX v FOR [1.0, 0.0] WHERE d.year > 2020 LIMIT 1) RETURN d.id AS id"))
          .containsExactly("b");
    }

    @Test
    void vectorIndexWithFilteringPropertiesOnARelationshipType() {
      command("CREATE VECTOR INDEX rv FOR ()-[r:REL]-() ON r.e WITH [r.kind] OPTIONS {indexConfig: {`vector.dimensions`: 2}}");
      assertThat(database.getSchema().getType("REL").getPolymorphicIndexByProperties("e").getName()).isEqualTo("rv");
    }

    @Test
    void vectorIndexWithoutDimensionsTakesThemFromTheVectorsAlreadyWritten() {
      command("CREATE (:L {id: 'none'})");
      command("CREATE (:L {id: 'a', e: [1.0, 0.0, 0.0]})");
      command("CREATE (:L {id: 'b', e: [0.0, 1.0, 0.0]})");

      command("CREATE VECTOR INDEX v FOR (n:L) ON n.e");

      final LSMVectorIndex vector = vectorIndex((TypeIndex) database.getSchema().getIndexByName("v"));
      assertThat(vector.getDimensions()).isEqualTo(3);
      assertThat(vector.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.COSINE);
      assertThat(nearestIds("v", 1, List.of(0.0, 1.0, 0.0))).containsExactly("b");

      // the inferred dimension is named by SHOW INDEXES, so the statement it lists builds the same index without the data
      assertThat(createStatement("v")).contains("`vector.dimensions`: 3");
    }

    @Test
    void valuesThatAreNoVectorAreSkippedWhenTheDimensionsAreTakenFromTheData() {
      // a scalar, a string a vector could be parsed from, a list of strings and an empty list come before the first vector
      command("CREATE (:L {id: 'n', e: 5})");
      command("CREATE (:L {id: 's', e: '[1.0, 2.0]'})");
      command("CREATE (:L {id: 'l', e: ['x', 'y']})");
      command("CREATE (:L {id: 'z', e: []})");
      command("CREATE (:L {id: 'a', e: [1.0, 0.0, 0.0, 0.0]})");

      command("CREATE VECTOR INDEX v FOR (n:L) ON n.e");

      assertThat(vectorIndex((TypeIndex) database.getSchema().getIndexByName("v")).getDimensions()).isEqualTo(4);
    }

    @Test
    void vectorIndexWithOptionsButWithoutDimensionsTakesThemFromTheData() {
      command("CREATE (:L {id: 'a', e: [1.0, 0.0]})");

      command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.similarity_function`: 'euclidean'}}");

      final LSMVectorIndex vector = vectorIndex((TypeIndex) database.getSchema().getIndexByName("v"));
      assertThat(vector.getDimensions()).isEqualTo(2);
      assertThat(vector.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
    }

    @Test
    void vectorsOfAnotherLengthThanTheInferredOneAreLeftOutAsWithNamedDimensions() {
      command("CREATE (:L {id: 'a', e: [1.0, 0.0, 0.0]})");
      command("CREATE (:L {id: 'b', e: [0.0, 1.0]})");
      command("CREATE (:M {id: 'a', e: [1.0, 0.0, 0.0]})");
      command("CREATE (:M {id: 'b', e: [0.0, 1.0]})");

      // the first vector decides; the record whose vector has another length is not indexed, exactly as when the statement
      // names that dimension itself
      command("CREATE VECTOR INDEX inferred FOR (n:L) ON n.e");
      command("CREATE VECTOR INDEX named FOR (n:M) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 3}}");

      assertThat(vectorIndex((TypeIndex) database.getSchema().getIndexByName("inferred")).getDimensions()).isEqualTo(3);
      assertThat(nearestIds("inferred", 2, List.of(1.0, 0.0, 0.0))).containsExactly("a");
      assertThat(nearestIds("named", 2, List.of(1.0, 0.0, 0.0))).containsExactly("a");
    }

    @Test
    void standardAnalyzerDropsEnglishStopWords() {
      command("CREATE FULLTEXT INDEX f FOR (n:Doc) ON EACH [n.text] OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard'}}");
      command("CREATE FULLTEXT INDEX g FOR (n:Note) ON EACH [n.text]");

      final LSMTreeFullTextIndex fullText = (LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName("f"))
          .getIndexesOnBuckets()[0];
      assertThat(fullText.getFullTextMetadata().getAnalyzerClass()).isEqualTo(StandardEnglishStopWordsAnalyzer.class.getName());

      command("CREATE (:Doc {id: 'a', text: 'The Quick fox'}), (:Note {id: 'a', text: 'The Quick fox'})");

      // 'the' is an English stop word for 'standard', and a term like any other for the default 'standard-no-stop-words'
      assertThat(fulltextIds("f", "the")).isEmpty();
      assertThat(fulltextIds("f", "quick")).containsExactly("a");
      assertThat(fulltextIds("g", "the")).containsExactly("a");

      // SHOW INDEXES names the analyzer by its Neo4j name, and the statement it lists runs as it is
      final String statement = createStatement("f");
      assertThat(statement).contains("`fulltext.analyzer`: 'standard'");
      command("DROP INDEX f");
      command(statement);
      assertThat(createStatement("f")).isEqualTo(statement);
      assertThat(fulltextIds("f", "the")).isEmpty();
    }

    @Test
    void standardAnalyzerTokenizesAsTheStandardAnalyzer() throws IOException {
      try (final StandardEnglishStopWordsAnalyzer analyzer = new StandardEnglishStopWordsAnalyzer()) {
        final List<String> tokens = new ArrayList<>();
        try (final TokenStream stream = analyzer.tokenStream("f", "An Apple and THE pear-tree, it is")) {
          final CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
          stream.reset();
          while (stream.incrementToken())
            tokens.add(term.toString());
          stream.end();
        }
        assertThat(tokens).containsExactly("apple", "pear", "tree");
      }
    }

    private static LSMVectorIndex vectorIndex(final TypeIndex index) {
      for (final IndexInternal bucketIndex : index.getIndexesOnBuckets())
        if (bucketIndex instanceof LSMVectorIndex vector)
          return vector;
      throw new AssertionError("no vector index behind " + index.getName());
    }

    private List<String> ids(final String cypher) {
      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      return ids;
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
  }

  /**
   * Issue #10914: {@code CREATE VECTOR INDEX} and {@code CREATE FULLTEXT INDEX} over several labels or relationship types
   * ({@code FOR (n:A|B)}) build one index per type, queried, listed and dropped by one name.
   */
  @Nested
  class Issue10914MultiLabelVectorAndFulltextIndex extends TestHelper {

    private static final String FULLTEXT_DDL = "CREATE FULLTEXT INDEX docs IF NOT EXISTS FOR (n:Article|Post) ON EACH [n.title, n.body]";
    private static final String VECTOR_DDL   = "CREATE VECTOR INDEX emb FOR (n:Article|Post) ON n.e OPTIONS {indexConfig: "
        + "{`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}";

    @Test
    void fulltextIndexOverSeveralLabelsIsQueriedByOneName() {
      assertThat(stats(FULLTEXT_DDL).getIndexesAdded()).isEqualTo(1);
      command("CREATE (:Article {id: 'a1', title: 'graph databases', body: 'fast traversal'})");
      command("CREATE (:Post {id: 'p1', title: 'cooking', body: 'a graph of recipes'})");
      command("CREATE (:Post {id: 'p2', title: 'travel', body: 'mountains'})");

      assertThat(fulltextIds("docs", "graph")).containsExactlyInAnyOrder("a1", "p1");
      assertThat(fulltextIds("docs", "mountains")).containsExactly("p2");

      // idempotent
      assertThat(stats(FULLTEXT_DDL).getIndexesAdded()).isZero();
    }

    @Test
    void fulltextIndexOverSeveralRelationshipTypes() {
      command("CREATE FULLTEXT INDEX notes FOR ()-[r:LIKES|RATES]-() ON EACH [r.note]");
      command("CREATE (a:P {id: 'a'})-[:LIKES {id: 'l1', note: 'loved the graph'}]->(b:P {id: 'b'})");
      command("MATCH (a:P {id: 'a'}), (b:P {id: 'b'}) CREATE (a)-[:RATES {id: 'r1', note: 'rated the graph'}]->(b)");
      command("MATCH (a:P {id: 'a'}), (b:P {id: 'b'}) CREATE (a)-[:RATES {id: 'r2', note: 'nothing'}]->(b)");

      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.fulltext.queryRelationships('notes', 'graph') YIELD relationship RETURN relationship.id AS id")) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      assertThat(ids).containsExactlyInAnyOrder("l1", "r1");

      assertThatThrownBy(() -> fulltextIds("notes", "graph")).hasMessageContaining("queryRelationships");
    }

    @Test
    void vectorIndexOverSeveralLabelsMergesTheNearestOfEveryType() {
      assertThat(stats(VECTOR_DDL).getIndexesAdded()).isEqualTo(1);
      command("CREATE (:Article {id: 'a1', e: [1.0, 0.0]})");
      command("CREATE (:Post {id: 'p1', e: [0.9, 0.1]})");
      command("CREATE (:Article {id: 'a2', e: [0.0, 1.0]})");
      command("CREATE (:Post {id: 'p2', e: [0.1, 0.9]})");

      assertThat(nearestIds("emb", 3, "[1.0, 0.0]")).containsExactly("a1", "p1", "p2");

      final List<String> viaSearch = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (d) SEARCH d IN (VECTOR INDEX emb FOR [0.0, 1.0] LIMIT 2) SCORE AS s RETURN d.id AS id ORDER BY s DESC")) {
        while (rs.hasNext())
          viaSearch.add(rs.next().getProperty("id"));
      }
      assertThat(viaSearch).containsExactly("a2", "p2");
    }

    @Test
    void showIndexesListsOneRowWithEveryLabel() {
      command(FULLTEXT_DDL);
      command("CREATE FULLTEXT INDEX single FOR (n:Other) ON EACH [n.t]");

      final List<String> names = new ArrayList<>();
      List<Object> labels = null;
      String createStatement = null;
      try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES YIELD name, labelsOrTypes, createStatement")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          names.add(row.getProperty("name"));
          if ("docs".equals(row.getProperty("name"))) {
            labels = row.getProperty("labelsOrTypes");
            createStatement = row.getProperty("createStatement");
          }
        }
      }
      assertThat(names).containsExactlyInAnyOrder("docs", "single");
      assertThat(labels).containsExactly("Article", "Post");
      assertThat(createStatement).startsWith("CREATE FULLTEXT INDEX `docs` FOR (n:`Article`|`Post`) ON EACH [n.`title`, n.`body`]");
    }

    @Test
    void dropIndexDropsEveryIndexItBuiltAndNothingElse() {
      command("CREATE FULLTEXT INDEX pre FOR (n:Article) ON EACH [n.title, n.body]");
      command(FULLTEXT_DDL);
      final TypeIndex postIndex = database.getSchema().getType("Post").getPolymorphicIndexByProperties("title", "body");
      assertThat(postIndex).isNotNull();

      assertThat(stats("DROP INDEX docs").getIndexesRemoved()).isEqualTo(1);

      // the name is gone, the index that was there before it stays, and the one it built for Post goes
      assertThatThrownBy(() -> fulltextIds("docs", "x")).isInstanceOf(RuntimeException.class);
      assertThat(database.getSchema().existsIndex("pre")).isTrue();
      assertThat(database.getSchema().getType("Post").getPolymorphicIndexByProperties("title", "body")).isNull();
      assertThat(stats("DROP INDEX docs IF EXISTS").getIndexesRemoved()).isZero();
      // the name is free again
      assertThat(stats(FULLTEXT_DDL).getIndexesAdded()).isEqualTo(1);
    }

    @Test
    void aTakenNameIsRefusedAndAFailedStatementLeavesNoIndexBehind() {
      command(FULLTEXT_DDL);
      assertThatThrownBy(() -> command("CREATE FULLTEXT INDEX docs FOR (n:A|B) ON EACH [n.t]"))
          .hasRootCauseInstanceOf(AlreadyExistsException.class);
      assertThatThrownBy(() -> command("CREATE VECTOR INDEX docs FOR (n:A|B) ON n.e OPTIONS {indexConfig: {`vector.dimensions`: 2}}"))
          .hasRootCauseInstanceOf(AlreadyExistsException.class);

      // a bad OPTIONS is refused before any type or index is created
      assertThatThrownBy(() -> command("CREATE FULLTEXT INDEX bad FOR (n:C|D) ON EACH [n.t] OPTIONS {indexConfig: "
          + "{`fulltext.analyzer`: 'klingon'}}")).isInstanceOf(CommandParsingException.class);
      assertThat(database.getSchema().existsType("C")).isFalse();
      assertThat(database.getSchema().existsType("D")).isFalse();
    }

    @Test
    void typesWithOtherVectorDimensionsAreRefusedAndNothingIsLeftBehind() {
      command("CREATE (:Foo {id: 'f', e: [1.0, 0.0]})");
      command("CREATE (:Bar {id: 'b', e: [1.0, 0.0, 0.0]})");

      assertThatThrownBy(() -> command("CREATE VECTOR INDEX mixed FOR (n:Foo|Bar) ON n.e"))
          .isInstanceOf(CommandParsingException.class).hasMessageContaining("same vector.dimensions");
      assertThat(database.getSchema().getType("Foo").getPolymorphicIndexByProperties("e")).isNull();
      assertThat(database.getSchema().getType("Bar").getPolymorphicIndexByProperties("e")).isNull();
      assertThatThrownBy(() -> nearestIds("mixed", 1, "[1.0, 0.0]")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void aFailedStatementDropsThePropertiesItDeclaredOnTheTypesItAlreadyBuilt() {
      command("CREATE (:Foo {id: 'f', e: [1.0, 0.0]})");
      command("CREATE (:Bar {id: 'b', e: [1.0, 0.0, 0.0]})");
      // the vector property is declared by the statement on a type that has none yet; the failing second type must undo it
      database.getSchema().getType("Foo").dropProperty("e");
      database.getSchema().getType("Bar").dropProperty("e");

      assertThatThrownBy(() -> command("CREATE VECTOR INDEX mixed FOR (n:Foo|Bar) ON n.e"))
          .isInstanceOf(CommandParsingException.class);
      assertThat(database.getSchema().getType("Foo").existsProperty("e")).isFalse();
      assertThat(database.getSchema().getType("Bar").existsProperty("e")).isFalse();
    }

    /**
     * The indexes a multi-label statement builds are ordinary per-type indexes, so SQL searches each type through its own
     * index and a record SQL writes is in the index the logical name resolves to. The logical name itself is a Cypher
     * concept: SQL names the per-type index.
     */
    @Test
    void fulltextDataIsSearchableFromSqlPerTypeAndSqlWritesAreFoundByTheLogicalName() {
      command(FULLTEXT_DDL);
      command("CREATE (:Article {id: 'a1', title: 'graph databases', body: 'fast traversal'})");
      command("CREATE (:Post {id: 'p1', title: 'cooking', body: 'a graph of recipes'})");

      assertThat(sqlIds("SELECT id FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'graph') = true")).containsExactly("a1");
      assertThat(sqlIds("SELECT id FROM Post WHERE SEARCH_INDEX('Post[title,body]', 'graph') = true")).containsExactly("p1");

      database.transaction(() -> database.command("sql", "INSERT INTO Post SET id = 'p2', title = 'trip', body = 'a graph of roads'").close());
      assertThat(fulltextIds("docs", "graph")).containsExactlyInAnyOrder("a1", "p1", "p2");
      assertThat(sqlIds("SELECT id FROM Post WHERE SEARCH_INDEX('Post[title,body]', 'graph') = true")).containsExactlyInAnyOrder("p1", "p2");
    }

    @Test
    void vectorDataIsSearchableFromSqlPerTypeAndSqlWritesAreFoundByTheLogicalName() {
      command(VECTOR_DDL);
      command("CREATE (:Article {id: 'a1', e: [1.0, 0.0]})");
      command("CREATE (:Post {id: 'p1', e: [0.9, 0.1]})");

      assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('Article[e]', [1.0, 0.0], 1)))")).containsExactly("a1");
      assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('Post[e]', [1.0, 0.0], 1)))")).containsExactly("p1");

      database.transaction(() -> database.command("sql", "INSERT INTO Post SET id = 'p2', e = [0.0, 1.0]").close());
      assertThat(nearestIds("emb", 1, "[0.0, 1.0]")).containsExactly("p2");
    }

    @Test
    void ifNotExistsReusesAPerTypeIndexAlreadyThere() {
      command("CREATE FULLTEXT INDEX pre FOR (n:Article) ON EACH [n.title, n.body]");

      // the Article index is reused, the Post one is built: one index added
      assertThat(stats(FULLTEXT_DDL).getIndexesAdded()).isEqualTo(1);
      command("CREATE (:Article {id: 'a1', title: 'graph', body: 'x'})");
      command("CREATE (:Post {id: 'p1', title: 'graph', body: 'y'})");
      assertThat(fulltextIds("docs", "graph")).containsExactlyInAnyOrder("a1", "p1");

      // the reused one is still listed under its own name next to the logical one
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES YIELD name")) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      assertThat(names).containsExactlyInAnyOrder("pre", "docs");
    }

    @Test
    void theLogicalIndexSurvivesASchemaReload() {
      command(FULLTEXT_DDL);
      command("CREATE (:Post {id: 'p1', title: 'graph', body: 'y'})");

      reopenDatabase();

      assertThat(fulltextIds("docs", "graph")).containsExactly("p1");
    }

    private List<String> nearestIds(final String index, final int k, final String vector) {
      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL db.index.vector.queryNodes($index, $k, " + vector + ") YIELD node, score RETURN node.id AS id ORDER BY score DESC",
          "index", index, "k", k)) {
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

    private List<String> sqlIds(final String sql) {
      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      return ids;
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

  /**
   * Issue #11014: the logical name of a Cypher {@code CREATE FULLTEXT INDEX} / {@code CREATE VECTOR INDEX} over several labels is
   * resolved by SQL too: {@code SEARCH_INDEX} and {@code vector.neighbors} fan out to the per-type indexes, {@code DROP INDEX} of the
   * name drops what the statement built, and {@code DROP INDEX} of a member is refused.
   */
  @Nested
  class Issue11014SqlResolvesMultiTypeIndexName extends TestHelper {

    private static final String FULLTEXT_DDL = "CREATE FULLTEXT INDEX docs FOR (n:Article|Post) ON EACH [n.title, n.body]";
    private static final String VECTOR_DDL   = "CREATE VECTOR INDEX emb FOR (n:Article|Post) ON n.e OPTIONS {indexConfig: "
        + "{`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}";

    @BeforeEach
    void createIndexes() {
      // a common supertype, so one SQL statement can target the types the index spans
      sql("CREATE VERTEX TYPE Doc");
      sql("CREATE VERTEX TYPE Article EXTENDS Doc");
      sql("CREATE VERTEX TYPE Post EXTENDS Doc");
      cypher(FULLTEXT_DDL);
      cypher(VECTOR_DDL);
      cypher("CREATE (:Article {id: 'a1', title: 'graph databases', body: 'fast traversal', e: [1.0, 0.0]})");
      cypher("CREATE (:Post {id: 'p1', title: 'cooking', body: 'a graph of recipes', e: [0.9, 0.1]})");
      cypher("CREATE (:Post {id: 'p2', title: 'travel', body: 'mountains', e: [0.0, 1.0]})");
    }

    @Test
    void searchIndexByTheLogicalNameFindsEveryType() {
      assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactlyInAnyOrder("a1", "p1");
      assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'mountains') = true")).containsExactly("p2");
    }

    @Test
    void searchIndexByTheLogicalNameIsRestrictedByTheTargetType() {
      // the member index of Post is not a candidate for Article rows: the per-row evaluation answers from the whole logical index
      assertThat(sqlIds("SELECT id FROM Article WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactly("a1");
      assertThat(sqlIds("SELECT id FROM Post WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactly("p1");
    }

    @Test
    void searchIndexByTheLogicalNameHonorsTheScoreOrderAndTheLimit() {
      assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true ORDER BY $score DESC LIMIT 2"))
          .containsExactlyInAnyOrder("a1", "p1");
      assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true ORDER BY $score DESC LIMIT 1")).hasSize(1);
      assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true LIMIT 0")).isEmpty();
    }

    @Test
    void searchIndexByTheLogicalNameIsExplained() {
      try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("docs");
      }
    }

    @Test
    void recordsWrittenThroughSqlAreFoundByTheLogicalName() {
      database.transaction(() -> database.command("sql", "INSERT INTO Post SET id = 'p3', title = 'trip', body = 'a graph of roads'").close());
      assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactlyInAnyOrder("a1", "p1", "p3");
    }

    @Test
    void searchIndexByAnUnknownNameStillFails() {
      assertThatThrownBy(() -> sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('nope', 'graph') = true")).isInstanceOf(SchemaException.class);
    }

    @Test
    void searchIndexByAVectorLogicalNameIsRefused() {
      assertThatThrownBy(() -> sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('emb', 'graph') = true")).hasMessageContaining("not a full-text");
    }

    @Test
    void vectorNeighborsByTheLogicalNameMergesTheNearestOfEveryType() {
      assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [1.0, 0.0], 2)))")).containsExactly("a1", "p1");
      assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [0.0, 1.0], 1)))")).containsExactly("p2");

      database.transaction(() -> database.command("sql", "INSERT INTO Article SET id = 'a2', e = [0.3, 0.9]").close());
      assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [0.0, 1.0], 2)))")).containsExactly("p2", "a2");
    }

    @Test
    void vectorNeighborsByTheLogicalNameHonorsAnEmptyFilter() {
      assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [1.0, 0.0], 2, {filter: []})))")).isEmpty();
    }

    @Test
    void vectorNeighborsByAFullTextLogicalNameIsRefused() {
      assertThatThrownBy(() -> sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('docs', [1.0, 0.0], 2)))"))
          .hasMessageContaining("not a vector index");
    }

    @Test
    void dropIndexOfAMemberIsRefusedAndLeavesTheLogicalIndexWhole() {
      assertThatThrownBy(() -> sql("DROP INDEX `Post[title,body]`")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("member of the multi-type index 'docs'");
      assertThatThrownBy(() -> sql("DROP INDEX `Post[title,body]` IF EXISTS")).isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().existsIndex("Post[title,body]")).isTrue();
      assertThat(sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).containsExactlyInAnyOrder("a1", "p1");
    }

    @Test
    void dropIndexOfAMemberIsRefusedFromCypherToo() {
      assertThatThrownBy(() -> cypher("DROP INDEX `Post[title,body]`")).hasRootCauseInstanceOf(SchemaException.class)
          .rootCause().hasMessageContaining("member of the multi-type index 'docs'");
      assertThat(database.getSchema().existsIndex("Post[title,body]")).isTrue();
    }

    @Test
    void dropIndexOfTheLogicalNameDropsWhatItsStatementBuilt() {
      sql("DROP INDEX docs");

      assertThat(MultiTypeIndexes.find(database.getSchema(), "docs")).isNull();
      assertThat(database.getSchema().existsIndex("Article[title,body]")).isFalse();
      assertThat(database.getSchema().existsIndex("Post[title,body]")).isFalse();
      // the other logical index is untouched
      assertThat(sqlIds("SELECT id FROM (SELECT expand(vector.neighbors('emb', [1.0, 0.0], 1)))")).containsExactly("a1");
      assertThatThrownBy(() -> sqlIds("SELECT id FROM Doc WHERE SEARCH_INDEX('docs', 'graph') = true")).isInstanceOf(SchemaException.class);
    }

    @Test
    void dropIndexOfTheLogicalNameKeepsAPerTypeIndexThatWasThereBefore() {
      cypher("DROP INDEX docs");
      cypher("CREATE FULLTEXT INDEX pre FOR (n:Article) ON EACH [n.title, n.body]");
      cypher(FULLTEXT_DDL.replace("docs FOR", "docs IF NOT EXISTS FOR"));
      assertThat(MultiTypeIndexes.find(database.getSchema(), "docs")).isNotNull();

      sql("DROP INDEX docs");

      // the Article member was there before `docs`, so it stays; the Post one was created by `docs`
      assertThat(database.getSchema().existsIndex("pre")).isTrue();
      assertThat(database.getSchema().existsIndex("Post[title,body]")).isFalse();
    }

    @Test
    void dropAllIndexesForgetsTheLogicalNames() {
      sql("DROP INDEX *");

      assertThat(MultiTypeIndexes.entries(database.getSchema())).isEmpty();
      assertThat(database.getSchema().getIndexes()).isEmpty();
    }

    @Test
    void createIndexCannotTakeTheLogicalName() {
      assertThatThrownBy(() -> {
        sql("CREATE PROPERTY Article.id STRING");
        sql("CREATE INDEX docs IF NOT EXISTS ON Article (id) NOTUNIQUE");
      }).isInstanceOf(AlreadyExistsException.class);
    }

    @Test
    void aPlainIndexIsStillDroppedByName() {
      sql("CREATE PROPERTY Article.id STRING");
      sql("CREATE INDEX ON Article (id) NOTUNIQUE");
      sql("DROP INDEX `Article[id]`");
      assertThat(database.getSchema().existsIndex("Article[id]")).isFalse();
    }

    private List<String> sqlIds(final String sql) {
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
}
