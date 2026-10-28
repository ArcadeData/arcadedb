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
import com.arcadedb.index.fulltext.StandardEnglishStopWordsAnalyzer;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10863: forms of Neo4j's {@code CREATE VECTOR INDEX} and {@code CREATE FULLTEXT INDEX} that #10678 still refused - the
 * additional filtering properties of a vector index ({@code WITH [n.a, n.b]}), a vector index without
 * {@code vector.dimensions}, and the {@code standard} analyzer (the standard analyzer with English stop words).
 */
class Issue10863CypherIndexDdlFollowUpsTest extends TestHelper {

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
  void vectorIndexWithOptionsButWithoutDimensionsTakesThemFromTheData() {
    command("CREATE (:L {id: 'a', e: [1.0, 0.0]})");

    command("CREATE VECTOR INDEX v FOR (n:L) ON n.e OPTIONS {indexConfig: {`vector.similarity_function`: 'euclidean'}}");

    final LSMVectorIndex vector = vectorIndex((TypeIndex) database.getSchema().getIndexByName("v"));
    assertThat(vector.getDimensions()).isEqualTo(2);
    assertThat(vector.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
  }

  @Test
  void vectorIndexWithoutDimensionsOverALabelWithNoVectorIsRefused() {
    command("CREATE (:L {id: 'a', other: 1})");

    final Throwable refusal = refusal("CREATE VECTOR INDEX v FOR (n:L) ON n.e");
    assertThat(refusal).isInstanceOf(CommandParsingException.class);
    assertThat(refusal.getMessage()).contains("vector.dimensions").contains("no vector");

    assertThat(database.getSchema().existsIndex("v")).isFalse();
    assertThat(database.getSchema().getType("L").existsProperty("e")).isFalse();
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
