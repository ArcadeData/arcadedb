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
package com.arcadedb.query.sql.functions.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.*;

/**
 * Test suite for "Advanced Search & Reranking" examples from the JVector blog post.
 * This test validates the hybrid search code examples shown in the blog post section:
 * "Advanced Search & Reranking (6 functions)"
 *
 * Tests identify which features work as documented and which need fixes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLVectorHybridSearchBlogPostTest extends TestHelper {

  // =============================================================================
  // Test Setup: Create Document type with sample data
  // =============================================================================

  private void setupDocuments() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("DocTest");
      docType.createProperty("title", Type.STRING);
      docType.createProperty("content", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Create LSM vector index
      database.getSchema()
          .buildTypeIndex("DocTest", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(3)
          .withSimilarity("COSINE")
          .create();

      // Insert test documents
      final MutableDocument doc1 = database.newDocument("DocTest");
      doc1.set("title", "Machine Learning");
      doc1.set("content", "Deep learning and neural networks are powerful keywords");
      doc1.set("embedding", normalizeVector(new float[] { 1.0f, 0.5f, 0.2f }));
      doc1.save();

      final MutableDocument doc2 = database.newDocument("DocTest");
      doc2.set("title", "Database Systems");
      doc2.set("content", "SQL databases and vector search with keywords");
      doc2.set("embedding", normalizeVector(new float[] { 0.2f, 0.8f, 0.5f }));
      doc2.save();

      final MutableDocument doc3 = database.newDocument("DocTest");
      doc3.set("title", "Neural Networks");
      doc3.set("content", "Training models for image recognition");
      doc3.set("embedding", normalizeVector(new float[] { 0.9f, 0.3f, 0.1f }));
      doc3.save();
    });
  }

  // =============================================================================
  // Blog Post Example 1: JavaScript UDF for hybrid ranking
  // =============================================================================

  @Test
  void testBlogPost_DefineJavaScriptFunction_WORKS() {
    setupDocuments();

    database.transaction(() -> {
      // Correct syntax using DEFINE FUNCTION (not CREATE FUNCTION)
      database.command("sql", """
          DEFINE FUNCTION Hybrid.rank "var vectorSim = 1.0 - vectorDist; var textScore = hasTextMatch ? 1.0 : 0.0; return (vectorSim * alpha) + (textScore * (1.0 - alpha));" PARAMETERS [vectorDist, hasTextMatch, alpha] LANGUAGE js
          """);

      // Test the function directly (JavaScript returns Double, not Float)
      final double result = (double) database.getSchema()
          .getFunction("Hybrid", "rank")
          .execute(0.3, true, 0.7);

      // Expected: (1.0 - 0.3) * 0.7 + 1.0 * 0.3 = 0.7 * 0.7 + 0.3 = 0.49 + 0.3 = 0.79
      assertThat(result).isCloseTo(0.79, offset(0.01));
      // RESULT: ✅ DEFINE FUNCTION with JavaScript WORKS!
    });
  }

  // =============================================================================
  // Blog Post Example 2: Hybrid search with JavaScript function
  // =============================================================================

  @Test
  void testBlogPost_HybridSearchWithJavaScriptFunction_WORKS() {
    setupDocuments();

    database.transaction(() -> {
      // Step 1: Define the hybrid ranking function
      database.command("sql", """
          DEFINE FUNCTION Hybrid.rank "var vectorSim = 1.0 - vectorDist; var textScore = hasTextMatch ? 1.0 : 0.0; return (vectorSim * alpha) + (textScore * (1.0 - alpha));" PARAMETERS [vectorDist, hasTextMatch, alpha] LANGUAGE js
          """);

      final float[] queryVector = normalizeVector(new float[] { 1.0f, 0.4f, 0.2f });

      // Step 2: Hybrid search - calculate vector distance and text match separately
      // then use the function to combine them
      final ResultSet rs = database.query("sql", """
          SELECT title, content,
            vectorCosineSimilarity(embedding, ?) as vecSim,
            (content LIKE '%keywords%') as hasTextMatch
          FROM DocTest
          WHERE vectorCosineSimilarity(embedding, ?) > 0.3
            OR content LIKE '%keywords%'
          """, queryVector, queryVector);

      assertThat(rs.hasNext()).isTrue();

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        final Number vecSimNum = result.getProperty("vecSim");
        final double vecSim = vecSimNum.doubleValue();
        final boolean hasTextMatch = result.getProperty("hasTextMatch");

        // Calculate hybrid score using the function (convert similarity to distance)
        final double vectorDist = 1.0 - vecSim;
        final double hybridScore = (double) database.getSchema()
            .getFunction("Hybrid", "rank")
            .execute(vectorDist, hasTextMatch, 0.7);

        assertThat(hybridScore).isGreaterThan(0.0);
        count++;
      }
      assertThat(count).isGreaterThan(0);
      // RESULT: ✅ Hybrid search with JavaScript UDF WORKS!
    });
  }

  // =============================================================================
  // Blog Post Example 3: Alternative with textMatchScore JavaScript helper
  // =============================================================================

  @Test
  void testBlogPost_TextMatchScoreJavaScriptHelper_WORKS() {
    setupDocuments();

    database.transaction(() -> {
      // Step 1: Define the textMatchScore helper function
      database.command("sql", """
          DEFINE FUNCTION Text.matchScore "return text.toLowerCase().includes(keywords.toLowerCase()) ? 1.0 : 0.0;" PARAMETERS [text, keywords] LANGUAGE js
          """);

      // Step 2: Hybrid search using vectorHybridScore + textMatchScore
      final float[] queryVector = normalizeVector(new float[] { 1.0f, 0.4f, 0.2f });
      final String keywords = "keywords";

      final ResultSet rs = database.query("sql", """
          SELECT title, content,
            vectorHybridScore(
              vectorCosineSimilarity(embedding, ?),
              `Text.matchScore`(content, ?),
              0.7
            ) as hybrid_score
          FROM DocTest
          WHERE vectorCosineSimilarity(embedding, ?) > 0.3
            OR content LIKE ?
          ORDER BY hybrid_score DESC
          LIMIT 10
          """, queryVector, keywords, queryVector, "%" + keywords + "%");

      assertThat(rs.hasNext()).isTrue();

      while (rs.hasNext()) {
        final Result result = rs.next();
        final Number hybridScoreNum = result.getProperty("hybrid_score");
        assertThat(hybridScoreNum).isNotNull();
        assertThat(hybridScoreNum.doubleValue()).isGreaterThan(0.0);
      }
      // RESULT: ✅ textMatchScore JavaScript helper WORKS!
    });
  }

  // =============================================================================
  // What DOES work: Built-in vectorHybridScore function
  // =============================================================================

  @Test
  void testWorking_VectorHybridScoreWithLiterals() {
    setupDocuments();

    database.transaction(() -> {
      // ✅ vectorHybridScore with literal values DOES work
      final ResultSet rs = database.query("sql",
          "SELECT vectorHybridScore(0.8, 0.5, 0.7) as score");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Float score = result.getProperty("score");

      // Expected: 0.8 * 0.7 + 0.5 * 0.3 = 0.56 + 0.15 = 0.71
      assertThat(score).isNotNull();
      assertThat(score).isCloseTo(0.71f, offset(0.01f));
      // RESULT: ✅ Built-in vectorHybridScore function works
    });
  }

  @Test
  void testWorking_VectorDistanceWithLiteralArrays() {
    setupDocuments();

    database.transaction(() -> {
      // ✅ vectorDistance with literal arrays DOES work
      final ResultSet rs = database.query("sql",
          "SELECT (1 - vectorCosineSimilarity([1.0, 0.0, 0.0], [1.0, 0.0, 0.0])) as dist");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Float distance = result.getProperty("dist");

      assertThat(distance).isNotNull();
      assertThat(distance).isCloseTo(0.0f, offset(0.001f));
      // RESULT: ✅ vectorDistance with literal arrays works
    });
  }

  @Test
  void testWorking_SimpleTextSearch() {
    setupDocuments();

    database.transaction(() -> {
      // ✅ Simple text search with LIKE DOES work
      final ResultSet rs = database.query("sql",
          "SELECT title, content FROM DocTest WHERE content LIKE '%keywords%'");

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        final String content = result.getProperty("content");
        assertThat(content).containsIgnoringCase("keywords");
        count++;
      }
      assertThat(count).isEqualTo(2);  // Two documents contain "keywords"
      // RESULT: ✅ Text search with LIKE works
    });
  }

  // =============================================================================
  // Working Alternative: Manual hybrid search without UDFs
  // =============================================================================

  @Test
  void testWorking_ManualHybridSearchWithoutUDFs() {
    setupDocuments();

    database.transaction(() -> {
      // ✅ This is a WORKING alternative to the blog post example
      // Step 1: Get vector candidates using vectorCosineSimilarity
      final float[] queryVector = normalizeVector(new float[] { 1.0f, 0.4f, 0.2f });

      final ResultSet rs = database.query("sql",
          "SELECT title, content, embedding, " +
              "vectorCosineSimilarity(embedding, ?) as vecSim " +
              "FROM DocTest",
          (Object) queryVector);

      int resultCount = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        final Float vecSim = result.getProperty("vecSim");
        final String content = result.getProperty("content");

        assertThat(vecSim).isNotNull();
        assertThat(content).isNotNull();

        // Manual hybrid scoring: vectorSim * 0.7 + textMatch * 0.3
        final float textMatch = content.contains("keywords") ? 1.0f : 0.0f;
        final float hybridScore = (vecSim * 0.7f) + (textMatch * 0.3f);
        assertThat(hybridScore).isGreaterThanOrEqualTo(0.0f);

        resultCount++;
      }
      assertThat(resultCount).isEqualTo(3);
      // RESULT: ✅ Manual hybrid search WITHOUT UDFs works
    });
  }

  @Test
  void testWorking_HybridScoreInSelectWithCalculatedValues() {
    setupDocuments();

    database.transaction(() -> {
      // ✅ Use vectorHybridScore with calculated values
      // NOTE: This test uses a simpler approach - calculating text score in Java
      final float[] queryVector = normalizeVector(new float[] { 1.0f, 0.4f, 0.2f });

      final ResultSet rs = database.query("sql",
          "SELECT title, content, " +
              "vectorCosineSimilarity(embedding, ?) as vecSim " +
              "FROM DocTest " +
              "ORDER BY vecSim DESC " +
              "LIMIT 10",
          (Object) queryVector);

      assertThat(rs.hasNext()).isTrue();

      while (rs.hasNext()) {
        final Result result = rs.next();
        final Float vecSim = result.getProperty("vecSim");
        final String content = result.getProperty("content");

        assertThat(vecSim).isNotNull();

        // Calculate hybrid score in Java: vecSim * 0.7 + textScore * 0.3
        final float textScore = content.contains("keywords") ? 1.0f : 0.0f;
        final float hybridScore = (vecSim * 0.7f) + (textScore * 0.3f);

        assertThat(hybridScore).isGreaterThan(0.0f);
      }
      // RESULT: ✅ Hybrid scoring can be done in application code
    });
  }

  // =============================================================================
  // Helper Methods
  // =============================================================================

  private float[] normalizeVector(final float[] vector) {
    float magnitude = 0.0f;
    for (float v : vector) {
      magnitude += v * v;
    }
    magnitude = (float) Math.sqrt(magnitude);

    if (magnitude == 0.0f) {
      return vector;
    }

    final float[] normalized = new float[vector.length];
    for (int i = 0; i < vector.length; i++) {
      normalized[i] = vector[i] / magnitude;
    }
    return normalized;
  }
}
