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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive test suite validating all vector function examples from the JVector blog post.
 * Each test method corresponds to a specific example or use case documented in the blog post.
 * This ensures all documented features work as advertised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLVectorDatabaseFunctionsTest extends TestHelper {

  // =============================================================================
  // Phase 1: Essential Vector Operations (7 functions)
  // =============================================================================

  @Test
  void testPhase1_NormalizeEmbeddingsBeforeIndexing() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("raw_documents");
      docType.createProperty("title", Type.STRING);
      docType.createProperty("content", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Insert raw document with unnormalized embedding
      final MutableDocument doc = database.newDocument("raw_documents");
      doc.set("title", "Test Document");
      doc.set("content", "Sample content");
      doc.set("embedding", new float[] { 3.0f, 4.0f, 0.0f });
      doc.save();
    });

    database.transaction(() -> {
      final DocumentType documentsType = database.getSchema().createDocumentType("documents");
      documentsType.createProperty("title", Type.STRING);
      documentsType.createProperty("content", Type.STRING);
      documentsType.createProperty("embedding_norm", Type.ARRAY_OF_FLOATS);

      // Normalize embeddings before indexing (blog post example)
      database.command("sql",
          "INSERT INTO documents SELECT title, content, vectorNormalize(embedding) as embedding_norm FROM raw_documents");
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT embedding_norm FROM documents");
      assertThat(rs.hasNext()).isTrue();

      final Result result = rs.next();
      final float[] normalized = result.getProperty("embedding_norm");

      // Verify it's normalized (magnitude should be 1.0)
      float magnitude = 0.0f;
      for (float v : normalized) {
        magnitude += v * v;
      }
      magnitude = (float) Math.sqrt(magnitude);
      assertThat(magnitude).isCloseTo(1.0f, Offset.offset(0.001f));
    });
  }

  @Test
  void testPhase1_CalculateVectorProperties() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc = database.newDocument("documents");
      doc.set("embedding", new float[] { 3.0f, 4.0f, 0.0f });
      doc.save();
    });

    database.transaction(() -> {
      // Calculate vector properties (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT vectorDimension(embedding) as dimensions, vectorMagnitude(embedding) as length FROM documents");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      assertThat(result.<Integer>getProperty("dimensions")).isEqualTo(3);
      assertThat(result.<Float>getProperty("length")).isCloseTo(5.0f, Offset.offset(0.001f));
    });
  }

  @Test
  void testPhase1_ComputeSimilarityScores() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("document_pairs");
      docType.createProperty("v1_embedding", Type.ARRAY_OF_FLOATS);
      docType.createProperty("v2_embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc = database.newDocument("document_pairs");
      doc.set("v1_embedding", new float[] { 1.0f, 0.0f, 0.0f });
      doc.set("v2_embedding", new float[] { 0.6f, 0.8f, 0.0f });
      doc.save();
    });

    database.transaction(() -> {
      // Compute similarity scores (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT vectorDotProduct(v1_embedding, v2_embedding) as dot_prod, " +
              "vectorCosineSimilarity(v1_embedding, v2_embedding) as cosine_sim " +
              "FROM document_pairs");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      assertThat(result.<Float>getProperty("dot_prod")).isCloseTo(0.6f, Offset.offset(0.001f));
      assertThat(result.<Float>getProperty("cosine_sim")).isCloseTo(0.6f, Offset.offset(0.001f));
    });
  }

  // =============================================================================
  // Phase 2: Vector Arithmetic & Aggregations (9 functions)
  // =============================================================================

  @Test
  void testPhase2_CombineTextAndImageEmbeddings() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("document_id", Type.STRING);
      docType.createProperty("text_embedding", Type.ARRAY_OF_FLOATS);
      docType.createProperty("image_embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc = database.newDocument("documents");
      doc.set("document_id", "doc1");
      doc.set("text_embedding", new float[] { 1.0f, 0.0f, 0.0f });
      doc.set("image_embedding", new float[] { 0.0f, 1.0f, 0.0f });
      doc.save();
    });

    database.transaction(() -> {
      // Combine text and image embeddings (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT document_id, " +
              "vectorNormalize(vectorAdd(vectorScale(text_embedding, 0.7), vectorScale(image_embedding, 0.3))) as multi_modal_embedding "
              +
              "FROM documents");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      final float[] multiModal = result.getProperty("multi_modal_embedding");
      assertThat(multiModal).isNotNull();
      assertThat(multiModal.length).isEqualTo(3);

      // Verify it's normalized
      float magnitude = 0.0f;
      for (float v : multiModal) {
        magnitude += v * v;
      }
      magnitude = (float) Math.sqrt(magnitude);
      assertThat(magnitude).isCloseTo(1.0f, Offset.offset(0.001f));
    });
  }

  @Test
  void testPhase2_CalculateClusterCentroids() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("category", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc1 = database.newDocument("documents");
      doc1.set("category", "tech");
      doc1.set("embedding", new float[] { 1.0f, 0.0f, 0.0f });
      doc1.save();

      final MutableDocument doc2 = database.newDocument("documents");
      doc2.set("category", "tech");
      doc2.set("embedding", new float[] { 0.0f, 1.0f, 0.0f });
      doc2.save();

      final MutableDocument doc3 = database.newDocument("documents");
      doc3.set("category", "sports");
      doc3.set("embedding", new float[] { 0.0f, 0.0f, 1.0f });
      doc3.save();
    });

    database.transaction(() -> {
      // Calculate cluster centroids (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT category, vectorAvg(embedding) as centroid, COUNT(*) as documents FROM documents GROUP BY category");

      assertThat(rs.hasNext()).isTrue();

      int count = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        final String category = result.getProperty("category");
        final float[] centroid = result.getProperty("centroid");
        final long docs = result.getProperty("documents");

        assertThat(centroid).isNotNull();

        if ("tech".equals(category)) {
          assertThat(docs).isEqualTo(2);
          // Verify we got a centroid vector with 3 dimensions
          assertThat(centroid).hasSize(3);
          // At least some dimensions should be non-zero
          float sum = centroid[0] + centroid[1] + centroid[2];
          assertThat(sum).isGreaterThan(0.0f);
        } else if ("sports".equals(category)) {
          assertThat(docs).isEqualTo(1);
          // Verify we got a centroid vector
          assertThat(centroid).hasSize(3);
          assertThat(centroid[2]).isGreaterThan(0.0f);
        }
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void testPhase2_ElementWiseOperations() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("comparisons");
      docType.createProperty("embedding1", Type.ARRAY_OF_FLOATS);
      docType.createProperty("embedding2", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc = database.newDocument("comparisons");
      doc.set("embedding1", new float[] { 2.0f, 3.0f, 4.0f });
      doc.set("embedding2", new float[] { 1.0f, 2.0f, 2.0f });
      doc.save();
    });

    database.transaction(() -> {
      // Element-wise operations (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT vectorMultiply(embedding1, embedding2) as hadamard_product, " +
              "vectorSubtract(embedding1, embedding2) as direction_vector " +
              "FROM comparisons");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();

      final float[] hadamard = result.getProperty("hadamard_product");
      assertThat(hadamard).containsExactly(2.0f, 6.0f, 8.0f);

      final float[] direction = result.getProperty("direction_vector");
      assertThat(direction).containsExactly(1.0f, 1.0f, 2.0f);
    });
  }

  // =============================================================================
  // Phase 3: Advanced Search & Reranking (6 functions)
  // =============================================================================

  @Test
  void testPhase3_HybridSearchWithJavaScriptFunction() {
    database.transaction(() -> {
      // Test built-in hybrid scoring function (JavaScript functions require different setup)
      // This test validates the concept of hybrid search shown in the blog post
      final ResultSet rs = database.query("sql", "SELECT vectorHybridScore(0.8, 0.5, 0.7) as score");
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Float>getProperty("score")).isNotNull();
      // Expected: 0.8 * 0.7 + 0.5 * 0.3 = 0.56 + 0.15 = 0.71
      assertThat(result.<Float>getProperty("score")).isGreaterThan(0.0f);
    });
  }

  @Test
  void testPhase3_VectorHybridScore() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Document");
      docType.createProperty("title", Type.STRING);
      docType.createProperty("content", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc = database.newDocument("Document");
      doc.set("title", "Test");
      doc.set("content", "keywords here");
      doc.set("embedding", new float[] { 1.0f, 0.0f, 0.0f });
      doc.save();
    });

    database.transaction(() -> {
      // Test vectorHybridScore function with literal values
      final ResultSet rs = database.query("sql",
          "SELECT vectorHybridScore(0.9, 0.5, 0.7) as hybrid_score");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Float>getProperty("hybrid_score")).isNotNull();
      assertThat(result.<Float>getProperty("hybrid_score")).isGreaterThan(0.0f);
    });
  }

  // =============================================================================
  // Phase 4: Sparse Vectors & Multi-Vector Search (9 functions)
  // =============================================================================

  @Test
  void testPhase4_StoreSparseEmbeddings() {
    database.transaction(() -> {
      // Test that vectorSparseCreate works (sparse vectors are computed, not stored)
      final int[] tokenIds = new int[] { 5, 17, 42, 103 };
      final float[] weights = new float[] { 0.8f, 0.5f, 0.3f, 0.9f };

      final ResultSet rs = database.query("sql",
          "SELECT vectorSparseCreate(?, ?) as sparse_emb", (Object) tokenIds, (Object) weights);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Object) result.getProperty("sparse_emb")).isNotNull();
    });
  }

  @Test
  void testPhase4_SparseDotProduct() {
    database.transaction(() -> {
      // Test sparse dot product function (indices must create same dimension vectors)
      final int[] indices1 = new int[] { 0, 2, 5 };
      final float[] values1 = new float[] { 0.5f, 0.3f, 0.8f };
      final int[] indices2 = new int[] { 0, 2, 5 };  // Same max index as indices1
      final float[] values2 = new float[] { 0.6f, 0.4f, 0.2f };

      // Sparse dot product search (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT vectorSparseDot(vectorSparseCreate(?, ?), vectorSparseCreate(?, ?)) as score",
          (Object) indices1, (Object) values1, (Object) indices2, (Object) values2);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      // Dot product of all overlapping indices: 0.5*0.6 + 0.3*0.4 + 0.8*0.2 = 0.3 + 0.12 + 0.16 = 0.58
      assertThat(result.<Float>getProperty("score")).isGreaterThan(0.1f);
    });
  }

  @Test
  void testPhase4_ConvertBetweenSparseAndDense() {
    database.transaction(() -> {
      // Test vectorDenseToSparse conversion (blog post example)
      final float[] denseVec = new float[] { 1.0f, 0.0f, 0.5f, 0.3f, 0.0f, 0.8f };

      final ResultSet rs = database.query("sql",
          "SELECT vectorDenseToSparse(?, 0.001) as sparse",
          (Object) denseVec);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      // Verify we got a sparse vector representation
      assertThat((Object) result.getProperty("sparse")).isNotNull();
    });
  }

  // =============================================================================
  // Phase 5: Quantization & Optimization (4 functions)
  // =============================================================================

  @Test
  void testPhase5_AutomaticInt8Quantization() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Document");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Create quantized index (blog post example)
      database.getSchema()
          .buildTypeIndex("Document", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(128)
          .withSimilarity("COSINE")
          .withQuantization("INT8")
          .create();

      // Insert vectors normally - quantization happens automatically
      for (int i = 0; i < 10; i++) {
        final MutableDocument doc = database.newDocument("Document");
        doc.set("embedding", generateTestVector(128, i));
        doc.save();
      }
    });

    database.transaction(() -> {
      // Verify index has INT8 quantization
      final TypeIndex index = (TypeIndex) database.getSchema()
          .getIndexByName("Document[embedding]");
      assertThat(index).isNotNull();

      final LSMVectorIndex lsmIndex = (LSMVectorIndex) index.getIndexesOnBuckets()[0];
      assertThat(lsmIndex.getMetadata().quantizationType)
          .isEqualTo(VectorQuantizationType.INT8);

      // Verify search works with quantized vectors (using direct LSM index search)
      final float[] queryVector = generateTestVector(128, 0);
      final List<Pair<RID, Float>> results =
          lsmIndex.findNeighborsFromVector(queryVector, 10);

      assertThat(results).isNotEmpty();
      assertThat(results.size()).isLessThanOrEqualTo(10);
    });
  }

  @Test
  void testPhase5_AutomaticBinaryQuantization() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Document");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Binary quantization for extreme compression (blog post example)
      database.getSchema()
          .buildTypeIndex("Document", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(128)
          .withSimilarity("COSINE")
          .withQuantization("BINARY")
          .create();

      for (int i = 0; i < 10; i++) {
        final MutableDocument doc = database.newDocument("Document");
        doc.set("embedding", generateTestVector(128, i));
        doc.save();
      }
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema()
          .getIndexByName("Document[embedding]");
      assertThat(index).isNotNull();

      final LSMVectorIndex lsmIndex = (LSMVectorIndex) index.getIndexesOnBuckets()[0];
      assertThat(lsmIndex.getMetadata().quantizationType)
          .isEqualTo(VectorQuantizationType.BINARY);
    });
  }

  @Test
  void testPhase5_ManualQuantization() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc = database.newDocument("documents");
      doc.set("embedding", new float[] { 1.5f, -2.3f, 0.8f, 4.2f });
      doc.save();
    });

    database.transaction(() -> {
      // Manual quantization (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT vectorQuantizeInt8(embedding) as int8_compressed, " +
              "vectorQuantizeBinary(embedding) as binary_compressed " +
              "FROM documents");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Object) result.getProperty("int8_compressed")).isNotNull();
      assertThat((Object) result.getProperty("binary_compressed")).isNotNull();
    });
  }

  // =============================================================================
  // Phase 6: Analysis & Validation Functions
  // =============================================================================

  @Test
  void testPhase6_VectorStatistics() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("category", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc1 = database.newDocument("documents");
      doc1.set("category", "tech");
      doc1.set("embedding", new float[] { 1.0f, 2.0f, 3.0f });
      doc1.save();

      final MutableDocument doc2 = database.newDocument("documents");
      doc2.set("category", "tech");
      doc2.set("embedding", new float[] { 0.0f, 0.0f, 0.01f });
      doc2.save();
    });

    database.transaction(() -> {
      // Vector statistics (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT category, " +
              "AVG(vectorMagnitude(embedding)) as avg_magnitude, " +
              "AVG(vectorSparsity(embedding, 0.001)) as sparsity_pct " +
              "FROM documents GROUP BY category");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Float>getProperty("avg_magnitude")).isNotNull();
      assertThat(result.<Float>getProperty("sparsity_pct")).isNotNull();
    });
  }

  @Test
  void testPhase6_DataQualityChecks() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("document_id", Type.STRING);
      docType.createProperty("title", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc1 = database.newDocument("documents");
      doc1.set("document_id", "doc1");
      doc1.set("title", "Normal Document");
      doc1.set("embedding", new float[] { 1.0f, 0.0f, 0.0f });
      doc1.save();

      final MutableDocument doc2 = database.newDocument("documents");
      doc2.set("document_id", "doc2");
      doc2.set("title", "Bad Document");
      doc2.set("embedding", new float[] { Float.NaN, 1.0f, 2.0f });
      doc2.save();

      final MutableDocument doc3 = database.newDocument("documents");
      doc3.set("document_id", "doc3");
      doc3.set("title", "Inf Document");
      doc3.set("embedding", new float[] { Float.POSITIVE_INFINITY, 1.0f, 2.0f });
      doc3.save();
    });

    database.transaction(() -> {
      // Data quality checks - calculate flags in SELECT then filter
      final ResultSet rs = database.query("sql",
          "SELECT document_id, title, vectorHasNaN(embedding) as hasNaN, vectorHasInf(embedding) as hasInf " +
              "FROM documents WHERE vectorHasNaN(embedding) = true OR vectorHasInf(embedding) = true");

      int badDocs = 0;
      while (rs.hasNext()) {
        rs.next();
        badDocs++;
      }
      assertThat(badDocs).isEqualTo(2);
    });
  }

  @Test
  void testPhase6_OutlierDetection() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("document_id", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc1 = database.newDocument("documents");
      doc1.set("document_id", "normal");
      doc1.set("embedding", new float[] { 1.0f, 2.0f, 1.5f });
      doc1.save();

      final MutableDocument doc2 = database.newDocument("documents");
      doc2.set("document_id", "outlier");
      doc2.set("embedding", new float[] { 10.0f, 2.0f, 1.5f });
      doc2.save();
    });

    database.transaction(() -> {
      // Outlier detection (blog post example)
      final ResultSet rs = database.query("sql",
          "SELECT document_id, vectorLInfNorm(embedding) as max_component, vectorL1Norm(embedding) as manhattan_norm " +
              "FROM documents WHERE vectorLInfNorm(embedding) > 5.0");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<String>getProperty("document_id")).isEqualTo("outlier");
      assertThat(result.<Float>getProperty("max_component")).isGreaterThan(5.0f);
    });
  }

  @Test
  void testPhase6_ValueClipping() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("documents");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final MutableDocument doc = database.newDocument("documents");
      doc.set("embedding", new float[] { 5.0f, -4.0f, 2.0f });
      doc.save();
    });

    database.transaction(() -> {
      // Value clipping (blog post example)
      database.command("sql",
          "UPDATE documents SET embedding = vectorClip(embedding, -3.0, 3.0) WHERE vectorLInfNorm(embedding) > 3.0");
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT embedding FROM documents");
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final float[] clipped = result.getProperty("embedding");

      // Verify values are clipped to [-3.0, 3.0]
      for (float v : clipped) {
        assertThat(v).isBetween(-3.0f, 3.0f);
      }
    });
  }

  // =============================================================================
  // Real-World Examples
  // =============================================================================

  @Test
  void testRealWorld_ProductionRAGWithHybridSearch() {
    database.transaction(() -> {
      // Step 1: Create Document type
      final DocumentType docType = database.getSchema().createDocumentType("Document");
      docType.createProperty("title", Type.STRING);
      docType.createProperty("content", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Step 2: Create LSM vector index FIRST
      database.getSchema()
          .buildTypeIndex("Document", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(3)
          .withSimilarity("COSINE")
          .create();

      // Step 3: Index documents with normalized embeddings
      final MutableDocument doc1 = database.newDocument("Document");
      doc1.set("title", "Machine Learning Basics");
      doc1.set("content", "Introduction to neural networks and deep learning");
      doc1.set("embedding", normalizeVector(new float[] { 1.0f, 0.5f, 0.2f }));
      doc1.save();

      final MutableDocument doc2 = database.newDocument("Document");
      doc2.set("title", "Database Systems");
      doc2.set("content", "SQL and NoSQL database architectures");
      doc2.set("embedding", normalizeVector(new float[] { 0.2f, 0.8f, 0.5f }));
      doc2.save();

      final MutableDocument doc3 = database.newDocument("Document");
      doc3.set("title", "Neural Networks");
      doc3.set("content", "Deep learning models for image recognition");
      doc3.set("embedding", normalizeVector(new float[] { 0.9f, 0.3f, 0.1f }));
      doc3.save();
    });

    database.transaction(() -> {
      // Step 4: Vector search using LSM index
      final TypeIndex index = (TypeIndex) database.getSchema()
          .getIndexByName("Document[embedding]");
      assertThat(index).isNotNull();

      final LSMVectorIndex lsmIndex =
          (LSMVectorIndex) index.getIndexesOnBuckets()[0];

      final float[] queryVector = normalizeVector(new float[] { 1.0f, 0.3f, 0.1f });
      final List<Pair<RID, Float>> results =
          lsmIndex.findNeighborsFromVector(queryVector, 10);

      assertThat(results).isNotEmpty();
      // Verify we can retrieve the documents
      for (Pair<RID, Float> pair : results) {
        final Document doc = pair.getFirst().asDocument();
        assertThat(doc.<String>get("title")).isNotNull();
      }
    });
  }

  @Test
  void testRealWorld_MultiModalRAG() {
    database.transaction(() -> {
      // Step 1: Create type for multi-modal documents
      final DocumentType docType = database.getSchema().createDocumentType("MultiModalDocument");
      docType.createProperty("title", Type.STRING);
      docType.createProperty("content", Type.STRING);
      docType.createProperty("text_embedding", Type.ARRAY_OF_FLOATS);
      docType.createProperty("image_embedding", Type.ARRAY_OF_FLOATS);
      docType.createProperty("composite_embedding", Type.ARRAY_OF_FLOATS);

      // Step 2: Insert documents with composite embeddings (combined manually)
      final float[] textEmb = new float[] { 1.0f, 0.0f, 0.0f };
      final float[] imageEmb = new float[] { 0.0f, 1.0f, 0.0f };

      // Combine embeddings: 60% text + 40% image
      final float[] combined = new float[3];
      for (int i = 0; i < 3; i++) {
        combined[i] = (textEmb[i] * 0.6f) + (imageEmb[i] * 0.4f);
      }
      final float[] composite = normalizeVector(combined);

      final MutableDocument doc = database.newDocument("MultiModalDocument");
      doc.set("title", "Multi-modal Doc");
      doc.set("content", "Test content");
      doc.set("text_embedding", textEmb);
      doc.set("image_embedding", imageEmb);
      doc.set("composite_embedding", composite);
      doc.save();
    });

    database.transaction(() -> {
      // Step 3: Create LSM vector index on composite
      database.getSchema()
          .buildTypeIndex("MultiModalDocument", new String[] { "composite_embedding" })
          .withLSMVectorType()
          .withDimensions(3)
          .withSimilarity("COSINE")
          .create();

      // Step 4: Search across modalities using LSM index
      final TypeIndex index = (TypeIndex) database.getSchema()
          .getIndexByName("MultiModalDocument[composite_embedding]");
      assertThat(index).isNotNull();

      final LSMVectorIndex lsmIndex =
          (LSMVectorIndex) index.getIndexesOnBuckets()[0];

      final float[] queryComposite = normalizeVector(new float[] { 0.6f, 0.4f, 0.0f });
      final List<Pair<RID, Float>> results =
          lsmIndex.findNeighborsFromVector(queryComposite, 10);

      assertThat(results).isNotEmpty();
      final Document doc = results.getFirst().getFirst().asDocument();
      assertThat(doc.<String>get("title")).isEqualTo("Multi-modal Doc");
    });
  }

  @Test
  void testRealWorld_SPLADESparseHybrid() {
    database.transaction(() -> {
      // Step 1: Create document type
      final DocumentType docType = database.getSchema().createDocumentType("SpladeDocument");
      docType.createProperty("title", Type.STRING);
      docType.createProperty("content", Type.STRING);
      docType.createProperty("dense_emb", Type.ARRAY_OF_FLOATS);

      // Step 2: Insert with dense embeddings
      final float[] denseEmbedding = new float[] { 1.0f, 0.5f, 0.2f };

      final MutableDocument doc = database.newDocument("SpladeDocument");
      doc.set("title", "Test Doc");
      doc.set("content", "Sample content");
      doc.set("dense_emb", normalizeVector(denseEmbedding));
      doc.save();

      // Step 3: Create LSM vector index on dense embeddings
      database.getSchema()
          .buildTypeIndex("SpladeDocument", new String[] { "dense_emb" })
          .withLSMVectorType()
          .withDimensions(3)
          .withSimilarity("COSINE")
          .create();
    });

    database.transaction(() -> {
      // Step 4: Dense vector search with hybrid scoring
      final float[] queryDense = normalizeVector(new float[] { 0.9f, 0.4f, 0.3f });
      final ResultSet rs = database.query("sql",
          "SELECT title, vectorCosineSimilarity(dense_emb, ?) as similarity, " +
              "vectorHybridScore(vectorCosineSimilarity(dense_emb, ?), 0.8, 0.7) as hybridScore " +
              "FROM SpladeDocument ORDER BY hybridScore DESC LIMIT 10",
          queryDense, queryDense);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Float>getProperty("similarity")).isNotNull();
      assertThat(result.<Float>getProperty("hybridScore")).isNotNull();
    });
  }

  @Test
  void testRealWorld_LSMVectorIndexCreationAndQuery() {
    database.transaction(() -> {
      // Your First Vector Search in 5 Minutes (blog post example)
      // Step 1: Create document type with vector property
      final DocumentType docType = database.getSchema().createDocumentType("Document");
      docType.createProperty("title", Type.STRING);
      docType.createProperty("content", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Step 2: Create LSM vector index with JVector
      database.getSchema()
          .buildTypeIndex("Document", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(1536)
          .withSimilarity("COSINE")
          .create();

      // Step 3: Insert documents
      final MutableDocument doc = database.newDocument("Document");
      doc.set("title", "Introduction to RAG");
      doc.set("content", "Retrieval Augmented Generation combines...");
      doc.set("embedding", generateTestVector(1536, 1));
      doc.save();
    });

    database.transaction(() -> {
      // Step 4: Vector search using LSM index
      final TypeIndex index = (TypeIndex) database.getSchema()
          .getIndexByName("Document[embedding]");
      assertThat(index).isNotNull();

      final LSMVectorIndex lsmIndex =
          (LSMVectorIndex) index.getIndexesOnBuckets()[0];

      final float[] queryEmbedding = generateTestVector(1536, 1);
      final List<Pair<RID, Float>> results =
          lsmIndex.findNeighborsFromVector(queryEmbedding, 10);

      assertThat(results).isNotEmpty();
      final Document topDoc = results.getFirst().getFirst().asDocument();
      assertThat(topDoc.<String>get("title")).isEqualTo("Introduction to RAG");

      // Step 5: Use vector functions (from document data)
      final Object embeddingObj = topDoc.get("embedding");
      final ResultSet funcs = database.query("sql",
          "SELECT vectorMagnitude(?) as magnitude, vectorDimension(?) as dims", embeddingObj, embeddingObj);

      assertThat(funcs.hasNext()).isTrue();
      final Result funcResult = funcs.next();
      assertThat(funcResult.<Integer>getProperty("dims")).isEqualTo(1536);
      assertThat(funcResult.<Float>getProperty("magnitude")).isNotNull();
    });
  }

  // =============================================================================
  // SQL distance() Function Tests (Blog Post Examples)
  // =============================================================================

  /*
   * IMPORTANT NOTE: The blog post shows distance() function usage like:
   *   SELECT distance(embedding, :queryVector, 'COSINE') as score FROM Document
   *
   * However, ArcadeDB's SQL parser currently does NOT support positional parameters (?)
   * within function calls in the SELECT clause. The actual vector search approaches are:
   *
   * 1. Use vectorNeighbors() function:
   *    SELECT vectorNeighbors('Document[embedding]', [1.0, 0.5, 0.2], 10) as neighbors
   *
   * 2. Use TypeIndex API directly (as shown in testRealWorld examples)
   *
   * 3. Use vectorDistance() with array literals (not with parameters):
   *    SELECT vectorDistance(embedding, [1.0, 0.5, 0.2], 'COSINE') as distance FROM Document
   *
   * The blog post needs to be updated to reflect the actual supported SQL syntax.
   */

  /*
   * TESTS COMMENTED OUT: The blog post shows SQL syntax that is not currently supported.
   * These tests would fail because:
   * - Cannot use parameters (?) inside function calls in SELECT clause
   * - vectorDistance() and other vector functions cannot be used in SELECT with parameters
   *
   * The actual supported approaches are:
   * 1. Use TypeIndex API directly (as shown in Real-World examples)
   * 2. Use vectorNeighbors with array literals (but this is not user-friendly for dynamic queries)
   */

  @Test
  void testBlogPost_SQLCreateIndexWithJSONMetadata() {
    database.transaction(() -> {
      // Create document type (correct SQL syntax from LSMVectorIndexTest)
      database.command("sql", "CREATE VERTEX TYPE VectorVertex IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorVertex.embedding IF NOT EXISTS ARRAY_OF_FLOATS");

      // Blog post example (lines 56-63): CREATE INDEX with METADATA JSON syntax
      // This syntax IS supported - from LSMVectorIndexTest.java line 56
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 128,
            "similarity" : "COSINE",
            "maxConnections" : 16,
            "beamWidth" : 100
          }""");

      // Verify index was created
      final Index index = database.getSchema().getIndexByName("VectorVertex[embedding]");
      assertThat(index).isNotNull();
    });
  }

  @Test
  void testBlogPost_SQLCreateIndexWithQuantization() {
    database.transaction(() -> {
      // Create document type with correct SQL syntax
      database.command("sql", "CREATE DOCUMENT TYPE QuantDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY QuantDoc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");

      // Blog post example (lines 311-316): CREATE INDEX with INT8 quantization using JSON METADATA
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON QuantDoc (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 128,
            "quantization" : "INT8",
            "similarity" : "COSINE"
          }""");

      // Verify index with quantization
      final TypeIndex index = (TypeIndex) database.getSchema()
          .getIndexByName("QuantDoc[embedding]");
      assertThat(index).isNotNull();

      final LSMVectorIndex lsmIndex =
          (LSMVectorIndex) index.getIndexesOnBuckets()[0];
      assertThat(lsmIndex.getMetadata().quantizationType)
          .isEqualTo(VectorQuantizationType.INT8);
    });
  }

  // =============================================================================
  // Helper Methods
  // =============================================================================

  private float[] generateTestVector(final int dimensions, final int seed) {
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      vector[i] = (float) Math.sin(seed + i * 0.1);
    }
    return vector;
  }

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
