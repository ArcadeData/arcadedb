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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * INT8 pre-quantized ingest tests for LSMVectorIndex (issue #4132). Covers the wire / storage
 * encoding distinction (FLOAT32 default vs INT8 opt-in), the byte[] -> float[] dequantization on
 * the put path, and parity between FLOAT32 and INT8 result sets on the same logical vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexInt8IngestTest extends TestHelper {
  private static final int DIMENSIONS  = 64;
  private static final int NUM_VECTORS = 50;

  @Test
  void defaultEncodingIsFloat32() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .create();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().encoding).isEqualTo(VectorEncoding.FLOAT32);
      assertThat(lsm.getKeyTypes()).containsExactly(Type.ARRAY_OF_FLOATS);
    });
  }

  @Test
  void int8EncodingFlipsKeyTypesToBinary() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("embedding", Type.BINARY);

      database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding(VectorEncoding.INT8)
          .create();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().encoding).isEqualTo(VectorEncoding.INT8);
      assertThat(lsm.getKeyTypes()).containsExactly(Type.BINARY);
    });
  }

  @Test
  void int8IngestEndToEnd() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("id", Type.INTEGER);
      docType.createProperty("embedding", Type.BINARY);

      database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding("INT8")
          .create();

      for (int i = 0; i < NUM_VECTORS; i++) {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("id", i);
        doc.set("embedding", quantize(generateNormalizedTestVector(DIMENSIONS, i)));
        doc.save();
      }
    });

    database.transaction(() -> {
      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];

      // Query with the dequantized form of seed 0; the matching record was indexed from the same
      // calibration, so it must come back as the top hit.
      final float[] floatQuery = VectorUtils.dequantizeInt8ToFloat(quantize(generateNormalizedTestVector(DIMENSIONS, 0)));
      final List<Pair<RID, Float>> floatResults = lsm.findNeighborsFromVector(floatQuery, 5);
      assertThat(floatResults).isNotEmpty();
      assertThat(floatResults.size()).isLessThanOrEqualTo(5);

      // Same logical query passed as raw bytes - the index must produce the same top RID. We round
      // through Object[] keys to exercise the IndexInternal#get(Object[]) byte[] branch.
      final byte[] byteQuery = quantize(generateNormalizedTestVector(DIMENSIONS, 0));
      final var cursor = lsm.get(new Object[] { byteQuery }, 5);
      assertThat(cursor.hasNext()).isTrue();
      final RID byteTopRid = (RID) cursor.next();
      assertThat(byteTopRid).isEqualTo(floatResults.get(0).getFirst());
    });
  }

  @Test
  void int8AndFloat32ParityOnSameLogicalVectors() {
    // Build two indexes on the same logical vectors, one FLOAT32 and one INT8 (after lossy
    // quantization). The top result for the seed-0 query must be the same record in both - we are
    // only checking that the INT8 path does not catastrophically reorder neighbours, not that
    // recall is identical (lossy quantization moves scores around).
    database.transaction(() -> {
      final DocumentType floatDoc = database.getSchema().createDocumentType("FloatDoc");
      floatDoc.createProperty("id", Type.INTEGER);
      floatDoc.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex("FloatDoc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .create();

      final DocumentType byteDoc = database.getSchema().createDocumentType("ByteDoc");
      byteDoc.createProperty("id", Type.INTEGER);
      byteDoc.createProperty("embedding", Type.BINARY);

      database.getSchema()
          .buildTypeIndex("ByteDoc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding(VectorEncoding.INT8)
          .create();

      for (int i = 0; i < NUM_VECTORS; i++) {
        final float[] vec = generateNormalizedTestVector(DIMENSIONS, i);

        final MutableDocument fd = database.newDocument("FloatDoc");
        fd.set("id", i);
        fd.set("embedding", vec);
        fd.save();

        final MutableDocument bd = database.newDocument("ByteDoc");
        bd.set("id", i);
        bd.set("embedding", quantize(vec));
        bd.save();
      }
    });

    database.transaction(() -> {
      final LSMVectorIndex floatIdx = (LSMVectorIndex) ((TypeIndex) database.getSchema()
          .getIndexByName("FloatDoc[embedding]")).getIndexesOnBuckets()[0];
      final LSMVectorIndex byteIdx = (LSMVectorIndex) ((TypeIndex) database.getSchema()
          .getIndexByName("ByteDoc[embedding]")).getIndexesOnBuckets()[0];

      final float[] query = generateNormalizedTestVector(DIMENSIONS, 0);

      final List<Pair<RID, Float>> floatHits = floatIdx.findNeighborsFromVector(query, 5);
      final List<Pair<RID, Float>> byteHits = byteIdx.findNeighborsFromVector(query, 5);

      assertThat(floatHits).isNotEmpty();
      assertThat(byteHits).isNotEmpty();

      // The seed-0 record must be the top hit in both indexes - lossy int8 quantization shifts
      // scores but should not displace the perfect-match neighbour from rank 1.
      final int floatTopId = ((Number) floatHits.get(0).getFirst().asDocument().get("id")).intValue();
      final int byteTopId = ((Number) byteHits.get(0).getFirst().asDocument().get("id")).intValue();
      assertThat(floatTopId).isEqualTo(0);
      assertThat(byteTopId).isEqualTo(0);
    });
  }

  @Test
  void sqlCreateIndexAcceptsEncodingMetadataKey() {
    // Pin the SQL DDL surface: `CREATE INDEX ... LSM_VECTOR METADATA {..., encoding: 'INT8'}`
    // must reach withMetadata(JSONObject) -> withEncoding and produce an INT8 index. A drift in
    // the JSON key name (e.g. "encoding" vs "vectorEncoding") would silently fall through to the
    // FLOAT32 default and waste 4x payload, so the contract gets a dedicated test.
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc")
          .createProperty("embedding", Type.BINARY);

      database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
              + "{dimensions: " + DIMENSIONS + ", similarity: 'COSINE', encoding: 'INT8'}");

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().encoding).isEqualTo(VectorEncoding.INT8);
      assertThat(lsm.getKeyTypes()).containsExactly(Type.BINARY);
    });
  }

  @Test
  void int8EncodingSurvivesReopen() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("embedding", Type.BINARY);

      database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding("INT8")
          .create();
    });
    reopenDatabase();
    database.transaction(() -> {
      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().encoding).isEqualTo(VectorEncoding.INT8);
    });
  }

  @Test
  void int8EncodingRejectsUnknownName() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("embedding", Type.BINARY);

      assertThatThrownBy(() -> database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withEncoding("FP16")
          .create())
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("Invalid vector encoding");
    });
  }

  @Test
  void rejectsInt8EncodingWithFloatPropertyType() {
    // Property-type / encoding mismatch: INT8 encoding requires the document property to be
    // BINARY (one byte per dim). Declaring ARRAY_OF_FLOATS with encoding=INT8 would silently
    // store floats and confuse downstream put/query paths - reject up front.
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc")
          .createProperty("embedding", Type.ARRAY_OF_FLOATS);

      assertThatThrownBy(() -> database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding(VectorEncoding.INT8)
          .create())
          .rootCause()
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("encoding=INT8 requires property")
          .hasMessageContaining("BINARY");
    });
  }

  @Test
  void rejectsFloat32EncodingWithBinaryPropertyType() {
    // Mirror image: FLOAT32 encoding does not understand a BINARY property column.
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc")
          .createProperty("embedding", Type.BINARY);

      assertThatThrownBy(() -> database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .create())
          .rootCause()
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("encoding=FLOAT32")
          .hasMessageContaining("BINARY");
    });
  }

  @Test
  void rejectsInt8EncodingPlusInt8Quantization() {
    // Picking encoding=INT8 and quantization=INT8 means dequantize-on-ingest then
    // re-quantize-internally - silent double-processing on data that was already lossy at the wire
    // level. The factory handler now blocks the combination outright (issue #4132 review fix).
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("embedding", Type.BINARY);

      assertThatThrownBy(() -> database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding(VectorEncoding.INT8)
          .withQuantization(VectorQuantizationType.INT8)
          .create())
          .rootCause()
          .hasMessageContaining("Combining encoding=INT8 with quantization=INT8");
    });
  }

  @Test
  void int8VectorsSurviveRebuildAfterReopen() {
    // Regression guard for the rebuild paths that previously called the deprecated
    // VectorUtils.convertToFloatArray (returns null for byte[]). Without the fix, an INT8 index
    // reopened from disk would silently rebuild the HNSW graph from an empty vector set: every
    // vector would be flagged invalid by the validation pass, and downstream search returns
    // nothing. We insert N int8 vectors, run a search to materialise the graph, reopen the
    // database to force a rebuild from disk, and assert the same query still returns hits.
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("id", Type.INTEGER);
      docType.createProperty("embedding", Type.BINARY);

      database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding(VectorEncoding.INT8)
          .create();

      for (int i = 0; i < NUM_VECTORS; i++) {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("id", i);
        doc.set("embedding", quantize(generateNormalizedTestVector(DIMENSIONS, i)));
        doc.save();
      }

      // Force the HNSW graph to be built and persisted before the reopen.
      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      final List<Pair<RID, Float>> beforeReopen = lsm.findNeighborsFromVector(
          generateNormalizedTestVector(DIMENSIONS, 0), 5);
      assertThat(beforeReopen).isNotEmpty();
    });

    reopenDatabase();

    database.transaction(() -> {
      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().encoding).isEqualTo(VectorEncoding.INT8);

      // Searching after reopen exercises the validation + preload paths that previously called
      // convertToFloatArray. If any of those paths silently dropped int8 vectors, we'd get an
      // empty hit list here.
      final List<Pair<RID, Float>> hits = lsm.findNeighborsFromVector(
          generateNormalizedTestVector(DIMENSIONS, 0), 5);
      assertThat(hits).isNotEmpty();
      final int topId = ((Number) hits.get(0).getFirst().asDocument().get("id")).intValue();
      assertThat(topId).isEqualTo(0);
    });
  }

  @Test
  void sqlVectorNeighborsAcceptsByteArrayQuery() {
    // SQL surface coverage: the vector.neighbors function must accept a raw byte[] query when the
    // target index is INT8-encoded. Prior to the encoding-aware overload, extractQueryVector ran
    // raw byte[] through the unconditional dequantize - this test pins the contract via the SQL
    // entry point so a future regression in either path is caught.
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("id", Type.INTEGER);
      docType.createProperty("embedding", Type.BINARY);

      database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding(VectorEncoding.INT8)
          .create();

      for (int i = 0; i < NUM_VECTORS; i++) {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("id", i);
        doc.set("embedding", quantize(generateNormalizedTestVector(DIMENSIONS, i)));
        doc.save();
      }
    });

    database.transaction(() -> {
      final byte[] byteQuery = quantize(generateNormalizedTestVector(DIMENSIONS, 0));
      final ResultSet rs = database.command("sql",
          "SELECT expand(`vector.neighbors`('Doc[embedding]', :q, 5))", Map.of("q", byteQuery));

      int rowCount = 0;
      int firstId = Integer.MIN_VALUE;
      while (rs.hasNext()) {
        final var row = rs.next();
        if (rowCount == 0)
          firstId = ((Number) row.getProperty("id")).intValue();
        rowCount++;
      }
      rs.close();

      assertThat(rowCount).isGreaterThan(0);
      assertThat(firstId).isEqualTo(0);
    });
  }

  /**
   * Quantizes a {@code float} vector to signed int8 using the Cohere/OpenAI calibration convention
   * ({@code round(v * 127)}, clamped to [-127, 127]). Mirror image of {@link
   * VectorUtils#dequantizeInt8ToFloat(byte[])} so the round trip is lossless within byte resolution.
   */
  private static byte[] quantize(final float[] v) {
    final byte[] out = new byte[v.length];
    for (int i = 0; i < v.length; i++) {
      final int q = Math.round(v[i] * 127f);
      out[i] = (byte) Math.max(-127, Math.min(127, q));
    }
    return out;
  }

  private static float[] generateNormalizedTestVector(final int dimensions, final int seed) {
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      vector[i] = (float) Math.sin(seed + i * 0.1);
    float norm = 0f;
    for (final float x : vector)
      norm += x * x;
    norm = (float) Math.sqrt(norm);
    if (norm > 0f)
      for (int i = 0; i < dimensions; i++)
        vector[i] /= norm;
    return vector;
  }
}
