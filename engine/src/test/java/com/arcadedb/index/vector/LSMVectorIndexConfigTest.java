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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4134: ensure that the consolidated {@link LSMVectorIndexConfig}
 * record propagates every field (including {@code encoding}) into the index metadata at
 * construction time, replacing the historical 17-positional-arg constructor + post-construction
 * mutation pattern. The factory handler path must keep working, and INT8 / FLOAT32 encodings must
 * survive the trip through the config record into the metadata.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexConfigTest extends TestHelper {
  private static final int DIMENSIONS = 32;

  @Test
  void configRecordExposesEveryField() {
    final LSMVectorIndexConfig config = new LSMVectorIndexConfig(
        "Doc", new String[] { "embedding" }, DIMENSIONS,
        VectorSimilarityFunction.COSINE, VectorEncoding.INT8, VectorQuantizationType.NONE,
        16, 100, "id",
        -1, -1, -1, false, true,
        8, 256, true, 4096);

    assertThat(config.typeName()).isEqualTo("Doc");
    assertThat(config.propertyNames()).containsExactly("embedding");
    assertThat(config.dimensions()).isEqualTo(DIMENSIONS);
    assertThat(config.similarityFunction()).isEqualTo(VectorSimilarityFunction.COSINE);
    assertThat(config.encoding()).isEqualTo(VectorEncoding.INT8);
    assertThat(config.quantizationType()).isEqualTo(VectorQuantizationType.NONE);
    assertThat(config.maxConnections()).isEqualTo(16);
    assertThat(config.beamWidth()).isEqualTo(100);
    assertThat(config.idPropertyName()).isEqualTo("id");
    assertThat(config.locationCacheSize()).isEqualTo(-1);
    assertThat(config.graphBuildCacheSize()).isEqualTo(-1);
    assertThat(config.mutationsBeforeRebuild()).isEqualTo(-1);
    assertThat(config.storeVectorsInGraph()).isFalse();
    assertThat(config.addHierarchy()).isTrue();
    assertThat(config.pqSubspaces()).isEqualTo(8);
    assertThat(config.pqClusters()).isEqualTo(256);
    assertThat(config.pqCenterGlobally()).isTrue();
    assertThat(config.pqTrainingLimit()).isEqualTo(4096);
  }

  @Test
  void factoryHandlerPropagatesEncodingFloat32ByDefault() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("DocFloat");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex("DocFloat", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .create();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("DocFloat[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().encoding).isEqualTo(VectorEncoding.FLOAT32);
      assertThat(lsm.getMetadata().dimensions).isEqualTo(DIMENSIONS);
      assertThat(lsm.getMetadata().similarityFunction).isEqualTo(VectorSimilarityFunction.COSINE);
    });
  }

  @Test
  void factoryHandlerPropagatesEncodingInt8AtConstructionTime() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("DocInt8");
      docType.createProperty("embedding", Type.BINARY);

      database.getSchema()
          .buildTypeIndex("DocInt8", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding(VectorEncoding.INT8)
          .create();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("DocInt8[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().encoding).isEqualTo(VectorEncoding.INT8);
    });
  }

  @Test
  void factoryHandlerPropagatesAllNonDefaultMetadataFields() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("DocCustom");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex("DocCustom", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("DOT_PRODUCT")
          .withMaxConnections(24)
          .withBeamWidth(150)
          .withIdProperty("docId")
          .create();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("DocCustom[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().similarityFunction).isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
      assertThat(lsm.getMetadata().maxConnections).isEqualTo(24);
      assertThat(lsm.getMetadata().beamWidth).isEqualTo(150);
      assertThat(lsm.getMetadata().idPropertyName).isEqualTo("docId");
    });
  }
}
