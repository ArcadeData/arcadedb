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
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4134: every construction-time setting must reach the index metadata before the instance
 * escapes, replacing the historical 17-positional-arg constructor + post-construction mutation pattern. The settings now
 * travel as one {@link LSMVectorIndexMetadata}: the intermediate value object that used to carry them had a field list
 * of its own, and that list fell behind the metadata's, silently dropping four settings (issue #5639).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexMetadataPropagationTest extends TestHelper {
  private static final int DIMENSIONS = 32;

  /**
   * {@link LSMVectorIndexMetadata#copy} is the hop the settings take from the builder into the index, so a field it
   * forgets is a setting the user cannot reach. Every field is given a non-default value here: a copy() that misses one
   * fails on that field instead of being invisible until someone reports the knob does nothing.
   * <p>
   * The positive {@code locationCacheSize} below is NOT a sign that positive values are still accepted - they are
   * refused at both user-facing entrances (see {@code Issue5559LocationCacheSizeTest}). It is written straight to the
   * field, and {@code copy()} deliberately bypasses the refusing setter, because a copy of a value tolerated from an
   * older schema must not throw.
   */
  @Test
  void copyCarriesEverySetting() {
    final LSMVectorIndexMetadata source = new LSMVectorIndexMetadata("Doc", new String[] { "embedding" }, 7);
    source.dimensions = DIMENSIONS;
    source.similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    source.quantizationType = VectorQuantizationType.PRODUCT;
    source.encoding = VectorEncoding.INT8;
    source.maxConnections = 24;
    source.beamWidth = 150;
    source.efSearch = 250;
    source.neighborOverflowFactor = 1.4f;
    source.alphaDiversityRelaxation = 1.3f;
    source.idPropertyName = "docId";
    source.locationCacheSize = 1024;
    source.graphBuildCacheSize = 2048;
    source.mutationsBeforeRebuild = 4242;
    source.inactivityRebuildTimeoutMs = 7000;
    source.storeVectorsInGraph = true;
    source.addHierarchy = true;
    source.pqSubspaces = 8;
    source.pqClusters = 128;
    source.pqCenterGlobally = false;
    source.pqTrainingLimit = 4096;
    source.buildState = "BUILDING";

    final LSMVectorIndexMetadata copy = source.copy("Other", new String[] { "vector" }, -1);

    assertThat(copy.typeName).isEqualTo("Other");
    assertThat(copy.propertyNames).containsExactly("vector");
    assertThat(copy.associatedBucketId).isEqualTo(-1);
    assertThat(copy.dimensions).isEqualTo(DIMENSIONS);
    assertThat(copy.similarityFunction).isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
    assertThat(copy.quantizationType).isEqualTo(VectorQuantizationType.PRODUCT);
    assertThat(copy.encoding).isEqualTo(VectorEncoding.INT8);
    assertThat(copy.maxConnections).isEqualTo(24);
    assertThat(copy.beamWidth).isEqualTo(150);
    assertThat(copy.efSearch).isEqualTo(250);
    assertThat(copy.neighborOverflowFactor).isEqualTo(1.4f);
    assertThat(copy.alphaDiversityRelaxation).isEqualTo(1.3f);
    assertThat(copy.idPropertyName).isEqualTo("docId");
    assertThat(copy.locationCacheSize).isEqualTo(1024);
    assertThat(copy.graphBuildCacheSize).isEqualTo(2048);
    assertThat(copy.mutationsBeforeRebuild).isEqualTo(4242);
    assertThat(copy.inactivityRebuildTimeoutMs).isEqualTo(7000);
    assertThat(copy.storeVectorsInGraph).isTrue();
    assertThat(copy.addHierarchy).isTrue();
    assertThat(copy.pqSubspaces).isEqualTo(8);
    assertThat(copy.pqClusters).isEqualTo(128);
    assertThat(copy.pqCenterGlobally).isFalse();
    assertThat(copy.pqTrainingLimit).isEqualTo(4096);
    // buildState is per-index lifecycle state, not a setting: a rebuilt index must start from its own default.
    assertThat(copy.buildState).isEqualTo("READY");
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

  /**
   * Issue #5352: {@code maxConnections} is JVector's Vamana per-layer degree (applied verbatim to the base
   * layer, not doubled like hnswlib's {@code M}). The default was raised from 16 to 32 so out-of-the-box
   * density matches hnswlib {@code M=16} (2*16) and recall no longer silently lands at half the intended
   * graph density. This test pins the new default across the builder path so a future regression is caught.
   */
  @Test
  void defaultMaxConnectionsIsThirtyTwoIssue5352() {
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("DocDefault");
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex("DocDefault", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .create();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("DocDefault[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
      assertThat(lsm.getMetadata().maxConnections).isEqualTo(32);
      assertThat(lsm.getMaxConnections()).isEqualTo(32);
    });
  }

  /**
   * Issue #5352: the same default (32) must apply through the SQL {@code CREATE INDEX ... METADATA} path when
   * {@code maxConnections} is omitted, and an explicit value must still win.
   */
  @Test
  void defaultMaxConnectionsIsThirtyTwoViaSqlIssue5352() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("DocSqlDefault").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql",
          "CREATE INDEX ON DocSqlDefault (embedding) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: 'COSINE'}");

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("DocSqlDefault[embedding]");
      assertThat(((LSMVectorIndex) idx.getIndexesOnBuckets()[0]).getMetadata().maxConnections).isEqualTo(32);

      database.getSchema().createDocumentType("DocSqlExplicit").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql",
          "CREATE INDEX ON DocSqlExplicit (embedding) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS
              + ", similarity: 'COSINE', maxConnections: 48}");

      final TypeIndex explicit = (TypeIndex) database.getSchema().getIndexByName("DocSqlExplicit[embedding]");
      assertThat(((LSMVectorIndex) explicit.getIndexesOnBuckets()[0]).getMetadata().maxConnections).isEqualTo(48);
    });
  }
}
