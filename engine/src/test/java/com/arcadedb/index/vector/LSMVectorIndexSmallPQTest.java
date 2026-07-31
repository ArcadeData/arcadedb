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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5417: an LSM_VECTOR index configured with PRODUCT quantization and holding fewer vectors than
 * the configured number of PQ clusters (256 by default) used to fail its graph build with
 * {@code IllegalArgumentException: Number of clusters 256 cannot exceed number of points N}, leaving the index permanently
 * without a graph so that searches returned no results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexSmallPQTest extends TestHelper {
  private static final int DIMENSIONS = 8;

  @Test
  void productQuantizationWithFewerVectorsThanClusters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    // Only 3 vectors: far fewer than the default 256 PQ clusters
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Doc").set("id", i).set("embedding", vector(i)).save();
    });

    // The graph is built eagerly at CREATE INDEX time: before the fix this threw IndexException
    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
          "dimensions": 8,
          "similarity": "COSINE",
          "quantization": "PRODUCT"
        }""");

    final LSMVectorIndex index = vectorIndex();
    assertThat(index.getMetadata().quantizationType).isEqualTo(VectorQuantizationType.PRODUCT);

    // PQ is not trainable at this size, so the index must simply run without it
    assertThat(index.isPQSearchAvailable()).isFalse();

    final List<Pair<RID, Float>> exact = index.findNeighborsFromVector(vector(0), 3);
    assertThat(exact).hasSize(3);

    // The approximate entry point must degrade to exact search instead of returning nothing
    final List<Pair<RID, Float>> approximate = index.findNeighborsFromVectorApproximate(vector(0), 3, null);
    assertThat(approximate).hasSize(3);
  }

  @Test
  void productQuantizationRebuildAfterGrowingPastClusters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Doc").set("id", i).set("embedding", vector(i)).save();
    });

    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
          "dimensions": 8,
          "similarity": "COSINE",
          "quantization": "PRODUCT",
          "pqClusters": 16
        }""");

    assertThat(vectorIndex().isPQSearchAvailable()).isFalse();

    // Grow past the cluster count: the next rebuild from scratch must now train PQ for real
    database.transaction(() -> {
      for (int i = 3; i < 200; i++)
        database.newDocument("Doc").set("id", i).set("embedding", vector(i)).save();
    });

    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();

    assertThat(index.isPQSearchAvailable()).isTrue();
    assertThat(index.getPQVectorCount()).isEqualTo(200);

    final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(vector(7), 5, null);
    assertThat(results).hasSize(5);
  }

  @Test
  void productQuantizationDiscardedWhenIndexShrinksBelowClusters() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Doc").set("id", i).set("embedding", vector(i)).save();
    });

    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
          "dimensions": 8,
          "similarity": "COSINE",
          "quantization": "PRODUCT"
        }""");

    assertThat(vectorIndex().isPQSearchAvailable()).isTrue();

    // Shrink well below the cluster count: the stale codebook refers to ordinals that no longer exist, so the rebuild
    // must drop it instead of scoring the wrong vectors with it
    database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE id >= 5"));

    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();

    assertThat(index.isPQSearchAvailable()).isFalse();
    assertThat(index.findNeighborsFromVectorApproximate(vector(1), 5, null)).hasSize(5);
  }

  @Test
  void moreThan256PQClustersRejectedAtCreation() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    // The builder still raises IllegalArgumentException; the SQL layer wraps every METADATA value it cannot
    // read into a parsing exception, so the HTTP answer to a malformed statement is a 400 (issue #5607).
    assertThatThrownBy(() -> database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
          "dimensions": 8,
          "similarity": "COSINE",
          "quantization": "PRODUCT",
          "pqClusters": 512
        }"""))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("pqClusters cannot exceed 256")
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  private LSMVectorIndex vectorIndex() {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    assertThat(index).isNotNull();
    return (LSMVectorIndex) index.getIndexesOnBuckets()[0];
  }

  private static float[] vector(final int seed) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) Math.sin((seed + 1) * 0.37 + i * 0.91) + 1.5f;
    return v;
  }
}
