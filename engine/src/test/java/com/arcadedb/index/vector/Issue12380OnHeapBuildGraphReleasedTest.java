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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12380: with {@code storeVectorsInGraph} a build persists its graph and swaps the resident one for the
 * on-disk copy, but {@code persistedGraphSource} kept the on-heap build graph - and through its diversity provider the
 * build's reader and vector cache, about 40 MB per index of 12,500 768-dimension vectors - until the next build. On a schema
 * of dozens of bucket indexes that alone filled the heap.
 */
class Issue12380OnHeapBuildGraphReleasedTest extends TestHelper {
  private static final int DIMENSIONS = 32;
  private static final int VECTORS    = 300;

  @ParameterizedTest(name = "storeVectorsInGraph={0}")
  @ValueSource(booleans = { true, false })
  void anIndexKeepsTheOnHeapBuildGraphOnlyWhileItIsTheResidentOne(final boolean storeVectorsInGraph) {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create()
          .createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"storeVectorsInGraph\": " + storeVectorsInGraph + " }");
    });
    database.transaction(() -> {
      for (int i = 0; i < VECTORS; i++)
        database.newDocument("Doc").set("embedding", vector(i)).save();
    });

    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]"))
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();

    if (storeVectorsInGraph) {
      assertThat(index.getGraphIndex()).as("precondition: the index reads its graph from the pages").isInstanceOf(
          OnDiskGraphIndex.class);
      assertThat(index.persistedGraphSourceForTest())
          .as("nothing keeps the on-heap build graph once the on-disk graph is resident").isNull();
    } else {
      // The control: an on-heap resident graph stays the certified twin the heap gate demotes to the pages (#7260)
      assertThat(index.getGraphIndex()).isInstanceOf(OnHeapGraphIndex.class);
      assertThat(index.persistedGraphSourceForTest()).isSameAs(index.getGraphIndex());
    }
  }

  private static float[] vector(final int i) {
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) Math.sin(i * 0.31 + j * 0.77) + 1.5f;
    return v;
  }
}
