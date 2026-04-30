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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that EXTERNAL property storage keeps the primary bucket dense: when a heavy property is flagged EXTERNAL,
 * the primary bucket grows in proportion to the inline-only data while the heavy payload accumulates in the paired
 * external bucket. This exercises the page-cache density win that motivates the feature.
 *
 * Tagged @slow because it inserts thousands of records with multi-KB embeddings; not run in the default suite.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class ExternalPropertyDensitySlowTest extends TestHelper {

  @Test
  void primaryStaysSmallWhenHeavyPropertyIsExternal() {
    final VertexType type = database.getSchema().createVertexType("V");
    type.createProperty("name", Type.STRING);
    type.createProperty("embedding", Type.ARRAY_OF_FLOATS).setExternal(true);

    final int recordCount = 2000;
    final int dim = 1024; // 4 KB per embedding

    database.transaction(() -> {
      for (int i = 0; i < recordCount; i++) {
        final float[] e = new float[dim];
        for (int j = 0; j < dim; j++)
          e[j] = (float) (i * 0.001 + j);
        final MutableVertex v = database.newVertex("V").set("name", "u" + i).set("embedding", e);
        v.save();
      }
    });

    final LocalBucket primary = (LocalBucket) type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);

    final long primaryPages = primary.getTotalPages();
    final long externalPages = external.getTotalPages();

    // External bucket should hold the bulk of bytes (at least ~5x the primary bucket's page count given a 4KB
    // payload per record vs. a small name+pointer per record). The exact ratio depends on page packing, so we use
    // a conservative lower bound to keep the test stable.
    assertThat(externalPages).as("external pages=%d primary pages=%d", externalPages, primaryPages)
        .isGreaterThan(primaryPages * 3);

    // Sanity: count of records is recordCount.
    assertThat(database.countType("V", false)).isEqualTo(recordCount);
  }
}
