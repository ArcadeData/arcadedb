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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the JSON shape that {@link LSMSparseVectorIndexMetrics#buildJSON} produces for the
 * Server tab card (Tier 1 follow-up to #4068). If the keys, ordering, or value types ever
 * drift, the dashboard would render zeros silently and only this test would catch it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMSparseVectorIndexMetricsTest extends TestHelper {

  private static final String TYPE_NAME = "MetricsDoc";
  private static final String IDX_NAME  = "MetricsDoc[tokens,weights]";

  @Test
  void buildsEmptyJsonOnDatabaseWithoutSparseIndex() {
    database.transaction(() -> database.getSchema().createDocumentType("Plain"));

    final JSONObject metrics = LSMSparseVectorIndexMetrics.buildJSON(database);
    assertThat(metrics.toString()).isEqualTo("{}");
  }

  @Test
  void buildsEmptyJsonOnNullDatabase() {
    assertThat(LSMSparseVectorIndexMetrics.buildJSON(null).toString()).isEqualTo("{}");
  }

  @Test
  void emitsOneEntryPerSparseIndexBucketWithExpectedFields() {
    final int[]   indices = new int[] { 1, 5, 9 };
    final float[] values  = new float[] { 0.3f, 0.7f, 0.5f };

    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      final TypeIndex idx = database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(100)
          .create();
      assertThat(idx.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);

      // A handful of rows so memtable posting count is non-zero pre-flush. The exact totals
      // depend on bucket selection (one sub-index per bucket), so the assertions below are on
      // the field shape and aggregate sums rather than specific per-bucket values.
      for (int i = 0; i < 5; i++) {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("tokens", indices);
        doc.set("weights", values);
        doc.save();
      }
    });

    final JSONObject metrics = LSMSparseVectorIndexMetrics.buildJSON(database);

    assertThat(metrics.length())
        .as("at least one LSMSparseVectorIndex sub-index should be reported")
        .isGreaterThanOrEqualTo(1);

    // The card aggregates per-bucket sub-indexes under the user-facing TypeIndex name, so Studio
    // shows one row regardless of how many buckets the partition has split into.
    assertThat(metrics.length()).isEqualTo(1);
    assertThat(metrics.has(IDX_NAME)).as("aggregated key must be the TypeIndex name").isTrue();

    final JSONObject entry = metrics.getJSONObject(IDX_NAME);
    assertThat(entry.has("memtablePostings"))
        .as("entry must expose memtablePostings").isTrue();
    assertThat(entry.has("segmentCount"))
        .as("entry must expose segmentCount").isTrue();
    assertThat(entry.has("totalPostings"))
        .as("entry must expose totalPostings").isTrue();
    assertThat(entry.getLong("memtablePostings")).isGreaterThanOrEqualTo(0L);
    assertThat(entry.getInt("segmentCount")).isGreaterThanOrEqualTo(0);
    // 5 docs * 3 nnz = 15 postings, summed across however many buckets exist.
    assertThat(entry.getLong("totalPostings")).isEqualTo(15L);
  }
}
