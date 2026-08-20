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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code efSearch: 100} must mean what it says.
 * <p>
 * The search path used to decide whether the user had configured a beam by testing
 * {@code metadata.efSearch != 100}. Since 100 is also the default, that made 100 the one value the knob could
 * not express: an index created with {@code "efSearch": 100} was treated as unconfigured and given the
 * adaptive beam instead, which above 10,000 nodes is {@code max(k * 2, 20)}.
 * <p>
 * The assertion below fails on main, where the configured 100 is discarded and the adaptive beam is used.
 */
@Tag("vector")
class EfSearch100IsRequestableTest extends TestHelper {
  private static final int DIMENSIONS  = 64;
  // Above the adaptive path's 10,000-node branch, so an unconfigured index would take the narrow beam.
  private static final int NUM_VECTORS = 12_000;
  private static final int K           = 10;

  @Test
  void efSearch100IsHonouredRatherThanReadAsUnset() {
    final Random rng = new Random(7);
    final List<float[]> vectors = new ArrayList<>(NUM_VECTORS);

    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType("Doc");
      t.createProperty("id", Type.INTEGER);
      t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      for (int i = 0; i < NUM_VECTORS; i++) {
        final float[] v = randomVector(rng);
        vectors.add(v);
        database.newDocument("Doc").set("id", i).set("embedding", v).save();
      }
    });

    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
        + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", \"maxConnections\": 32, "
        + "\"efSearch\": 100 }");

    final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
    for (int i = 0; i < 900 && lsm.getStats().get("graphNodeCount") == 0L; i++)
      try { Thread.sleep(100); } catch (final InterruptedException ignored) { break; }

    // Asserted through BEHAVIOUR, not through the new field: a test that references the fix does not compile
    // against unpatched code, so it cannot demonstrate the defect it exists to pin. This compiles on main and
    // fails there.
    database.transaction(() -> {
      for (int q = 0; q < 20; q++) {
        final float[] query = vectors.get(q * 137 % NUM_VECTORS);
        final List<com.arcadedb.database.RID> fromMetadata = rids(lsm.findNeighborsFromVector(query, K, -1));
        final List<com.arcadedb.database.RID> fromExplicit = rids(lsm.findNeighborsFromVector(query, K, 100));
        assertThat(fromMetadata)
            .as("query %d: the configured efSearch=100 must give the same result as asking for 100 per query", q)
            .isEqualTo(fromExplicit);
      }
    });
  }

  private static List<com.arcadedb.database.RID> rids(
      final List<com.arcadedb.utility.Pair<com.arcadedb.database.RID, Float>> hits) {
    final List<com.arcadedb.database.RID> out = new ArrayList<>(hits.size());
    for (final com.arcadedb.utility.Pair<com.arcadedb.database.RID, Float> h : hits)
      out.add(h.getFirst());
    return out;
  }

  private static float[] randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) rng.nextGaussian();
    return v;
  }
}
