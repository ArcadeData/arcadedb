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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/**
 * Issue #8957: COMPACT INDEX renumbers every vector id, but the search vector cache is keyed by vector id and was left
 * populated, so a cached id answered with the vector of the record that held it before the compaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8957CompactionSearchCacheTest extends TestHelper {
  private static final int DIMENSIONS = 16;

  private final Map<RID, float[]> vectors = new LinkedHashMap<>();

  @Test
  void searchAfterCompactionScoresTheRightVectors() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");

    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < 2000; i++) {
        final float[] v = randomVector(random);
        vectors.put(database.newDocument("Doc").set("vector", v).save().getIdentity(), v);
      }
    });
    database.transaction(() -> {
      final List<RID> rids = new ArrayList<>(vectors.keySet());
      for (int i = 0; i < 500; i++) {
        final float[] v = randomVector(random);
        database.lookupByRID(rids.get(i), true).asDocument().modify().set("vector", v).save();
        vectors.put(rids.get(i), v);
      }
    });

    assertThatEveryQueryFindsItsOwnRecord();
    database.command("sql", "COMPACT INDEX `Doc[vector]`").close();
    assertThatEveryQueryFindsItsOwnRecord();
  }

  private void assertThatEveryQueryFindsItsOwnRecord() {
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
    final List<RID> rids = new ArrayList<>(vectors.keySet());
    for (int q = 0; q < 100; q++) {
      final RID own = rids.get(q * 20);
      final float[] query = vectors.get(own);
      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(query, 5, 100);
      assertThat(hits.get(0).getFirst()).isEqualTo(own);
      for (final Pair<RID, Float> hit : hits)
        assertThat((double) hit.getSecond()).isCloseTo(cosineDistance(query, vectors.get(hit.getFirst())),
            offset(1e-4));
    }
  }

  private static float[] randomVector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat() * 2 - 1;
    return v;
  }

  private static double cosineDistance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return 1 - dot / Math.sqrt(na * nb);
  }
}
