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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Oracle test of a search made inside the transaction that wrote to the index (modelhub and Graphiti search between the writes of
 * one batch): the rows the transaction deleted or re-embedded must not answer with their committed vector, the rows it inserted and
 * re-embedded must answer with the pending one, and the answer, for every k up to and past the number of rows the transaction sees,
 * is the exact top-k of that set, each record once. A search that the graph answers completely pays for no brute-force completion.
 */
class VectorTransactionOverlaySearchOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int VECTORS    = 1_500; // above the 1000 nodes below which a graph is rebuilt synchronously at each search
  private static final int EF_SEARCH  = 3_000; // wide enough to visit every node, so the answer of a NONE index is the exact one

  private final Random            random = new Random(0x7A5C0DEL);
  private final Map<RID, float[]> model  = new LinkedHashMap<>(); // what the transaction sees

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aSearchInsideTheWritingTransactionSeesExactlyWhatTheTransactionSees(final String quantization) {
    createIndex(quantization);
    final List<RID> graphRids = insertRows(VECTORS);
    index().buildVectorGraphNow();
    final double tolerance = "NONE".equals(quantization) ? 2e-4 : 0.05;

    database.transaction(() -> {
      for (final RID rid : graphRids.subList(10, 70)) { // deleted
        database.lookupByRID(rid, true).delete();
        model.remove(rid);
      }
      for (final RID rid : graphRids.subList(100, 120)) { // re-embedded
        final float[] v = randomVector();
        database.lookupByRID(rid, true).asDocument().modify().set("vector", v).save();
        model.put(rid, v);
      }
      for (int i = 0; i < 25; i++) { // inserted
        final float[] v = randomVector();
        model.put(database.newDocument("Doc").set("id", 100_000 + i).set("vector", v).save().getIdentity(), v);
      }
      final int visible = model.size();
      assertThat(visible).isEqualTo(VECTORS - 60 + 25);

      for (final int k : new int[] { 1, 10, 100, 1_000, visible - 1, visible, visible + 40 }) {
        final long scansBefore = index().getStats().getOrDefault("bruteForceScans", 0L);
        for (int probe = 0; probe < 2; probe++)
          assertAnswersTheExactTopK("k=" + k, randomVector(), k, tolerance);
        assertThat(index().getStats().getOrDefault("bruteForceScans", 0L)).as("k=%d: an answer the graph completes is not completed by a scan", k)
            .isEqualTo(scansBefore);
      }
    });
  }

  private void assertAnswersTheExactTopK(final String where, final float[] q, final int k, final double tolerance) {
    final List<Pair<RID, Float>> answer = index().findNeighborsFromVector(q, k, EF_SEARCH, null);
    final List<Double> exact = new ArrayList<>();
    for (final float[] v : model.values())
      exact.add(distance(q, v));
    Collections.sort(exact);
    assertThat(answer).as("%s: rows", where).hasSize(Math.min(k, model.size()));
    final Set<RID> seen = new HashSet<>();
    for (int rank = 0; rank < answer.size(); rank++) {
      final RID rid = answer.get(rank).getFirst();
      assertThat(seen.add(rid)).as("%s: %s answers once", where, rid).isTrue();
      assertThat(model).as("%s: only a record the transaction sees answers, not %s", where, rid).containsKey(rid);
      assertThat((double) answer.get(rank).getSecond()).as("%s: %s answers with the vector the transaction sees", where, rid)
          .isCloseTo(distance(q, model.get(rid)), within(tolerance));
      assertThat(distance(q, model.get(rid))).as("%s: rank %d is at the exact rank-%d distance", where, rank, rank)
          .isCloseTo(exact.get(rank), within(tolerance));
    }
  }

  private void createIndex(final String quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
          + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quantization + "\"}");
    });
  }

  private float[] randomVector() {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) random.nextGaussian();
    return v;
  }

  private List<RID> insertRows(final int count) {
    final List<RID> rids = new ArrayList<>(count);
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] v = randomVector();
        final RID rid = database.newDocument("Doc").set("id", model.size()).set("vector", v).save().getIdentity();
        model.put(rid, v);
        rids.add(rid);
      }
    });
    return rids;
  }

  private LSMVectorIndex index() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
  }

  private static double distance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }
}
