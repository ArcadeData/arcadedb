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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Oracle test of the merge of the delta buffer (vectors committed after the graph was built) into a search, at the sizes and
 * the values where a bounded top-k merge goes wrong: {@code k} at, below and above the number of buffered vectors, exact
 * duplicates of graph vectors and of one another (ties at the k-th place), buffered vectors that supersede a graph vector, and
 * deletions of graph and of buffered vectors. Held to the brute-force top-k of the live set: the answer has exactly
 * {@code min(k, live)} rows, each live record once, and the distance of rank {@code r} is the exact rank-{@code r} distance.
 */
class VectorDeltaScanTieOracleTest extends TestHelper {
  /** A graph below 1000 nodes is rebuilt synchronously at search time. */
  private static final int    GRAPH_ROWS = 1_100;
  private static final int    DIMENSIONS = 16;
  /** Above the 1,100 nodes of these graphs, so the walk visits every node and the answer of a NONE index is the exact one. */
  private static final int    EF_SEARCH  = 1500;
  private static final String INDEX_NAME = "Doc[vector]";

  /** The live set the index has to answer for: every record that holds a vector, with the vector it holds. */
  private final Map<RID, float[]> model  = new LinkedHashMap<>();
  private final Random            random = new Random(0x5EEDL);
  private       String            quantization;

  private void createIndex(final String quant) {
    quantization = quant;
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
          + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quant + "\"}");
    });
  }

  private float[] randomVector() {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) random.nextGaussian();
    return v;
  }

  private void insertRows(final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] v = randomVector();
        final RID rid = database.newDocument("Doc").set("id", model.size()).set("vector", v).save().getIdentity();
        model.put(rid, v);
      }
    });
  }

  private LSMVectorIndex index() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(INDEX_NAME)).getIndexesOnBuckets()[0];
  }

  private long stat(final String key) {
    final Long value = index().getStats().get(key);
    assertThat(value).as("stat %s", key).isNotNull();
    return value;
  }

  private double tolerance() {
    return "NONE".equals(quantization) ? 2e-4 : 0.05;
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

  /** The exact distances of {@code q} to every vector of the model, ascending. */
  private List<Double> exactDistances(final float[] q) {
    final List<Double> sorted = new ArrayList<>(model.size());
    for (final float[] v : model.values())
      sorted.add(distance(q, v));
    sorted.sort(Double::compare);
    return sorted;
  }

  private List<Pair<RID, Float>> search(final float[] q, final int k) {
    return index().findNeighborsFromVector(q, k, EF_SEARCH);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void bufferedVectorsAnswerLikeTheLiveSetAtEveryK(final String quant) {
    createIndex(quant);
    insertRows(GRAPH_ROWS);
    final float[] probe = randomVector();
    assertThat(search(probe, 10)).hasSize(10); // builds the graph
    assertThat(stat("deltaVectorsCount")).isZero();

    final List<RID> graphRids = new ArrayList<>(model.keySet());
    final List<float[]> graphVectors = new ArrayList<>(model.values());

    // A committed batch of the shapes a top-k merge gets wrong
    final float[] twin = randomVector();
    final List<RID> added = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        added.add(put(randomVector()));
      for (int i = 0; i < 6; i++)
        added.add(put(twin)); // ties with one another
      for (int i = 0; i < 5; i++)
        added.add(put(graphVectors.get(i * 97).clone())); // ties with graph vectors
      added.add(put(probe.clone())); // at distance zero from the probe, and tied with nothing
    });
    // A graph vector re-embedded (its old vector must stop answering) and a graph vector deleted
    database.transaction(() -> {
      final RID reembedded = graphRids.get(3);
      final float[] v = randomVector();
      database.lookupByRID(reembedded, true).asDocument().modify().set("vector", v).save();
      model.put(reembedded, v);
      final RID gone = graphRids.get(500);
      database.lookupByRID(gone, true).delete();
      model.remove(gone);
    });
    // A buffered vector deleted again, and one superseded
    database.transaction(() -> {
      final RID gone = added.get(2);
      database.lookupByRID(gone, true).delete();
      model.remove(gone);
      final RID reembedded = added.get(10);
      final float[] v = randomVector();
      database.lookupByRID(reembedded, true).asDocument().modify().set("vector", v).save();
      model.put(reembedded, v);
    });
    assertThat(stat("deltaVectorsCount")).as("the vectors are answered from the buffer, not from a rebuilt graph").isPositive();

    final int live = model.size();
    final int buffered = (int) stat("deltaVectorsCount");
    for (final int k : new int[] { 1, 2, 5, 6, 7, buffered - 1, buffered, buffered + 1, 40, 100, 1_000, live, live + 1, live + 50 }) {
      if (k < 1)
        continue;
      for (final float[] q : new float[][] { probe, twin, graphVectors.get(97), randomVector() })
        assertAnswersTheExactTopK("k=" + k, q, k);
    }
    assertThat(stat("deltaVectorsCount")).as("no rebuild happened under the searches").isPositive();
  }

  @Test
  void tiesAtTheKthPlaceAreFilledFromTheBuffer() {
    createIndex("NONE");
    insertRows(GRAPH_ROWS);
    search(randomVector(), 10); // builds the graph
    final float[] twin = randomVector();
    final Set<RID> twins = new HashSet<>();
    database.transaction(() -> {
      for (int i = 0; i < 12; i++)
        twins.add(put(twin));
    });
    // Twelve records at distance zero: any k up to twelve is answered by the buffer alone, each record once
    for (final int k : new int[] { 1, 3, 11, 12, 13 }) {
      final List<Pair<RID, Float>> answer = search(twin, k);
      assertThat(answer).as("k=%d", k).hasSize(k);
      final Set<RID> seen = new HashSet<>();
      for (int rank = 0; rank < Math.min(k, 12); rank++) {
        assertThat(twins).as("k=%d rank %d is a twin", k, rank).contains(answer.get(rank).getFirst());
        assertThat(seen.add(answer.get(rank).getFirst())).as("each twin once").isTrue();
        assertThat(answer.get(rank).getSecond()).isCloseTo(0f, within(1e-5f));
      }
      if (k == 13)
        assertThat(twins).as("the 13th is the nearest of the graph").doesNotContain(answer.get(12).getFirst());
    }
  }

  private RID put(final float[] vector) {
    final RID rid = database.newDocument("Doc").set("id", model.size() + 100_000).set("vector", vector).save().getIdentity();
    model.put(rid, vector);
    return rid;
  }

  private void assertAnswersTheExactTopK(final String where, final float[] q, final int k) {
    final List<Double> exact = exactDistances(q);
    final List<Pair<RID, Float>> answer = search(q, k);
    assertThat(answer).as("%s: rows", where).hasSize(Math.min(k, model.size()));
    final Set<RID> seen = new HashSet<>();
    for (int rank = 0; rank < answer.size(); rank++) {
      final RID rid = answer.get(rank).getFirst();
      assertThat(seen.add(rid)).as("%s: %s answers once", where, rid).isTrue();
      assertThat(model).as("%s: only live records answer, not %s", where, rid).containsKey(rid);
      assertThat(distance(q, model.get(rid))).as("%s: rank %d is at the exact rank-%d distance", where, rank, rank)
          .isCloseTo(exact.get(rank), within(tolerance()));
    }
    for (final Map.Entry<RID, float[]> e : model.entrySet())
      if (!seen.contains(e.getKey()) && answer.size() == Math.min(k, model.size()) && !answer.isEmpty())
        assertThat(distance(q, e.getValue())).as("%s: %s left out although nearer than the last answer", where, e.getKey())
            .isGreaterThanOrEqualTo(distance(q, model.get(answer.getLast().getFirst())) - tolerance());
  }
}
