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
 * Shared fixture of the oracle tests of the persisted-graph reuse and of the index writes: one {@code Doc} type with an
 * {@code LSM_VECTOR} index, a model of the live set ({@link #model}, RID to vector) and brute-force exact search over that
 * model as the oracle every answer of the index is held to.
 */
abstract class VectorReuseOracleFixture extends TestHelper {
  static final int    DIMENSIONS = 16;
  /** Wide enough to visit every node of the graphs these tests build, so the answer of a NONE index is the exact one. */
  static final int    EF_SEARCH  = 1500;
  static final String INDEX_NAME = "Doc[vector]";

  /** The live set the index has to answer for: every record that holds a vector, with the vector it holds. */
  protected final Map<RID, float[]> model  = new LinkedHashMap<>();
  protected final Random            random = new Random(0x5EEDL);
  protected       String            quantization;

  protected void createIndex(final String quant) {
    createIndex(quant, "");
  }

  /** @param extraMetadata further METADATA members, each preceded by a comma, such as {@code ,"efSearch":64} */
  protected void createIndex(final String quant, final String extraMetadata) {
    quantization = quant;
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
          + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quant + "\"" + extraMetadata + "}");
    });
  }

  protected float[] randomVector() {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) random.nextGaussian();
    return v;
  }

  /** Saves {@code count} records with a fresh vector each, in one transaction, and returns their RIDs in insert order. */
  protected List<RID> insertRows(final int count) {
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

  protected LSMVectorIndex index() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(INDEX_NAME)).getIndexesOnBuckets()[0];
  }

  protected long stat(final String key) {
    final Long value = index().getStats().get(key);
    assertThat(value).as("stat %s", key).isNotNull();
    return value;
  }

  protected double tolerance() {
    return "NONE".equals(quantization) ? 2e-4 : 0.05;
  }

  protected static double distance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }

  /** The exact distances of {@code q} to every vector of the model, ascending. */
  protected List<Double> exactDistances(final float[] q) {
    final List<Double> sorted = new ArrayList<>(model.size());
    for (final float[] v : model.values())
      sorted.add(distance(q, v));
    sorted.sort(Double::compare);
    return sorted;
  }

  protected List<Pair<RID, Float>> search(final float[] q, final int k) {
    return index().findNeighborsFromVector(q, k, EF_SEARCH);
  }

  /**
   * Holds the index to the brute-force oracle over {@link #model}: the answer of {@code probes} random queries has the exact
   * size, lists only live records, each once, and its rank {@code r} is at the rank-{@code r} distance of the exact order; and
   * every {@code stride}-th live record finds itself at rank 1 by its own vector.
   */
  protected void assertMatchesModel(final String where, final int stride, final int probes) {
    final double tolerance = tolerance();
    int n = 0;
    for (final Map.Entry<RID, float[]> e : model.entrySet()) {
      if (n++ % stride != 0)
        continue;
      final List<Pair<RID, Float>> answer = search(e.getValue(), 1);
      assertThat(answer).as("%s: %s finds itself", where, e.getKey()).hasSize(1);
      assertThat(distance(e.getValue(), model.get(answer.get(0).getFirst())))
          .as("%s: the nearest neighbour of the vector of %s is itself", where, e.getKey()).isCloseTo(0.0, within(tolerance));
    }
    for (int p = 0; p < probes; p++) {
      final float[] q = randomVector();
      final int k = 10;
      final List<Double> exact = exactDistances(q);
      final List<Pair<RID, Float>> answer = search(q, k);
      assertThat(answer).as("%s: probe %d rows", where, p).hasSize(Math.min(k, model.size()));
      final Set<RID> seen = new HashSet<>();
      for (int rank = 0; rank < answer.size(); rank++) {
        final RID rid = answer.get(rank).getFirst();
        assertThat(seen.add(rid)).as("%s: %s answers once", where, rid).isTrue();
        assertThat(model).as("%s: only live records answer, not %s", where, rid).containsKey(rid);
        assertThat(distance(q, model.get(rid))).as("%s: probe %d rank %d is the exact rank", where, p, rank)
            .isCloseTo(exact.get(rank), within(tolerance));
      }
    }
  }
}
