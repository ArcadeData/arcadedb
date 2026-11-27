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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * An index of more than a thousand vectors (a graph that is reused rather than rebuilt) that is closed and reopened again and
 * again, with inserts, rewrites, deletes and removed vectors in between, and with or without a search before the close (a search
 * folds the changes into the graph the close persists, none leaves a persisted graph that is stale by the changes since). After
 * each reopen a brute-force model decides: every record written since the last reopen and a sample of the others find themselves
 * by their own vector, a record that was deleted or lost its vector answers nothing, a rewritten record answers at the distance of its
 * new vector, an allow-list of at most a tenth of the index is answered exactly.
 */
class VectorReopenOracleTest extends TestHelper {
  private static final int DIM    = 8;
  private static final int ROWS   = 1100;
  /** per cycle: deleted, rewritten, vector removed, inserted records: the first four keep or change the count in one way each */
  private static final int[][] CYCLES = { { 30, 0, 0, 30 }, { 0, 0, 0, 40 }, { 40, 0, 0, 0 }, { 0, 40, 0, 0 }, { 20, 20, 8, 20 } };

  private final Map<RID, float[]> model = new LinkedHashMap<>();
  private       Random            rnd;
  private       String            quantization;

  private float[] randomVector() {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  private RID insert() {
    final float[] v = randomVector();
    final RID rid = database.newDocument("Doc").set("name", "n").set("vector", v).save().getIdentity();
    model.put(rid, v);
    return rid;
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

  private double tolerance() {
    return quantization.equals("NONE") ? 2e-4 : 0.05;
  }

  private List<Pair<RID, Float>> search(final float[] q, final int k, final Collection<RID> allow) {
    final List<Pair<RID, Float>> answer = new ArrayList<>();
    final String options = allow == null ? "{efSearch: 300}" : "{efSearch: 300, filter: ?}";
    final Object[] args = allow == null ? new Object[] { q, k } : new Object[] { q, k, new ArrayList<>(allow) };
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('Doc[vector]', ?, ?, " + options + ")))", args)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        answer.add(new Pair<>(((Identifiable) r.getProperty("rid")).getIdentity(), r.<Number>getProperty("distance").floatValue()));
      }
    }
    return answer;
  }

  private void assertRowsAreLive(final String where, final float[] q, final List<Pair<RID, Float>> answer) {
    final Set<RID> seen = new HashSet<>();
    for (final Pair<RID, Float> hit : answer) {
      assertThat(seen.add(hit.getFirst())).as("%s: %s once", where, hit.getFirst()).isTrue();
      assertThat(model).as("%s: only live records answer, not %s", where, hit.getFirst()).containsKey(hit.getFirst());
      assertThat((double) hit.getSecond()).as("%s: distance of %s is to its current vector", where, hit.getFirst())
          .isCloseTo(distance(q, model.get(hit.getFirst())), within(tolerance()));
    }
  }

  @ParameterizedTest
  @CsvSource({ "NONE,true", "NONE,false", "INT8,true", "INT8,false" })
  void everyReopenAnswersWhatTheRecordsHold(final String quant, final boolean searchBeforeClose) {
    quantization = quant;
    rnd = new Random(53);
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\": " + DIM
          + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quant + "\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        insert();
    });
    // the first graph is built and persisted by a first search and the close that follows it
    search(randomVector(), 5, null);
    reopenDatabase();

    for (int cycle = 1; cycle <= CYCLES.length; cycle++) {
      final int[] plan = CYCLES[cycle - 1];
      final Set<RID> touched = new HashSet<>();
      final Map<RID, float[]> gone = new LinkedHashMap<>();
      final Map<RID, float[]> replaced = new LinkedHashMap<>();
      final List<RID> candidates = new ArrayList<>(model.keySet());
      database.transaction(() -> {
        for (int i = 0; i < plan[0]; i++) {
          final RID victim = candidates.remove(rnd.nextInt(candidates.size()));
          gone.put(victim, model.remove(victim));
          database.lookupByRID(victim, true).asDocument(true).delete();
        }
        for (int i = 0; i < plan[1]; i++) {
          final RID rid = candidates.remove(rnd.nextInt(candidates.size()));
          final float[] v = randomVector();
          replaced.put(rid, model.put(rid, v));
          database.lookupByRID(rid, true).asDocument(true).modify().set("vector", v).save();
          touched.add(rid);
        }
        for (int i = 0; i < plan[2]; i++) {
          final RID rid = candidates.remove(rnd.nextInt(candidates.size()));
          gone.put(rid, model.remove(rid));
          final MutableDocument d = database.lookupByRID(rid, true).asDocument(true).modify();
          d.remove("vector");
          d.save();
        }
        for (int i = 0; i < plan[3]; i++)
          touched.add(insert());
      });
      if (searchBeforeClose)
        search(randomVector(), 5, null);
      reopenDatabase();

      final String where = "cycle " + cycle + " " + Arrays.toString(plan) + (searchBeforeClose ? " after a search" : " without a search");
      final List<RID> probes = new ArrayList<>(touched);
      final List<RID> all = new ArrayList<>(model.keySet());
      for (int i = 0; i < 40; i++)
        probes.add(all.get(rnd.nextInt(all.size())));
      for (final RID rid : probes) {
        final List<Pair<RID, Float>> answer = search(model.get(rid), 3, null);
        assertThat(answer.stream().map(Pair::getFirst)).as("%s: %s finds itself by its own vector", where, rid).contains(rid);
        assertRowsAreLive(where + " (self query of " + rid + ")", model.get(rid), answer);
      }
      for (final Map.Entry<RID, float[]> old : gone.entrySet())
        assertThat(search(old.getValue(), 5, null).stream().map(Pair::getFirst)).as("%s: %s holds no vector any more", where, old.getKey())
            .doesNotContain(old.getKey());
      for (final Map.Entry<RID, float[]> old : replaced.entrySet())
        assertRowsAreLive(where + " (old vector of " + old.getKey() + ")", old.getValue(), search(old.getValue(), 5, null));

      // the records written since the last reopen are at most a tenth of the index: the pre-filter plan answers them exactly
      for (final int k : new int[] { 1, 10, 100 }) {
        final float[] q = randomVector();
        final List<Double> sorted = new ArrayList<>();
        for (final RID rid : touched)
          sorted.add(distance(q, model.get(rid)));
        sorted.sort(Double::compare);
        final List<Pair<RID, Float>> answer = search(q, k, touched);
        assertThat(answer).as("%s: rows of the filter, k=%d", where, k).hasSize(Math.min(k, sorted.size()));
        assertRowsAreLive(where + " (filter, k=" + k + ")", q, answer);
        for (int rank = 0; rank < answer.size(); rank++)
          assertThat((double) answer.get(rank).getSecond()).as("%s: rank %d of the filter, k=%d", where, rank, k)
              .isCloseTo(sorted.get(rank), within(tolerance()));
      }
      final float[] q = randomVector();
      assertRowsAreLive(where + " (unfiltered)", q, search(q, 20, null));

      // the persisted graph is reused with its tombstones and the records written since in the delta buffer, not rebuilt: a reopen of a
      // large index must not pay a graph build (the later cycles may meet the mutation threshold of a rebuild)
      if (cycle <= 4) {
        final Map<String, Long> stats = ((LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]"))
            .getIndexesOnBuckets()[0]).getStats();
        assertThat(stats.get("graphRebuildCount")).as("%s: graph rebuilds since the reopen", where).isZero();
        assertThat(stats.get("graphReusesWithTombstonedNodes")).as("%s: the persisted graph is reused with its tombstones", where).isEqualTo(1L);
        assertThat(stats.get("stalePrefixGraphReuses")).as("%s: and with the records written since it was built", where).isEqualTo(1L);
        assertThat(stats.get("graphNodeCount")).as("%s: the graph of the first build", where).isEqualTo((long) ROWS);
      }
    }
  }
}
