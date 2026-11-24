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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * A transaction that fails at commit leaves the answers of {@code vector.neighbors} exactly as they were (issue #7931). The loser
 * rewrites, deletes and removes the vectors of records that sit in the graph AND of records written after the graph was built (still
 * in the delta buffer, where the commit replay drops their entries and the abort has to put them back), adds rows, and loses to a
 * concurrent write of a record whose vector it rewrote: the index operations are replayed before the page versions are checked.
 * After each abort a brute-force model decides the answers (a record finds itself by its own vector, an allow-list of buffered rows
 * is answered exactly, nothing answers at a distance that is not the one to its current vector) and the counters of the index are
 * where they were; the same writes then commit and the model follows them, a deleted record and an old vector answer nothing.
 */
class VectorAbortedCommitOracleTest extends TestHelper {
  private static final int  DIM           = 8;
  private static final long TIMEOUT_MS    = 60_000;
  private static final int  GRAPH_ROWS    = 1000; // from this size on a search does not rebuild the graph before it answers
  private static final int  BUFFERED_ROWS = 12;

  private final Map<RID, float[]> model        = new LinkedHashMap<>();
  private final List<RID>         graphRids    = new ArrayList<>();
  private final List<RID>         bufferedRids = new ArrayList<>();
  private       Random            rnd;
  private       String            quantization;
  private       boolean           permitsParked;

  @AfterEach
  void releasePermits() {
    if (permitsParked)
      LSMVectorIndex.releaseAllRebuildPermitsForTest();
  }

  private float[] randomVector() {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  private LSMVectorIndex index() {
    final Index typeIndex = database.getSchema().getIndexByName("Doc[vector]");
    return (LSMVectorIndex) ((TypeIndex) typeIndex).getIndexesOnBuckets()[0];
  }

  private RID insert(final Map<RID, float[]> into) {
    final float[] v = randomVector();
    final RID rid = database.newDocument("Doc").set("name", "n").set("vector", v).save().getIdentity();
    into.put(rid, v);
    return rid;
  }

  private void load(final String quant) {
    quantization = quant;
    rnd = new Random(71);
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\": " + DIM
          + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quant + "\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < GRAPH_ROWS; i++)
        graphRids.add(insert(model));
    });
    index().buildVectorGraphNow();
    // no rebuild may fold the buffer into the graph while the test looks at it
    LSMVectorIndex.acquireAllRebuildPermitsForTest();
    permitsParked = true;
    database.transaction(() -> {
      for (int i = 0; i < BUFFERED_ROWS; i++)
        bufferedRids.add(insert(model));
    });
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

  /** an answer holds each record once, only live records, each at the distance of {@code q} to its current vector */
  private void assertRowsAreLive(final String where, final float[] q, final List<Pair<RID, Float>> answer) {
    final Set<RID> seen = new HashSet<>();
    for (final Pair<RID, Float> hit : answer) {
      assertThat(seen.add(hit.getFirst())).as("%s: %s once", where, hit.getFirst()).isTrue();
      assertThat(model).as("%s: only live records answer, not %s", where, hit.getFirst()).containsKey(hit.getFirst());
      assertThat((double) hit.getSecond()).as("%s: distance of %s is to its current vector", where, hit.getFirst())
          .isCloseTo(distance(q, model.get(hit.getFirst())), within(tolerance()));
    }
  }

  private void assertModel(final String where) {
    final double tolerance = tolerance();
    final List<RID> probes = new ArrayList<>(bufferedRids);
    probes.addAll(graphRids.subList(0, 8));
    for (final RID rid : probes) {
      if (!model.containsKey(rid))
        continue;
      final List<Pair<RID, Float>> answer = search(model.get(rid), 3, null);
      assertThat(answer.stream().map(Pair::getFirst)).as("%s: %s finds itself by its own vector", where, rid).contains(rid);
      for (final Pair<RID, Float> hit : answer)
        if (hit.getFirst().equals(rid))
          assertThat((double) hit.getSecond()).as("%s: distance of %s to its own vector", where, rid).isLessThan(tolerance);
    }

    // an allow-list of at most a tenth of the index takes the pre-filter plan: the exact top-k of its members
    for (final int filterKind : new int[] { 1, 2 }) {
      final Set<RID> allow = new HashSet<>();
      if (filterKind == 1)
        allow.addAll(bufferedRids);
      else {
        allow.addAll(bufferedRids.subList(0, 6));
        allow.addAll(graphRids.subList(0, 6));
      }
      for (final int k : new int[] { 1, 5, 200 }) {
        final float[] q = randomVector();
        final List<Double> sorted = new ArrayList<>();
        for (final Map.Entry<RID, float[]> e : model.entrySet())
          if (allow.contains(e.getKey()))
            sorted.add(distance(q, e.getValue()));
        sorted.sort(Double::compare);
        final List<Pair<RID, Float>> answer = search(q, k, allow);
        final String ctx = where + " (filter " + filterKind + ", k=" + k + ")";
        assertThat(answer).as("rows of %s", ctx).hasSize(Math.min(k, sorted.size()));
        assertRowsAreLive(ctx, q, answer);
        for (int rank = 0; rank < answer.size(); rank++) {
          assertThat(allow).as("%s: inside the filter", ctx).contains(answer.get(rank).getFirst());
          assertThat((double) answer.get(rank).getSecond()).as("%s: rank %d is the exact rank", ctx, rank)
              .isCloseTo(sorted.get(rank), within(tolerance));
        }
      }
    }

    final float[] q = randomVector();
    assertRowsAreLive(where, q, search(q, 20, null));
  }

  /** the writes of the transaction under test: graph rows, buffered rows, a row rewritten twice, deletes, a removed vector, new rows */
  private Map<RID, float[]> writes() {
    final Map<RID, float[]> after = new LinkedHashMap<>(model);
    for (int i = 0; i < 2; i++) {
      // a buffered row rewritten twice: the second rewrite supersedes a vector id this transaction allocated
      final RID rid = bufferedRids.get(i);
      database.lookupByRID(rid, true).asDocument(true).modify().set("vector", randomVector()).save();
      final float[] second = randomVector();
      database.lookupByRID(rid, true).asDocument(true).modify().set("vector", second).save();
      after.put(rid, second);
    }
    database.lookupByRID(bufferedRids.get(2), true).asDocument(true).delete();
    after.remove(bufferedRids.get(2));
    final MutableDocument removed = database.lookupByRID(bufferedRids.get(3), true).asDocument(true).modify();
    removed.remove("vector");
    removed.save();
    after.remove(bufferedRids.get(3));
    for (int i = 0; i < 2; i++) {
      final RID rid = graphRids.get(i);
      final float[] v = randomVector();
      database.lookupByRID(rid, true).asDocument(true).modify().set("vector", v).save();
      after.put(rid, v);
    }
    database.lookupByRID(graphRids.get(2), true).asDocument(true).delete();
    after.remove(graphRids.get(2));
    // the record the concurrent transaction writes
    final float[] conflictVector = randomVector();
    database.lookupByRID(graphRids.get(10), true).asDocument(true).modify().set("vector", conflictVector).save();
    after.put(graphRids.get(10), conflictVector);
    for (int i = 0; i < 2; i++)
      insert(after);
    return after;
  }

  private void winnerInAnotherThread(final RID rid, final String name) throws InterruptedException {
    final Throwable[] failure = new Throwable[1];
    final Thread concurrent = new Thread(() -> {
      try {
        database.transaction(() -> database.lookupByRID(rid, true).asDocument(true).modify().set("name", name).save());
      } catch (final Throwable t) {
        failure[0] = t;
      }
    });
    concurrent.start();
    concurrent.join(TIMEOUT_MS);
    assertThat(concurrent.isAlive()).as("the concurrent commit of %s finished", rid).isFalse();
    assertThat(failure[0]).as("the concurrent transaction commits").isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aTransactionThatLosesItsCommitLeavesEveryAnswerAsItWasAndItsRetryCommitsThem(final String quant) throws Exception {
    load(quant);
    assertModel("before the writes");

    final RID conflict = graphRids.get(10);
    final Map<String, Long> before = index().getStats();
    for (int round = 1; round <= 2; round++) {
      database.begin();
      writes();
      winnerInAnotherThread(conflict, "winner" + round);
      try {
        database.commit();
        throw new AssertionError("the commit must be refused: the record was rewritten by a concurrent transaction");
      } catch (final ConcurrentModificationException expected) {
        // the index operations have been replayed by now: the abort must put every one of them back
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
      // what the abort restores is also what the counters say: the buffer, its resident payloads, the tombstones, the ids
      final Map<String, Long> after = index().getStats();
      for (final String counter : List.of("deltaVectorsCount", "deltaResidentVectors", "deletedVectors", "activeVectors", "totalVectors",
          "mutationsSinceRebuild", "graphState"))
        assertThat(after.get(counter)).as("%s after the aborted transaction, round %d", counter, round).isEqualTo(before.get(counter));
      assertModel("after the aborted transaction, round " + round);
    }

    // the same kind of writes, committed: the answers follow them
    final Map<RID, float[]> previous = new HashMap<>(model);
    database.begin();
    final Map<RID, float[]> after = writes();
    database.commit();
    model.clear();
    model.putAll(after);
    bufferedRids.removeIf(r -> !model.containsKey(r));
    graphRids.removeIf(r -> !model.containsKey(r));
    assertModel("after the writes committed");

    // the vector a record had before the writes finds nothing of it any more: a deleted or vector-less record does not answer,
    // a rewritten one answers at the distance of its new vector
    for (final Map.Entry<RID, float[]> old : previous.entrySet()) {
      if (model.containsKey(old.getKey()) && model.get(old.getKey()) == old.getValue())
        continue;
      final List<Pair<RID, Float>> answer = search(old.getValue(), 5, null);
      assertRowsAreLive("after the writes committed, by the old vector of " + old.getKey(), old.getValue(), answer);
    }
  }
}
