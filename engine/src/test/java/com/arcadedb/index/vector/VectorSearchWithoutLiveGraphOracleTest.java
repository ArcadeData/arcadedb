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
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * {@code vector.neighbors} of an index that has no graph to walk, or none with a live vector in it: a fresh index written to
 * inside one transaction (issue #7378), an index whose every committed record was deleted, and the rows written since. The
 * answer is the brute-force top-k over the rows a reader can see, a transaction's own writes included, and nothing else.
 */
class VectorSearchWithoutLiveGraphOracleTest extends TestHelper {
  private static final int DIM = 8;

  private final Map<RID, float[]> committed = new LinkedHashMap<>();
  private final Map<RID, float[]> pending   = new LinkedHashMap<>();
  private       Random            rnd;
  private       String            quantization;

  private void createIndex(final String quant) {
    quantization = quant;
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Chunk");
      database.command("sql", "CREATE PROPERTY Chunk.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Chunk (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIM
          + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quant + "\"}");
    });
  }

  private float[] randomVector() {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  private RID insert(final Map<RID, float[]> into) {
    final float[] v = randomVector();
    final RID rid = database.newVertex("Chunk").set("embedding", v).save().getIdentity();
    into.put(rid, v);
    return rid;
  }

  private void buildGraph() {
    for (final Index sub : ((TypeIndex) database.getSchema().getIndexByName("Chunk[embedding]")).getIndexesOnBuckets())
      ((LSMVectorIndex) sub).buildVectorGraphNow();
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

  /** the answer of k nearest must be the k nearest of {@code visible}, each at its exact distance, ascending, once */
  private void assertAnswers(final Map<RID, float[]> visible, final int k, final String where) {
    final float[] q = randomVector();
    final List<RID> expected = new ArrayList<>(visible.keySet());
    expected.sort((a, b) -> Double.compare(distance(q, visible.get(a)), distance(q, visible.get(b))));
    final int want = Math.min(k, expected.size());

    final Map<RID, Float> answer = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('Chunk[embedding]', ?, ?, {efSearch: 200})))", q, k)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(answer.put(((Identifiable) r.getProperty("rid")).getIdentity(), r.<Number>getProperty("distance").floatValue()))
            .as("one row per record %s", where).isNull();
      }
    }
    assertThat(answer).as("%s: %d visible, k=%d", where, visible.size(), k).hasSize(want);
    final double tolerance = quantization.equals("NONE") ? 2e-4 : 0.05;
    final List<RID> got = new ArrayList<>(answer.keySet());
    for (int i = 0; i < want; i++) {
      assertThat(visible).as("%s: only visible rows answer", where).containsKey(got.get(i));
      assertThat((double) answer.get(got.get(i))).as("%s: distance of rank %d", where, i)
          .isCloseTo(distance(q, visible.get(got.get(i))), within(tolerance));
      // the k nearest, as a set: the rank-i row of the answer is as near as the rank-i row of the brute force
      assertThat((double) answer.get(got.get(i))).as("%s: rank %d is the exact rank", where, i)
          .isCloseTo(distance(q, visible.get(expected.get(i))), within(tolerance));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aFreshIndexWrittenToInsideOneTransactionAnswersItsOwnRows(final String quant) {
    rnd = new Random(31);
    createIndex(quant);
    database.begin();
    for (int i = 0; i < 6; i++)
      insert(pending);
    assertAnswers(pending, 3, "inside the writing transaction, k below the rows");
    assertAnswers(pending, 10, "inside the writing transaction, k above the rows");
    database.commit();
    committed.putAll(pending);
    pending.clear();
    assertAnswers(committed, 3, "after the commit, k below the rows");
    assertAnswers(committed, 10, "after the commit, k above the rows");
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aRolledBackWriteLeavesNothingToAnswer(final String quant) {
    rnd = new Random(32);
    createIndex(quant);
    database.begin();
    for (int i = 0; i < 6; i++)
      insert(pending);
    assertAnswers(pending, 4, "inside the transaction");
    database.rollback();
    pending.clear();
    assertAnswers(committed, 4, "after the rollback");
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void anIndexWhoseEveryRecordWasDeletedAnswersOnlyTheRowsWrittenSince(final String quant) {
    rnd = new Random(33);
    createIndex(quant);
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        insert(committed);
    });
    buildGraph();
    assertAnswers(committed, 5, "graph built");

    database.transaction(() -> {
      for (final RID rid : committed.keySet())
        database.lookupByRID(rid, true).asVertex().delete();
    });
    committed.clear();
    assertAnswers(committed, 5, "every record deleted");

    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        insert(committed);
    });
    assertAnswers(committed, 10, "four rows written after the delete, k above them");
    assertAnswers(committed, 2, "four rows written after the delete, k below them");

    // a transaction adds two rows and removes one of the four: its own view, then the committed one again
    database.begin();
    final RID removed = committed.keySet().iterator().next();
    database.lookupByRID(removed, true).asVertex().delete();
    pending.putAll(committed);
    pending.remove(removed);
    insert(pending);
    insert(pending);
    assertAnswers(pending, 10, "inside a transaction that adds two rows and removes one");
    assertAnswers(pending, 3, "inside a transaction that adds two rows and removes one, k below");
    database.rollback();
    pending.clear();
    assertAnswers(committed, 10, "after the rollback");
  }
}
