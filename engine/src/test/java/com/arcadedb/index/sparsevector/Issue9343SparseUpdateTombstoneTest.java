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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9343: an UPDATE of the {@code tokens} and {@code weights} of a document reaches the
 * sparse index as a remove of the old postings followed by a put of the new ones for the same RID. The tombstone of
 * an OLD dim used to be read by the scorer as a delete of the whole RID, so the document vanished from every query
 * that mentioned both one of its new dims and one of its old ones.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9343SparseUpdateTombstoneTest {
  private static final String DB_PATH = "target/test-databases/Issue9343SparseUpdateTombstoneTest";

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void anUpdatedDocumentAnswersOnItsNewDimsWhateverTheQueryMixes() {
    for (final boolean flushBeforeUpdate : new boolean[] { false, true })
      for (final boolean viaUpdate : new boolean[] { true, false }) {
        FileUtils.deleteRecursively(new File(DB_PATH));
        try (final Database db = new DatabaseFactory(DB_PATH).create()) {
          db.command("sql", "CREATE DOCUMENT TYPE Doc");
          db.command("sql", "CREATE PROPERTY Doc.id LONG");
          db.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
          db.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
          db.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR METADATA {\"dimensions\": 10}");
          db.transaction(() -> {
            db.newDocument("Doc").set("id", 0L).set("tokens", new int[] { 0, 1 }).set("weights", new float[] { 1f, 1f }).save();
            db.newDocument("Doc").set("id", 1L).set("tokens", new int[] { 2, 3 }).set("weights", new float[] { 1f, 1f }).save();
            db.newDocument("Doc").set("id", 2L).set("tokens", new int[] { 4, 5 }).set("weights", new float[] { 1f, 1f }).save();
          });
          if (flushBeforeUpdate)
            flushSparseIndexes(db);

          db.begin();
          if (viaUpdate)
            db.command("sql", "UPDATE Doc SET tokens = :t, weights = :w WHERE id = 1",
                Map.of("t", new int[] { 6, 7 }, "w", new float[] { 1f, 1f })).close();
          else {
            db.command("sql", "DELETE FROM Doc WHERE id = 1").close();
            db.newDocument("Doc").set("id", 1L).set("tokens", new int[] { 6, 7 }).set("weights", new float[] { 1f, 1f }).save();
          }
          db.commit();

          final String how = (viaUpdate ? "update" : "delete+insert") + (flushBeforeUpdate ? ", flushed" : ", memtable");
          for (int round = 0; round < 2; round++) {
            assertThat(ask(db, new int[] { 6 }, new float[] { 1f })).as(how + " {6}").containsExactly(1L);
            assertThat(ask(db, new int[] { 2 }, new float[] { 1f })).as(how + " {2}").isEmpty();
            assertThat(ask(db, new int[] { 6, 2 }, new float[] { 1f, 1f })).as(how + " {6,2}").containsExactly(1L);
            assertThat(ask(db, new int[] { 7, 3 }, new float[] { 1f, 1f })).as(how + " {7,3}").containsExactly(1L);
            assertThat(ask(db, new int[] { 6, 7 }, new float[] { 1f, 1f })).as(how + " {6,7}").containsExactly(1L);
            assertThat(ask(db, new int[] { 6, 3, 0 }, new float[] { 1f, 1f, 1f })).as(how + " {6,3,0}").containsExactlyInAnyOrder(0L, 1L);
            // a second round after a flush of the update itself, so the tombstones are read from a sealed segment
            flushSparseIndexes(db);
          }
        }
      }
  }

  @Test
  void aDeletedDocumentStillVanishesFromEveryQuery() {
    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Doc");
      db.command("sql", "CREATE PROPERTY Doc.id LONG");
      db.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
      db.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR METADATA {\"dimensions\": 10}");
      db.transaction(() -> {
        db.newDocument("Doc").set("id", 0L).set("tokens", new int[] { 0, 1 }).set("weights", new float[] { 1f, 1f }).save();
        db.newDocument("Doc").set("id", 1L).set("tokens", new int[] { 2, 3 }).set("weights", new float[] { 1f, 1f }).save();
      });
      flushSparseIndexes(db);
      db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id = 1").close());

      assertThat(ask(db, new int[] { 2 }, new float[] { 1f })).isEmpty();
      assertThat(ask(db, new int[] { 3 }, new float[] { 1f })).isEmpty();
      assertThat(ask(db, new int[] { 2, 3, 0 }, new float[] { 1f, 1f, 1f })).containsExactly(0L);
    }
  }

  /**
   * Many documents whose dims are rewritten at random, against a model kept by the test: every document must answer
   * exactly on the dims it holds now, whatever mix of live and tombstoned dims a query names, in the memtable and
   * once sealed into segments.
   */
  @Test
  void randomUpdatesAnswerLikeTheModel() {
    final Random random = new Random(9343);
    final int docs = 300;
    final int dims = 40;
    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE Doc");
      db.command("sql", "CREATE PROPERTY Doc.id LONG");
      db.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
      db.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR METADATA {\"dimensions\": " + dims + "}");
      final Map<Long, Map<Integer, Float>> model = new HashMap<>();
      db.transaction(() -> {
        for (long id = 0; id < docs; id++) {
          final Map<Integer, Float> vector = randomVector(random, dims);
          model.put(id, vector);
          db.newDocument("Doc").set("id", id).set("tokens", dimsOf(vector)).set("weights", weightsOf(vector)).save();
        }
      });
      for (int round = 0; round < 3; round++) {
        db.transaction(() -> {
          for (int u = 0; u < 100; u++) {
            final long id = random.nextInt(docs);
            final Map<Integer, Float> vector = randomVector(random, dims);
            model.put(id, vector);
            db.command("sql", "UPDATE Doc SET tokens = :t, weights = :w WHERE id = :id",
                Map.of("t", dimsOf(vector), "w", weightsOf(vector), "id", id)).close();
          }
        });
        if (round == 1)
          flushSparseIndexes(db);

        for (int q = 0; q < 25; q++) {
          final Map<Integer, Float> query = randomVector(random, dims);
          final Map<Long, Float> expected = new HashMap<>();
          for (final var doc : model.entrySet()) {
            float score = 0f;
            for (final var term : query.entrySet())
              score += term.getValue() * doc.getValue().getOrDefault(term.getKey(), 0f);
            if (score > 0f)
              expected.put(doc.getKey(), score);
          }
          final List<Long> got = ask(db, dimsOf(query), weightsOf(query), docs);
          assertThat(got).as("round " + round + " query " + query).containsExactlyInAnyOrderElementsOf(expected.keySet());
        }
      }
    }
  }

  private static Map<Integer, Float> randomVector(final Random random, final int dims) {
    final Map<Integer, Float> vector = new HashMap<>();
    final int size = 1 + random.nextInt(6);
    while (vector.size() < size)
      vector.put(random.nextInt(dims), 1f + random.nextInt(4));
    return vector;
  }

  private static int[] dimsOf(final Map<Integer, Float> vector) {
    return vector.keySet().stream().mapToInt(Integer::intValue).toArray();
  }

  private static float[] weightsOf(final Map<Integer, Float> vector) {
    final float[] weights = new float[vector.size()];
    int i = 0;
    for (final Integer dim : vector.keySet())
      weights[i++] = vector.get(dim);
    return weights;
  }

  private static void flushSparseIndexes(final Database db) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Doc[tokens,weights]");
    for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
      ((LSMSparseVectorIndex) idx).getEngine().flush();
  }

  private static List<Long> ask(final Database db, final int[] tokens, final float[] weights) {
    return ask(db, tokens, weights, 10);
  }

  private static List<Long> ask(final Database db, final int[] tokens, final float[] weights, final int k) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", "SELECT id, score FROM (SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?)))",
        "Doc[tokens,weights]", tokens, weights, k)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        ids.add(r.getProperty("id"));
      }
    }
    return ids;
  }
}
