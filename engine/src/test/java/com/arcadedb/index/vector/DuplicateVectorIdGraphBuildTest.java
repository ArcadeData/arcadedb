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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two records on the same vector id: the page state behind a persisted graph that is rejected on every load.
 * <p>
 * The graph build collects the live set from the pages keyed by RID, so it counts both records. The load replays the
 * pages keyed by vector id, where the later entry overwrites the earlier one, so it counts one. The graph then holds
 * more nodes than the load has live vectors, can never take the stale-prefix reuse (it is larger, not smaller), and
 * is rebuilt over the same set on every query. Whatever the build publishes must be what the next load reconstructs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class DuplicateVectorIdGraphBuildTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int LIVE       = 200;
  private static final int COLLISIONS = 7;

  /** The fixtures deliberately leave shared vector ids on the pages, which is what the base class' final check reports. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void aRebuiltGraphIsAcceptedByTheNextLoadWhenTwoRecordsShareAVectorId() {
    createTwoRecordsOnOneVectorId();

    vectorIndex().buildVectorGraphNow();
    final int[] built = vectorIndex().getOrdinalToVectorIdForTest();
    assertThat(java.util.Arrays.stream(built).distinct().count())
        .as("the graph must not carry two nodes for one vector id (%d nodes)", built.length).isEqualTo(built.length);
    reopenDatabase();

    assertThat(vectorIndex().findNeighborsFromVector(embedding(0), 5, 64)).isNotEmpty();
    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("the graph the build persisted must describe the set the next load reconstructs")
        .isEqualTo(0L);
  }

  @Test
  void checkDatabaseFixRebuildsTheIndexAndEveryRecordGetsAVectorIdOfItsOwn() {
    createTwoRecordsOnOneVectorId();

    assertThat(vectorIndex().checkIntegrity()).as("the shared ids must be reported").hasSize(1);
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      assertThat(rs.next().<java.util.Collection<String>>getProperty("corruptedIndexes"))
          .as("a plain check names the index and changes nothing")
          .anyMatch(name -> name.startsWith("Doc_0_"));
    }
    assertThat(vectorIndex().checkIntegrity()).as("a plain check repairs nothing").hasSize(1);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      rs.next();
    }

    assertThat(vectorIndex().checkIntegrity()).as("after the fix no two records share a vector id").isEmpty();
    assertThat(vectorIndex().getStats().get("activeVectors"))
        .as("every record holding an embedding is indexed again, the %d that lost theirs included", COLLISIONS)
        .isEqualTo((long) LIVE);
    for (int k = 0; k < COLLISIONS; k++)
      assertThat(vectorIndex().findNeighborsFromVector(embedding(k), 1, 64).getFirst().getFirst())
          .as("record %d found by its own embedding", k).isEqualTo(ridOf(k));

    vectorIndex().buildVectorGraphNow();
    reopenDatabase();
    assertThat(vectorIndex().findNeighborsFromVector(embedding(0), 5, 64)).isNotEmpty();
    assertThat(vectorIndex().getStats().get("graphRebuildCount")).as("the loop is over").isEqualTo(0L);
  }

  /** Leaves the pages with {@code COLLISIONS} pairs of records carrying the same vector id, then reopens the database. */
  private void createTwoRecordsOnOneVectorId() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.INT8).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < LIVE; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", i, embedding(i));
    });

    // Records without an embedding of their own, so the index holds nothing for them yet.
    database.transaction(() -> {
      for (int k = 0; k < COLLISIONS; k++)
        database.command("sql", "INSERT INTO Doc SET id = ?", 1000 + k);
    });

    // Record 1000+k is written onto the vector id record k already owns.
    final LSMVectorIndex index = vectorIndex();
    final int[] ids = new int[COLLISIONS];
    final RID[] rids = new RID[COLLISIONS];
    for (int k = 0; k < COLLISIONS; k++) {
      ids[k] = index.residentLocationsForTest().getVectorIdsForRid(ridOf(k))[0];
      rids[k] = ridOf(1000 + k);
    }
    database.transaction(() -> {
      for (int k = 0; k < COLLISIONS; k++)
        index.persistEntryForTest(ids[k], rids[k], embedding(1000 + k));
    });

    reopenDatabase();
  }

  private RID ridOf(final int id) {
    try (final ResultSet rs = database.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      return rs.next().getProperty("@rid");
    }
  }

  private static float[] embedding(final int seed) {
    final Random random = new Random(0xD0Fb1DL * 31 + seed);
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = (float) random.nextGaussian();
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
