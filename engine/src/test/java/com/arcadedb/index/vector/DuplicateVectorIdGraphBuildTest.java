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

import java.util.Arrays;
import java.util.Collection;
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
    assertThat(Arrays.stream(built).distinct().count())
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
      assertThat(rs.next().<Collection<String>>getProperty("corruptedIndexes"))
          .as("a plain check names the index and changes nothing")
          .anyMatch(name -> name.startsWith("Doc_0_"));
    }
    assertThat(vectorIndex().checkIntegrity()).as("a plain check repairs nothing").hasSize(1);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      rs.next();
    }

    assertThat(vectorIndex().checkIntegrity()).as("after the fix no two records share a vector id").isEmpty();
    assertThat(vectorIndex().compactionBlockedBySharedIdsForTest()).as("the rebuilt index may compact again").isFalse();
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

  @Test
  void threeRecordsOnOneVectorIdLeaveNoLoop() {
    createIndexedDocs();
    final LSMVectorIndex index = vectorIndex();
    final int id = index.residentLocationsForTest().getVectorIdsForRid(ridOf(0))[0];
    final RID second = ridOf(1000);
    final RID third = ridOf(1001);
    database.transaction(() -> {
      index.persistEntryForTest(id, second, embedding(1000));
      index.persistEntryForTest(id, third, embedding(1001));
    });
    reopenDatabase();

    assertThat(vectorIndex().checkIntegrity()).as("one finding naming the records that lost the id").hasSize(1);
    assertNoLoopAfterABuild();
  }

  /** A tombstone written for one record of a shared id removes the id for the other one as well, in a load. */
  @Test
  void aTombstoneForOneRecordOfASharedIdLeavesNoLoop() {
    createIndexedDocs();
    final LSMVectorIndex index = vectorIndex();
    final RID owner = ridOf(0);
    final int id = index.residentLocationsForTest().getVectorIdsForRid(owner)[0];
    final RID other = ridOf(1000);
    database.transaction(() -> index.persistEntryForTest(id, other, embedding(1000)));
    database.transaction(() -> index.persistTombstoneForTest(id, owner));
    reopenDatabase();

    assertNoLoopAfterABuild();
  }

  /** The same two entries in the other order: the tombstone lands first and the later live entry wins the id. */
  @Test
  void aTombstoneBeforeTheOtherRecordsEntryLeavesNoLoop() {
    createIndexedDocs();
    final LSMVectorIndex index = vectorIndex();
    final RID owner = ridOf(0);
    final int id = index.residentLocationsForTest().getVectorIdsForRid(owner)[0];
    final RID other = ridOf(1000);
    database.transaction(() -> index.persistTombstoneForTest(id, owner));
    database.transaction(() -> index.persistEntryForTest(id, other, embedding(1000)));
    reopenDatabase();

    assertNoLoopAfterABuild();
  }

  /** A compaction must not erase the evidence: the pages keep the shared ids until the index is rebuilt. */
  @Test
  void aCompactionDoesNotEraseTheSharedIdsFromThePages() throws Exception {
    createTwoRecordsOnOneVectorId();

    vectorIndex().scheduleCompaction();
    vectorIndex().compact();

    assertThat(vectorIndex().checkIntegrity()).as("still reported after a compaction").hasSize(1);
    assertThat(vectorIndex().getStats().get("compactionBlockedBySharedIds"))
        .as("visible to monitoring, since nothing else says compaction has stopped").isEqualTo(1L);
    assertThat(vectorIndex().compactionBlockedBySharedIdsForTest())
        .as("and the compaction trigger stops asking, or every commit would repeat a full graph build for nothing")
        .isTrue();
  }

  /** Ids past the dense arrays are judged too: a shared id there is reported and not silently skipped. */
  @Test
  void aSharedIdPastTheDenseArraysIsReported() {
    createIndexedDocs();
    final LSMVectorIndex index = vectorIndex();
    final int hugeId = 50_000_000;
    final RID first = ridOf(0);
    final RID second = ridOf(1000);
    database.transaction(() -> {
      index.persistEntryForTest(hugeId, first, embedding(0));
      index.persistEntryForTest(hugeId, second, embedding(1000));
    });
    reopenDatabase();

    assertThat(vectorIndex().checkIntegrity()).as("the loser of an id past the dense range").hasSize(1);
  }

  private void assertNoLoopAfterABuild() {
    vectorIndex().buildVectorGraphNow();
    final int[] built = vectorIndex().getOrdinalToVectorIdForTest();
    assertThat(Arrays.stream(built).distinct().count()).as("one node per vector id").isEqualTo(built.length);
    reopenDatabase();

    assertThat(vectorIndex().findNeighborsFromVector(embedding(5), 5, 64)).isNotEmpty();
    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("the graph the build persisted must describe the set the next load reconstructs").isEqualTo(0L);
  }

  /** The index, {@code LIVE} indexed records and a few more without an embedding, so nothing is indexed for them yet. */
  private void createIndexedDocs() {
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
      for (int k = 0; k < COLLISIONS; k++)
        database.command("sql", "INSERT INTO Doc SET id = ?", 1000 + k);
    });
  }

  /** Leaves the pages with {@code COLLISIONS} pairs of records carrying the same vector id, then reopens the database. */
  private void createTwoRecordsOnOneVectorId() {
    createIndexedDocs();

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
