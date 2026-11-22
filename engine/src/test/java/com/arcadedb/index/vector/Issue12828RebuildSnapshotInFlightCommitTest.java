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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A commit replays its index operations in its first phase, which hands out the new vector id and records its location, and
 * writes the pages in its second phase, which is when a reader of the pages can see them. A full rebuild that takes its snapshot
 * of the id sequence in between reads pages without the entry and carries over only the ids from the snapshot on, so the id
 * of the update it never read, and was not handed after the snapshot, got no location: the record held a vector in the pages
 * and none in the location index, and the index counted one record short until the next rebuild (issue #12828).
 */
class Issue12828RebuildSnapshotInFlightCommitTest {
  private static final String DB_PATH    = "target/test-databases/Issue12828RebuildSnapshotInFlightCommitTest";
  private static final int    DIMENSIONS = 8;
  private static final int    BASE       = 120;

  private final Random    random = new Random(12828);
  private final List<RID> rids   = new ArrayList<>();

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anUpdateReplayedBeforeTheRebuildSnapshotAndFlushedAfterItKeepsItsLocation() throws Exception {
    withIndex((db, index) -> {
      final RID updated = rids.get(7);
      final float[] newVector = vector();
      rebuildWhileACommitIsBetweenItsPhases(db, index, () -> {
        final MutableDocument document = db.lookupByRID(updated, true).asDocument().modify();
        document.set("vector", newVector);
        document.save();
      });

      assertThat(index.residentLocationsForTest().getVectorIdsForRid(updated)).as("the updated record keeps a live vector id").hasSize(1);
      assertThat(index.countEntries()).as("one vector per record").isEqualTo(BASE);
      // The record is found by its new vector: the id the publish carried over is live, so the delta entry of the update is kept
      assertNearestIs(db, index, newVector, updated);
    });
  }

  /**
   * The same window for an insert: its id was handed out before the snapshot and its page was written after the read.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anInsertReplayedBeforeTheRebuildSnapshotAndFlushedAfterItKeepsItsLocation() throws Exception {
    withIndex((db, index) -> {
      final float[] vector = vector();
      final AtomicReference<RID> inserted = new AtomicReference<>();
      rebuildWhileACommitIsBetweenItsPhases(db, index,
          () -> inserted.set(db.newDocument("Doc").set("vector", vector).save().getIdentity()));

      assertThat(index.residentLocationsForTest().getVectorIdsForRid(inserted.get())).as("the inserted record has a live vector id")
          .hasSize(1);
      assertThat(index.countEntries()).as("one vector per record").isEqualTo(BASE + 1);
      assertNearestIs(db, index, vector, inserted.get());

      // The location is what the delete writes its tombstone from: without it the next rebuild finds the vector live on the pages
      db.transaction(() -> db.lookupByRID(inserted.get(), false).asDocument().delete());
      index.buildVectorGraphNow();
      assertThat(index.countEntries()).isEqualTo(BASE);
    });
  }

  /**
   * The delete mix of the issue: a delete and an update in the same in-flight commit. The deleted record must stay deleted (the
   * extra pass must not bring back an id the replay tombstoned) and the surviving one must keep its location.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aDeleteAndAnUpdateReplayedBeforeTheRebuildSnapshotLeaveOnlyTheSurvivorLive() throws Exception {
    withIndex((db, index) -> {
      final RID deleted = rids.get(3);
      final RID updated = rids.get(11);
      final float[] newVector = vector();
      rebuildWhileACommitIsBetweenItsPhases(db, index, () -> {
        db.lookupByRID(deleted, false).asDocument().delete();
        final MutableDocument document = db.lookupByRID(updated, true).asDocument().modify();
        document.set("vector", newVector);
        document.save();
      });

      final VectorLocationIndex locations = index.residentLocationsForTest();
      assertThat(locations.getVectorIdsForRid(deleted)).as("the deleted record has no live vector id").isEmpty();
      assertThat(locations.getVectorIdsForRid(updated)).as("the updated record keeps a live vector id").hasSize(1);
      assertThat(index.countEntries()).as("one vector per surviving record").isEqualTo(BASE - 1);
      assertNearestIs(db, index, newVector, updated);

      // A second rebuild with no commit in flight agrees with the first
      index.buildVectorGraphNow();
      assertThat(index.countEntries()).isEqualTo(BASE - 1);
      assertThat(index.residentLocationsForTest().getVectorIdsForRid(deleted)).isEmpty();
    });
  }

  /**
   * Runs {@code changes} in a transaction on another thread, stops that commit between its first phase (the index replay) and its
   * second (the page writes), runs a full rebuild, and lets the commit write its pages once the rebuild has read them and before it
   * publishes.
   */
  private static void rebuildWhileACommitIsBetweenItsPhases(final Database db, final LSMVectorIndex index, final Runnable changes)
      throws InterruptedException {
    final CountDownLatch replayed = new CountDownLatch(1);
    final CountDownLatch pagesRead = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread committer = new Thread(() -> {
      try {
        db.begin();
        changes.run();
        final TransactionContext transaction = ((DatabaseInternal) db).getTransaction();
        // Phase 1 replays the index operations: the new vector has its id and its location, its page is not visible yet
        final TransactionContext.TransactionPhase1 phase1 = transaction.commit1stPhase(true);
        replayed.countDown();
        if (!pagesRead.await(1, TimeUnit.MINUTES))
          throw new IllegalStateException("the rebuild did not read the pages");
        transaction.commit2ndPhase(phase1);
      } catch (final Throwable t) {
        failure.set(t);
        replayed.countDown();
      }
    });
    committer.start();
    assertThat(replayed.await(1, TimeUnit.MINUTES)).isTrue();
    assertThat(failure.get()).isNull();

    // The rebuild has taken its snapshot and read the pages, which lack the commit: let the commit write them now, and publish
    index.setAfterLiveSetReadForTest(() -> {
      pagesRead.countDown();
      try {
        committer.join(TimeUnit.MINUTES.toMillis(1));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    try {
      index.buildVectorGraphNow();
    } finally {
      index.setAfterLiveSetReadForTest(null);
    }
    assertThat(committer.isAlive()).isFalse();
    assertThat(failure.get()).isNull();
  }

  private static void assertNearestIs(final Database db, final LSMVectorIndex index, final float[] query, final RID expected) {
    db.transaction(() -> {
      final List<Pair<RID, Float>> found = index.findNeighborsFromVector(query, 1);
      assertThat(found).hasSize(1);
      assertThat(found.getFirst().getFirst()).as("the record holding the query vector is the nearest").isEqualTo(expected);
    });
  }

  private void withIndex(final IndexTest test) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"EUCLIDEAN\" }");
        });
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++)
            rids.add(db.newDocument("Doc").set("vector", vector()).save().getIdentity());
        });
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
            .getIndexesOnBuckets()[0];
        index.buildVectorGraphNow();
        test.run(db, index);
      } finally {
        db.drop();
      }
    }
  }

  @FunctionalInterface
  private interface IndexTest {
    void run(Database db, LSMVectorIndex index) throws Exception;
  }

  private float[] vector() {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < vector.length; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }
}
