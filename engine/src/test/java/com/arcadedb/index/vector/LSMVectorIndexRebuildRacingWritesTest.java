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
import com.arcadedb.database.RID;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A full rebuild reads the live set from the index pages without holding the index lock, then publishes a location index
 * built from what it read. A delete or an insert committed in between used to be lost by that publish: the deleted vector
 * came back live, so a search returned its deleted record, and the inserted one lost its location, so a later delete of
 * its record wrote no tombstone and the next rebuild brought it back from the pages (issue #11099).
 * <p>
 * The publish therefore carries the tombstones over, including the ones a commit replay wrote and may still take back if
 * its transaction aborts (issue #7931): the abort has to lift them in the generation the rebuild published.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexRebuildRacingWritesTest {
  private static final String DB_PATH    = "target/test-databases/LSMVectorIndexRebuildRacingWritesTest";
  private static final int    DIMENSIONS = 16;
  private static final int    BASE       = 600;
  private static final int    DELETED    = 25;
  private static final int    INSERTED   = 9;

  private final Random        random      = new Random(5397);
  private final List<RID>     base        = new ArrayList<>();
  private final List<float[]> baseVectors = new ArrayList<>();

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
  void writesCommittedWhileARebuildReadsThePagesSurviveItsPublish() {
    withIndex((db, index) -> {
      // Committed by another thread once the rebuild has read the pages and before it publishes what it read
      final List<RID> inserted = new ArrayList<>();
      final List<float[]> insertedVectors = new ArrayList<>();
      rebuildCommittingInBetween(index, () -> commitInAnotherThread(db, () -> {
        for (int i = 0; i < DELETED; i++)
          db.lookupByRID(base.get(i), false).asDocument().delete();
        for (int i = 0; i < INSERTED; i++) {
          final float[] vector = vector();
          inserted.add(db.newDocument("Doc").set("vector", vector).save().getIdentity());
          insertedVectors.add(vector);
        }
      }));
      assertThat(inserted).hasSize(INSERTED);

      final VectorLocationIndex locations = index.residentLocationsForTest();
      for (int i = 0; i < DELETED; i++)
        assertThat(locations.getVectorIdsForRid(base.get(i))).as("the vector of deleted %s is not live", base.get(i)).isEmpty();
      for (final RID rid : inserted)
        assertThat(locations.getVectorIdsForRid(rid)).as("the vector of inserted %s is live", rid).hasSize(1);
      assertThat(index.countEntries()).isEqualTo(BASE - DELETED + INSERTED);
      for (int i = 0; i < DELETED; i++)
        assertNoDeletedRecordFound(db, index, baseVectors.get(i));

      // The insert's location is what its delete writes the tombstone from: without it the next rebuild finds the vector
      // still live on the pages
      db.transaction(() -> {
        for (final RID rid : inserted)
          db.lookupByRID(rid, false).asDocument().delete();
      });
      index.buildVectorGraphNow();
      assertThat(index.countEntries()).isEqualTo(BASE - DELETED);
      for (final float[] vector : insertedVectors)
        assertNoDeletedRecordFound(db, index, vector);
    });
  }

  /**
   * A commit replay tombstones in memory before its transaction is validated, and takes the tombstone back if the
   * transaction aborts (issue #7931). A rebuild that publishes in between carries that tombstone over like any other, so
   * the abort has to lift it in the new generation, or the record it never deleted stays unsearchable.
   * <p>
   * Driven through the journal directly, as {@code Issue7931AbortedTransactionVectorLeakTest} does: the window is inside
   * one {@code commit()}, between the replay and the rollback, where no test thread can step in.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anAbortedReplayLiftsTheTombstoneARebuildCarriedOver() {
    withIndex((db, index) -> {
      final RID kept = base.get(1);
      final VectorLocationIndex before = index.residentLocationsForTest();
      final int id = before.getVectorIdsForRid(kept)[0];

      // What the replay's remove() does in memory; its tombstone write on the pages goes away with the transaction
      final VectorIndexReplayUndo undo = new VectorIndexReplayUndo(index, before);
      undo.recordTombstoned(id, before.getOffsetAndFlag(id), kept);
      before.markDeleted(id);

      index.buildVectorGraphNow();
      final VectorLocationIndex after = index.residentLocationsForTest();
      assertThat(after).as("precondition: the rebuild republished the location index").isNotSameAs(before);
      assertThat(after.isDeleted(id)).as("precondition: the rebuild carried the replay's tombstone over").isTrue();

      undo.undoIndexReplay();

      assertThat(after.isLive(id)).as("the abort lifts the tombstone in the generation the rebuild published").isTrue();
      assertFound(db, index, baseVectors.get(1), kept);
    });
  }

  /**
   * The same abort for a vector the rebuild could not read from the pages: inserted and committed after it read them,
   * then tombstoned by a replay that aborts. The rebuild carries the tombstone of an id it was handed after its snapshot,
   * and the abort lifts it and puts back the delta-buffer entry the replay dropped, the only place a search finds that
   * vector until the next rebuild.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void anAbortedReplayRestoresAVectorInsertedWhileARebuildReadThePages() {
    withIndex((db, index) -> {
      final float[] vector = vector();
      final RID[] inserted = new RID[1];
      final VectorIndexReplayUndo[] undo = new VectorIndexReplayUndo[1];
      final int[] id = new int[1];
      rebuildCommittingInBetween(index, () -> {
        commitInAnotherThread(db, () -> inserted[0] = db.newDocument("Doc").set("vector", vector).save().getIdentity());

        // The replay of a delete of that record, by a transaction that will abort
        final VectorLocationIndex locations = index.residentLocationsForTest();
        id[0] = locations.getVectorIdsForRid(inserted[0])[0];
        undo[0] = new VectorIndexReplayUndo(index, locations);
        undo[0].recordTombstoned(id[0], locations.getOffsetAndFlag(id[0]), inserted[0]);
        locations.markDeleted(id[0]);
        final LSMVectorIndex.DeltaVectorEntry dropped = index.takeDeltaEntryForTest(id[0]);
        assertThat(dropped).as("precondition: the insert is in the delta buffer").isNotNull();
        undo[0].recordDroppedDelta(dropped);
      });

      final VectorLocationIndex after = index.residentLocationsForTest();
      assertThat(after.isDeleted(id[0])).as("the rebuild carries the tombstone of an id handed out after its snapshot").isTrue();

      undo[0].undoIndexReplay();

      assertThat(after.isLive(id[0])).as("the abort lifts it").isTrue();
      assertFound(db, index, vector, inserted[0]);
    });
  }

  private interface IndexTest {
    void run(Database db, LSMVectorIndex index);
  }

  private void withIndex(final IndexTest test) {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          db.getSchema().createDocumentType("Doc", 1).createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"EUCLIDEAN\" }");
        });
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++) {
            final float[] vector = vector();
            base.add(db.newDocument("Doc").set("vector", vector).save().getIdentity());
            baseVectors.add(vector);
          }
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

  /** A full rebuild that runs {@code inBetween} after reading the live set from the pages and before publishing it. */
  private static void rebuildCommittingInBetween(final LSMVectorIndex index, final Runnable inBetween) {
    index.setAfterLiveSetReadForTest(inBetween);
    try {
      index.buildVectorGraphNow();
    } finally {
      index.setAfterLiveSetReadForTest(null);
    }
  }

  /** Commits {@code writes} from another thread, as a concurrent client would, and waits for it. */
  private static void commitInAnotherThread(final Database db, final Runnable writes) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        db.transaction(writes::run);
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    writer.start();
    try {
      writer.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(failure.get()).as("the concurrent commit succeeds").isNull();
  }

  private static void assertFound(final Database db, final LSMVectorIndex index, final float[] query, final RID expected) {
    db.transaction(() -> assertThat(index.findNeighborsFromVector(query, 5).stream().map(Pair::getFirst))
        .as("%s is found by its own vector", expected).contains(expected));
  }

  private static void assertNoDeletedRecordFound(final Database db, final LSMVectorIndex index, final float[] query) {
    db.transaction(() -> {
      final List<Pair<RID, Float>> found = index.findNeighborsFromVector(query, 5);
      assertThat(found).isNotEmpty();
      for (final Pair<RID, Float> hit : found)
        assertThat(db.existsRecord(hit.getFirst())).as("%s returned by the search exists", hit.getFirst()).isTrue();
    });
  }

  private float[] vector() {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat();
    return v;
  }
}
