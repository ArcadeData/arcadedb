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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A delete, or a SET of the vector property to NULL, committed while a full rebuild of the index runs could leave the removed
 * record's vector live in the index for good: {@code countEntries()} stayed above the number of records that carry a vector, and
 * a search answered a record that is gone (issue #13148).
 * <p>
 * The rebuild reads the pages without the index lock, and when they show fewer vectors than the bucket has records (always the
 * case for a vector property only some records carry) it scans the records for the ones the pages lack. A record found there
 * was handed a second id of the build's own, which no commit could tombstone, because a commit tombstones the ids its location
 * index holds for the RID. The tests here keep the build in the window with the test hooks, so the interleaving is the same on
 * every run, and one test lets a writer and a rebuilder race for a few seconds.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13148RebuildRemovedRecordTest {
  private static final String DB_PATH    = "target/test-databases/Issue13148RebuildRemovedRecordTest";
  private static final int    DIMENSIONS = 8;
  /** Records: the even ones carry a vector, so the pages hold half of the bucket and the rebuild always scans the records. */
  private static final int    BASE       = 100;

  private final Random    random     = new Random(13148);
  private final List<RID> withVector = new ArrayList<>();

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * A record committed after the rebuild parsed the pages is in the location index (its commit replayed there) and in the records
   * the scan reads, but not in the pages. It keeps the one id it has: a second id of the build's own was carried over next to
   * the first and counted the record twice.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aRecordCommittedAfterThePagesWereParsedHasOneId() {
    withIndex((db, index) -> {
      final float[] vector = vector();
      final RID[] inserted = new RID[1];
      rebuildWith(index, () -> commitInAnotherThread(db, () -> inserted[0] = insert(db, vector)), null);

      assertThat(index.residentLocationsForTest().getVectorIdsForRid(inserted[0])).as("one live id for the new record").hasSize(1);
      assertThat(index.countEntries()).as("one vector per record that carries one").isEqualTo(recordsWithVector(db));
      assertThat(index.countEntries()).isEqualTo(BASE / 2 + 1);
      assertFound(db, index, vector, inserted[0]);

      // The id is the one its delete tombstones, so the next rebuild does not bring the vector back from the pages
      commitInAnotherThread(db, () -> db.lookupByRID(inserted[0], false).asDocument().delete());
      index.buildVectorGraphNow();
      assertThat(index.countEntries()).isEqualTo(BASE / 2);
      assertThat(index.countEntries()).isEqualTo(recordsWithVector(db));
    });
  }

  /**
   * The case of the issue: the scan has read the record when it is deleted, before the rebuild publishes. The delete tombstones
   * the id the location index has; an id minted by the scan had nobody to tombstone it and was published live.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aDeleteCommittedAfterTheScanReadTheRecordLeavesNoVector() {
    withIndex((db, index) -> {
      final RID[] inserted = new RID[1];
      rebuildWith(index, () -> commitInAnotherThread(db, () -> inserted[0] = insert(db, vector())),
          () -> commitInAnotherThread(db, () -> db.lookupByRID(inserted[0], false).asDocument().delete()));

      assertThat(index.residentLocationsForTest().getVectorIdsForRid(inserted[0])).as("the deleted record has no live id").isEmpty();
      assertThat(index.countEntries()).as("one vector per record that carries one").isEqualTo(recordsWithVector(db));
      assertThat(index.countEntries()).isEqualTo(BASE / 2);

      index.buildVectorGraphNow();
      assertThat(index.countEntries()).as("a second rebuild agrees").isEqualTo(BASE / 2);
    });
  }

  /** The same with a SET of the vector to NULL: the record stays, without a vector. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aNullCommittedAfterTheScanReadTheRecordLeavesNoVector() {
    withIndex((db, index) -> {
      final RID[] inserted = new RID[1];
      rebuildWith(index, () -> commitInAnotherThread(db, () -> inserted[0] = insert(db, vector())), () -> commitInAnotherThread(db, () -> {
        final MutableDocument document = db.lookupByRID(inserted[0], false).asDocument().modify();
        document.set("vector", null);
        document.save();
      }));

      assertThat(index.residentLocationsForTest().getVectorIdsForRid(inserted[0])).as("the record has no live id").isEmpty();
      assertThat(index.countEntries()).as("one vector per record that carries one").isEqualTo(recordsWithVector(db));
      assertThat(index.countEntries()).isEqualTo(BASE / 2);
    });
  }

  /**
   * A record the pages and the location index both lack (what the scan exists for: pages that lost entries) gets an id of the
   * build's own. A commit that removes the record afterwards has no id to tombstone, so the rebuild has to find out by the record
   * that it was removed, and leave the id out.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aRecordTheIndexLostIsRecoveredAndDroppedWhenItIsDeletedBeforeThePublish() {
    withIndex((db, index) -> {
      final RID lost = forgetInTheIndex(db, index, 4);

      rebuildWith(index, null, () -> commitInAnotherThread(db, () -> db.lookupByRID(lost, false).asDocument().delete()));

      assertThat(index.residentLocationsForTest().getVectorIdsForRid(lost)).as("the deleted record has no live id").isEmpty();
      assertThat(index.countEntries()).as("one vector per record that carries one").isEqualTo(recordsWithVector(db));
      assertThat(index.countEntries()).isEqualTo(BASE / 2 - 1);
    });
  }

  /** And without a removal, the record is recovered with an id that the next rebuild keeps instead of minting another. */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aRecordTheIndexLostIsRecoveredAndKeepsItsIdAcrossRebuilds() {
    withIndex((db, index) -> {
      final RID lost = forgetInTheIndex(db, index, 4);

      index.buildVectorGraphNow();
      final int[] recovered = index.residentLocationsForTest().getVectorIdsForRid(lost);
      assertThat(recovered).as("the scan recovered the record").hasSize(1);
      assertThat(index.countEntries()).isEqualTo(BASE / 2);

      index.buildVectorGraphNow();
      assertThat(index.residentLocationsForTest().getVectorIdsForRid(lost)).as("the next rebuild keeps the id").containsExactly(recovered);
      assertThat(index.countEntries()).isEqualTo(BASE / 2);

      // Known to the location index, so a delete tombstones it
      commitInAnotherThread(db, () -> db.lookupByRID(lost, false).asDocument().delete());
      assertThat(index.residentLocationsForTest().getVectorIdsForRid(lost)).isEmpty();
      assertThat(index.countEntries()).isEqualTo(BASE / 2 - 1);
    });
  }

  /**
   * The rebuild remembers the records removed while it runs up to a bound, and drops the ids it minted when it cannot vouch for
   * them any more. The record is recovered by the next rebuild, which is cheaper than keeping an id for a record that may be gone.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aRebuildThatOutlivesTheBoundOfRemovalsDropsTheIdsItMinted() {
    withIndex((db, index) -> {
      final RID lost = forgetInTheIndex(db, index, 4);
      index.setMaxTrackedRemovalsForTest(2);
      try {
        rebuildWith(index, null, () -> commitInAnotherThread(db, () -> {
          for (int i = 1; i <= 3; i++)
            db.lookupByRID(withVector.get(10 + i), false).asDocument().delete();
        }));
      } finally {
        index.setMaxTrackedRemovalsForTest(1 << 20);
      }

      assertThat(index.residentLocationsForTest().getVectorIdsForRid(lost)).as("not vouched for, so not published").isEmpty();
      assertThat(index.countEntries()).isEqualTo(BASE / 2 - 1 - 3);

      index.buildVectorGraphNow();
      assertThat(index.residentLocationsForTest().getVectorIdsForRid(lost)).as("recovered by the next rebuild").hasSize(1);
      assertThat(index.countEntries()).isEqualTo(recordsWithVector(db));
    });
  }

  /**
   * One writer (tombstone, re-set, delete, insert and update of a vector only some records carry) races a rebuilder, as in the
   * issue. Nothing runs when it stops: the index must hold exactly the records that carry a vector, and a search must not answer a
   * record that does not.
   */
  @Test
  @Tag("slow")
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void aWriterRacingRebuildsLeavesOneVectorPerRecord() throws Exception {
    withIndex((db, index) -> {
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread rebuilder = new Thread(() -> {
        while (!stop.get())
          try {
            index.buildVectorGraphNow();
            Thread.sleep(10);
          } catch (final NeedRetryException ignored) {
            // the index is busy with another rebuild or a compaction: the next round
          } catch (final InterruptedException e) {
            return;
          } catch (final Throwable t) {
            failure.set(t);
            return;
          }
      }, "rebuilder");
      rebuilder.start();

      try {
        write(db, 4_000);
      } finally {
        stop.set(true);
        try {
          rebuilder.join(TimeUnit.MINUTES.toMillis(1));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      assertThat(rebuilder.isAlive()).isFalse();
      assertThat(failure.get()).as("the rebuilds succeed").isNull();

      final long expected = recordsWithVector(db);
      assertThat(index.countEntries()).as("one vector per record that carries one").isEqualTo(expected);

      index.buildVectorGraphNow();
      assertThat(index.countEntries()).as("and after a rebuild with nothing running").isEqualTo(expected);

      db.transaction(() -> {
        for (final Pair<RID, Float> hit : index.findNeighborsFromVector(vector(), (int) expected + 100))
          assertThat(db.lookupByRID(hit.getFirst(), false).asDocument().get("vector")).as("%s answered by a search carries a vector", hit.getFirst())
              .isNotNull();
      });
    });
  }

  /** The writer of the issue: tombstone, re-set, delete, insert and update, one commit at a time, for {@code millis}. */
  private void write(final Database db, final long millis) {
    final Random writer = new Random(7);
    final List<RID> tombstoned = new ArrayList<>();
    final List<RID> without = new ArrayList<>();
    final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    while (System.nanoTime() < until) {
      final int op = writer.nextInt(100);
      if (op < 30 && withVector.size() > 20) {
        final RID rid = withVector.remove(writer.nextInt(withVector.size()));
        db.transaction(() -> {
          final MutableDocument document = db.lookupByRID(rid, false).asDocument().modify();
          document.set("vector", null);
          document.save();
        });
        tombstoned.add(rid);
      } else if (op < 50 && !tombstoned.isEmpty()) {
        final RID rid = tombstoned.remove(writer.nextInt(tombstoned.size()));
        final float[] vector = vector(writer);
        db.transaction(() -> {
          final MutableDocument document = db.lookupByRID(rid, false).asDocument().modify();
          document.set("vector", vector);
          document.save();
        });
        withVector.add(rid);
      } else if (op < 60 && withVector.size() + without.size() > 60) {
        final boolean fromVectors = writer.nextBoolean() && withVector.size() > 20;
        final RID rid = fromVectors ? withVector.remove(writer.nextInt(withVector.size())) : without.isEmpty() ? withVector.remove(0) :
            without.remove(writer.nextInt(without.size()));
        db.transaction(() -> db.lookupByRID(rid, false).asDocument().delete());
      } else if (op < 75) {
        final boolean has = writer.nextBoolean();
        final float[] vector = vector(writer);
        final RID[] inserted = new RID[1];
        db.transaction(() -> {
          final MutableDocument document = db.newDocument("Doc");
          if (has)
            document.set("vector", vector);
          inserted[0] = document.save().getIdentity();
        });
        (has ? withVector : without).add(inserted[0]);
      } else if (!withVector.isEmpty()) {
        final RID rid = withVector.get(writer.nextInt(withVector.size()));
        final float[] vector = vector(writer);
        db.transaction(() -> {
          final MutableDocument document = db.lookupByRID(rid, false).asDocument().modify();
          document.set("vector", vector);
          document.save();
        });
      }
    }
  }

  /**
   * Makes the index forget the {@code n}-th record that carries a vector: its entry is tombstoned on the pages and in memory, and the
   * record keeps its vector, which is the state the scan of a rebuild exists to repair.
   */
  private RID forgetInTheIndex(final Database db, final LSMVectorIndex index, final int n) {
    final RID lost = withVector.get(n);
    final int id = index.residentLocationsForTest().getVectorIdsForRid(lost)[0];
    db.transaction(() -> index.persistTombstoneForTest(id, lost));
    index.residentLocationsForTest().markDeleted(id);
    assertThat(index.countEntries()).as("precondition: the index lost the record").isEqualTo(BASE / 2 - 1);
    return lost;
  }

  private RID insert(final Database db, final float[] vector) {
    return db.newDocument("Doc").set("vector", vector).save().getIdentity();
  }

  private static long recordsWithVector(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM Doc WHERE vector IS NOT NULL")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * A full rebuild that runs {@code afterPagesParsed} once it has parsed the pages, which is before it reads the records, and
   * {@code afterLiveSetRead} once it has read them all, which is before it publishes.
   */
  private static void rebuildWith(final LSMVectorIndex index, final Runnable afterPagesParsed, final Runnable afterLiveSetRead) {
    index.setAfterPagesParsedForTest(afterPagesParsed);
    index.setAfterLiveSetReadForTest(afterLiveSetRead);
    try {
      index.buildVectorGraphNow();
    } finally {
      index.setAfterPagesParsedForTest(null);
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
              + ", \"similarity\": \"EUCLIDEAN\", \"mutationsBeforeRebuild\": 100000000, \"inactivityRebuildTimeoutMs\": 3600000 }");
        });
        db.transaction(() -> {
          for (int i = 0; i < BASE; i++) {
            final MutableDocument document = db.newDocument("Doc");
            if (i % 2 == 0)
              document.set("vector", vector());
            final RID rid = document.save().getIdentity();
            if (i % 2 == 0)
              withVector.add(rid);
          }
        });
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
            .getIndexesOnBuckets()[0];
        index.buildVectorGraphNow();
        assertThat(index.countEntries()).as("precondition").isEqualTo(BASE / 2);
        test.run(db, index);
      } finally {
        db.drop();
      }
    }
  }

  private float[] vector() {
    return vector(random);
  }

  private static float[] vector(final Random source) {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = source.nextFloat();
    return v;
  }
}
