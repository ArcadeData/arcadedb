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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7931: a transaction that fails the MVCC page-version check must leave nothing behind in
 * an {@code LSM_VECTOR} index.
 * <p>
 * {@code TransactionContext.commit1stPhase()} replays the queued index operations BEFORE it validates the page
 * versions. For an ordinary LSM index that ordering is harmless - the replay only writes transaction-local pages,
 * which the rollback discards. {@link LSMVectorIndex} is different: its replay also mutates process-wide, entirely
 * non-transactional in-memory state (the tombstone set and the resident locations of {@link VectorLocationIndex},
 * the delta buffer, the mutation counter, the graph state, the insert cursor). Before the fix none of that was
 * compensated when the transaction aborted, so a workload that retries write conflicts - which is every ingest
 * under contention - accumulated tombstones for records nobody deleted and delta entries for vectors no transaction
 * ever committed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue7931AbortedTransactionVectorLeakTest {
  private static final String DB_ROOT     = "target/test-databases/Issue7931AbortedTransactionVectorLeakTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 50;
  /** Small enough that a few hundred INT8-quantized entries need several data pages. */
  private static final int    SMALL_PAGE_SIZE = 4 * 1024;
  private static final int    SPILL_VECTORS   = 400;

  private String dbPath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(dbPath));
  }

  /**
   * The issue's own reproduction: the losing transaction rewrites the embedding (so the index replay both tombstones
   * the old vector id and allocates a new one) while the winner rewrites an unrelated property of the same record.
   */
  @Test
  void aConflictedVectorUpdateLeavesNoTombstoneBehind() throws Exception {
    withDatabase(db -> {
      final LSMVectorIndex index = vectorIndex(db);
      index.buildVectorGraphNow();

      final RID target = ridOf(db, 1);
      final Map<String, Long> before = index.getStats();

      // Park the sole JVM-wide rebuild permit for the whole observation window: an async rebuild republishes the
      // location index and would repair the leak before it could be asserted on.
      LSMVectorIndex.acquireAllRebuildPermitsForTest();
      try {
        db.begin();
        target.asDocument(true).modify().set("vector", embedding(NUM_VECTORS + 1)).save();

        commitInAnotherThread(db, target);

        assertConflictOnCommit(db);

        final Map<String, Long> after = index.getStats();
        assertThat(after.get("deletedVectors"))
            .as("a transaction that never committed must not tombstone the vector of a record it did not change")
            .isEqualTo(before.get("deletedVectors"));
        assertThat(after.get("deltaVectorsCount"))
            .as("nor may its vector stay in the delta buffer, where a search would score a value no transaction "
                + "ever committed")
            .isEqualTo(before.get("deltaVectorsCount"));
        assertThat(after.get("activeVectors"))
            .as("the record's own vector must still be live")
            .isEqualTo(before.get("activeVectors"));
        assertThat(after.get("totalVectors"))
            .as("and no orphan id may be resident")
            .isEqualTo(before.get("totalVectors"));
        assertThat(after.get("mutationsSinceRebuild"))
            .as("an aborted transaction owes the rebuild schedule nothing")
            .isEqualTo(before.get("mutationsSinceRebuild"));
        assertThat(after.get("graphState"))
            .as("a graph with nothing pending must not be left MUTABLE by a transaction that wrote nothing")
            .isEqualTo(before.get("graphState"));
      } finally {
        LSMVectorIndex.releaseAllRebuildPermitsForTest();
      }

      // The record is unchanged on disk, and the index must still find it by its ORIGINAL embedding.
      assertThat(readVector(db, target)).as("the losing update must not have reached the record")
          .containsExactly(embedding(1), Offset.offset(1e-6f));

      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(embedding(1), 5, 64);
      assertThat(hits.stream().map(Pair::getFirst))
          .as("the live record must remain searchable: before the fix its only vector id was tombstoned, so the "
              + "graph walk's live-bits filter refused it")
          .contains(target);
    });
  }

  /**
   * The insert-only half: the losing transaction only ADDS a vector. Nothing is tombstoned, but an id is allocated,
   * a location registered and a delta entry published for a record that never reached the disk.
   */
  @Test
  void aConflictedInsertLeavesNoOrphanVectorIdBehind() throws Exception {
    withDatabase(db -> {
      final LSMVectorIndex index = vectorIndex(db);
      index.buildVectorGraphNow();

      // The conflict is made on a record the new document shares a page with, so the insert's own page is the one
      // the concurrent commit invalidates.
      final RID neighbour = ridOf(db, NUM_VECTORS - 1);
      final Map<String, Long> before = index.getStats();

      LSMVectorIndex.acquireAllRebuildPermitsForTest();
      try {
        db.begin();
        db.newDocument("Doc").set("id", NUM_VECTORS + 7).set("vector", embedding(NUM_VECTORS + 7)).save();
        neighbour.asDocument(true).modify().set("name", "loser").save();

        commitInAnotherThread(db, neighbour);

        assertConflictOnCommit(db);

        final Map<String, Long> after = index.getStats();
        assertThat(after.get("totalVectors"))
            .as("the id the aborted insert allocated must not stay resident in the location index")
            .isEqualTo(before.get("totalVectors"));
        assertThat(after.get("activeVectors")).isEqualTo(before.get("activeVectors"));
        assertThat(after.get("deletedVectors")).isEqualTo(before.get("deletedVectors"));
        assertThat(after.get("deltaVectorsCount"))
            .as("nor may its delta entry survive: a search would return a RID that does not exist")
            .isEqualTo(before.get("deltaVectorsCount"));
        assertThat(after.get("mutationsSinceRebuild")).isEqualTo(before.get("mutationsSinceRebuild"));
      } finally {
        LSMVectorIndex.releaseAllRebuildPermitsForTest();
      }

      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(embedding(NUM_VECTORS + 7), 10, 64);
      assertThat(hits.stream().map(Pair::getFirst))
          .as("no search may return a vector belonging to a record that was never committed")
          .allSatisfy(hit -> assertThat(db.lookupByRID(hit, false)).isNotNull());
    });
  }

  /**
   * The counter-check that keeps the compensation from being a blanket undo: a transaction that COMMITS must keep
   * every effect of its replay, and the record must be findable by its new embedding alone.
   */
  @Test
  void aCommittedVectorUpdateKeepsItsTombstoneAndItsNewVector() throws Exception {
    withDatabase(db -> {
      final LSMVectorIndex index = vectorIndex(db);
      index.buildVectorGraphNow();

      final RID target = ridOf(db, 2);
      final Map<String, Long> before = index.getStats();

      LSMVectorIndex.acquireAllRebuildPermitsForTest();
      try {
        db.transaction(() -> target.asDocument(true).modify().set("vector", embedding(NUM_VECTORS + 3)).save());

        final Map<String, Long> after = index.getStats();
        assertThat(after.get("deletedVectors"))
            .as("the superseded vector id of a COMMITTED update is a real tombstone")
            .isEqualTo(before.get("deletedVectors") + 1);
        assertThat(after.get("deltaVectorsCount"))
            .as("and its new vector must be in the delta buffer, where the search can see it")
            .isEqualTo(before.get("deltaVectorsCount") + 1);
        assertThat(after.get("mutationsSinceRebuild"))
            .as("one delete plus one insert")
            .isEqualTo(before.get("mutationsSinceRebuild") + 2);

        final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(embedding(NUM_VECTORS + 3), 5, 64);
        assertThat(hits.stream().map(Pair::getFirst)).as("the committed embedding must be the one that answers")
            .contains(target);
      } finally {
        LSMVectorIndex.releaseAllRebuildPermitsForTest();
      }
    });
  }

  /**
   * The index must stay WRITABLE after the compensation: the insert cursor is one more piece of non-transactional
   * state the replay moves, and a rollback that discards the page it points at leaves the next insert addressing a
   * page that does not exist.
   */
  @Test
  void theIndexStillAcceptsWritesAfterAnAbortedTransaction() throws Exception {
    withDatabase(db -> {
      final LSMVectorIndex index = vectorIndex(db);
      index.buildVectorGraphNow();

      final RID target = ridOf(db, 3);

      db.begin();
      target.asDocument(true).modify().set("vector", embedding(NUM_VECTORS + 11)).save();
      commitInAnotherThread(db, target);
      assertConflictOnCommit(db);

      // The retry every conflict-tolerant workload performs. It must succeed, and its effect must be complete.
      // The permit is parked across the retry AND the reading of the counters it moves: a rebuild republishes the
      // location index and drops the tombstone set with it, which would make this assertion pass vacuously.
      LSMVectorIndex.acquireAllRebuildPermitsForTest();
      try {
        db.transaction(() -> target.asDocument(true).modify().set("vector", embedding(NUM_VECTORS + 11)).save());

        assertThat(index.getStats().get("deletedVectors"))
            .as("exactly one tombstone: the one the SUCCESSFUL retry left, never the aborted attempt's")
            .isEqualTo(1L);
      } finally {
        LSMVectorIndex.releaseAllRebuildPermitsForTest();
      }

      assertThat(readVector(db, target))
          .containsExactly(embedding(NUM_VECTORS + 11), Offset.offset(1e-6f));

      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(embedding(NUM_VECTORS + 11), 5, 64);
      assertThat(hits.stream().map(Pair::getFirst)).as("the retried update must be searchable by its new embedding")
          .contains(target);
    });
  }

  /**
   * The mixed transaction: one record inserted, another's vector rewritten TWICE, and a third deleted, all before
   * the conflict aborts the lot. This is the shape closest to violating the invariant {@code undoReplay()} asserts -
   * that no id is both allocated and tombstoned by one replay - and it is the one a real ingest actually performs.
   */
  @Test
  void aConflictedMixedTransactionLeavesNothingBehind() throws Exception {
    withDatabase(db -> {
      final LSMVectorIndex index = vectorIndex(db);
      index.buildVectorGraphNow();

      final RID rewritten = ridOf(db, 5);
      final RID deleted = ridOf(db, 6);
      final RID loser = ridOf(db, 7);
      final Map<String, Long> before = index.getStats();

      LSMVectorIndex.acquireAllRebuildPermitsForTest();
      try {
        db.begin();
        db.newDocument("Doc").set("id", NUM_VECTORS + 21).set("name", "inserted")
            .set("vector", embedding(NUM_VECTORS + 21)).save();
        // Twice, so the second update supersedes an id this very transaction allocated.
        rewritten.asDocument(true).modify().set("vector", embedding(NUM_VECTORS + 22)).save();
        rewritten.asDocument(true).modify().set("vector", embedding(NUM_VECTORS + 23)).save();
        deleted.asDocument(true).modify().delete();
        loser.asDocument(true).modify().set("name", "loser").save();

        commitInAnotherThread(db, loser);

        assertConflictOnCommit(db);

        final Map<String, Long> after = index.getStats();
        assertThat(after.get("deletedVectors")).as("no tombstone may survive the abort")
            .isEqualTo(before.get("deletedVectors"));
        assertThat(after.get("totalVectors")).as("no allocated id may stay resident")
            .isEqualTo(before.get("totalVectors"));
        assertThat(after.get("activeVectors")).as("every record's own vector must still be live")
            .isEqualTo(before.get("activeVectors"));
        assertThat(after.get("deltaVectorsCount")).isEqualTo(before.get("deltaVectorsCount"));
        assertThat(after.get("mutationsSinceRebuild")).isEqualTo(before.get("mutationsSinceRebuild"));
      } finally {
        LSMVectorIndex.releaseAllRebuildPermitsForTest();
      }

      // Every record is untouched on disk, and all three are still searchable by their ORIGINAL embeddings.
      for (final int id : new int[] { 5, 6, 7 }) {
        final RID rid = ridOf(db, id);
        assertThat(readVector(db, rid)).as("record %d must be unchanged", id)
            .containsExactly(embedding(id), Offset.offset(1e-6f));
        assertThat(index.findNeighborsFromVector(embedding(id), 5, 64).stream().map(Pair::getFirst))
            .as("record %d must still be found by its own embedding", id).contains(rid);
      }
    });
  }

  /**
   * The page-level half of the same leak: an aborted insert big enough to spill onto new index data pages also moves
   * the insert cursor and the mutable-page gauge, neither of which is transactional. The pages themselves go away
   * with the rollback, so a cursor left pointing past the end addresses a page that does not exist.
   */
  @Test
  void anAbortedInsertThatSpilledOntoNewPagesLeavesThePageAccountingWhereItWas() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populateWithSmallPages(db);

        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();

        final RID neighbour = ridOf(db, 0);
        final long pagesBefore = index.getStats().get("mutablePages");
        final int totalPagesBefore = index.getTotalPages();

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        try {
          db.begin();
          for (int i = 0; i < SPILL_VECTORS; i++)
            db.newDocument("Doc").set("id", 1_000 + i).set("name", "spill" + i)
                .set("vector", embedding(1_000 + i)).save();
          neighbour.asDocument(true).modify().set("name", "loser").save();

          commitInAnotherThread(db, neighbour);
          assertConflictOnCommit(db);

          assertThat(index.getTotalPages())
              .as("precondition: the rollback discards the data pages the aborted insert created")
              .isEqualTo(totalPagesBefore);
          assertThat(index.getStats().get("mutablePages"))
              .as("so the gauge that counts them must come back too")
              .isEqualTo(pagesBefore);
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }

        // The insert cursor: before the fix it still pointed at a page the rollback had thrown away.
        final RID[] retried = new RID[1];
        db.transaction(() -> retried[0] = db.newDocument("Doc").set("id", 2_000).set("name", "after")
            .set("vector", embedding(2_000)).save().getIdentity());

        assertThat(index.findNeighborsFromVector(embedding(2_000), 5, 64).stream().map(Pair::getFirst))
            .as("a write accepted after the abort must be indexed, not written onto a lost page")
            .contains(retried[0]);
      } finally {
        if (db.isOpen())
          db.drop();
      }
    }
  }

  /**
   * The branch no end-to-end case can reach: a compaction or a rebuild republishes {@code residentLocations}
   * wholesale between the replay and the abort, and the offsets the journal captured then address a generation
   * the index is no longer reading. Every other test in this class parks the rebuild permit precisely so nothing
   * repairs the leak before it can be asserted on, which also means none of them ever swaps the instance.
   * <p>
   * Driven directly rather than through a transaction because the swap has to land INSIDE {@code commit()},
   * between {@code indexChanges.commit()} and the rollback - a window no test thread can step into. The journal
   * is built with the same package-private API the replay uses, so what runs here is the production
   * {@code undoReplay} on a genuinely republished index.
   * <p>
   * The id is DELETED for real before the swap, which is what makes the assertion bite: the replacement is built
   * from the committed pages and so does not carry the id at all, and lifting a tombstone is
   * {@code addOrUpdate(..., deleted=false)} - it would make the id LIVE again in a generation that never had it,
   * pointing at an offset for a record that no longer exists. Without the identity check this test fails.
   */
  @Test
  void aRepublishedLocationIndexIsNeverHandedAnOffsetFromTheGenerationBeforeIt() throws Exception {
    withDatabase(db -> {
      final LSMVectorIndex index = vectorIndex(db);
      index.buildVectorGraphNow();

      final RID doomed = ridOf(db, 1);
      final VectorLocationIndex before = index.residentLocationsForTest();
      final int tombstoned = before.getVectorIdsForRid(doomed)[0];

      // Exactly what a replay's remove() records, in the same order: read the location, then let the delete
      // below tombstone it.
      final VectorIndexReplayUndo undo = new VectorIndexReplayUndo(index, before);
      undo.recordTombstoned(tombstoned, before.getOffsetAndFlag(tombstoned), doomed);

      db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id = ?", 1));

      // The swap. A rebuild republishes the locations from the COMMITTED pages, which no longer hold this id.
      index.buildVectorGraphNow();
      final VectorLocationIndex after = index.residentLocationsForTest();
      assertThat(after).as("precondition: the rebuild must have republished the location index")
          .isNotSameAs(before);
      assertThat(after.isLive(tombstoned))
          .as("precondition: the replacement is built from the committed pages, which no longer carry the id")
          .isFalse();

      undo.undoIndexReplay();

      assertThat(index.residentLocationsForTest())
          .as("the compensation must not swap the location index itself").isSameAs(after);
      assertThat(after.isLive(tombstoned))
          .as("lifting the tombstone against a REPLACEMENT would make an id live in a generation that never had "
              + "it, pointing at an offset for a record that no longer exists")
          .isFalse();

      // The index is still usable afterwards, which is the point of declining rather than throwing.
      assertThat(index.findNeighborsFromVector(embedding(2), 5, 64).stream().map(Pair::getFirst))
          .as("the deleted record must not come back through the compensation").doesNotContain(doomed);
    });
  }

  /**
   * The mid-replay re-anchor: one transaction's replay is not one locked section, so a rebuild can republish the
   * locations BETWEEN two of its operations and leave a single journal describing two generations. The journal's
   * contract under that swap is what this pins, directly - the window itself is unreachable from a test thread,
   * being inside one {@code commit()} between two internal lock acquisitions.
   * <p>
   * What must survive the re-anchor is everything that is generation-independent, and only that: the ids the
   * replay allocated (forgotten and swept out of the delta buffer whatever generation is current) and the
   * index-wide counters. What must not is everything addressed by a captured offset.
   */
  @Test
  void reAnchoringDropsOnlyWhatTheSwapMadeUnrestorable() throws Exception {
    withDatabase(db -> {
      final LSMVectorIndex index = vectorIndex(db);
      index.buildVectorGraphNow();

      final VectorLocationIndex before = index.residentLocationsForTest();
      final int live = before.getActiveVectorIds().findFirst().orElseThrow();

      final VectorIndexReplayUndo undo = new VectorIndexReplayUndo(index, before);
      undo.recordAllocated(4242);
      undo.recordTombstoned(live, before.getOffsetAndFlag(live), before.getRid(live));
      undo.mutationsCharged = 2;
      undo.mutablePagesCreated = 1;

      index.buildVectorGraphNow();
      final VectorLocationIndex after = index.residentLocationsForTest();
      assertThat(after).as("precondition: the rebuild must have republished the location index").isNotSameAs(before);

      undo.rebaseTo(after);

      assertThat(undo.locationsAtReplay).as("the journal must now speak for the generation in force").isSameAs(after);
      assertThat(undo.tombstonedCount)
          .as("the captured offsets address a generation the index no longer reads, so they must go").isZero();
      assertThat(undo.droppedDeltaEntries)
          .as("and so must the delta entries the deletes dropped, for the mirror reason").isNull();
      assertThat(undo.allocatedCount)
          .as("an id minted by this replay is illegitimate in EVERY generation, so it must survive the re-anchor "
              + "- dropping it is what would leave an uncommitted vector searchable")
          .isEqualTo(1);
      assertThat(undo.mutationsCharged).as("the counters are index-wide, not per generation").isEqualTo(2);
      assertThat(undo.mutablePagesCreated).isEqualTo(1);
    });
  }

  private interface DatabaseTest {
    void run(Database db) throws Exception;
  }

  private void withDatabase(final DatabaseTest test) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        test.run(db);
      } finally {
        if (db.isOpen())
          db.drop();
      }
    }
  }

  /**
   * The bound is a hang detector, not a latency bound: this commit contends with one open transaction on one
   * record, so a run that needs anywhere near this long has stopped making progress - most plausibly on an index
   * or file lock the transaction under test is holding. Generous, because a wide bound cannot turn a passing run
   * red, and a diagnostic failure is what the suite needs here instead of a thread parked forever on join().
   */
  private static final long CONCURRENT_COMMIT_TIMEOUT_MS = 60_000;

  private static void commitInAnotherThread(final Database db, final RID rid) throws InterruptedException {
    final Throwable[] failure = new Throwable[1];
    final Thread concurrent = new Thread(() -> {
      try {
        db.transaction(() -> rid.asDocument(true).modify().set("name", "winner").save());
      } catch (final Throwable t) {
        failure[0] = t;
      }
    });
    concurrent.start();
    concurrent.join(CONCURRENT_COMMIT_TIMEOUT_MS);
    if (concurrent.isAlive()) {
      final StackTraceElement[] where = concurrent.getStackTrace();
      concurrent.interrupt();
      concurrent.join(5_000);
      throw new AssertionError("the concurrent commit of " + rid + " did not finish within "
          + CONCURRENT_COMMIT_TIMEOUT_MS + " ms; it was parked at " + (where.length > 0 ? where[0] : "an unknown frame"));
    }
    if (failure[0] != null)
      throw new AssertionError("the concurrent transaction must commit", failure[0]);
  }

  private static void assertConflictOnCommit(final Database db) {
    try {
      db.commit();
      throw new AssertionError("the commit must be refused: the record was rewritten by a concurrent transaction");
    } catch (final ConcurrentModificationException expected) {
      // THE PRECONDITION UNDER TEST: THE LOSER ABORTS AFTER ITS INDEX OPERATIONS HAVE ALREADY BEEN REPLAYED
    } finally {
      if (db.isTransactionActive())
        db.rollback();
    }
  }

  /**
   * The same corpus on an index whose data pages are small enough, and whose entries big enough (INT8 quantization
   * writes the codes onto the page), that a few hundred vectors span several of them.
   */
  private static void populateWithSmallPages(final Database db) {
    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) db.getSchema()
          .buildTypeIndex("Doc", new String[] { "vector" }).withLSMVectorType().withPageSize(SMALL_PAGE_SIZE);
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.INT8).create();
    });

    db.transaction(() -> {
      for (int i = 0; i < NUM_VECTORS; i++)
        db.newDocument("Doc").set("id", i).set("name", "doc" + i).set("vector", embedding(i)).save();
    });
  }

  private static void populate(final Database db) {
    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\" }");
    });

    db.transaction(() -> {
      for (int i = 0; i < NUM_VECTORS; i++)
        db.newDocument("Doc").set("id", i).set("name", "doc" + i).set("vector", embedding(i)).save();
    });
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x7931L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static float[] readVector(final Database db, final RID rid) {
    final float[][] holder = new float[1][];
    db.transaction(() -> holder[0] = (float[]) rid.asDocument(true).get("vector"));
    return holder[0];
  }

  private static RID ridOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      assertThat(rs.hasNext()).as("record %d must exist", id).isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
