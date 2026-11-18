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

package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the database engine: ConcurrentRecordWrite. Each nested class keeps the regression of one reported issue (see the
 * Javadoc of each).
 */
public class ConcurrentRecordWriteTest {
  /**
   * Issue #5279: concurrent transactions that each INSERT a brand-new record into the same bucket used to pick the very
   * same free slot of the very same page. They therefore received the SAME optimistic RID and, at commit, every
   * transaction but the first failed with a page-level {@link ConcurrentModificationException} even though the records
   * are logically unrelated - two inserts of different records always commute.
   * <p>
   * Slots of an existing page are now reserved per in-flight transaction, so concurrent inserts land on DIFFERENT slots
   * (different RIDs) and the disjoint-slot merge (#5381) replays them on top of each other without any conflict.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentInsert extends TestHelper {
    private boolean savedSlotMerge;

    @BeforeEach
    void saveConfig() {
      savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
    }

    /**
     * The exact scenario of the issue, minus the network: N transactions are opened, each creates ONE new record in a
     * single-bucket type whose page already exists (so the record is appended to a REUSED page, not to a brand-new
     * one), and only then are they all committed. Every transaction must get its own RID and every commit must
     * succeed: there is no logical conflict anywhere.
     */
    @Test
    void concurrentInsertsInTheSamePageGetDistinctRidsAndAllCommit() throws Exception {
      final int concurrentIntent = 10;

      database.transaction(() -> {
        database.getSchema().createDocumentType("SimpleVertexEx", 1).createProperty("svex", Type.STRING);
        // Seed the bucket so page 0 exists and is REUSED by all the transactions below.
        database.newDocument("SimpleVertexEx").set("svex", "seed").save();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final List<RID> assignedRids = new CopyOnWriteArrayList<>();

      // Phase 1: every transaction stages its own insert (this is where the slot is picked).
      final CyclicBarrier staged = new CyclicBarrier(concurrentIntent);
      // Phase 2: nobody commits until every transaction has staged its record.
      final CountDownLatch commitNow = new CountDownLatch(1);
      final AtomicInteger committed = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();

      for (int i = 0; i < concurrentIntent; i++) {
        final int n = i;
        final Thread thread = new Thread(() -> {
          try {
            database.begin();
            final MutableDocument doc = database.newDocument("SimpleVertexEx");
            doc.set("svex", "concurrent test" + n);
            doc.save();
            assignedRids.add(doc.getIdentity());

            staged.await();
            commitNow.await();

            // NO RETRY: two inserts of different records must never conflict.
            database.commit();
            committed.incrementAndGet();
          } catch (final Throwable e) {
            errors.add(e);
            if (database.isTransactionActive())
              database.rollback();
          }
        }, "insert-" + i);
        threads.add(thread);
        thread.start();
      }

      // Let everybody stage, then release all the commits at once.
      Thread.sleep(200);
      commitNow.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed.get()).isEqualTo(concurrentIntent);

      final Set<RID> distinctRids = new HashSet<>(assignedRids);
      assertThat(distinctRids).as("every concurrent insert must get its own RID").hasSize(concurrentIntent);

      database.transaction(() -> {
        assertThat(database.countType("SimpleVertexEx", false)).isEqualTo(concurrentIntent + 1L);
        for (final RID rid : assignedRids)
          assertThat(rid.asDocument(true).getString("svex")).startsWith("concurrent test");
      });
    }

    /**
     * Sustained multi-user insert load on a single bucket, with attempts=1 so nothing is hidden by a retry loop: every
     * insert must commit and the type count must be exact.
     */
    @Test
    void sustainedConcurrentInsertsOnASingleBucketNeverConflict() throws Exception {
      final int threadCount = 8;
      final int insertsPerThread = 250;

      database.transaction(() -> {
        database.getSchema().createDocumentType("Case", 1).createProperty("payload", Type.STRING);
        database.newDocument("Case").set("payload", "seed").save();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final AtomicInteger conflicts = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int id = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < insertsPerThread; i++) {
              final String payload = "t" + id + "-" + i;
              try {
                database.transaction(() -> database.newDocument("Case").set("payload", payload).save(), true, 1);
              } catch (final ConcurrentModificationException e) {
                conflicts.incrementAndGet();
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "case-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      // Before the fix this was ~1750 of 2000. The tolerance covers only the unavoidable residue: the transaction
      // that happens to be re-placing its record exactly when a page fills up can still lose the race and retry.
      assertThat(conflicts.get()).as("pure inserts must not raise concurrent modifications").isLessThanOrEqualTo(2);

      // ...and the count has to be told about that tolerance, because the insert runs at attempts=1: a conflict
      // tolerated above means that record was NOT written. Expecting the full total while allowing a conflict asserts
      // both that a conflict may happen and that it may not, so the run where one DOES happen fails here (2000 against
      // 2001) instead of at the tolerance meant to absorb it (issue #6281, same trap as the sibling Update test).
      database.transaction(() -> assertThat(database.countType("Case", false))//
          .isEqualTo(1L + (long) threadCount * insertsPerThread - conflicts.get()));
    }

    /**
     * The reporter's real workload: every user creates a small graph of its own (a case file made of several vertices
     * wired by edges) in ONE transaction, while everybody else does the same on the same single-bucket types. No two
     * transactions ever touch the same record, so with a couple of retries every transaction must go through and the
     * graph must be exactly what was written.
     */
    @Test
    void concurrentTransactionsCreatingWholeGraphsAllCommit() throws Exception {
      final int threadCount = 8;
      final int casesPerThread = 40;
      final int nodesPerCase = 6;

      database.transaction(() -> {
        database.getSchema().createVertexType("Node", 1).createProperty("name", Type.STRING);
        database.getSchema().createEdgeType("Link", 1);
        // Materialise page 0 of every involved bucket (vertices, edges and both edge-list segment buckets) so the
        // threads below exercise steady-state contention on REUSED pages.
        final MutableVertex a = database.newVertex("Node");
        a.set("name", "seed-a").save();
        final MutableVertex b = database.newVertex("Node");
        b.set("name", "seed-b").save();
        a.newEdge("Link", b).save();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final AtomicInteger conflicts = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int id = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int c = 0; c < casesPerThread; c++) {
              final String caseId = "t" + id + "-c" + c;
              try {
                database.transaction(() -> {
                  final MutableVertex[] nodes = new MutableVertex[nodesPerCase];
                  for (int n = 0; n < nodesPerCase; n++) {
                    nodes[n] = database.newVertex("Node");
                    nodes[n].set("name", caseId + "-n" + n);
                    nodes[n].save();
                  }
                  for (int n = 1; n < nodesPerCase; n++)
                    nodes[0].newEdge("Link", nodes[n]).save();
                }, true, 1);
              } catch (final ConcurrentModificationException e) {
                conflicts.incrementAndGet();
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "graph-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      final int cases = threadCount * casesPerThread - conflicts.get();
      database.transaction(() -> {
        assertThat(database.countType("Node", false)).isEqualTo(2L + (long) cases * nodesPerCase);
        assertThat(database.countType("Link", false)).isEqualTo(1L + (long) cases * (nodesPerCase - 1));
      });

      // Before the fix this was ~270 of 320: every case creation collided on the shared vertex, edge and
      // edge-list-segment pages. Same page-fill residue as above.
      assertThat(conflicts.get()).as("transactions building independent sub-graphs must not conflict")
          .isLessThanOrEqualTo(3);
    }

    /**
     * A slot reserved by a transaction that then ROLLS BACK must go back to the bucket: the very next insert has to
     * land on it, exactly as before the reservation existed. A leak here would burn one slot (and its space) per
     * rolled-back insert for the whole life of the page.
     */
    @Test
    void aRolledBackInsertGivesItsSlotBack() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Rolled", 1).createProperty("v", Type.STRING);
        database.newDocument("Rolled").set("v", "seed").save();
      });

      database.begin();
      final RID abandoned = database.newDocument("Rolled").set("v", "gone").save().getIdentity();
      database.rollback();

      database.begin();
      final RID reused = database.newDocument("Rolled").set("v", "kept").save().getIdentity();
      database.commit();

      assertThat(reused).as("the slot of a rolled-back insert must be handed out again").isEqualTo(abandoned);
      database.transaction(() -> assertThat(reused.asDocument(true).getString("v")).isEqualTo("kept"));
    }

    /**
     * The slot a delete frees must still be recycled by the next insert - the reservation must not make a page grow
     * for ever - and a transaction that deletes and re-inserts in one go has to recycle its own slots too.
     */
    @Test
    void deletedSlotsAreRecycledByTheNextInsert() {
      final RID[] rids = new RID[8];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Recycled", 1).createProperty("v", Type.STRING);
        for (int i = 0; i < rids.length; i++)
          rids[i] = database.newDocument("Recycled").set("v", "first-" + i).save().getIdentity();
      });

      // Separate transactions: the hole is committed before it gets reused.
      database.transaction(() -> rids[3].asDocument(true).delete());
      database.transaction(() -> assertThat(database.newDocument("Recycled").set("v", "second").save().getIdentity())//
          .isEqualTo(rids[3]));

      // A slot freed in the CURRENT transaction is deliberately never re-used before that transaction commits
      // (LocalBucket.getFreeSpaceInPage skips it), and the reservation must not change that either.
      database.transaction(() -> {
        rids[5].asDocument(true).delete();
        assertThat(database.newDocument("Recycled").set("v", "third").save().getIdentity()).isNotEqualTo(rids[5]);
      });

      database.transaction(() -> assertThat(database.countType("Recycled", false)).isEqualTo(rids.length));
    }

    /**
     * Now that two concurrent inserts into one page take DIFFERENT slots, a transaction that already modified that
     * page can no longer see, through its own older image of it, a record a concurrent transaction committed there.
     * The unique-index check must not read that as a dangling index entry: "repairing" it would delete a healthy
     * entry and let a duplicate key through.
     */
    @Test
    void aUniqueKeyCommittedIntoAPageThisTransactionAlreadyModifiedStillConflicts() throws Exception {
      database.transaction(() -> {
        database.getSchema().createDocumentType("UniqueDoc", 1).createProperty("id", Type.INTEGER);
        database.getSchema().getType("UniqueDoc").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        database.newDocument("UniqueDoc").set("id", 0).save();
      });

      // Our transaction first inserts an unrelated record: from now on its image of that bucket page is its own.
      database.begin();
      database.newDocument("UniqueDoc").set("id", 1).save();

      // Somebody else takes key 2 and commits it into the very same page.
      final Thread other = new Thread(() -> database.transaction(() -> database.newDocument("UniqueDoc").set("id", 2).save()));
      other.start();
      other.join();

      // Claiming the same key must fail: the key is taken, however invisible the winner is to us.
      try {
        database.newDocument("UniqueDoc").set("id", 2).save();
        database.commit();
        throw new AssertionError("Expected a DuplicatedKeyException on a key committed by a concurrent transaction");
      } catch (final DuplicatedKeyException expected) {
        // correct
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      database.transaction(() -> {
        assertThat(database.countType("UniqueDoc", false)).isEqualTo(2L);
        try (final ResultSet rs = database.query("SQL", "SELECT count(*) AS c FROM UniqueDoc WHERE id = 2")) {
          assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
        }
      });
    }
  }

  /**
   * Issue #5279 (2nd round): after concurrent INSERTs stopped fighting for the same slot, the reporter hit the same
   * page-level {@link ConcurrentModificationException} on concurrent UPDATEs of DIFFERENT records that merely share a
   * bucket page. The disjoint-slot merge (#5381) already replayed a same-or-smaller in-place overwrite, but an update
   * that makes the record GROW re-flowed the page content and therefore poisoned the page - and a grown record is the
   * normal case (setting a longer string, adding a property).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentUpdate extends BucketPageLayoutTestSupport {
    private boolean savedSlotMerge;

    @BeforeEach
    void saveConfig() {
      savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
    }

    /**
     * The reporter's testPageConcurrentModificationException2(), minus the network: N records are created and committed
     * in one single-bucket type (so they all live on page 0), then N transactions each update a DIFFERENT one of them to
     * a LONGER value and only then everybody commits. No two transactions touch the same record: every commit must go
     * through with no retry at all.
     */
    @Test
    void concurrentUpdatesOfDifferentRecordsInTheSamePageAllCommit() throws Exception {
      final int concurrentIntent = 10;
      final List<RID> rids = new ArrayList<>();

      database.transaction(() -> {
        database.getSchema().createDocumentType("SimpleVertexEx", 1).createProperty("svex", Type.STRING);
        for (int i = 0; i < concurrentIntent; i++)
          rids.add(database.newDocument("SimpleVertexEx").set("svex", "concurrent test" + i).save().getIdentity());
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(concurrentIntent);
      final CountDownLatch commitNow = new CountDownLatch(1);
      final AtomicInteger committed = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();

      for (int i = 0; i < concurrentIntent; i++) {
        final int n = i;
        final Thread thread = new Thread(() -> {
          try {
            database.begin();
            // A LONGER value than the committed one: the record grows in place.
            rids.get(n).asDocument(true).modify().set("svex", "concurrent modification " + n).save();

            staged.await();
            commitNow.await();

            // NO RETRY: updates of different records must never conflict.
            database.commit();
            committed.incrementAndGet();
          } catch (final Throwable e) {
            errors.add(e);
            if (database.isTransactionActive())
              database.rollback();
          }
        }, "update-" + i);
        threads.add(thread);
        thread.start();
      }

      Thread.sleep(200);
      commitNow.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed.get()).isEqualTo(concurrentIntent);

      database.transaction(() -> {
        assertThat(database.countType("SimpleVertexEx", false)).isEqualTo(concurrentIntent);
        for (int i = 0; i < concurrentIntent; i++)
          assertThat(rids.get(i).asDocument(true).getString("svex")).isEqualTo("concurrent modification " + i);
      });
    }

    /**
     * The same shape where every update SHRINKS the record: already covered by the plain in-place branch of the
     * disjoint-slot merge, kept as the regression guard of that branch.
     */
    @Test
    void concurrentShrinkingUpdatesOfDifferentRecordsInTheSamePageAllCommit() throws Exception {
      final int concurrentIntent = 10;
      final List<RID> rids = new ArrayList<>();

      database.transaction(() -> {
        database.getSchema().createDocumentType("Shrinking", 1).createProperty("v", Type.STRING);
        for (int i = 0; i < concurrentIntent; i++)
          rids.add(database.newDocument("Shrinking").set("v", "a rather long initial value " + i).save().getIdentity());
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(concurrentIntent);
      final CountDownLatch commitNow = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int i = 0; i < concurrentIntent; i++) {
        final int n = i;
        final Thread thread = new Thread(() -> {
          try {
            database.begin();
            rids.get(n).asDocument(true).modify().set("v", "s" + n).save();
            staged.await();
            commitNow.await();
            database.commit();
          } catch (final Throwable e) {
            errors.add(e);
            if (database.isTransactionActive())
              database.rollback();
          }
        }, "shrink-" + i);
        threads.add(thread);
        thread.start();
      }

      Thread.sleep(200);
      commitNow.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      database.transaction(() -> {
        for (int i = 0; i < concurrentIntent; i++)
          assertThat(rids.get(i).asDocument(true).getString("v")).isEqualTo("s" + i);
      });
    }

    /**
     * Sustained multi-user update load on a single bucket with attempts=1: every thread owns its own records, so nothing
     * may conflict and every record must end up with its last written value.
     */
    @Test
    void sustainedConcurrentUpdatesOnASingleBucketNeverConflict() throws Exception {
      final int threadCount = 8;
      final int recordsPerThread = 12;
      final int roundsPerThread = 30;

      final RID[][] owned = new RID[threadCount][recordsPerThread];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Case", 1).createProperty("payload", Type.STRING);
        for (int t = 0; t < threadCount; t++)
          for (int r = 0; r < recordsPerThread; r++)
            owned[t][r] = database.newDocument("Case").set("payload", "t" + t + "-r" + r).save().getIdentity();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final AtomicInteger conflicts = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int id = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int round = 0; round < roundsPerThread; round++) {
              final int r = round;
              for (int i = 0; i < recordsPerThread; i++) {
                final RID rid = owned[id][i];
                // Alternate growing and shrinking values so both update shapes are exercised.
                final String payload = r % 2 == 0 ? "t" + id + "-r" + i + "-round" + r + "-padded-to-grow" : "t" + id + "-" + r;
                try {
                  database.transaction(() -> rid.asDocument(true).modify().set("payload", payload).save(), true, 1);
                } catch (final ConcurrentModificationException e) {
                  conflicts.incrementAndGet();
                }
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "case-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(conflicts.get()).as("updates of records owned by different threads must not conflict").isZero();

      database.transaction(() -> {
        assertThat(database.countType("Case", false)).isEqualTo((long) threadCount * recordsPerThread);
        for (int t = 0; t < threadCount; t++)
          for (int i = 0; i < recordsPerThread; i++)
            assertThat(owned[t][i].asDocument(true).getString("payload")).isEqualTo("t" + t + "-" + (roundsPerThread - 1));
      });
    }

    /**
     * The reporter's production shape: every user creates its own two vertices, commits, and then in a SECOND
     * transaction wires them with an edge - while everybody else does the same on the same single-bucket types. The edge
     * transaction writes to four shared pages at once (both vertex records, the edge record, the edge-list segment), so
     * it is the sharpest guard that the update path still commutes with the insert reservation (#5279) and the
     * edge-append merge. It already passed before growth became rebasable - a vertex record does not grow when its
     * edge-list head pointer is set, because the two pointers are always serialized (as -1 when absent) - so this is
     * here to keep it that way.
     */
    @Test
    void concurrentEdgeCreationBetweenOwnVerticesNeverConflicts() throws Exception {
      final int threadCount = 8;
      final int edgesPerThread = 20;

      database.transaction(() -> {
        database.getSchema().createVertexType("Node", 1).createProperty("name", Type.STRING);
        database.getSchema().createEdgeType("Link", 1);
        final MutableVertex seedA = database.newVertex("Node");
        seedA.set("name", "seed-a").save();
        final MutableVertex seedB = database.newVertex("Node");
        seedB.set("name", "seed-b").save();
        seedA.newEdge("Link", seedB).save();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final AtomicInteger conflicts = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int id = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int n = 0; n < edgesPerThread; n++) {
              final String pairId = "t" + id + "-e" + n;
              final RID[] pair = new RID[2];

              // 1st transaction: the two brand-new vertices, with no edge yet. Retries allowed - only the edge
              // transaction below is the one under test.
              database.transaction(() -> {
                pair[0] = database.newVertex("Node").set("name", pairId + "-a").save().getIdentity();
                pair[1] = database.newVertex("Node").set("name", pairId + "-b").save().getIdentity();
              });

              // 2nd transaction: the edge, which makes BOTH vertex records grow.
              try {
                database.transaction(() -> pair[0].asVertex(true).newEdge("Link", pair[1]).save(), true, 1);
              } catch (final ConcurrentModificationException ex) {
                conflicts.incrementAndGet();
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "edge-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      // The tolerance covers only the unavoidable residue: a record whose page has just filled up has to spill into a
      // placeholder, which no merge can replay.
      assertThat(conflicts.get()).as("connecting two vertices of one's own must not conflict").isLessThanOrEqualTo(1);

      database.transaction(() -> {
        // Every vertex transaction retries until it commits, so all of them are there...
        assertThat(database.countType("Node", false)).isEqualTo(2L + 2L * threadCount * edgesPerThread);
        // ...but the edge transaction runs at attempts=1, so a conflict tolerated above means that edge was NOT
        // created. Counting the conflicts out is what makes the two assertions agree: expecting the full total while
        // allowing one conflict asserts both that a conflict may happen and that it may not, and the run where one
        // DOES happen then fails here (160 against 161) instead of at the tolerance meant to absorb it.
        assertThat(database.countType("Link", false)).isEqualTo(1L + (long) threadCount * edgesPerThread - conflicts.get());
      });
    }

    /**
     * The guarantee #5279 actually makes, at the shape that stresses it hardest: every round makes every record LONGER
     * while all of them still fit their single shared page, so every write goes through the in-page growth branch and
     * the disjoint-slot merge has to absorb all of it. Records are owned per thread, so with attempts=1 the conflict
     * count must be exactly zero - a single conflict here means a growth stopped being rebasable.
     * <p>
     * The layout assertion afterwards is what keeps the zero from being accidental: if the payloads had outgrown the
     * page, the records would have spilled into placeholders or chunk chains, whose updates are NOT rebasable, and the
     * zero would then be luck rather than the merge doing its job.
     */
    @Test
    void growingUpdatesThatStayInsideTheirPageNeverConflict() throws Exception {
      final int threadCount = 8;
      final int recordsPerThread = 5;
      final int rounds = 8;

      final RID[][] owned = new RID[threadCount][recordsPerThread];
      database.transaction(() -> {
        database.getSchema().createDocumentType("InPageGrowing", 1).createProperty("payload", Type.STRING);
        for (int t = 0; t < threadCount; t++)
          for (int r = 0; r < recordsPerThread; r++)
            owned[t][r] = database.newDocument("InPageGrowing").set("payload", "t" + t + "-r" + r).save().getIdentity();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final AtomicInteger conflicts = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int id = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int round = 0; round < rounds; round++)
              for (int i = 0; i < recordsPerThread; i++) {
                final RID rid = owned[id][i];
                final String payload = inPagePayloadOf(id, i, round);
                try {
                  database.transaction(() -> rid.asDocument(true).modify().set("payload", payload).save(), true, 1);
                } catch (final ConcurrentModificationException e) {
                  conflicts.incrementAndGet();
                }
              }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "in-page-growing-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(conflicts.get()).as("growing a record inside its own page must commute with writes to other slots").isZero();

      final Map<String, Object> layout = bucketStats("InPageGrowing");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the records must never have left their page").isZero();
      assertThat((Long) layout.get("totalMultiPageRecords")).isZero();

      database.transaction(() -> {
        for (int t = 0; t < threadCount; t++)
          for (int i = 0; i < recordsPerThread; i++)
            assertThat(owned[t][i].asDocument(true).getString("payload")).isEqualTo(inPagePayloadOf(t, i, rounds - 1));
      });
    }

    /**
     * The other side of the same run: the records keep growing until they no longer fit their page and spill into chunk
     * chains. This used to be where the merge gave up - the spill and every later update of a chunked record poisoned
     * the shared page, so on a single-bucket type the eight threads serialized behind it, only one of them won each
     * round, and a transaction that lost the race retried and spilled again instead of converging (#6127 item 3, the
     * re-triage of what used to look like a flake). It needed an explicit budget of 50 attempts. Since #6129 the head
     * chunk of a multi-page record - including the update that creates it - is a tracked slot like any other, so the
     * {@code TX_RETRIES} DEFAULT is enough: that is what the absence of an {@code attempts} argument below asserts.
     * What the test still pins down is that the final content of every record is exactly what was last written and that
     * the database stays structurally sound.
     */
    @Test
    void growingUpdatesUnderContentionKeepTheDatabaseConsistent() throws Exception {
      final int threadCount = 8;
      final int recordsPerThread = 5;
      final int rounds = 8;

      final RID[][] owned = new RID[threadCount][recordsPerThread];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Growing", 1).createProperty("payload", Type.STRING);
        for (int t = 0; t < threadCount; t++)
          for (int r = 0; r < recordsPerThread; r++)
            owned[t][r] = database.newDocument("Growing").set("payload", "t" + t + "-r" + r).save().getIdentity();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int id = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int round = 0; round < rounds; round++)
              for (int i = 0; i < recordsPerThread; i++) {
                final RID rid = owned[id][i];
                final String payload = payloadOf(id, i, round);
                // What matters is that a retry converges and never corrupts the page. The value depends only on
                // (thread, record, round), so should a retry be needed it rewrites exactly the same content.
                database.transaction(() -> rid.asDocument(true).modify().set("payload", payload).save());
              }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "growing-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      // The premise of the test: the records really did outgrow their page. Without this the run above would be the
      // in-page case again, and the retry budget would be excusing a contention regime that never happened.
      final Map<String, Object> layout = bucketStats("Growing");
      assertThat((Long) layout.get("totalPlaceholderRecords") + (Long) layout.get("totalMultiPageRecords")).as(
          "the payloads must have outgrown their page: " + layout).isPositive();

      database.transaction(() -> {
        assertThat(database.countType("Growing", false)).isEqualTo((long) threadCount * recordsPerThread);
        for (int t = 0; t < threadCount; t++)
          for (int i = 0; i < recordsPerThread; i++)
            assertThat(owned[t][i].asDocument(true).getString("payload")).isEqualTo(payloadOf(t, i, rounds - 1));
      });

      checkDatabase();
    }

    /**
     * Round after round the payload gets longer, so every update is a growth and the records end up outgrowing their
     * page. Deterministic in (thread, record, round) so a retried transaction rewrites the very same value.
     */
    private static String payloadOf(final int threadId, final int record, final int round) {
      return "t" + threadId + "-r" + record + "-round" + round + "-" + "x".repeat(400 * (round + 1));
    }

    /**
     * The same growing shape kept small enough that all 40 records still share one 64 KB page at the last round, so
     * every update stays on the in-page growth branch.
     */
    private static String inPagePayloadOf(final int threadId, final int record, final int round) {
      return "t" + threadId + "-r" + record + "-round" + round + "-" + "x".repeat(100 * (round + 1));
    }

    /**
     * The one update shape that reaches the growth branch WITHOUT being a plain record: a slot holding a placeholder
     * POINTER whose content record cannot absorb the new value either, so the content is deleted and the slot is
     * rebuilt from scratch. The "pre-image" of that slot is the 8-byte pointer, not record content, and the content
     * record lives on another page - so the page must be poisoned and the write must never be replayed from it. Here
     * the page is made to conflict for real, so the transaction has to go through the fallback and still land the
     * exact value, next to an untouched co-located record.
     */
    @Test
    void aPlaceholderPointerRebuiltUnderConflictFallsBackAndStaysCorrect() throws Exception {
      final String big = "b".repeat(30 * 1024);
      final String huge = "h".repeat(70 * 1024);

      // Same LENGTH as the value the neighbour starts from: page 0 is sealed below, so the concurrent write must be an
      // in-place overwrite - a growth would have to spill out of the page itself and stop being a plain neighbour.
      final String neighbourRewritten = "N".repeat(19);

      final RID[] placeholder = new RID[1];
      final RID[] neighbour = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Holder", 1).createProperty("v", Type.STRING);
        // Both tiny and both on page 0; the placeholder one is written FIRST so it is never the last record of the
        // page (a last record would be grown into the free tail instead of being turned into a placeholder).
        placeholder[0] = database.newDocument("Holder").set("v", "p").save().getIdentity();
        neighbour[0] = database.newDocument("Holder").set("v", "n".repeat(19)).save().getIdentity();
      });
      // Since #6149 a page with a free tail lends the spilling record the few bytes a chunk header needs, so a
      // placeholder is only produced on a page with NO free tail left at all: seal page 0 to get one.
      sealFirstPage("Holder");

      // Page 0 can no longer host 30 KB and the record's own 9 bytes cannot hold a chunk header: it becomes a
      // placeholder POINTER to a content record on another page.
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", big).save());
      final Map<String, Object> layout = bucketStats("Holder");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the slot must hold a placeholder pointer, not chunks")
          .isEqualTo(1L);
      // The only chunked record so far is the one that sealed page 0: the 30 KB value went into a placeholder CONTENT
      // record, which is a plain record on a page of its own.
      assertThat((Long) layout.get("totalMultiPageRecords")).isEqualTo(1L);
      assertThat((Long) layout.get("totalSurrogateRecords")).isEqualTo(1L);

      // Our transaction rebuilds that placeholder: 70 KB does not fit ANY page, so the content record cannot grow
      // either and the whole slot is rebuilt (old content deleted, new chunked placeholder created).
      database.begin();
      placeholder[0].asDocument(true).modify().set("v", huge).save();

      // Somebody else commits a change to the co-located record, bumping page 0's version.
      final Thread other = new Thread(
          () -> database.transaction(() -> neighbour[0].asDocument(true).modify().set("v", neighbourRewritten).save()));
      other.start();
      other.join();

      try {
        database.commit();
        throw new AssertionError("Expected the rebuilt placeholder to fall back to a retry on a conflicting page");
      } catch (final ConcurrentModificationException expected) {
        // correct: the page was poisoned, nothing was replayed from it
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      // The retry must land the exact value, and the concurrent write must be intact.
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", huge).save());

      database.transaction(() -> {
        assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo(huge);
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo(neighbourRewritten);
      });

      // The content record was really REBUILT (the old one deleted and a chunked one created), which is what proves
      // the update went through the placeholder-pointer fall-through and not through an in-place content update.
      final Map<String, Object> rebuilt = bucketStats("Holder");
      assertThat((Long) rebuilt.get("totalPlaceholderRecords")).isEqualTo(1L);
      // Still one content record, and still only one multi-page record - the one that sealed page 0. Since #6196 a
      // content record spilled into a chain of its own is counted as the SURROGATE it is, not as a record.
      assertThat((Long) rebuilt.get("totalSurrogateRecords")).isEqualTo(1L);
      assertThat((Long) rebuilt.get("totalMultiPageRecords")).isEqualTo(1L);
      // What proves the rebuild: the new content record fits no page, so it brought continuation chunks the plain
      // 30 KB one it replaced did not have.
      assertThat((Long) rebuilt.get("totalChunks")).as("the rebuilt content record must be a chain: " + rebuilt)
          .isGreaterThan((Long) layout.get("totalChunks"));

      checkDatabase();
    }

    /**
     * The opposite guarantee: two transactions updating the SAME record must still conflict - that is the signal the
     * application needs to reload and decide. The merge must never silently drop one of the two writes.
     */
    @Test
    void concurrentUpdatesOfTheSameRecordStillConflict() throws Exception {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Contended", 1).createProperty("v", Type.STRING);
        // A second record on the same page, so the page is shared but only one record is contended.
        database.newDocument("Contended").set("v", "other").save();
        rid[0] = database.newDocument("Contended").set("v", "initial").save().getIdentity();
      });

      database.begin();
      rid[0].asDocument(true).modify().set("v", "mine, and definitely longer").save();

      final List<Throwable> otherErrors = new CopyOnWriteArrayList<>();
      final Thread other = new Thread(() -> {
        try {
          database.transaction(() -> rid[0].asDocument(true).modify().set("v", "theirs, also longer").save(), true, 1);
        } catch (final Throwable e) {
          otherErrors.add(e);
        }
      });
      other.start();
      other.join();
      assertThat(otherErrors).isEmpty();

      try {
        database.commit();
        throw new AssertionError("Expected a ConcurrentModificationException on the very same record");
      } catch (final ConcurrentModificationException expected) {
        // correct: a true conflict
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      // The winner's value must be intact.
      database.transaction(() -> assertThat(rid[0].asDocument(true).getString("v")).isEqualTo("theirs, also longer"));
    }
  }

  /**
   * Issue #5569 (follow-up to #5279 / #5381): a plain single-record DELETE was the last common false-conflict class on a
   * bucket page. Deleting record A poisoned the whole page, so every concurrent transaction that merely updated record B
   * on that page failed with a {@link ConcurrentModificationException} even though the two writes commute: a plain
   * in-place record is deleted by zeroing its slot-table entry (plus the optional content wipe-out), a single-slot
   * change exactly like the inserts and in-page updates the disjoint-slot merge already replays.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConcurrentDelete extends TestHelper {
    private boolean savedSlotMerge;

    @BeforeEach
    void saveConfig() {
      savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
    }

    /**
     * The core of the issue: N transactions delete their own victim while N others update their own survivor, all of
     * them on the SAME page, and everybody commits at the same moment with attempts=1. Nothing here is a real conflict,
     * so no transaction may fail: every victim must be gone and every survivor must hold its exact new value.
     */
    @Test
    void concurrentDeleteAndUpdateOnTheSamePageNeverConflict() throws Exception {
      final int pairs = 8;
      final RID[] victim = new RID[pairs];
      final RID[] survivor = new RID[pairs];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Mix", 1).createProperty("tag", Type.STRING);
        for (int i = 0; i < pairs; i++) {
          victim[i] = database.newDocument("Mix").set("role", "victim").set("tag", "v" + i).save().getIdentity();
          survivor[i] = database.newDocument("Mix").set("role", "survivor").set("tag", "s" + i).save().getIdentity();
        }
      });
      assertThat(allOnTheSamePage("Mix", victim, survivor)).as("the fixture must live on one page").isTrue();

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(pairs * 2);
      final CountDownLatch commitNow = new CountDownLatch(1);
      final AtomicInteger committed = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();

      for (int i = 0; i < pairs; i++) {
        final int n = i;

        threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
            () -> victim[n].asDocument(true).delete()), "deleter-" + n));

        threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
            () -> survivor[n].asDocument(true).modify().set("tag", "updated-" + n).save()), "updater-" + n));
      }

      for (final Thread thread : threads)
        thread.start();
      Thread.sleep(200);
      commitNow.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed.get()).isEqualTo(pairs * 2);
      // The merge is what made it possible: without it every commit but the first would have thrown.
      assertThat(((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges)
          .as("the disjoint-slot merge must fire").isGreaterThan(0);

      database.transaction(() -> {
        for (int i = 0; i < pairs; i++) {
          assertThat(survivor[i].asDocument(true).getString("tag")).isEqualTo("updated-" + i);
          assertThat(recordExists(victim[i])).as("victim " + i + " must be deleted").isFalse();
        }
        assertThat(database.countType("Mix", false)).isEqualTo(pairs);
      });
    }

    /**
     * Concurrent deletes of DIFFERENT records of one page: they commute with each other just as they do with an update,
     * so with attempts=1 every one of them must commit.
     */
    @Test
    void concurrentDeletesOfDifferentRecordsInTheSamePageNeverConflict() throws Exception {
      final int records = 10;
      final RID[] rids = new RID[records];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Victims", 1).createProperty("tag", Type.STRING);
        for (int i = 0; i < records; i++)
          rids[i] = database.newDocument("Victims").set("tag", "t" + i).save().getIdentity();
      });
      assertThat(allOnTheSamePage("Victims", rids)).as("the fixture must live on one page").isTrue();

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(records);
      final CountDownLatch commitNow = new CountDownLatch(1);
      final AtomicInteger committed = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();

      for (int i = 0; i < records; i++) {
        final int n = i;
        threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
            () -> rids[n].asDocument(true).delete()), "deleter-" + n));
      }

      for (final Thread thread : threads)
        thread.start();
      Thread.sleep(200);
      commitNow.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed.get()).isEqualTo(records);

      database.transaction(() -> {
        assertThat(database.countType("Victims", false)).isZero();
        for (int i = 0; i < records; i++)
          assertThat(recordExists(rids[i])).as("record " + i + " must be deleted").isFalse();
      });
    }

    /**
     * The other side of the contract: deleting a record another transaction is updating is a TRUE conflict, and the
     * byte-for-byte pre-image check must keep raising it. Whichever of the two commits first wins, the other one gets a
     * {@link ConcurrentModificationException} and the surviving state is exactly the winner's.
     */
    @Test
    void deletingARecordAnotherTransactionIsUpdatingStillConflicts() throws Exception {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Duel", 1).createProperty("tag", Type.STRING);
        rid[0] = database.newDocument("Duel").set("tag", "initial").save().getIdentity();
        // A co-located record nobody touches, so the page is shared exactly as in the mergeable cases above.
        database.newDocument("Duel").set("tag", "bystander").save();
      });

      final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(2);
      final CountDownLatch commitNow = new CountDownLatch(1);
      final AtomicInteger conflicts = new AtomicInteger();
      final boolean[] deleteWon = new boolean[1];
      final boolean[] updateWon = new boolean[1];

      final Thread deleter = new Thread(() -> {
        try {
          database.begin();
          rid[0].asDocument(true).delete();
          staged.await();
          commitNow.await();
          database.commit();
          deleteWon[0] = true;
        } catch (final ConcurrentModificationException e) {
          conflicts.incrementAndGet();
        } catch (final Throwable e) {
          unexpected.add(e);
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      }, "deleter");

      final Thread updater = new Thread(() -> {
        try {
          database.begin();
          rid[0].asDocument(true).modify().set("tag", "updated").save();
          staged.await();
          commitNow.await();
          database.commit();
          updateWon[0] = true;
        } catch (final ConcurrentModificationException e) {
          conflicts.incrementAndGet();
        } catch (final Throwable e) {
          unexpected.add(e);
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      }, "updater");

      deleter.start();
      updater.start();
      Thread.sleep(200);
      commitNow.countDown();
      deleter.join();
      updater.join();

      if (!unexpected.isEmpty())
        throw new AssertionError("unexpected failure: " + unexpected.getFirst(), unexpected.getFirst());

      assertThat(conflicts.get()).as("two writes to the SAME record must conflict").isEqualTo(1);
      assertThat(deleteWon[0] ^ updateWon[0]).as("exactly one of the two must win").isTrue();

      database.transaction(() -> {
        if (deleteWon[0])
          assertThat(recordExists(rid[0])).as("the delete won: the record must be gone").isFalse();
        else
          assertThat(rid[0].asDocument(true).getString("tag")).isEqualTo("updated");
      });
    }

    /**
     * A record CREATED and deleted within the same transaction never existed on the committed page, so its net effect
     * there is nothing: the replay must skip the slot altogether instead of looking for a pre-image that cannot be
     * there - which would turn a perfectly mergeable transaction into a conflict.
     */
    @Test
    void insertAndDeleteOfTheSameRecordInOneTransactionStillMerges() throws Exception {
      final RID[] survivor = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Ephemeral", 1).createProperty("tag", Type.STRING);
        survivor[0] = database.newDocument("Ephemeral").set("tag", "initial").save().getIdentity();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(2);
      // The churner must commit LAST, so it is always the one forced to rebase its page: if it committed first there
      // would be no conflict to resolve and the test would prove nothing.
      final CountDownLatch updaterCommitted = new CountDownLatch(1);
      final AtomicInteger committed = new AtomicInteger();
      final RID[] ephemeral = new RID[1];

      final Thread churner = new Thread(() -> {
        try {
          database.begin();
          final MutableDocument doc = database.newDocument("Ephemeral").set("tag", "ephemeral");
          doc.save();
          ephemeral[0] = doc.getIdentity();
          doc.delete();
          staged.await();
          updaterCommitted.await();
          database.commit();
          committed.incrementAndGet();
        } catch (final Throwable e) {
          errors.add(e);
          if (database.isTransactionActive())
            database.rollback();
        }
      }, "churner");

      final Thread updater = new Thread(() -> {
        try {
          database.begin();
          survivor[0].asDocument(true).modify().set("tag", "updated").save();
          staged.await();
          database.commit();
          committed.incrementAndGet();
        } catch (final Throwable e) {
          errors.add(e);
          if (database.isTransactionActive())
            database.rollback();
        } finally {
          updaterCommitted.countDown();
        }
      }, "updater");

      churner.start();
      updater.start();
      churner.join();
      updater.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed.get()).isEqualTo(2);
      // The created-then-deleted record must have landed on the very page the updater was writing to, otherwise the
      // test would not exercise the replay at all.
      assertThat(pageOf("Ephemeral", ephemeral[0])).isEqualTo(pageOf("Ephemeral", survivor[0]));

      database.transaction(() -> {
        assertThat(survivor[0].asDocument(true).getString("tag")).isEqualTo("updated");
        assertThat(recordExists(ephemeral[0])).as("the ephemeral record must not exist").isFalse();
        assertThat(database.countType("Ephemeral", false)).isEqualTo(1);
      });
    }

    /**
     * One transaction that inserts, updates AND deletes on the same page, replayed as a whole on top of a newer
     * committed version of it. The three kinds are replayed in the (unspecified) order of the tracked slot map, so this
     * is the guard that they do not depend on each other: the insert must land after the last live record whatever the
     * delete freed, and the delete must still find its own pre-image whatever the update shifted.
     */
    @Test
    void insertUpdateAndDeleteOfOneTransactionAreAllReplayedTogether() throws Exception {
      final int victims = 4;
      final RID[] victim = new RID[victims];
      final RID[] grower = new RID[victims];
      final RID[] outsider = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("AllKinds", 1).createProperty("tag", Type.STRING);
        outsider[0] = database.newDocument("AllKinds").set("tag", "outsider").save().getIdentity();
        for (int i = 0; i < victims; i++) {
          victim[i] = database.newDocument("AllKinds").set("tag", "victim-" + i).save().getIdentity();
          grower[i] = database.newDocument("AllKinds").set("tag", "grower-" + i).save().getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(2);
      // The mixed transaction commits LAST, so it is always the one forced to rebase all of its slot writes at once.
      final CountDownLatch outsiderCommitted = new CountDownLatch(1);
      final AtomicInteger committed = new AtomicInteger();
      final RID[] inserted = new RID[victims];

      final Thread mixed = new Thread(() -> {
        try {
          database.begin();
          for (int i = 0; i < victims; i++) {
            inserted[i] = database.newDocument("AllKinds").set("tag", "inserted-" + i).save().getIdentity();
            grower[i].asDocument(true).modify().set("tag", "grower-" + i + "-now-a-much-longer-value").save();
            victim[i].asDocument(true).delete();
          }
          staged.await();
          outsiderCommitted.await();
          database.commit();
          committed.incrementAndGet();
        } catch (final Throwable e) {
          errors.add(e);
          if (database.isTransactionActive())
            database.rollback();
        }
      }, "mixed");

      final Thread other = new Thread(() -> {
        try {
          database.begin();
          outsider[0].asDocument(true).modify().set("tag", "outsider-updated").save();
          staged.await();
          database.commit();
          committed.incrementAndGet();
        } catch (final Throwable e) {
          errors.add(e);
          if (database.isTransactionActive())
            database.rollback();
        } finally {
          outsiderCommitted.countDown();
        }
      }, "outsider");

      mixed.start();
      other.start();
      mixed.join();
      other.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed.get()).isEqualTo(2);

      database.transaction(() -> {
        assertThat(outsider[0].asDocument(true).getString("tag")).isEqualTo("outsider-updated");
        for (int i = 0; i < victims; i++) {
          assertThat(recordExists(victim[i])).as("victim " + i + " must be deleted").isFalse();
          assertThat(grower[i].asDocument(true).getString("tag")).isEqualTo("grower-" + i + "-now-a-much-longer-value");
          assertThat(inserted[i].asDocument(true).getString("tag")).isEqualTo("inserted-" + i);
        }
        assertThat(database.countType("AllKinds", false)).isEqualTo(1 + 2L * victims);
      });

      assertCheckDatabaseIsClean();
    }

    /**
     * A merged delete must remove the record from the indexes exactly like a non-merged one: the index changes are
     * transaction-level and do not depend on which page image is committed, and this is the regression guard for it.
     */
    @Test
    void aMergedDeleteStillRemovesTheIndexEntries() throws Exception {
      final int pairs = 6;
      final RID[] victim = new RID[pairs];
      final RID[] survivor = new RID[pairs];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Indexed", 1).createProperty("k", Type.STRING);
        database.getSchema().getType("Indexed").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
        for (int i = 0; i < pairs; i++) {
          victim[i] = database.newDocument("Indexed").set("k", "victim-" + i).save().getIdentity();
          survivor[i] = database.newDocument("Indexed").set("k", "survivor-" + i).save().getIdentity();
        }
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CyclicBarrier staged = new CyclicBarrier(pairs * 2);
      final CountDownLatch commitNow = new CountDownLatch(1);
      final AtomicInteger committed = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();

      for (int i = 0; i < pairs; i++) {
        final int n = i;
        threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
            () -> victim[n].asDocument(true).delete()), "deleter-" + n));
        threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
            () -> survivor[n].asDocument(true).modify().set("payload", "p" + n).save()), "updater-" + n));
      }

      for (final Thread thread : threads)
        thread.start();
      Thread.sleep(200);
      commitNow.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(committed.get()).isEqualTo(pairs * 2);

      database.transaction(() -> {
        for (int i = 0; i < pairs; i++) {
          try (final ResultSet rs = database.query("SQL", "SELECT FROM Indexed WHERE k = ?", "victim-" + i)) {
            assertThat(rs.hasNext()).as("index entry of victim " + i + " must be gone").isFalse();
          }
          try (final ResultSet rs = database.query("SQL", "SELECT FROM Indexed WHERE k = ?", "survivor-" + i)) {
            assertThat(rs.hasNext()).as("survivor " + i + " must still be indexed").isTrue();
            assertThat(rs.next().getIdentity().get()).isEqualTo(survivor[i]);
          }
        }
        // The unique index must accept the freed keys again.
        for (int i = 0; i < pairs; i++)
          database.newDocument("Indexed").set("k", "victim-" + i).save();
      });
    }

    /**
     * Deleting a record that does NOT live entirely in its own slot - here a multi-page one - is still not a single-slot
     * change, so it must keep poisoning the page and falling back to a normal retry. Co-located writes stay exact and
     * the record really disappears.
     */
    @Test
    void deletingAMultiPageRecordCoLocatedWithATrackedUpdateStillFallsBackCleanly() throws Exception {
      final RID[] big = new RID[1];
      final RID[] small = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Huge", 1).createProperty("tag", Type.STRING);
        small[0] = database.newDocument("Huge").set("tag", "initial").save().getIdentity();
        // Far bigger than a page: the record is stored as a chunk chain whose head sits on the shared page.
        big[0] = database.newDocument("Huge").set("blob", "x".repeat(400_000)).save().getIdentity();
      });

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);

      final Thread deleter = new Thread(() -> {
        try {
          start.await();
          // Retries allowed: the point is that the fallback converges and never corrupts the page.
          database.transaction(() -> big[0].asDocument(true).delete(), true, 50);
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "deleter");

      final int[] lastCommitted = new int[1];
      final Thread updater = new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= 200; i++) {
            final int n = i;
            try {
              database.transaction(() -> small[0].asDocument(true).modify().set("tag", "v" + n).save(), true, 1);
              lastCommitted[0] = n;
            } catch (final ConcurrentModificationException ignore) {
              // Expected while the delete holds the page poisoned.
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "updater");

      deleter.start();
      updater.start();
      start.countDown();
      deleter.join();
      updater.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      database.transaction(() -> {
        assertThat(small[0].asDocument(true).getString("tag")).isEqualTo("v" + lastCommitted[0]);
        assertThat(recordExists(big[0])).as("the multi-page record must be deleted").isFalse();
        assertThat(database.countType("Huge", false)).isEqualTo(1);
      });

      assertCheckDatabaseIsClean();
    }

    /**
     * A long, contended mix of inserts, updates and deletes on a single-bucket type: whatever combination of merges,
     * poisoned pages and retries that produces, every record must hold exactly what its owner last wrote, every deleted
     * record must be gone and {@code check database} must come back clean.
     */
    @Test
    void contendedMixOfInsertsUpdatesAndDeletesKeepsTheDatabaseConsistent() throws Exception {
      final int threadCount = 6;
      final int roundsPerThread = 40;

      database.transaction(() -> database.getSchema().createDocumentType("Churn", 1).createProperty("tag", Type.STRING));

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final List<RID> survivors = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int id = t;
        threads.add(new Thread(() -> {
          try {
            start.await();
            for (int round = 0; round < roundsPerThread; round++) {
              final int r = round;
              final RID[] created = new RID[1];

              // Retries stay on (this is a consistency guard, not a no-conflict one): whatever mix of merges and
              // retries the contention produces, the end state must be exact.
              // INSERT
              database.transaction(() -> created[0] = database.newDocument("Churn").set("owner", id)//
                  .set("tag", "t" + id + "-r" + r).save().getIdentity(), true, 10);

              // UPDATE (a growth, the normal shape)
              database.transaction(() -> created[0].asDocument(true).modify()//
                  .set("tag", "t" + id + "-r" + r + "-updated-and-padded").save(), true, 10);

              // DELETE every other one, keep the rest as survivors with a known final value
              if (r % 2 == 0)
                database.transaction(() -> created[0].asDocument(true).delete(), true, 10);
              else
                survivors.add(created[0]);
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "churn-" + t));
      }

      for (final Thread thread : threads)
        thread.start();
      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      database.transaction(() -> {
        assertThat(database.countType("Churn", false)).isEqualTo(survivors.size());
        for (final RID rid : survivors)
          assertThat(rid.asDocument(true).getString("tag")).endsWith("-updated-and-padded");
      });

      assertCheckDatabaseIsClean();
    }

    /**
     * Runs {@code body} in an explicit transaction that waits, right before committing, until every other participant
     * has staged its own write - so all the commits race on the very same page version, with NO retry (attempts are
     * irrelevant here: {@code commit()} is called once).
     */
    private void stagedTransaction(final List<Throwable> errors, final CyclicBarrier staged, final CountDownLatch commitNow,
        final AtomicInteger committed, final Runnable body) {
      try {
        database.begin();
        body.run();
        staged.await();
        commitNow.await();
        database.commit();
        committed.incrementAndGet();
      } catch (final Throwable e) {
        errors.add(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    private boolean recordExists(final RID rid) {
      try {
        return rid.asDocument(true) != null;
      } catch (final RecordNotFoundException e) {
        return false;
      }
    }

    private int pageOf(final String typeName, final RID rid) {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getType(typeName).getBuckets(false).getFirst();
      return (int) (rid.getPosition() / bucket.getMaxRecordsInPage());
    }

    @SafeVarargs
    private boolean allOnTheSamePage(final String typeName, final RID[]... groups) {
      int page = -1;
      for (final RID[] group : groups)
        for (final RID rid : group) {
          final int p = pageOf(typeName, rid);
          if (page == -1)
            page = p;
          else if (page != p)
            return false;
        }
      return true;
    }

    private void assertCheckDatabaseIsClean() {
      try (final ResultSet rs = database.command("SQL", "check database")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(numberProperty(row, "totalErrors")).as("check database: " + row.toJSON()).isZero();
          assertThat(numberProperty(row, "autoFix")).as("check database: " + row.toJSON()).isZero();
        }
      }
    }

    private static long numberProperty(final Result row, final String name) {
      final Object value = row.getProperty(name);
      return value instanceof Number n ? n.longValue() : 0L;
    }
  }

  /**
   * Issue #10661: a record inserted into a free slot of an existing page no longer keeps a second copy of its bytes
   * alive until commit for the disjoint-slot merge. The page the transaction wrote already holds them, so the merge
   * reads the inserted image back from that page when (and only when) it has to replay the insert on a newer committed
   * version of the page.
   */
  @Nested
  class PendingInsertRetention extends TestHelper {
    private boolean savedSlotMerge;
    private long    savedMaxBytes;

    @BeforeEach
    void saveConfig() {
      savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
      savedMaxBytes = GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.getValueAsLong();
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
      GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(savedMaxBytes);
    }

    /**
     * The retention cap bounds the bytes the merge holds on to. An insert holds none any more, so a transaction made of
     * inserts whose images add up to far more than the cap must still merge with a concurrent insert on the same page.
     * Before the fix every insert was counted, the cap switched the merge off and the commit failed with a page conflict.
     */
    @Test
    void insertsDoNotCountAgainstTheRetentionCap() throws Exception {
      GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(4_096L);
      createSeededType();

      final int inserts = 60;
      final RID[] rids = new RID[inserts];
      final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;

      database.begin();
      for (int i = 0; i < inserts; i++) {
        final MutableDocument doc = database.newDocument("Retained").set("payload", payload(i, 200));
        doc.save();
        rids[i] = doc.getIdentity();
      }
      assertThat(rids[0].getPosition()).as("the inserts must land on the reused page 0").isLessThan(1_000L);

      final RID concurrent = commitConcurrentInsert("concurrent");

      // NO RETRY: the two transactions inserted different records, so the page conflict is a false one
      database.commit();

      assertThat(((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges).isGreaterThan(mergesBefore);
      database.transaction(() -> {
        for (int i = 0; i < inserts; i++)
          assertThat(rids[i].asDocument(true).getString("payload")).isEqualTo(payload(i, 200));
        assertThat(concurrent.asDocument(true).getString("payload")).isEqualTo("concurrent");
        assertThat(database.countType("Retained", false)).isEqualTo(inserts + 2L);
      });
    }

    /**
     * The image replayed for an inserted slot is the one the slot holds at commit, whatever happened to it after the
     * insert: overwritten with a shorter value, grown in place, shrunk then grown back, deleted. Enough slots are written to outgrow the initial
     * capacity of the per-page slot arrays, and a committed record is updated next to them so an UPDATE (which keeps
     * its own images) is replayed in the same rebase.
     */
    @Test
    void rebaseReplaysTheFinalImageOfEveryInsertedSlot() throws Exception {
      createSeededType();
      final RID seed = database.query("sql", "select from Retained").next().getIdentity().orElseThrow();

      final int inserts = 40;
      final RID[] rids = new RID[inserts];
      final String[] expected = new String[inserts];

      database.begin();
      final MutableDocument[] docs = new MutableDocument[inserts];
      for (int i = 0; i < inserts; i++) {
        docs[i] = database.newDocument("Retained").set("payload", payload(i, 40));
        docs[i].save();
        rids[i] = docs[i].getIdentity();
        expected[i] = payload(i, 40);
      }
      // shrink in place
      docs[3].set("payload", "short").save();
      expected[3] = "short";
      // grow in place (the page has plenty of room, so the record stays in its slot)
      docs[20].set("payload", payload(20, 300)).save();
      expected[20] = payload(20, 300);
      // shrink in place, then grow back past the original size: the image is the last one, wherever it now sits
      docs[9].set("payload", "s").save();
      docs[9].set("payload", payload(9, 120)).save();
      expected[9] = payload(9, 120);
      // insert-and-delete: replayed as nothing
      docs[31].delete();
      expected[31] = null;
      // an UPDATE of a committed record on the same page
      seed.asDocument(true).modify().set("payload", "seed-updated").save();

      final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
      final RID concurrent = commitConcurrentInsert("concurrent");
      database.commit();

      assertThat(((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges).isGreaterThan(mergesBefore);
      database.transaction(() -> {
        for (int i = 0; i < inserts; i++)
          if (expected[i] == null)
            assertThat(database.existsRecord(rids[i])).isFalse();
          else
            assertThat(rids[i].asDocument(true).getString("payload")).isEqualTo(expected[i]);
        assertThat(seed.asDocument(true).getString("payload")).isEqualTo("seed-updated");
        assertThat(concurrent.asDocument(true).getString("payload")).isEqualTo("concurrent");
        assertThat(database.countType("Retained", false)).isEqualTo(inserts - 1 + 2L);
      });
    }

    /**
     * The read-back the merge relies on answers the body of a plain record, and {@code null} - which the merge turns into
     * a retryable refusal - for a slot that holds none: a deleted record, a slot past the page's record count.
     */
    @Test
    void readPlainRecordBodyAnswersOnlyAPlainRecord() {
      createSeededType();
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        for (int i = 0; i < rids.length; i++) {
          final MutableDocument doc = database.newDocument("Retained").set("payload", payload(i, 40));
          doc.save();
          rids[i] = doc.getIdentity();
        }
      });
      database.transaction(() -> rids[1].asDocument(true).delete());

      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rids[0].getBucketId());
      database.transaction(() -> {
        try {
          final BasePage page = ((DatabaseInternal) database).getTransaction()
              .getPage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize());
          final int liveSlot = (int) rids[0].getPosition();
          final int deletedSlot = (int) rids[1].getPosition();

          final byte[] body = bucket.readPlainRecordBody(page, liveSlot);
          assertThat(body).isNotNull();
          assertThat(new String(body)).contains(payload(0, 40));

          assertThat(bucket.readPlainRecordBody(page, deletedSlot)).as("a deleted record").isNull();
          assertThat(bucket.readPlainRecordBody(page, 1_000)).as("a slot past the record count").isNull();
        } catch (final Exception e) {
          throw new AssertionError(e);
        }
      });
    }

    private void createSeededType() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Retained", 1).createProperty("payload", Type.STRING);
        // page 0 exists before the test transactions, so their inserts go to free slots of a REUSED page
        database.newDocument("Retained").set("payload", "seed").save();
      });
    }

    /** Inserts and commits one record on another thread while the caller's transaction is still open. */
    private RID commitConcurrentInsert(final String payload) throws Exception {
      final AtomicReference<RID> rid = new AtomicReference<>();
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final Thread thread = new Thread(() -> {
        try {
          database.transaction(() -> {
            final MutableDocument doc = database.newDocument("Retained").set("payload", payload);
            doc.save();
            rid.set(doc.getIdentity());
          });
        } catch (final Throwable e) {
          error.set(e);
        }
      });
      thread.start();
      thread.join();
      if (error.get() != null)
        throw new AssertionError("the concurrent insert failed", error.get());
      assertThat(rid.get().getPosition()).as("the concurrent insert must land on the same page").isLessThan(1_000L);
      return rid.get();
    }

    private static String payload(final int i, final int length) {
      final String prefix = "r" + i + "-";
      return prefix + "x".repeat(length - prefix.length());
    }
  }

  /**
   * Issue #10813: a document saved in a transaction kept both its property map and its serialized buffer alive until the commit,
   * because the transaction held the document itself. With read-your-writes off (the importer, {@code GraphBatch}) nothing in the
   * transaction reads it back, so it holds the RID and only a weak reference, for the identity reset of a rollback (#4562): a document
   * the caller dropped is collectable before the commit, one the caller holds is still reset.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NewRecordRetention extends TestHelper {
    private static final String TYPE = "Doc";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "id");
    }

    @AfterEach
    void restoreReadYourWrites() {
      database.setReadYourWrites(true);
    }

    @Test
    void withReadYourWritesOffADroppedNewDocumentIsNotHeldUntilTheCommit() {
      database.setReadYourWrites(false);
      database.begin();
      final WeakReference<MutableDocument> saved = saveAndDrop(1);
      final RID rid = identityOf(saved);
      assertThat(rid).isNotNull();

      assertThat(collected(saved)).as("nothing in the transaction holds the document").isTrue();
      assertThat(((DatabaseInternal) database).getTransaction().isCreatedInTransaction(rid))
          .as("the transaction still knows it created the record").isTrue();
      database.commit();

      assertThat(database.countType(TYPE, false)).isEqualTo(1);
      assertThat(database.lookupByRID(rid, true).asDocument().getInteger("id")).isEqualTo(1);
    }

    /** The control: with read-your-writes on, the transaction's record cache holds the document, as a lookup in it returns it. */
    @Test
    void withReadYourWritesOnTheTransactionStillHoldsTheDocument() {
      database.begin();
      final WeakReference<MutableDocument> saved = saveAndDrop(1);
      assertThat(collected(saved)).isFalse();
      assertThat(database.lookupByRID(saved.get().getIdentity(), false)).isSameAs(saved.get());
      database.commit();
    }

    @Test
    void withReadYourWritesOffARollbackStillResetsADocumentTheCallerHolds() {
      database.setReadYourWrites(false);
      database.begin();
      final MutableDocument held = database.newDocument(TYPE).set("id", 1);
      held.save();
      // the entry of the dropped one is cleared before the rollback walks the entries
      assertThat(collected(saveAndDrop(2))).isTrue();
      assertThat(held.getIdentity()).isNotNull();
      database.rollback();

      assertThat(held.getIdentity()).as("identity reset to provisional by the rollback").isNull();
      database.transaction(held::save);
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    }

    /** A statement that fails inside the transaction takes back its own records, and leaves the others, held or collected. */
    @Test
    void withReadYourWritesOffAFailedStatementTakesBackOnlyItsOwnRecords() {
      database.setReadYourWrites(false);
      database.begin();
      final WeakReference<MutableDocument> dropped = saveAndDrop(1);
      final MutableDocument held = database.newDocument(TYPE).set("id", 2);
      held.save();
      assertThat(collected(dropped)).isTrue();

      // the second row violates the unique index: the statement is undone, the transaction goes on (#10346)
      final Throwable failure = catchThrowable(
          () -> database.command("sqlscript", "INSERT INTO " + TYPE + " SET id = 3; INSERT INTO " + TYPE + " SET id = 2;"));
      assertThat(failure).isNotNull();
      assertThat(held.getIdentity()).as("a record of an earlier statement is left alone").isNotNull();
      database.commit();

      assertThat(database.countType(TYPE, false)).isEqualTo(2);
      assertThat(database.query("sql", "SELECT FROM " + TYPE + " WHERE id = 3").hasNext()).isFalse();
    }

    /** Saves a new document and returns only a weak reference to it, so this frame does not keep it reachable. */
    private WeakReference<MutableDocument> saveAndDrop(final int id) {
      final MutableDocument document = database.newDocument(TYPE).set("id", id);
      document.save();
      return new WeakReference<>(document);
    }

    /** Reads the reference once, in a frame of its own, so the caller holds nothing that keeps the document reachable. */
    private static RID identityOf(final WeakReference<MutableDocument> reference) {
      final MutableDocument document = reference.get();
      return document != null ? document.getIdentity() : null;
    }

    private static boolean collected(final WeakReference<?> reference) {
      for (int i = 0; i < 50 && reference.get() != null; i++) {
        System.gc();
        try {
          Thread.sleep(20);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      return reference.get() == null;
    }
  }

  /**
   * Issue #11526: a transaction that writes a UNIQUE key and then updates an EXTERNAL property of the record must still be refused
   * when a concurrent transaction commits the same key into another bucket.
   * <p>
   * The update is deferred to the commit, where it writes the value into the paired {@code <bucket>_ext} bucket. That file was not
   * in the lock set computed before, so the commit found it as a late joiner (#4937) and released EVERY lock it held to take a wider
   * set, after the unique-key check had already passed: in that window the concurrent transaction committed the same key into
   * another bucket's sub-index, and nothing checked the key again. A type with a single bucket was not affected, because both
   * transactions then write the same index page and one of them fails its version check.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UniqueKeyExternalPropertyUpdateRace {
    private static final int BUCKETS = 8;
    private static final int KEYS    = 40;

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-11526-unique-key-external-update");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    /** The reported shape: create the record, then UPDATE its EXTERNAL property, in each of two transactions racing on one key. */
    @Test
    void concurrentCreateAndExternalUpdateOfTheSameKeyKeepsTheKeyUnique() throws Exception {
      createType(true);
      assertKeysAreUnique(race((name, thread) -> {
        database.newDocument("T").set("name", name).save();
        database.command("sql", "update T set ext = 'x' where name = :name", Map.of("name", name));
      }));
    }

    /**
     * The same window without a record created in the transaction: an existing record takes the key in a deferred update that also
     * writes its EXTERNAL property, while the other transaction inserts the key.
     */
    @Test
    void deferredUpdateOfAnExistingRecordToATakenKeyWithAnExternalPropertyIsRefused() throws Exception {
      createType(true);

      final List<RID> existing = new ArrayList<>(KEYS);
      database.transaction(() -> {
        for (int k = 0; k < KEYS; k++)
          existing.add(database.newDocument("T").set("name", "old-" + k).save().getIdentity());
      });

      // THE BUCKETS ARE PICKED ROUND ROBIN, SO THE RECORD INSERTED FOR key-k LANDS IN THE BUCKET OF existing[k]: THE UPDATED RECORD
      // IS TAKEN A FEW BUCKETS AWAY, OR BOTH WRITE THE SAME INDEX PAGE AND THE VERSION CHECK REFUSES ONE WITHOUT THE KEY CHECK
      final Map<String, RID> recordPerKey = new HashMap<>();
      for (int k = 0; k < KEYS; k++)
        recordPerKey.put("key-" + k, existing.get((k + 3) % KEYS));

      assertKeysAreUnique(race((name, thread) -> {
        if (thread == 0)
          database.lookupByRID(recordPerKey.get(name), true).asDocument().modify().set("name", name).set("ext", "x").save();
        else
          database.newDocument("T").set("name", name).save();
      }));
    }

    /** Control: an inline (non-EXTERNAL) property never joined the lock set late, so the race was always refused. */
    @Test
    void theSameRaceWithAnInlinePropertyIsRefused() throws Exception {
      createType(false);
      assertKeysAreUnique(race((name, thread) -> {
        database.newDocument("T").set("name", name).save();
        database.command("sql", "update T set ext = 'x' where name = :name", Map.of("name", name));
      }));
    }

    /**
     * The lock set a commit takes before its unique-key check already covers the paired external bucket the deferred update will
     * write, so the commit never has to release its locks to take it.
     */
    @Test
    void theLockSetTakenUpFrontCoversThePairedExternalBucketOfADeferredUpdate() throws Exception {
      createType(true);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("T").set("name", "a").save().getIdentity());

      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("T");
      final Integer externalBucketId = type.getExternalBucketIdFor(rid[0].getBucketId());
      assertThat(externalBucketId).as("the type pairs an external bucket with bucket %d", rid[0].getBucketId()).isNotNull();

      database.begin();
      try {
        database.lookupByRID(rid[0], true).asDocument().modify().set("ext", "x").save();

        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        final Method lockFilesFromChanges = TransactionContext.class.getDeclaredMethod("lockFilesFromChanges");
        lockFilesFromChanges.setAccessible(true);
        final IntHashSet files = (IntHashSet) lockFilesFromChanges.invoke(tx);

        assertThat(files.contains(rid[0].getBucketId())).as("the record's own bucket is locked").isTrue();
        assertThat(files.contains(externalBucketId)).as("the paired external bucket %d the deferred update writes is locked up front",
            externalBucketId).isTrue();
      } finally {
        database.rollback();
      }

      // And the commit itself goes through, writing the value into the external bucket.
      database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("ext", "y").save());
      assertThat(database.lookupByRID(rid[0], true).asDocument().getString("ext")).isEqualTo("y");
    }

    /**
     * A file whose lock an enclosing frame of the same thread already holds (a sparse vector index flushing its own transaction under
     * its file lock) is left out of the commit's own lock list, so the late-joiner coverage finds it. It is covered, and only that
     * frame may release it: the commit must not take it over and release it on its way out.
     */
    @Test
    void aFileLockHeldByAnEnclosingFrameOfTheSameThreadSurvivesTheCommit() throws Exception {
      database.getSchema().createDocumentType("Plain", 1);
      final int fileId = database.getSchema().getType("Plain").getBuckets(false).getFirst().getFileId();
      final TransactionManager manager = ((DatabaseInternal) database).getTransactionManager();

      assertThat(manager.tryLockFile(fileId, 5_000, Thread.currentThread())).isEqualTo(LockManager.LOCK_STATUS.YES);
      try {
        database.transaction(() -> database.newDocument("Plain").set("name", "a").save());
        assertThat(database.countType("Plain", false)).isEqualTo(1);

        final LockManager.LOCK_STATUS[] fromAnotherThread = new LockManager.LOCK_STATUS[1];
        final Object anotherRequester = new Object();
        final Thread other = new Thread(() -> {
          fromAnotherThread[0] = manager.tryLockFile(fileId, 50, anotherRequester);
          if (fromAnotherThread[0] == LockManager.LOCK_STATUS.YES)
            manager.unlockFile(fileId, anotherRequester);
        });
        other.start();
        other.join();
        assertThat(fromAnotherThread[0]).as("the lock of file %d is still held by the frame that took it", fileId)
            .isEqualTo(LockManager.LOCK_STATUS.NO);
      } finally {
        manager.unlockFile(fileId, Thread.currentThread());
      }
    }

    private void createType(final boolean external) {
      final DocumentType type = database.getSchema().createDocumentType("T", BUCKETS);
      type.createProperty("name", Type.STRING);
      type.createProperty("ext", Type.STRING).setExternal(external);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    }

    @FunctionalInterface
    private interface TransactionBody {
      void run(String name, int thread) throws Exception;
    }

    /** Two threads per key run {@code body} and commit together; returns the commits that succeeded per key. */
    private Map<String, Integer> race(final TransactionBody body) throws Exception {
      final Map<String, Integer> committed = new HashMap<>();
      final AtomicInteger unexpected = new AtomicInteger();
      for (int k = 0; k < KEYS; k++) {
        final String name = "key-" + k;
        final CyclicBarrier beforeCommit = new CyclicBarrier(2);
        final AtomicInteger commits = new AtomicInteger();
        final Thread[] threads = new Thread[2];
        for (int t = 0; t < 2; t++) {
          final int thread = t;
          threads[t] = new Thread(() -> {
            database.begin();
            try {
              body.run(name, thread);
              beforeCommit.await();
              database.commit();
              commits.incrementAndGet();
            } catch (final DuplicatedKeyException | NeedRetryException e) {
              // REFUSED: THE OTHER TRANSACTION WON THE KEY
              if (database.isTransactionActive())
                database.rollback();
            } catch (final Throwable e) {
              unexpected.incrementAndGet();
              beforeCommit.reset();
              if (database.isTransactionActive())
                database.rollback();
            }
          });
          threads[t].start();
        }
        for (final Thread thread : threads)
          thread.join();
        committed.put(name, commits.get());
      }
      assertThat(unexpected.get()).as("transactions that failed with anything but a refusal of the key").isZero();
      return committed;
    }

    private void assertKeysAreUnique(final Map<String, Integer> committed) {
      // A SCAN OF THE TYPE, NOT AN INDEX LOOKUP: THE DUPLICATE IS A RECORD THE INDEX DOES NOT HOLD
      final Map<String, Integer> recordsPerKey = new HashMap<>();
      final Iterator<Record> scan = database.iterateType("T", false);
      while (scan.hasNext())
        recordsPerKey.merge(scan.next().asDocument().getString("name"), 1, Integer::sum);

      for (final Map.Entry<String, Integer> entry : committed.entrySet()) {
        final int records = recordsPerKey.getOrDefault(entry.getKey(), 0);
        assertThat(records).as("records holding the key %s (commits that succeeded: %d)", entry.getKey(), entry.getValue())
            .isLessThanOrEqualTo(1);
        assertThat(entry.getValue()).as("commits of the key %s: at most one of the two transactions may commit it", entry.getKey())
            .isLessThanOrEqualTo(1);

        // THE INDEX AGREES WITH THE SCAN
        int indexed = 0;
        for (final IndexCursor cursor = database.lookupByKey("T", "name", entry.getKey()); cursor.hasNext(); cursor.next())
          ++indexed;
        assertThat(indexed).as("index entries for the key %s", entry.getKey()).isEqualTo(records);
      }
    }
  }

  /**
   * Issue #8610: under READ_COMMITTED, reading a record, letting a concurrent transaction commit a change to it,
   * then writing a value derived from the read through {@code modify()} lost the concurrent change silently, because
   * {@code modify()} reloads a vertex whose page moved on and the stale read disappeared with the reload. A document in
   * the same situation was already refused by the #6950 record-image check.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Nested
  class StaleReadModify {
    private Database database;
    private RID      rid;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-8610-stale-read");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.command("sqlscript", "CREATE VERTEX TYPE V; CREATE EDGE TYPE E; CREATE DOCUMENT TYPE D");
      final AtomicReference<RID> created = new AtomicReference<>();
      database.transaction(() -> created.set(database.newVertex("V").set("n", 0).save().getIdentity()));
      rid = created.get();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen()) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    /**
     * The issue: a read-modify-write on a vertex read in this transaction, over a concurrent commit, must be refused as a
     * retryable conflict, never commit a value computed from the stale read.
     */
    @Test
    void readModifyWriteOverAConcurrentCommitIsRefused() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      final int n = read.getInteger("n");
      commitConcurrently("n", 5);

      assertThatThrownBy(() -> {
        read.modify().set("n", n + 1).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);

      assertThat(readN()).as("the concurrent write survives").isEqualTo(5);
    }

    /**
     * The same for a vertex read through a scan.
     */
    @Test
    void readModifyWriteOfAScannedVertexOverAConcurrentCommitIsRefused() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Iterator<Record> it = database.iterateType("V", false);
      final Vertex read = it.next().asVertex();
      final int n = read.getInteger("n");
      commitConcurrently("n", 5);

      assertThatThrownBy(() -> {
        read.modify().set("n", n + 1).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);

      assertThat(readN()).isEqualTo(5);
    }

    /**
     * Retrying the transaction, as the retry helper does, reads the vertex again and succeeds: the refusal is retryable.
     */
    @Test
    void theRetryReadsAgainAndCommits() {
      final AtomicReference<Boolean> first = new AtomicReference<>(true);
      database.transaction(() -> {
        final Vertex read = rid.asVertex();
        final int n = read.getInteger("n");
        if (first.getAndSet(false))
          commitConcurrently("n", 5);
        read.modify().set("n", n + 1).save();
      }, false, 3);

      assertThat(readN()).as("the retry built on the concurrent 5").isEqualTo(6);
    }

    /**
     * A document read through a scan: its content is reloaded on modify() when its page moved on (issue #8312), which
     * hid the stale read from the #6950 record-image check the same way.
     */
    @Test
    void readModifyWriteOfAScannedDocumentOverAConcurrentCommitIsRefused() {
      final AtomicReference<RID> doc = new AtomicReference<>();
      database.transaction(() -> doc.set(database.newDocument("D").set("n", 0).save().getIdentity()));

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Document read = database.iterateType("D", false).next().asDocument();
      final int n = read.getInteger("n");
      commitConcurrently(() -> doc.get().asDocument().modify().set("n", 5).save());

      assertThatThrownBy(() -> {
        read.modify().set("n", n + 1).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);

      assertThat(doc.get().asDocument().getInteger("n")).isEqualTo(5);
    }

    /**
     * The same for an edge read through a scan.
     */
    @Test
    void readModifyWriteOfAScannedEdgeOverAConcurrentCommitIsRefused() {
      final AtomicReference<RID> edge = new AtomicReference<>();
      database.transaction(() -> {
        final MutableVertex other = database.newVertex("V").save();
        edge.set(rid.asVertex().newEdge("E", other, "n", 0).getIdentity());
      });

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Edge read = database.iterateType("E", false).next().asEdge();
      final int n = read.getInteger("n");
      commitConcurrently(() -> edge.get().asEdge().modify().set("n", 5).save());

      assertThatThrownBy(() -> {
        read.modify().set("n", n + 1).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);

      assertThat(edge.get().asEdge().getInteger("n")).isEqualTo(5);
    }

    /**
     * The mark survives a second modify() of the same record: that one finds the page pinned and does not reload, so a
     * value computed from the read before the first modify() must still be refused.
     */
    @Test
    void aSecondModifyOfTheSameStaleReadIsRefusedToo() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      final int n = read.getInteger("n");
      commitConcurrently("n", 5);
      read.modify(); // discarded

      assertThatThrownBy(() -> {
        read.modify().set("n", n + 1).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);

      assertThat(readN()).isEqualTo(5);
    }

    /**
     * The refusal is per record, as the #6950 check is for a document: a write to another property of a record changed
     * concurrently is refused too, since it may be computed from the property that changed (SET b = a + 1).
     */
    @Test
    void aWriteToAnotherPropertyOfAConcurrentlyChangedRecordIsRefused() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      final int n = read.getInteger("n");
      commitConcurrently("n", 5);

      assertThatThrownBy(() -> {
        read.modify().set("derived", n + 1).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);

      assertThat(readN()).isEqualTo(5);
      assertThat(rid.asVertex().has("derived")).isFalse();
    }

    /**
     * An UPDATE by RID loads the record lazily, so its first content read is the reload modify() performs after pinning
     * the page: a commit landing there is refused by the commit-time page check, as it must be. Committed or refused, the
     * concurrent write is never lost.
     */
    @Test
    void sqlUpdateByRidNeverLosesACommitLandingInsideTheStatement() {
      final boolean committed = runWithConcurrentCommitAfterFirstRead(
          () -> database.command("sql", "UPDATE V SET n = n + 1 WHERE @rid = ?", rid).close());

      assertThat(readN()).isEqualTo(committed ? 6 : 5);
    }

    /**
     * SQL UPDATE evaluates its SET against the record it converted with modify(), i.e. against the reloaded content: a
     * concurrent commit landing between the scan that read the record and the write is built upon, not refused.
     */
    @Test
    void sqlUpdateOverAScanBuildsOnACommitLandingInsideTheStatement() {
      final boolean committed = runWithConcurrentCommitAfterFirstRead(
          () -> database.command("sql", "UPDATE V SET n = n + 1").close());

      assertThat(committed).isTrue();
      assertThat(readN()).isEqualTo(6);
    }

    /**
     * And for an openCypher SET (the MERGE actions are covered by CypherConcurrentWriteTest.Issue8538MergeOnMatchLostUpdate).
     */
    @Test
    void cypherSetBuildsOnACommitLandingInsideTheStatement() {
      final boolean committed = runWithConcurrentCommitAfterFirstRead(
          () -> database.command("cypher", "MATCH (v:V) SET v.n = v.n + 1").close());

      assertThat(committed).isTrue();
      assertThat(readN()).isEqualTo(6);
    }

    /**
     * The read-transaction id stays non-negative however far the begin sequence goes: a negative id means "unknown" and
     * would switch the check off, and -2 marks a stale read.
     */
    @Test
    void theReadTransactionIdIsNeverNegative() {
      assertThat(ImmutableDocument.readTransactionId(-1)).isEqualTo(-1);
      assertThat(ImmutableDocument.readTransactionId(0)).isZero();
      assertThat(ImmutableDocument.readTransactionId((1L << 30) - 1)).isEqualTo((1 << 30) - 1);
      assertThat(ImmutableDocument.readTransactionId(1L << 30)).isZero();
      assertThat(ImmutableDocument.readTransactionId(1L << 31)).isZero();
      assertThat(ImmutableDocument.readTransactionId((1L << 32) - 2)).isNotNegative();
      assertThat(ImmutableDocument.readTransactionId(Long.MAX_VALUE)).isNotNegative();
      // The stale marker, -2 - id, stays inside an int for every id
      assertThat(-2L - ImmutableDocument.readTransactionId(Long.MAX_VALUE)).isGreaterThanOrEqualTo(Integer.MIN_VALUE);
    }

    /**
     * A transaction rolled back and begun again is another transaction: a record read in the first one is refreshed
     * silently when modified in the second, like any record held across transactions.
     */
    @Test
    void aRecordReadInARolledBackTransactionIsRefreshedInTheNextOne() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      database.rollback();

      commitConcurrently("n", 5);

      database.transaction(() -> read.modify().set("other", 1).save());
      assertThat(readN()).isEqualTo(5);
      assertThat(rid.asVertex().getInteger("other")).isEqualTo(1);
    }

    /**
     * SQL UPDATE MERGE and CONTENT go through the same conversion as SET, so they build on a commit landing inside the
     * statement too.
     */
    @Test
    void sqlUpdateMergeBuildsOnACommitLandingInsideTheStatement() {
      final boolean committed = runWithConcurrentCommitAfterFirstRead(
          () -> database.command("sql", "UPDATE V MERGE {\"m\": 1}").close());

      assertThat(committed).isTrue();
      assertThat(readN()).isEqualTo(5);
      assertThat(rid.asVertex().getInteger("m")).isEqualTo(1);
    }

    @Test
    void sqlUpdateContentBuildsOnACommitLandingInsideTheStatement() {
      final boolean committed = runWithConcurrentCommitAfterFirstRead(
          () -> database.command("sql", "UPDATE V CONTENT {\"n\": 7}").close());

      assertThat(committed).isTrue();
      assertThat(readN()).isEqualTo(7);
    }

    /**
     * An openCypher REMOVE writes no value computed from what the MATCH read, so a commit landing in between leaves
     * nothing to lose: it commits, on top of the concurrent write.
     */
    @Test
    void cypherRemoveCommitsOverACommitLandingInsideTheStatement() {
      database.transaction(() -> rid.asVertex().modify().set("tag", "x").save());

      final boolean committed = runWithConcurrentCommitAfterFirstRead(
          () -> database.command("cypher", "MATCH (v:V) REMOVE v.tag").close());

      assertThat(committed).isTrue();
      assertThat(readN()).isEqualTo(5);
      assertThat(rid.asVertex().has("tag")).isFalse();
    }

    /**
     * The same for the merge.node procedure, whose onMatch properties are constants.
     */
    @Test
    void mergeNodeProcedureCommitsOverACommitLandingInsideTheStatement() {
      database.command("sql", "CREATE PROPERTY V.key STRING");
      database.transaction(() -> rid.asVertex().modify().set("key", "k").save());

      final boolean committed = runWithConcurrentCommitAfterFirstRead(() -> database.command("cypher",
          "CALL merge.node(['V'], {key: 'k'}, {}, {flag: true}) YIELD node RETURN node").close());

      assertThat(committed).isTrue();
      assertThat(readN()).isEqualTo(5);
      assertThat(rid.asVertex().getBoolean("flag")).isTrue();
    }

    /**
     * Moving an edge endpoint through set("@in") is a property write too: computed from a stale read, it is refused.
     */
    @Test
    void anEdgeEndpointMoveFromAStaleReadIsRefused() {
      final AtomicReference<RID> edge = new AtomicReference<>();
      final AtomicReference<RID> target = new AtomicReference<>();
      database.transaction(() -> {
        final MutableVertex other = database.newVertex("V").save();
        target.set(database.newVertex("V").save().getIdentity());
        edge.set(rid.asVertex().newEdge("E", other, "n", 0).getIdentity());
      });

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Edge read = database.iterateType("E", false).next().asEdge();
      commitConcurrently(() -> edge.get().asEdge().modify().set("n", 5).save());

      assertThatThrownBy(() -> {
        read.modify().set("@in", target.get()).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);
      // The move already rewired the edge inside the transaction before the save refused it: the transaction is doomed
      database.rollback();

      assertThat(edge.get().asEdge().getInteger("n")).isEqualTo(5);
    }

    /**
     * A stale mark belongs to the transaction that made it: a retry loop that reuses the record it read in the refused
     * attempt holds that record across transactions, and the next attempt refreshes it silently instead of being refused
     * until the retries run out.
     */
    @Test
    void aStaleMarkDoesNotOutliveItsTransaction() {
      final Vertex[] held = new Vertex[1];
      final AtomicReference<Boolean> first = new AtomicReference<>(true);
      database.transaction(() -> {
        if (held[0] == null)
          held[0] = rid.asVertex();
        final int n = held[0].getInteger("n");
        if (first.getAndSet(false))
          commitConcurrently("n", 5);
        held[0].modify().set("n", n + 1).save();
      }, false, 3);

      // The retry reused the instance reloaded by the refused attempt, so it read the concurrent 5 and wrote 6
      assertThat(readN()).isEqualTo(6);
    }

    /**
     * The merge.relationship procedure writes constant onMatch properties too.
     */
    @Test
    void mergeRelationshipProcedureCommitsOverACommitLandingInsideTheStatement() {
      database.transaction(() -> {
        final MutableVertex other = database.newVertex("V").set("name", "other").save();
        rid.asVertex().modify().set("name", "me").save();
        rid.asVertex().newEdge("E", other, "n", 0);
      });

      final boolean committed = runWithConcurrentCommitAfterFirstRead(() -> database.command("cypher",
          "MATCH (a:V {name: 'me'}), (b:V {name: 'other'}) "
              + "CALL merge.relationship(a, 'E', {}, {}, b, {flag: true}) YIELD rel RETURN rel").close());

      assertThat(committed).isTrue();
      assertThat(readN()).isEqualTo(5);
      try (final ResultSet rs = database.query("sql", "SELECT flag FROM E")) {
        assertThat(rs.next().<Boolean>getProperty("flag")).isTrue();
      }
    }

    /**
     * This transaction's own writes never count as a concurrent change: modifying the same record again after saving
     * it, while an unrelated record on the same page is committed by another transaction, is no conflict.
     */
    @Test
    void ownWritesAndAnUnrelatedCommitOnTheSamePageAreNoConflict() {
      final AtomicReference<RID> neighbour = new AtomicReference<>();
      database.transaction(() -> neighbour.set(database.newVertex("V").set("n", 0).save().getIdentity()));

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      read.modify().set("a", 1).save();
      commitConcurrently(() -> neighbour.get().asVertex().modify().set("n", 9).save());
      read.modify().set("b", 2).save();
      database.commit();

      final Vertex reloaded = rid.asVertex();
      assertThat(reloaded.getInteger("a")).isEqualTo(1);
      assertThat(reloaded.getInteger("b")).isEqualTo(2);
      assertThat(neighbour.get().asVertex().getInteger("n")).isEqualTo(9);
    }

    /**
     * A nested transaction is a transaction of its own: a record read in the outer one and modified in the nested one is
     * held across transactions, and refreshed silently.
     */
    @Test
    void aRecordReadInTheOuterTransactionIsRefreshedInANestedOne() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      commitConcurrently("n", 5);

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      read.modify().set("other", 1).save();
      database.commit();
      database.commit();

      assertThat(readN()).isEqualTo(5);
      assertThat(rid.asVertex().getInteger("other")).isEqualTo(1);
    }

    /**
     * A scan kept across commit() and begin() reads its next batch in the new transaction, so a record of that batch is
     * checked there: its stale read is refused, not refreshed silently.
     */
    @Test
    void aScanBatchReadAfterANewBeginIsCheckedInThatTransaction() {
      // More records than one prefetch batch (1024), so the scan reads a second batch
      database.transaction(() -> {
        for (int i = 0; i < 1_100; i++)
          database.newDocument("D").set("i", i, "n", 0).save();
      });

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Iterator<Record> scan = database.iterateType("D", false);
      // The iterator refills as soon as a batch runs out: stop one short so the second batch is read after the new begin
      for (int i = 0; i < 1_023; i++)
        scan.next();
      database.commit();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      scan.next(); // the last record of the first batch, which reads the second batch now, in this transaction
      final Document read = scan.next().asDocument();
      final int n = read.getInteger("n");
      final RID readRid = read.getIdentity();
      commitConcurrently(() -> readRid.asDocument().modify().set("n", 5).save());

      assertThatThrownBy(() -> {
        read.modify().set("n", n + 1).save();
        database.commit();
      }).isInstanceOf(ConcurrentModificationException.class);

      assertThat(readRid.asDocument().getInteger("n")).isEqualTo(5);
    }

    /**
     * Creating an edge changes only the vertex edge lists, which is what the reload exists for: a concurrent change to the
     * vertex properties must not turn it into a conflict, nor be lost.
     */
    @Test
    void edgeCreationOverAConcurrentPropertyChangeCommits() {
      final AtomicReference<RID> other = new AtomicReference<>();
      database.transaction(() -> other.set(database.newVertex("V").set("n", 100).save().getIdentity()));

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      commitConcurrently("n", 5);
      read.newEdge("E", other.get().asVertex());
      database.commit();

      assertThat(readN()).isEqualTo(5);
      assertThat(rid.asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
    }

    /**
     * A vertex read outside this transaction (held across transactions) keeps the refresh it always had: its content is
     * reloaded by modify() and the write commits. Only a read made inside the transaction is checked.
     */
    @Test
    void aVertexReadBeforeTheTransactionIsStillRefreshed() {
      final Vertex held = rid.asVertex();
      database.transaction(() -> held.modify().set("a", 1).save());
      database.transaction(() -> held.modify().set("b", 2).save());

      final Vertex reloaded = rid.asVertex();
      assertThat(reloaded.getInteger("a")).isEqualTo(1);
      assertThat(reloaded.getInteger("b")).isEqualTo(2);
    }

    /**
     * Nothing changed concurrently: no conflict, however the vertex was read.
     */
    @Test
    void aCurrentReadCommits() {
      database.transaction(() -> {
        final Vertex read = rid.asVertex();
        read.modify().set("n", read.getInteger("n") + 1).save();
      });
      assertThat(readN()).isEqualTo(1);
    }

    /**
     * The switch restores the previous behavior (the reload wins silently), for an application that relies on it.
     */
    @Test
    void theCheckCanBeDisabled() {
      database.getConfiguration().setValue(GlobalConfiguration.TX_STALE_READ_CHECK, false);

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final Vertex read = rid.asVertex();
      final int n = read.getInteger("n");
      commitConcurrently("n", 5);
      read.modify().set("n", n + 1).save();
      database.commit();

      assertThat(readN()).as("pre-#8610 behavior: the value computed from the stale read overwrites the concurrent 5")
          .isEqualTo(1);
    }

    private void commitConcurrently(final String property, final int value) {
      commitConcurrently(() -> rid.asVertex().modify().set(property, value).save());
    }

    private void commitConcurrently(final Runnable write) {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread concurrent = new Thread(() -> {
        try {
          database.transaction(write::run);
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      concurrent.start();
      try {
        concurrent.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      assertThat(failure.get()).isNull();
    }

    /**
     * Runs {@code body} in a READ_COMMITTED transaction; right after the first V record it reads, another transaction
     * sets {@code n = 5} and commits.
     *
     * @return whether the transaction committed; {@code false} when it was refused with a retryable conflict
     */
    private boolean runWithConcurrentCommitAfterFirstRead(final Runnable body) {
      final Thread bodyThread = Thread.currentThread();
      final AtomicBoolean armed = new AtomicBoolean(true);
      final AfterRecordReadListener interleave = record -> {
        if (Thread.currentThread() == bodyThread && armed.compareAndSet(true, false))
          commitConcurrently("n", 5);
        return record;
      };
      database.getSchema().getType("V").getEvents().registerListener(interleave);
      try {
        database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
        body.run();
        database.commit();
        return true;
      } catch (final RuntimeException e) {
        // A query engine may report the conflict wrapped in its own exception
        for (Throwable t = e; t != null; t = t.getCause())
          if (t instanceof ConcurrentModificationException)
            return false;
        throw e;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.getSchema().getType("V").getEvents().unregisterListener(interleave);
        assertThat(armed.get()).as("the concurrent commit must have been interleaved").isFalse();
      }
    }

    private int readN() {
      return rid.asVertex().getInteger("n");
    }
  }

  /**
   * Issue #8984: a {@code DELETE} that selected a record another transaction then deleted and committed failed with a
   * {@code RecordNotFoundException} (or, for a vertex, a {@code VertexNotFoundException} advising {@code CHECK DATABASE FIX}),
   * neither retryable, so {@code database.transaction(block, false, retries)} gave up after the first attempt. The record was
   * read by this very transaction, so its absence at the delete can only be a concurrent delete: a retryable conflict.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // hang detector only, not a latency bound
  @Timeout(120)
  @Nested
  class ConcurrentDeleteRetry extends TestHelper {

    private void runRace(final String type, final boolean vertex, final String sql, final int retries) throws Exception {
      final ExecutorService otherThread = Executors.newSingleThreadExecutor();
      final AtomicReference<RID> target = new AtomicReference<>();
      final AtomicBoolean armed = new AtomicBoolean();
      final BeforeRecordDeleteListener listener = record -> {
        if (record.getIdentity().equals(target.get()) && armed.compareAndSet(true, false))
          try {
            otherThread.submit(() -> database.transaction(() -> database.deleteRecord(database.lookupByRID(target.get(), true))))
                .get();
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        return true;
      };
      database.getEvents().registerListener(listener);
      try {
        database.transaction(() -> target.set(
            (vertex ? database.newVertex(type) : database.newDocument(type)).set("key", 1).save().getIdentity()));
        armed.set(true);

        final AtomicInteger attempts = new AtomicInteger();
        database.transaction(() -> {
          attempts.incrementAndGet();
          database.command("sql", sql).close();
        }, false, retries);

        assertThat(armed.get()).as("the concurrent delete must have run").isFalse();
        assertThat(attempts.get()).as("the statement must have been retried").isEqualTo(2);
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
          assertThat(rs.next().<Long>getProperty("c")).isZero();
        }
      } finally {
        database.getEvents().unregisterListener(listener);
        otherThread.shutdown();
      }
    }

    @Test
    void documentDeleteLosingTheRaceIsRetried() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.key INTEGER");
      database.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
      runRace("Doc", false, "DELETE FROM Doc WHERE key = 1", 3);
    }

    @Test
    void vertexDeleteLosingTheRaceIsRetried() throws Exception {
      database.command("sql", "CREATE VERTEX TYPE Vtx");
      database.command("sql", "CREATE PROPERTY Vtx.key INTEGER");
      database.command("sql", "CREATE INDEX ON Vtx (key) UNIQUE");
      runRace("Vtx", true, "DELETE FROM Vtx WHERE key = 1", 3);
    }

    @Test
    void theConflictIsRetryableWhenSurfaced() throws Exception {
      database.command("sql", "CREATE DOCUMENT TYPE Doc2");
      final ExecutorService otherThread = Executors.newSingleThreadExecutor();
      try {
        final RID[] rid = new RID[1];
        database.transaction(() -> rid[0] = database.newDocument("Doc2").set("key", 1).save().getIdentity());

        database.begin();
        final Document read = database.lookupByRID(rid[0], true).asDocument();
        otherThread.submit(() -> database.transaction(() -> database.deleteRecord(database.lookupByRID(rid[0], true)))).get();

        final Throwable thrown = catchThrowable(() -> database.deleteRecord(read));
        database.rollback();
        assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
      } finally {
        otherThread.shutdown();
      }
    }
  }

  /**
   * Issue #8985: concurrent read-modify-write transactions on vertices that grow past one page lost updates with the
   * disjoint-slot merge on: two committed transactions wrote the same {@code n} after reading the same version.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Tag("slow")
  @Nested
  class LostUpdateGrowingVertices extends TestHelper {
    private static final int RECORDS            = 40;
    private static final int THREADS            = 12;
    private static final int UPDATES_PER_THREAD = 300;
    private static final int ROUNDS             = 5;

    /**
     * The window the stress below hits, made deterministic: a record taken for modification, another transaction commits a
     * change to it, and only then is the first one saved. A record that spans pages used to slip through: the pin took the
     * newer head page, the commit-time checks compared that page with itself, and the older value overwrote the change.
     */
    @Test
    void aMultiPageRecordModifiedBeforeAConcurrentCommitIsRefusedAtSave() {
      database.getSchema().createDocumentType("Big");
      final RID[] rid = new RID[1];
      database.transaction(
          () -> rid[0] = database.newDocument("Big").set("n", 0).set("s", "x".repeat(150_000)).save().getIdentity());

      database.begin();
      try {
        final MutableDocument mine = database.lookupByRID(rid[0], true).asDocument().modify();

        final Thread other = new Thread(() -> database.transaction(() -> {
          final MutableDocument d = database.lookupByRID(rid[0], true).asDocument().modify();
          d.set("n", 1);
          d.set("s", d.getString("s") + "y");
          d.save();
        }));
        other.start();
        try {
          other.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        mine.set("n", mine.getInteger("n") + 1);
        assertThatThrownBy(() -> {
          mine.save();
          database.commit();
        }).isInstanceOf(ConcurrentModificationException.class);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("n")).isEqualTo(1);
    }

    @Test
    void noCommittedUpdateIsLost() throws Exception {
      database.getSchema().createVertexType("V");

      for (int round = 0; round < ROUNDS; round++) {
        final String type = "V";
        final String chunk = "y".repeat(2000);
        final RID[] rids = new RID[RECORDS];
        database.transaction(() -> {
          for (int i = 0; i < RECORDS; i++)
            rids[i] = database.newVertex(type).set("n", 0).set("s", "").save().getIdentity();
        });

        final AtomicIntegerArray committed = new AtomicIntegerArray(RECORDS);
        final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
          final int seed = t + round * 100;
          futures.add(pool.submit(() -> {
            final Random random = new Random(seed);
            for (int u = 0; u < UPDATES_PER_THREAD; u++) {
              final int i = random.nextInt(RECORDS);
              database.transaction(() -> {
                final MutableVertex v = database.lookupByRID(rids[i], true).asVertex().modify();
                v.set("n", v.getInteger("n") + 1);
                v.set("s", v.getString("s") + chunk);
                v.save();
              }, false, 100_000);
              committed.incrementAndGet(i);
            }
          }));
        }
        for (final Future<?> f : futures)
          f.get();
        pool.shutdown();

        int lost = 0;
        for (int i = 0; i < RECORDS; i++) {
          final Vertex v = database.lookupByRID(rids[i], true).asVertex();
          final int n = v.getInteger("n");
          if (n != committed.get(i) || v.getString("s").length() != n * chunk.length())
            lost += committed.get(i) - n;
        }
        assertThat(lost).as("round " + round + ": updates committed but not reflected in the data").isZero();

        database.transaction(() -> database.command("sql", "DELETE FROM V"));
      }
    }
  }
}
