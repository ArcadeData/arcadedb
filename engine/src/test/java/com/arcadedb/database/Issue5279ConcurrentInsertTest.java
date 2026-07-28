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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue5279ConcurrentInsertTest extends TestHelper {
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

    database.transaction(
        () -> assertThat(database.countType("Case", false)).isEqualTo(1L + (long) threadCount * insertsPerThread));
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
