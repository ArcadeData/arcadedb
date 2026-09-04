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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5279 (2nd round): after concurrent INSERTs stopped fighting for the same slot, the reporter hit the same
 * page-level {@link ConcurrentModificationException} on concurrent UPDATEs of DIFFERENT records that merely share a
 * bucket page. The disjoint-slot merge (#5381) already replayed a same-or-smaller in-place overwrite, but an update
 * that makes the record GROW re-flowed the page content and therefore poisoned the page - and a grown record is the
 * normal case (setting a longer string, adding a property).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5279ConcurrentUpdateTest extends BucketPageLayoutTestSupport {
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
      throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.get(0), errors.get(0));

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
      throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.get(0), errors.get(0));

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
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.get(0), errors.get(0));

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
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.get(0), errors.get(0));

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
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.get(0), errors.get(0));

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
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.get(0), errors.get(0));

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
