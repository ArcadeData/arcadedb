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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7933: a transaction that fails the MVCC page-version check must leave nothing behind in
 * an {@code LSM_SPARSE_VECTOR} index.
 * <p>
 * {@code TransactionContext.commit1stPhase()} replays the queued index operations BEFORE it validates the page
 * versions, so a transaction that then loses the check has already applied every one of its index operations. For an
 * ordinary LSM index that is harmless - the replay only writes transaction-local pages, which the rollback discards.
 * {@link LSMSparseVectorIndex} is different: its postings travel the append-only lane straight into the engine's
 * shared {@link Memtable}, an instance field of {@link PaginatedSparseVectorEngine} that every transaction on the
 * index writes to and that no rollback can reach.
 * <p>
 * A phantom posting from a rolled-back insert surfaces in {@code topK}. A phantom TOMBSTONE from a rolled-back
 * delete or vector rewrite is worse: the scorer reads a tombstone-aligned cursor as a whole-document delete, so a
 * live document silently vanishes from every query mentioning that dim. Neither is transient - the memtable seals
 * itself into a {@code .sparseseg} segment on its own schedule, which bakes an aborted transaction's work
 * permanently into the on-disk index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7933AbortedTransactionSparseVectorLeakTest {
  private static final String DB_ROOT    = "target/test-databases/Issue7933AbortedTransactionSparseVectorLeakTest";
  private static final int    DIMENSIONS = 64;
  private static final int    NUM_DOCS   = 20;
  private static final String TYPE_NAME  = "SparseDoc";
  private static final String IDX_NAME   = "SparseDoc[tokens,weights]";

  /**
   * The bound is a hang detector, not a latency bound: the concurrent commit contends with one open transaction on
   * one record, so a run that needs anywhere near this long has stopped making progress. A wide bound cannot turn a
   * passing run red, and a diagnostic failure beats a thread parked forever on join().
   */
  private static final long CONCURRENT_COMMIT_TIMEOUT_MS = 60_000;

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
   * The issue's own reproduction, and the damaging half of it: the losing transaction REWRITES the sparse vector, so
   * its replay tombstones every dim of the old vector before it puts the new ones. The tombstones are what make a
   * live, unmodified document disappear from a query that mentions one of its own dims.
   */
  @Test
  void aConflictedSparseRewriteLeavesNoTombstoneBehind() throws Exception {
    withDatabase(db -> {
      final RID target = ridOf(db, 1);

      final long postingsBefore = totalPostings(db);

      db.begin();
      target.asDocument(true).modify().set("tokens", dims(3)).set("weights", weights(3)).save();

      commitInAnotherThread(db, target);
      assertConflictOnCommit(db);

      assertThat(totalPostings(db))
          .as("a transaction that never committed must not add postings to - nor tombstone anything in - the "
              + "engine's shared memtable")
          .isEqualTo(postingsBefore);

      assertThat(topKRids(db, dims(1), unitWeights(1), 5))
          .as("the live document must remain searchable by its OWN dims: before the fix the aborted rewrite had "
              + "already tombstoned them, and the scorer reads a tombstone as a whole-document delete")
          .contains(target);

      assertThat(topKRids(db, dims(3), unitWeights(3), 5))
          .as("nor may the vector the aborted transaction tried to write be searchable")
          .doesNotContain(target);
    });
  }

  /** A rolled-back DELETE must not take the document out of the index. */
  @Test
  void aConflictedDeleteLeavesTheDocumentSearchable() throws Exception {
    withDatabase(db -> {
      final RID target = ridOf(db, 2);

      final long postingsBefore = totalPostings(db);

      db.begin();
      target.asDocument(true).modify().delete();

      commitInAnotherThread(db, target);
      assertConflictOnCommit(db);

      assertThat(totalPostings(db))
          .as("the aborted delete must leave no tombstone in the shared memtable")
          .isEqualTo(postingsBefore);
      assertThat(topKRids(db, dims(2), unitWeights(2), 5))
          .as("the document was never deleted, so it must still be found by its own dims")
          .contains(target);
    });
  }

  /**
   * The reason this could not be fixed with the in-memory journal of #7931: the memtable can seal itself into a
   * {@code .sparseseg} segment at any moment, and nothing that only reaches the memtable can reach a sealed segment.
   * A flush right after the abort is what turns a transient leak into a permanent one, so the fix has to be that the
   * aborted work never entered the memtable at all.
   */
  @Test
  void anAbortedTransactionsWorkCannotBeSealedIntoASegment() throws Exception {
    withDatabase(db -> {
      final RID target = ridOf(db, 3);

      db.begin();
      target.asDocument(true).modify().set("tokens", dims(5)).set("weights", weights(5)).save();

      commitInAnotherThread(db, target);
      assertConflictOnCommit(db);

      // Seal whatever the memtable holds. Everything below is then answered from the on-disk segment.
      flushAll(db);

      assertThat(topKRids(db, dims(3), unitWeights(3), 5))
          .as("the document must survive a flush that happens after the aborted rewrite")
          .contains(target);
      assertThat(topKRids(db, dims(5), unitWeights(5), 5))
          .as("and the vector no transaction ever committed must not be baked into the sealed segment")
          .doesNotContain(target);
    });
  }

  /**
   * The other side of the contract: deferring the memtable write must not LOSE the postings of a transaction that
   * does commit, and a commit that follows an abort on the same record has to publish exactly its own work.
   */
  @Test
  void theRetryAfterTheConflictPublishesExactlyItsOwnPostings() throws Exception {
    withDatabase(db -> {
      final RID target = ridOf(db, 4);

      db.begin();
      target.asDocument(true).modify().set("tokens", dims(7)).set("weights", weights(7)).save();
      commitInAnotherThread(db, target);
      assertConflictOnCommit(db);

      // The retry every conflict-aware workload performs.
      db.transaction(() -> target.asDocument(true).modify().set("tokens", dims(7)).set("weights", weights(7)).save());

      assertThat(topKRids(db, dims(7), unitWeights(7), 5))
          .as("the committed rewrite must be searchable by its new dims")
          .contains(target);
      assertThat(topKRids(db, dims(4), unitWeights(4), 5))
          .as("and no longer by the dims it replaced")
          .doesNotContain(target);
    });
  }

  /**
   * The one arm of {@code concludePhase2} that reaches {@code reset()} without durable changes, found in the review
   * of PR #7934. A commit refused because the database was fenced by an EARLIER failure appended nothing of its own,
   * so its buffered postings must be dropped rather than published - which is why that branch runs
   * {@code undoIndexReplay()} itself, instead of letting {@code reset()} take the publish route every other
   * non-rollback conclusion takes. It is durability, not the route, that decides which conclusion applies.
   * <p>
   * Driven through {@code commit1stPhase}/{@code commit2ndPhase} rather than {@code commit()} because that is the
   * only shape that reaches the branch. {@code commit()} is refused at the door on a fenced database - the fence is
   * one of the conditions {@code checkDatabaseIsOpen()} rejects - so the branch exists for a fence raised by a
   * CONCURRENT transaction while this one was already in phase 1, past that check and past its index replay. The
   * split call reproduces exactly that window, which is what the replication layer does routinely.
   */
  @Test
  void aCommitRefusedByTheRecoveryFencePublishesNothing() throws Exception {
    withDatabase(db -> {
      final RID target = ridOf(db, 6);
      // Resolved BEFORE the fence: every schema lookup is refused once the database is fenced, so the engines have
      // to be in hand already. Reading their posting counts does not go through the database at all.
      final List<LSMSparseVectorIndex> indexes = subIndexes(db);
      final long postingsBefore = totalPostings(indexes);

      db.begin();
      target.asDocument(true).modify().set("tokens", dims(9)).set("weights", weights(9)).save();

      final TransactionContext tx = ((DatabaseInternal) db).getTransaction();
      // Phase 1 replays the queued postings into the buffer; nothing of this transaction is durable yet.
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      assertThat(phase1).as("the transaction must have changes to publish, or the branch is never reached").isNotNull();

      ((LocalDatabase) ((DatabaseInternal) db).getEmbedded()).fenceForRecovery("issue #7933 regression test");

      try {
        tx.commit2ndPhase(phase1);
        throw new AssertionError("the 2nd phase must be refused: the database is fenced for recovery");
      } catch (final TransactionException expected) {
        // THE PRECONDITION UNDER TEST: REFUSED BEFORE ITS OWN WAL APPEND, PAST ITS INDEX REPLAY
      }

      assertThat(totalPostings(indexes))
          .as("a commit that appended nothing is not durable, so its postings must be dropped like a rollback's - "
              + "publishing them would bake a transaction that never committed into the index the reopen recovers")
          .isEqualTo(postingsBefore);
    });
  }

  // ---------- harness ----------

  @FunctionalInterface
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
          try {
            db.drop();
          } catch (final Exception dropRefused) {
            // A database fenced for recovery refuses the operations drop() needs; closing it is all that is left,
            // and the files go with the temporary directory in tearDown(). Only the fence test can reach this.
            db.close();
          }
      }
    }
  }

  private static void populate(final Database db) {
    db.transaction(() -> {
      final DocumentType type = db.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("id", Type.INTEGER);
      type.createProperty("name", Type.STRING);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      db.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType().withDimensions(DIMENSIONS).create();
    });

    db.transaction(() -> {
      for (int i = 0; i < NUM_DOCS; i++)
        db.newDocument(TYPE_NAME).set("id", i).set("name", "doc" + i)
            .set("tokens", dims(i)).set("weights", weights(i)).save();
    });
  }

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

  /** Disjoint 3-dim blocks, so every document's dims are its own and a query names exactly one document's vector. */
  private static int[] dims(final int id) {
    final int base = (id * 3) % DIMENSIONS;
    return new int[] { base, (base + 1) % DIMENSIONS, (base + 2) % DIMENSIONS };
  }

  private static float[] weights(final int id) {
    return new float[] { 1.0f, 0.9f - (id % 5) * 0.1f, 0.8f };
  }

  private static float[] unitWeights(final int id) {
    final float[] w = new float[dims(id).length];
    java.util.Arrays.fill(w, 1.0f);
    return w;
  }

  private static List<RID> topKRids(final Database db, final int[] tokens, final float[] weights, final int k) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, tokens, weights, k)) {
      while (rs.hasNext())
        rids.add(rs.next().getProperty("@rid"));
    }
    return rids;
  }

  private static RID ridOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM " + TYPE_NAME + " WHERE id = ?", id)) {
      assertThat(rs.hasNext()).as("record %d must exist", id).isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  /**
   * Summed over every bucket sub-index, not read off one of them: the type spans several buckets, each with its own
   * engine and its own memtable, and which bucket a record landed in is not something the test gets to choose.
   */
  private static long totalPostings(final Database db) {
    return totalPostings(subIndexes(db));
  }

  private static long totalPostings(final List<LSMSparseVectorIndex> indexes) {
    long total = 0;
    for (final LSMSparseVectorIndex idx : indexes)
      total += idx.getEngine().totalPostings();
    return total;
  }

  private static void flushAll(final Database db) {
    for (final LSMSparseVectorIndex idx : subIndexes(db))
      idx.getEngine().flush();
  }

  private static List<LSMSparseVectorIndex> subIndexes(final Database db) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName(IDX_NAME);
    final List<LSMSparseVectorIndex> indexes = new ArrayList<>();
    for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
      indexes.add((LSMSparseVectorIndex) idx);
    return indexes;
  }
}
