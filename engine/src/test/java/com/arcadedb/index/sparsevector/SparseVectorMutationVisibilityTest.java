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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for sparse vector index visibility across writes, aborts, updates and builds. Each nested class keeps the fixture of the issue it covers.
 */
class SparseVectorMutationVisibilityTest {

  /**
   * Regression for #7933: a transaction that fails the MVCC page-version check must leave nothing behind in
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
  @Nested
  class AbortedTransactionSparseVectorLeak {
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

    /**
     * {@code kill()} drops the registration instead of concluding it, and a LATER {@code reset()} of the same context
     * must therefore publish nothing. That asymmetry with {@code rollback()} is deliberate - {@code kill()}'s only
     * caller is a crash simulation, and a crash reverses nothing in memory - but the context outlives the kill where
     * the index instances do not, so it is the one place the drop has to be exact.
     * <p>
     * Pins the mechanism on the sparse index because its state is observable: an eagerly-published LSM_VECTOR replay
     * is dropped by the same line, and is unobservable afterwards for precisely the reason the drop is safe - the
     * simulation closes the schema, discarding the index instance that held it.
     */
    @Test
    void aKilledTransactionPublishesNothingWhenItsContextIsLaterReset() throws Exception {
      withDatabase(db -> {
        final RID  target = ridOf(db, 8);
        final long postingsBefore = totalPostings(db);

        db.begin();
        target.asDocument(true).modify().set("tokens", dims(11)).set("weights", weights(11)).save();

        final TransactionContext tx = ((DatabaseInternal) db).getTransaction();
        // Phase 1 is what registers the buffer: before it, there is nothing to drop.
        assertThat(tx.commit1stPhase(true)).isNotNull();
        assertThat(totalPostings(db))
            .as("the replay defers, so nothing is in the memtable yet")
            .isEqualTo(postingsBefore);

        tx.kill();

        // The reuse the drop exists for. reset() is the publish route every non-rollback conclusion takes, so if the
        // kill had left the registration behind, this is where a killed transaction's postings would surface.
        tx.reset();

        assertThat(totalPostings(db))
            .as("a killed transaction is abandoned, so a later reset() of its reused context must publish nothing")
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
          // Gated on the CONDITION, not on catching whatever drop() throws: a fenced database refuses the operations
          // drop() needs, so the fence test can only close and leave the files to tearDown() - but a drop that fails
          // in any of the other tests is a leaked database, and swallowing it here would hide exactly that.
          if (db.isOpen()) {
            if (((DatabaseInternal) db).getEmbedded() instanceof LocalDatabase local && local.isFencedForRecovery())
              db.close();
            else
              db.drop();
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
      Arrays.fill(w, 1.0f);
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

  /**
   * Regression for #7966: a sparse-vector search issued inside the transaction that wrote the row did not
   * see it.
   * <p>
   * Writes queue on {@code TransactionIndexContext} and reach the engine's memtable only at commit replay, while
   * {@code PaginatedSparseVectorEngine.topK} reads "an atomic snapshot of the current memtable + segments". So a
   * caller that wrote a sparse vector and ran {@code vector.sparseNeighbors} in the same transaction got nothing
   * back - and, as with the dense index in issue #7378, got FEWER results rather than an error, which an application
   * cannot tell apart from "no match".
   * <p>
   * The property every test here pins is the one #7378 established for the dense index: <b>a search inside the
   * transaction returns what the same search returns right after the commit.</b> That is what makes it a contract
   * rather than a heuristic, and it covers the update and delete cases as well as the insert one - a row the
   * transaction re-weighted must be scored at its new weight, and one it deleted must be gone.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SparseSearchReadsOwnWrites extends TestHelper {

    private static final int    DIMENSIONS = 128;
    private static final String TYPE_NAME  = "SparseDoc";
    private static final String IDX_NAME   = "SparseDoc[tokens,weights]";

    @Test
    void aRowInsertedInTheTransactionIsFoundByTheSameTransaction() {
      createSchema();
      // A committed corpus to search against, so the answer is a ranking and not just "the one row".
      seed(20);

      final Map<RID, Float> inside = new LinkedHashMap<>();
      final RID[] created = new RID[1];
      database.transaction(() -> {
        created[0] = newDoc(new int[] { 3, 9, 27 }, new float[] { 0.8f, 0.6f, 0.4f }).save().getIdentity();
        inside.putAll(search(new int[] { 3, 9, 27 }, new float[] { 1f, 1f, 1f }, 5));
      });

      assertThat(inside)
          .as("the row this transaction inserted must be visible to its own search")
          .containsKey(created[0]);
      assertThat(inside.get(created[0])).isCloseTo(1.8f, Offset.offset(1e-4f));
      assertAgrees(inside, search(new int[] { 3, 9, 27 }, new float[] { 1f, 1f, 1f }, 5));
    }

    @Test
    void aRowReweightedInTheTransactionIsScoredAtItsNewWeight() {
      createSchema();
      seed(20);

      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = newDoc(new int[] { 5 }, new float[] { 0.2f }).save().getIdentity());

      final Map<RID, Float> inside = new LinkedHashMap<>();
      database.transaction(() -> {
        database.lookupByRID(rid[0], true).asDocument(true).modify().set("weights", new float[] { 0.9f }).save();
        inside.putAll(search(new int[] { 5 }, new float[] { 1f }, 5));
      });

      assertThat(inside.get(rid[0]))
          .as("the search must score the row at the weight this transaction gave it, not the committed one")
          .isCloseTo(0.9f, Offset.offset(1e-4f));
      assertAgrees(inside, search(new int[] { 5 }, new float[] { 1f }, 5));
    }

    @Test
    void aRowDeletedInTheTransactionIsGoneFromTheSameTransaction() {
      createSchema();
      seed(20);

      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = newDoc(new int[] { 11 }, new float[] { 0.95f }).save().getIdentity());

      final Map<RID, Float> inside = new LinkedHashMap<>();
      database.transaction(() -> {
        database.lookupByRID(rid[0], true).asDocument(true).modify().delete();
        inside.putAll(search(new int[] { 11 }, new float[] { 1f }, 5));
      });

      assertThat(inside)
          .as("a row this transaction deleted must not come back from its own search")
          .doesNotContainKey(rid[0]);
      assertThat(indexLevelRids(new int[] { 11 }, new float[] { 1f }, 5))
          .as("nor from the index itself: the SQL layer drops it only because the record lookup fails, which is a "
              + "second line of defence and not the contract")
          .doesNotContain(rid[0]);
      assertAgrees(inside, search(new int[] { 11 }, new float[] { 1f }, 5));
    }

    /**
     * The answer may not depend on whether the function fanned its per-bucket searches out to the scoring pool: a
     * {@code getTransactionIfExists()} on a pool worker answers null, so an overlay resolved inside the index would
     * make the two plans disagree. Ten buckets take the fan-out; one takes the serial path.
     */
    @Test
    void theMultiBucketFanOutAndTheSerialPlanGiveTheSameAnswer() {
      createSchema(10);
      seed(60);

      final Map<RID, Float> fannedOut = new LinkedHashMap<>();
      final RID[] created = new RID[1];
      database.transaction(() -> {
        created[0] = newDoc(new int[] { 2, 4, 8 }, new float[] { 0.7f, 0.7f, 0.7f }).save().getIdentity();
        fannedOut.putAll(search(new int[] { 2, 4, 8 }, new float[] { 1f, 1f, 1f }, 10));
      });

      assertThat(fannedOut)
          .as("the row must be visible however many sub-indexes the function had to search")
          .containsKey(created[0]);
      assertAgrees(fannedOut, search(new int[] { 2, 4, 8 }, new float[] { 1f, 1f, 1f }, 10));
    }

    /**
     * The grouped plan pushes its per-group caps into the DAAT loop, so a pending row cannot simply be appended to
     * its result: the caps have to be re-applied over the union. What must hold is the same invariant.
     */
    @Test
    void theGroupedPlanAlsoSeesTheTransactionsOwnRows() {
      createSchema();
      database.transaction(() -> {
        database.getSchema().getType(TYPE_NAME).createProperty("category", Type.STRING);
      });
      seedWithCategories(20);

      final Map<RID, Float> inside = new LinkedHashMap<>();
      final RID[] created = new RID[1];
      database.transaction(() -> {
        created[0] = newDoc(new int[] { 6, 12 }, new float[] { 0.9f, 0.9f }).set("category", "fresh").save().getIdentity();
        inside.putAll(searchGrouped(new int[] { 6, 12 }, new float[] { 1f, 1f }, 5, 2));
      });

      assertThat(inside)
          .as("the grouped plan must see the row this transaction inserted")
          .containsKey(created[0]);
      assertAgrees(inside, searchGrouped(new int[] { 6, 12 }, new float[] { 1f, 1f }, 5, 2));
    }

    /**
     * The invariant: the search inside the transaction returned the same rows, in the same rank order, at the same
     * scores as the one after the commit.
     * <p>
     * Scores are compared with a tolerance rather than bitwise. The pending row is scored by summing the query's
     * dimensions in query order, while after the commit it is summed by the BMW traversal in the order its terms
     * leave the traversal - the same arithmetic, a different association, so the two can differ by a float ULP
     * (1.8 against 1.8000001 on the very first run of this test). Demanding bit equality would be pinning the
     * summation order of an implementation detail.
     */
    private static void assertAgrees(final Map<RID, Float> inside, final Map<RID, Float> afterCommit) {
      assertThat(inside.keySet())
          .as("a search inside the transaction must return the same rows in the same order as after the commit")
          .containsExactlyElementsOf(afterCommit.keySet());
      for (final Map.Entry<RID, Float> row : afterCommit.entrySet())
        assertThat(inside.get(row.getKey()))
            .as("the score of %s must be the same inside the transaction as after the commit", row.getKey())
            .isCloseTo(row.getValue(), Offset.offset(1e-5f));
    }

    /** What the index itself answers, bypassing the SQL layer's own record-lookup filter. */
    private List<RID> indexLevelRids(final int[] tokens, final float[] weights, final int k) {
      final List<RID> rids = new ArrayList<>();
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
      for (final IndexInternal sub : typeIndex.getIndexesOnBuckets())
        for (final RidScore r : ((LSMSparseVectorIndex) sub).topK(tokens, weights, k, null))
          rids.add(r.rid());
      return rids;
    }

    /**
     * The grouped plan when the distinct-group limit BINDS - the shape raised in the review of PR #8001, where a
     * pending row promotes a group the committed-only pass had ranked out.
     * <p>
     * The two-stage admission really can leave such a group short of its {@code groupSize}: the committed pass runs
     * its own {@code GroupAdmissionState} over the committed universe alone and never returns the members of a group
     * that did not place in ITS top {@code limit}. What this test pins is that the answer is still the SAME one the
     * search gives after the commit, because the committed-only path has that shortfall too - so widening the
     * committed pass here, which was tried, made the in-transaction answer better than the post-commit one and broke
     * the very invariant this class exists to hold. The shortfall is real and is issue #8002; it belongs where the
     * admission is decided, not in the overlay merge.
     */
    @Test
    void theGroupedPlanAgreesWithTheCommitWhenAPendingRowPromotesAGroup() {
      createSchema();
      database.transaction(() -> database.getSchema().getType(TYPE_NAME).createProperty("category", Type.STRING));

      // Three groups on one dimension, ranked far apart: "a" beats "b" beats "c" on the committed side alone.
      database.transaction(() -> {
        newDoc(new int[] { 1 }, new float[] { 0.90f }).set("category", "a").save();
        newDoc(new int[] { 1 }, new float[] { 0.80f }).set("category", "a").save();
        newDoc(new int[] { 1 }, new float[] { 0.70f }).set("category", "b").save();
        newDoc(new int[] { 1 }, new float[] { 0.60f }).set("category", "b").save();
        newDoc(new int[] { 1 }, new float[] { 0.50f }).set("category", "c").save();
        newDoc(new int[] { 1 }, new float[] { 0.40f }).set("category", "c").save();
      });

      final Map<RID, Float> inside = new LinkedHashMap<>();
      database.transaction(() -> {
        // A pending row that makes "c" the BEST group, which the committed-only pass had ranked third of three.
        newDoc(new int[] { 1 }, new float[] { 0.99f }).set("category", "c").save();
        // limit 2 groups of 2: the union's answer is c{0.99, 0.50} and a{0.90, 0.80}. The committed pass, run over
        // the committed universe alone with limit 2, returns a and b - so c's 0.50 row never reaches the merge.
        inside.putAll(searchGrouped(new int[] { 1 }, new float[] { 1f }, 2, 2));
      });

      assertThat(inside)
          .as("the row this transaction inserted must lead: it is the best score in the corpus")
          .isNotEmpty();
      assertAgrees(inside, searchGrouped(new int[] { 1 }, new float[] { 1f }, 2, 2));
    }

    /**
     * A caller that asks for more rows than the over-fetch ceiling, and narrows nothing, still gets them all.
     * <p>
     * The ceiling bounds the multiplier the over-fetch applies to compensate for a selective filter, never the
     * caller's own {@code k}. Capping {@code k} made this method quietly return at most 100,000 rows to a caller
     * that asked for more and would previously have got them - a truncation with no error to notice it by. Found in
     * the review of PR #8001, where the overflow guard introduced it.
     */
    @Test
    void aKAboveTheOverFetchCeilingIsNotTruncated() {
      createSchema();
      seed(30);

      // k far above the ceiling, no filter and no overlay: every indexed row must come back, not a capped slice.
      final List<RID> all = indexLevelRids(new int[] { 1, 6, 11, 16, 21, 26 }, new float[] { 1f, 1f, 1f, 1f, 1f, 1f },
          200_000);
      assertThat(all)
          .as("a k above the over-fetch ceiling must not be silently capped by it")
          .isNotEmpty();

      // The same query at a k the ceiling cannot reach answers with no more rows, which is what makes the assertion
      // above about the CEILING rather than about the corpus being small.
      assertThat(indexLevelRids(new int[] { 1, 6, 11, 16, 21, 26 }, new float[] { 1f, 1f, 1f, 1f, 1f, 1f }, 50))
          .as("and the corpus is smaller than the ceiling, so both answers are the whole matching set")
          .hasSameSizeAs(all);
    }

    /**
     * Pending rows must reach group admission in full, not pre-truncated by global score.
     * <p>
     * The overlay's own top-k was taken at {@code limit * groupSize} BEFORE the caps were applied, so a group whose
     * rows all rank below another group's surplus never reached the admission pass at all: with {@code limit=2},
     * {@code groupSize=1} and pending rows A:0.99, A:0.98, B:0.50, the overlay returned both A rows, admission kept
     * one and rejected the other, and B was never offered - while the same search after the commit returns A and B.
     * Found by CodeRabbit on PR #8001.
     */
    @Test
    void pendingRowsOfALowerScoringGroupStillReachGroupAdmission() {
      createSchema();
      database.transaction(() -> database.getSchema().getType(TYPE_NAME).createProperty("category", Type.STRING));

      final Map<RID, Float> inside = new LinkedHashMap<>();
      database.transaction(() -> {
        newDoc(new int[] { 1 }, new float[] { 0.99f }).set("category", "a").save();
        newDoc(new int[] { 1 }, new float[] { 0.98f }).set("category", "a").save();
        newDoc(new int[] { 1 }, new float[] { 0.50f }).set("category", "b").save();
        // Two groups of one: A's surplus row must not crowd B out of the answer.
        inside.putAll(searchGrouped(new int[] { 1 }, new float[] { 1f }, 2, 1));
      });

      assertThat(inside.values())
          .as("one row from each group, not two from the better-scoring one")
          .containsExactly(0.99f, 0.50f);
      assertAgrees(inside, searchGrouped(new int[] { 1 }, new float[] { 1f }, 2, 1));
    }

    // ---------- helpers ----------

    private void createSchema() {
      createSchema(1);
    }

    private void createSchema(final int buckets) {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, buckets);
        type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
        type.createProperty("weights", Type.ARRAY_OF_FLOATS);
        database.getSchema()
            .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
            .withSparseVectorType()
            .withDimensions(DIMENSIONS)
            .create();
      });
      // The fan-out must be reachable in this test, whatever the machine's core count would otherwise decide.
      database.getConfiguration().setValue(GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS, 4);
    }

    private void seed(final int docs) {
      database.transaction(() -> {
        for (int i = 0; i < docs; i++)
          newDoc(dims(i), weights()).save();
      });
    }

    private void seedWithCategories(final int docs) {
      database.transaction(() -> {
        for (int i = 0; i < docs; i++)
          newDoc(dims(i), weights()).set("category", "c" + (i % 4)).save();
      });
    }

    private MutableDocument newDoc(final int[] tokens, final float[] weights) {
      return database.newDocument(TYPE_NAME).set("tokens", tokens).set("weights", weights);
    }

    private static int[] dims(final int seed) {
      final int[] d = new int[3];
      for (int i = 0; i < d.length; i++)
        d[i] = (seed * 5 + i * 13 + 1) % DIMENSIONS;
      java.util.Arrays.sort(d);
      for (int i = 1; i < d.length; i++)
        if (d[i] <= d[i - 1])
          d[i] = d[i - 1] + 1;
      return d;
    }

    private static float[] weights() {
      return new float[] { 0.3f, 0.25f, 0.2f };
    }

    /** RID to score, in rank order, so a comparison catches a reordering as well as a missing row. */
    private Map<RID, Float> search(final int[] tokens, final float[] weights, final int k) {
      return collect(database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
          IDX_NAME, tokens, weights, k));
    }

    private Map<RID, Float> searchGrouped(final int[] tokens, final float[] weights, final int k, final int groupSize) {
      final Map<String, Object> options = new LinkedHashMap<>();
      options.put("groupBy", "category");
      options.put("groupSize", groupSize);
      return collect(database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, ?))",
          IDX_NAME, tokens, weights, k, options));
    }

    private static Map<RID, Float> collect(final ResultSet rs) {
      final Map<RID, Float> out = new LinkedHashMap<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.put(r.getProperty("@rid"), ((Number) r.getProperty("score")).floatValue());
      }
      return out;
    }
  }

  /**
   * Regression for #9343: an UPDATE of the {@code tokens} and {@code weights} of a document reaches the
   * sparse index as a remove of the old postings followed by a put of the new ones for the same RID. The tombstone of
   * an OLD dim used to be read by the scorer as a delete of the whole RID, so the document vanished from every query
   * that mentioned both one of its new dims and one of its old ones.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SparseUpdateTombstone {
    private static final String DB_PATH = "target/test-databases/Issue9343SparseUpdateTombstoneTest";

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void anUpdatedDocumentAnswersOnItsNewDimsWhateverTheQueryMixes() {
      for (final boolean flushBeforeUpdate : new boolean[] { false, true })
        for (final boolean viaUpdate : new boolean[] { true, false }) {
          FileUtils.deleteRecursively(new File(DB_PATH));
          try (final Database db = new DatabaseFactory(DB_PATH).create()) {
            db.command("sql", "CREATE DOCUMENT TYPE Doc");
            db.command("sql", "CREATE PROPERTY Doc.id LONG");
            db.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
            db.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
            db.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR METADATA {\"dimensions\": 10}");
            db.transaction(() -> {
              db.newDocument("Doc").set("id", 0L).set("tokens", new int[] { 0, 1 }).set("weights", new float[] { 1f, 1f }).save();
              db.newDocument("Doc").set("id", 1L).set("tokens", new int[] { 2, 3 }).set("weights", new float[] { 1f, 1f }).save();
              db.newDocument("Doc").set("id", 2L).set("tokens", new int[] { 4, 5 }).set("weights", new float[] { 1f, 1f }).save();
            });
            if (flushBeforeUpdate)
              flushSparseIndexes(db);

            db.begin();
            if (viaUpdate)
              db.command("sql", "UPDATE Doc SET tokens = :t, weights = :w WHERE id = 1",
                  Map.of("t", new int[] { 6, 7 }, "w", new float[] { 1f, 1f })).close();
            else {
              db.command("sql", "DELETE FROM Doc WHERE id = 1").close();
              db.newDocument("Doc").set("id", 1L).set("tokens", new int[] { 6, 7 }).set("weights", new float[] { 1f, 1f }).save();
            }
            db.commit();

            final String how = (viaUpdate ? "update" : "delete+insert") + (flushBeforeUpdate ? ", flushed" : ", memtable");
            for (int round = 0; round < 2; round++) {
              assertThat(ask(db, new int[] { 6 }, new float[] { 1f })).as(how + " {6}").containsExactly(1L);
              assertThat(ask(db, new int[] { 2 }, new float[] { 1f })).as(how + " {2}").isEmpty();
              assertThat(ask(db, new int[] { 6, 2 }, new float[] { 1f, 1f })).as(how + " {6,2}").containsExactly(1L);
              assertThat(ask(db, new int[] { 7, 3 }, new float[] { 1f, 1f })).as(how + " {7,3}").containsExactly(1L);
              assertThat(ask(db, new int[] { 6, 7 }, new float[] { 1f, 1f })).as(how + " {6,7}").containsExactly(1L);
              assertThat(ask(db, new int[] { 6, 3, 0 }, new float[] { 1f, 1f, 1f })).as(how + " {6,3,0}").containsExactlyInAnyOrder(0L, 1L);
              // a second round after a flush of the update itself, so the tombstones are read from a sealed segment
              flushSparseIndexes(db);
            }
          }
        }
    }

    @Test
    void aDeletedDocumentStillVanishesFromEveryQuery() {
      try (final Database db = new DatabaseFactory(DB_PATH).create()) {
        db.command("sql", "CREATE DOCUMENT TYPE Doc");
        db.command("sql", "CREATE PROPERTY Doc.id LONG");
        db.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
        db.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
        db.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR METADATA {\"dimensions\": 10}");
        db.transaction(() -> {
          db.newDocument("Doc").set("id", 0L).set("tokens", new int[] { 0, 1 }).set("weights", new float[] { 1f, 1f }).save();
          db.newDocument("Doc").set("id", 1L).set("tokens", new int[] { 2, 3 }).set("weights", new float[] { 1f, 1f }).save();
        });
        flushSparseIndexes(db);
        db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id = 1").close());

        assertThat(ask(db, new int[] { 2 }, new float[] { 1f })).isEmpty();
        assertThat(ask(db, new int[] { 3 }, new float[] { 1f })).isEmpty();
        assertThat(ask(db, new int[] { 2, 3, 0 }, new float[] { 1f, 1f, 1f })).containsExactly(0L);
      }
    }

    /**
     * Many documents whose dims are rewritten at random, against a model kept by the test: every document must answer
     * exactly on the dims it holds now, whatever mix of live and tombstoned dims a query names, in the memtable and
     * once sealed into segments.
     */
    @Test
    void randomUpdatesAnswerLikeTheModel() {
      final Random random = new Random(9343);
      final int docs = 300;
      final int dims = 40;
      try (final Database db = new DatabaseFactory(DB_PATH).create()) {
        db.command("sql", "CREATE DOCUMENT TYPE Doc");
        db.command("sql", "CREATE PROPERTY Doc.id LONG");
        db.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
        db.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
        db.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR METADATA {\"dimensions\": " + dims + "}");
        final Map<Long, Map<Integer, Float>> model = new HashMap<>();
        db.transaction(() -> {
          for (long id = 0; id < docs; id++) {
            final Map<Integer, Float> vector = randomVector(random, dims);
            model.put(id, vector);
            db.newDocument("Doc").set("id", id).set("tokens", dimsOf(vector)).set("weights", weightsOf(vector)).save();
          }
        });
        for (int round = 0; round < 3; round++) {
          db.transaction(() -> {
            for (int u = 0; u < 100; u++) {
              final long id = random.nextInt(docs);
              final Map<Integer, Float> vector = randomVector(random, dims);
              model.put(id, vector);
              db.command("sql", "UPDATE Doc SET tokens = :t, weights = :w WHERE id = :id",
                  Map.of("t", dimsOf(vector), "w", weightsOf(vector), "id", id)).close();
            }
          });
          if (round == 1)
            flushSparseIndexes(db);

          for (int q = 0; q < 25; q++) {
            final Map<Integer, Float> query = randomVector(random, dims);
            final Map<Long, Float> expected = new HashMap<>();
            for (final var doc : model.entrySet()) {
              float score = 0f;
              for (final var term : query.entrySet())
                score += term.getValue() * doc.getValue().getOrDefault(term.getKey(), 0f);
              if (score > 0f)
                expected.put(doc.getKey(), score);
            }
            final List<Long> got = ask(db, dimsOf(query), weightsOf(query), docs);
            assertThat(got).as("round " + round + " query " + query).containsExactlyInAnyOrderElementsOf(expected.keySet());
          }
        }
      }
    }

    private static Map<Integer, Float> randomVector(final Random random, final int dims) {
      final Map<Integer, Float> vector = new HashMap<>();
      final int size = 1 + random.nextInt(6);
      while (vector.size() < size)
        vector.put(random.nextInt(dims), 1f + random.nextInt(4));
      return vector;
    }

    private static int[] dimsOf(final Map<Integer, Float> vector) {
      return vector.keySet().stream().mapToInt(Integer::intValue).toArray();
    }

    private static float[] weightsOf(final Map<Integer, Float> vector) {
      final float[] weights = new float[vector.size()];
      int i = 0;
      for (final Integer dim : vector.keySet())
        weights[i++] = vector.get(dim);
      return weights;
    }

    private static void flushSparseIndexes(final Database db) {
      final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Doc[tokens,weights]");
      for (final IndexInternal idx : typeIndex.getIndexesOnBuckets())
        ((LSMSparseVectorIndex) idx).getEngine().flush();
    }

    private static List<Long> ask(final Database db, final int[] tokens, final float[] weights) {
      return ask(db, tokens, weights, 10);
    }

    private static List<Long> ask(final Database db, final int[] tokens, final float[] weights, final int k) {
      final List<Long> ids = new ArrayList<>();
      try (final ResultSet rs = db.query("sql", "SELECT id, score FROM (SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?)))",
          "Doc[tokens,weights]", tokens, weights, k)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          ids.add(r.getProperty("id"));
        }
      }
      return ids;
    }
  }

  /**
   * Regression for #9209 and #9210: (a query that starts between a memtable flush's swap and the segment's publication misses the
   * flushed postings) and #9210 (a query that starts while a flush or compaction runs waits for it to finish).
   * <p>
   * Both freeze the engine inside a mutation through its test hook, then query from another thread.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SparseQueryDuringMutation extends TestHelper {

    private static final int DIM = 7;

    private static PaginatedSparseVectorEngine newEngine(final DatabaseInternal db, final String name) {
      return new PaginatedSparseVectorEngine(db, name, SegmentParameters.defaults());
    }

    private static void put(final PaginatedSparseVectorEngine engine, final long from, final long to) {
      for (long i = from; i < to; i++)
        engine.put(DIM, new RID(0, i), 1f);
    }

    private static List<RidScore> query(final PaginatedSparseVectorEngine engine, final int k) throws Exception {
      return engine.topK(new int[] { DIM }, new float[] { 1f }, k);
    }

    /** Runs {@code mutation} on its own thread, frozen at the hook until the returned latch is released. */
    private static final class FrozenMutation {
      final CountDownLatch    inside  = new CountDownLatch(1);
      final CountDownLatch    release = new CountDownLatch(1);
      CompletableFuture<Long> result;

      void hook() {
        inside.countDown();
        try {
          release.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Test
    void queryDuringFlushSeesTheMemtableBeingFlushed() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9209Flush")) {
        put(engine, 0, 50);
        engine.flush(); // one published segment, so the refresh has something to compare against
        put(engine, 50, 100);

        final FrozenMutation frozen = new FrozenMutation();
        engine.setMutationHookForTest(frozen::hook);
        frozen.result = CompletableFuture.supplyAsync(() -> {
          return engine.flush();
        });
        try {
          if (!frozen.inside.await(30, TimeUnit.SECONDS))
            frozen.result.get(1, TimeUnit.SECONDS); // surfaces why the flush never got there
          assertThat(frozen.inside.getCount()).as("the flush reached the hook").isZero();

          // The memtable is swapped out and its segment is not published yet: the answer must still hold all 100, and it
          // must come back while the flush is frozen rather than after it.
          final CompletableFuture<List<RidScore>> q = CompletableFuture.supplyAsync(() -> {
            try {
              return query(engine, 1000);
            } catch (final Exception e) {
              throw new IllegalStateException(e);
            }
          });
          assertThat(q.get(20, TimeUnit.SECONDS)).hasSize(100);
        } finally {
          frozen.release.countDown();
        }
        assertThat(frozen.result.get(30, TimeUnit.SECONDS)).isGreaterThan(0L);
        assertThat(query(engine, 1000)).hasSize(100);
      }
    }

    @Test
    void aFlushThatFailsKeepsItsPostingsForTheNextFlush() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9209Failure")) {
        put(engine, 0, 50);
        engine.setMutationHookForTest(() -> {
          throw new IllegalStateException("injected");
        });
        assertThatThrownBy(engine::flush).isInstanceOf(IllegalStateException.class);
        engine.setMutationHookForTest(null);

        // The postings of the failed flush stay readable, and the next flush persists them.
        put(engine, 100, 120);
        assertThat(query(engine, 1000)).hasSize(70);
        assertThat(engine.flush()).isGreaterThan(0L);
        assertThat(query(engine, 1000)).hasSize(70);
        assertThat(engine.flush()).as("the retry also sealed the live memtable").isEqualTo(-1L);
        assertThat(query(engine, 1000)).hasSize(70);
      }
    }

    @Test
    void aDeleteDuringAFlushHidesThePostingOfTheSealedMemtable() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9209Shadow")) {
        put(engine, 0, 100);
        final FrozenMutation frozen = new FrozenMutation();
        engine.setMutationHookForTest(frozen::hook);
        frozen.result = CompletableFuture.supplyAsync(engine::flush);
        try {
          assertThat(frozen.inside.await(30, TimeUnit.SECONDS)).as("the flush reached the hook").isTrue();
          engine.remove(DIM, new RID(0, 10L)); // lands in the live memtable, newer than the sealed copy of the posting
          assertThat(query(engine, 1000)).hasSize(99).noneMatch(r -> r.rid().equals(new RID(0, 10L)));
        } finally {
          frozen.release.countDown();
        }
        frozen.result.get(30, TimeUnit.SECONDS);
        assertThat(query(engine, 1000)).hasSize(99).noneMatch(r -> r.rid().equals(new RID(0, 10L)));
      }
    }

    @Test
    void maybeFlushDoesNotQueueBehindAFlushInFlight() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9209MaybeFlush")) {
        put(engine, 0, 50);
        final FrozenMutation frozen = new FrozenMutation();
        engine.setMutationHookForTest(frozen::hook);
        frozen.result = CompletableFuture.supplyAsync(engine::flush);
        try {
          assertThat(frozen.inside.await(30, TimeUnit.SECONDS)).as("the flush reached the hook").isTrue();
          put(engine, 50, 55); // far below the threshold: a commit's after-commit callback must not flush it
          final CompletableFuture<Void> callback = CompletableFuture.runAsync(engine::maybeFlush);
          callback.get(20, TimeUnit.SECONDS);
        } finally {
          frozen.release.countDown();
        }
        frozen.result.get(30, TimeUnit.SECONDS);
        assertThat(engine.memtablePostings()).as("the small live memtable was left alone").isEqualTo(5L);
      }
    }

    @Test
    void queryDuringCompactionDoesNotWaitForIt() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9210Compact")) {
        put(engine, 0, 50);
        engine.flush();
        put(engine, 50, 100);
        engine.flush();

        final FrozenMutation frozen = new FrozenMutation();
        engine.setMutationHookForTest(frozen::hook);
        frozen.result = CompletableFuture.supplyAsync(engine::compactAll);
        try {
          assertThat(frozen.inside.await(30, TimeUnit.SECONDS)).as("the compaction reached the hook").isTrue();

          // The compaction registered its output file and holds the mutator lock. The query must answer from the
          // published segments instead of queueing behind it.
          final CompletableFuture<List<RidScore>> q = CompletableFuture.supplyAsync(() -> {
            try {
              return query(engine, 1000);
            } catch (final Exception e) {
              throw new IllegalStateException(e);
            }
          });
          assertThat(q.get(20, TimeUnit.SECONDS)).hasSize(100);
        } finally {
          frozen.release.countDown();
        }
        assertThat(frozen.result.get(30, TimeUnit.SECONDS)).isGreaterThan(0L);
        assertThat(query(engine, 1000)).hasSize(100);
      }
    }
  }

  /**
   * Regression for #7140: {@code Index.countEntries()} promises the number of LIVE entries - "keys deleted but not yet purged by
   * a compaction (tombstones) are NOT counted, so on an LSM index the value drops as records are removed instead of
   * settling on a residual (#5601)". {@link LSMSparseVectorIndex} answered it with
   * {@code PaginatedSparseVectorEngine.totalPostings()}, whose own javadoc says it is the on-disk + in-memory entry
   * count INCLUDING tombstones - exactly the one thing the contract forbids.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SparseVectorLiveCount extends TestHelper {

    /** A tombstone still sitting in the memtable must not be counted, and must not count its masked posting either. */
    @Test
    void memtableTombstonesAreNotCounted() throws IOException {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "Issue7140Memtable")) {
        for (int i = 0; i < 10; i++)
          engine.put(1, new RID(0, i), 0.5f);

        assertThat(engine.livePostings()).isEqualTo(10L);

        for (int i = 0; i < 4; i++)
          engine.remove(1, new RID(0, i));

        assertThat(engine.totalPostings())
            .as("totalPostings stays the sizing metric: the tombstones are still stored")
            .isGreaterThan(6L);
        assertThat(engine.livePostings())
            .as("the live count must DROP as postings are removed, not settle on a residual")
            .isEqualTo(6L);
      }
    }

    /** The tombstone in a newer source has to mask the live posting an older sealed segment still holds. */
    @Test
    void tombstoneInNewerSegmentMasksAnOlderLivePosting() throws IOException {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "Issue7140Segments")) {
        for (int i = 0; i < 6; i++)
          engine.put(7, new RID(0, i), 0.5f);
        engine.flush();
        assertThat(engine.segmentCount()).isEqualTo(1);
        assertThat(engine.livePostings()).isEqualTo(6L);

        // A second sealed segment carrying only tombstones for RIDs the first one holds live
        for (int i = 0; i < 3; i++)
          engine.remove(7, new RID(0, i));
        engine.flush();
        assertThat(engine.segmentCount()).as("the tombstone-only memtable must be sealed, not dropped").isEqualTo(2);

        assertThat(engine.totalPostings())
            .as("both segments still hold their entries on disk")
            .isEqualTo(9L);
        assertThat(engine.livePostings())
            .as("newest-source-wins: the 3 tombstones mask the 3 live postings of the older segment")
            .isEqualTo(3L);
      }
    }

    /** A RID re-added after a delete counts once, not twice: the merge is per (dim, RID), not per stored entry. */
    @Test
    void aRidPresentInSeveralSourcesCountsOnce() throws IOException {
      final DatabaseInternal db = (DatabaseInternal) database;
      try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "Issue7140Shadow")) {
        engine.put(3, new RID(0, 1), 0.5f);
        engine.flush();
        engine.put(3, new RID(0, 1), 0.9f);
        engine.flush();

        assertThat(engine.totalPostings()).isEqualTo(2L);
        assertThat(engine.livePostings()).as("one RID under one dim is one live entry").isEqualTo(1L);
      }
    }

    /** End to end through the index: deleting records must make countEntries() go down. */
    @Test
    void countEntriesOnTheIndexDropsWhenRecordsAreDeleted() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Issue7140Sparse");
        type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
        type.createProperty("weights", Type.ARRAY_OF_FLOATS);
        database.getSchema().buildTypeIndex("Issue7140Sparse", new String[] { "tokens", "weights" })
            .withSparseVectorType().withDimensions(16).create();
      });

      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("Issue7140Sparse")
              .set("tokens", new int[] { i % 16, (i + 1) % 16 })
              .set("weights", new float[] { 0.5f, 0.25f })
              .save();
      });

      final Index index = database.getSchema().getIndexByName("Issue7140Sparse[tokens,weights]");
      assertThat(index.countEntries()).as("2 postings per document").isEqualTo(20L);

      database.transaction(() -> database.command("sql", "delete from Issue7140Sparse limit 4"));

      assertThat(index.countEntries())
          .as("countEntries() must drop as records are deleted, not stay on the tombstone-inclusive total")
          .isEqualTo(12L);
    }

    /**
     * An engine whose size-tiered auto-compaction gate cannot fire under a unit-test workload, so the segment count -
     * and therefore which sources hold a tombstone - stays deterministic between assertions.
     */
    private static PaginatedSparseVectorEngine nonCompactingEngine(final DatabaseInternal db, final String name) {
      return new PaginatedSparseVectorEngine(db, name, SegmentParameters.defaults(),
          /* memtableFlushThreshold */ 1_000_000L,
          /* tierFanout */ 1_000_000,
          /* tierBasePostings */ 1L);
    }
  }

  /**
   * Regression for #11257: a flush that seals the live memtable as it is takes it out of its log's slot. When the node turns out not to be
   * the leader any more, no segment is built: the memtable goes back into its slot, under the postings the slot received since,
   * instead of staying aside where it hid the other log's memtable from every read of a follower.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FlushWithoutLeadership extends TestHelper {
    private static final String TYPE_NAME = "Issue11257Flush";
    private static final String IDX_NAME  = TYPE_NAME + "[tokens,weights]";

    @Test
    void aFlushThatLostTheLeadershipPutsItsMemtableBack() throws Exception {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
        type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
        type.createProperty("weights", Type.ARRAY_OF_FLOATS);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" }).withSparseVectorType()
            .withDimensions(100).create();
      });
      final PaginatedSparseVectorEngine engine =
          ((LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(IDX_NAME)).getIndexesOnBuckets()[0]).getEngine();

      // Written to the engine directly, so not logged: the active log holds nothing and the flush seals the live memtable as it is
      for (int i = 0; i < 5; i++)
        engine.put(1, new RID(0, i), i + 1f);

      final LocalDatabase real = (LocalDatabase) database;
      // A leader as far as the flush's first check goes, which loses the leadership before the segment is built; a posting lands
      // in the memtable that took the place of the one being sealed
      real.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            switch (method.getName()) {
            case "getWrappedDatabaseInstance":
              if (method.getParameterCount() == 0)
                return proxy;
              break;
            case "isReplicated", "isLeader":
              return true;
            case "runWithCompactionReplication":
              engine.put(1, new RID(0, 0), 100f);
              return false;
            default:
              break;
            }
            try {
              return method.invoke(real, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          }));
      try {
        assertThat(engine.flush()).as("no segment without the leadership").isEqualTo(-1L);
      } finally {
        real.setWrappedDatabaseInstance(real);
      }

      assertThat(engine.hasFlushingMemtableForTest()).as("the memtable is back in its slot").isFalse();
      assertThat(engine.segmentCount()).isZero();
      assertRanked(engine);

      // The memtable put back is the live one again: the next flush seals all of it
      assertThat(engine.flush()).isNotEqualTo(-1L);
      assertRanked(engine);
    }

    /** Every posting answers, and the one written after the swap ranks with its newer weight. */
    private static void assertRanked(final PaginatedSparseVectorEngine engine) throws Exception {
      final List<RidScore> top = engine.topK(new int[] { 1 }, new float[] { 1f }, 10);
      assertThat(top).hasSize(5);
      assertThat(top.getFirst().rid()).isEqualTo(new RID(0, 0));
    }
  }

  /**
   * Regression for #10789: the {@code LSM_SPARSE_VECTOR} write path converted the dimension ids of a record with {@code intValue()}, so
   * a list {@code [0, 2.5]} was indexed as dimension 2 and {@code 5000000000} as the wrapped int 705032704, while the record
   * itself kept what it was given. A declared {@code ARRAY_OF_INTEGERS} property converts on write and never reaches that code
   * with a list; a row stored before its property was declared does, when the index is built over it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SparseTokenIndexWholeNumber extends TestHelper {

    @Test
    void buildRefusesARowWhoseDimensionIsNotAWholeNumber() {
      final RID rid = storeRawRow("Frac", "frac", List.of(0, 2.5));

      assertThatThrownBy(() -> createIndex("Frac"))
          .isInstanceOf(IndexException.class)
          .hasMessageContaining(rid.toString())
          .hasMessageContaining("'tokens'")
          .hasMessageContaining("'2.5'");
    }

    @Test
    void buildRefusesARowWhoseDimensionDoesNotFitAnInt() {
      // 5000000000 wraps to the POSITIVE int 705032704: no ">= 0" check could catch it, only the conversion can
      storeRawRow("Wide", "wide", List.of(0, 5_000_000_000L));

      assertThatThrownBy(() -> createIndex("Wide"))
          .isInstanceOf(IndexException.class)
          .hasMessageContaining("'5000000000'");
    }

    @Test
    void buildIndexesAWholeNumberGivenAsADecimal() {
      // 2.0 is the dimension 2, whatever its Java type: refusing it would refuse every JSON client that writes "2.0"
      storeRawRow("Whole", "whole", List.of(0, 2.0d));
      createIndex("Whole");

      assertThat(neighbors("Whole", 2)).containsExactly("whole");
    }

    @Test
    void putRefusesAFractionalDimension() {
      createType("Put");
      createIndex("Put");
      final LSMSparseVectorIndex index = sparseIndex("Put");
      final RID rid = new RID(index.getAssociatedBucketId(), 0L);

      database.transaction(() -> assertThatThrownBy(
          () -> index.put(new Object[] { List.of(0, 2.5), List.of(0.5f, 0.9f) }, new RID[] { rid }))
          .isInstanceOf(IndexException.class)
          .hasMessageContaining(rid.toString()));
    }

    @Test
    void removeRetractsThePostingsATruncatedListWasIndexedWith() {
      // What a record stored before the fix carries: a fractional list, indexed as the truncated dimension. Its delete must
      // not be refused, and it has to retract the very posting the old put() wrote, or the dimension answers a ghost
      createType("Old");
      createIndex("Old");
      final LSMSparseVectorIndex index = sparseIndex("Old");
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Old").set("name", "old").save().getIdentity());
      database.transaction(() -> index.put(new Object[] { new int[] { 0, 2 }, new float[] { 0.5f, 0.9f } }, rid));
      assertThat(neighbors("Old", 2)).containsExactly("old");

      database.transaction(() -> index.remove(new Object[] { List.of(0, 2.5), List.of(0.5f, 0.9f) }, rid[0]));

      assertThat(neighbors("Old", 2)).isEmpty();
    }

    @Test
    void aDeclaredPropertyStillConvertsOnWrite() {
      // The control: a declared ARRAY_OF_INTEGERS converts the list before any index sees it, as an INTEGER property converts
      // 2.5 to 2. That is the schema's conversion rule for every integer property and is left as it is
      createType("Decl");
      createIndex("Decl");
      database.transaction(() -> database.command("sql", "INSERT INTO Decl CONTENT {name:'decl',tokens:[0,2.5],weights:[0.5,0.9]}"));

      assertThat(neighbors("Decl", 2)).containsExactly("decl");
    }

    private RID storeRawRow(final String type, final String name, final List<?> tokens) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      final RID[] rid = new RID[1];
      database.transaction(
          () -> rid[0] = database.newDocument(type).set("name", name, "tokens", tokens, "weights", List.of(0.5f, 0.9f)).save()
              .getIdentity());
      database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
      return rid[0];
    }

    private void createType(final String type) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
    }

    private void createIndex(final String type) {
      database.getSchema().buildTypeIndex(type, new String[] { "tokens", "weights" }).withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR)
          .create();
    }

    private LSMSparseVectorIndex sparseIndex(final String type) {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(type + "[tokens,weights]");
      return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    }

    private List<String> neighbors(final String type, final int dimension) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
          type + "[tokens,weights]", new int[] { dimension }, new float[] { 1.0f }, 10)) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      return names;
    }
  }

  /**
   * Regression for #8536: an {@code LSM_SPARSE_VECTOR} index created (or rebuilt) over a type that already holds rows reported
   * every row in {@code totalIndexed} while {@code vector.sparseNeighbors} answered nothing. The build delegated to the
   * underlying LSM-Tree, which indexed each scanned record into ITSELF - a registration shell postings never enter -
   * so the sparse engine the search reads was left empty.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SparseIndexBuildOverExistingRows extends TestHelper {

    private static final String ROWS = """
        [{name:"a",tokens:[1,5,9],weights:[0.9,0.5,0.1]}, {name:"b",tokens:[1,6],weights:[0.8,0.6]},
         {name:"c",tokens:[2,7],weights:[0.7,0.3]},       {name:"d",tokens:[3,8],weights:[0.2,0.3]},
         {name:"e",tokens:[4,10],weights:[0.2,0.3]},      {name:"f",tokens:[11,12],weights:[0.2,0.3]},
         {name:"g",tokens:[13,14],weights:[0.2,0.3]},     {name:"h",tokens:[15,16],weights:[0.2,0.3]},
         {name:"i",tokens:[17,18],weights:[0.2,0.3]},     {name:"j",tokens:[19,20],weights:[0.2,0.3]}]""";

    /** The control from the issue: the index exists before the rows, so every row reaches it through a live put. */
    @Test
    void indexCreatedBeforeTheRowsAnswers() {
      createType("SpA");
      database.command("sql", "CREATE INDEX ON SpA (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100, modifier: 'IDF' }");
      database.transaction(() -> database.command("sql", "INSERT INTO SpA CONTENT " + ROWS));

      assertThat(neighbors("SpA", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
    }

    /** The reported case: the index is created over rows already stored. */
    @Test
    void indexCreatedOverExistingRowsAnswers() {
      createType("SpD");
      database.transaction(() -> database.command("sql", "INSERT INTO SpD CONTENT " + ROWS));

      try (final ResultSet rs = database.command("sql",
          "CREATE INDEX ON SpD (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100, modifier: 'IDF' }")) {
        assertThat(rs.next().<Number>getProperty("totalIndexed").longValue()).isEqualTo(10L);
      }

      assertThat(neighbors("SpD", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
      assertThat(sparseIndex("SpD").countEntries()).as("every non-zero weight of the 10 rows is a posting").isEqualTo(21L);
    }

    /** Same without the IDF modifier, which the issue also reported broken. */
    @Test
    void indexCreatedOverExistingRowsAnswersWithoutIdf() {
      createType("SpN");
      database.transaction(() -> database.command("sql", "INSERT INTO SpN CONTENT " + ROWS));
      database.command("sql", "CREATE INDEX ON SpN (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100 }");

      assertThat(neighbors("SpN", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
    }

    /** REBUILD INDEX drops and recreates the index, then takes the same build path: it must repopulate the engine. */
    @Test
    void rebuildIndexRepopulatesTheEngine() {
      createType("SpR");
      database.command("sql", "CREATE INDEX ON SpR (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100 }");
      database.transaction(() -> database.command("sql", "INSERT INTO SpR CONTENT " + ROWS));
      assertThat(neighbors("SpR", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");

      try (final ResultSet rs = database.command("sql", "REBUILD INDEX `SpR[tokens,weights]`")) {
        assertThat(rs.next().<Number>getProperty("totalIndexed").longValue()).isEqualTo(10L);
      }

      assertThat(neighbors("SpR", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
      assertThat(sparseIndex("SpR").countEntries()).as("a rebuild must not double the postings").isEqualTo(21L);
    }

    /**
     * A build that spans many commit batches and more than one bucket, then survives a reopen: the postings have to be
     * committed through the transaction like live inserts are, so they are durable and replicated, not parked in a
     * memtable nobody replays.
     */
    @Test
    void buildAcrossBatchesAndBucketsSurvivesReopen() {
      database.command("sql", "CREATE DOCUMENT TYPE SpB BUCKETS 3");
      database.command("sql", "CREATE PROPERTY SpB.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY SpB.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY SpB.name STRING");

      final int docs = 2_000;
      database.transaction(() -> {
        for (int i = 0; i < docs; i++)
          // Every document carries dim 0 at a weight growing with i, plus its own private dim, so the top hit of a
          // query on the private dim is exactly that document.
          database.newDocument("SpB").set("name", "d" + i, "tokens", new int[] { 0, 1 + i },
              "weights", new float[] { 0.001f * (i + 1), 1.0f }).save();
      });

      final TypeIndex typeIndex = database.getSchema().buildTypeIndex("SpB", new String[] { "tokens", "weights" })
          .withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR).withBatchSize(97).create();
      assertThat(typeIndex.getIndexesOnBuckets()).hasSize(3);

      assertThat(neighbors("SpB", new int[] { 0 }, new float[] { 1.0f }, 3)).containsExactly("d1999", "d1998", "d1997");
      assertThat(neighbors("SpB", new int[] { 1 + 1234 }, new float[] { 1.0f }, 1)).containsExactly("d1234");

      reopenDatabase();

      assertThat(neighbors("SpB", new int[] { 0 }, new float[] { 1.0f }, 3)).containsExactly("d1999", "d1998", "d1997");
      assertThat(neighbors("SpB", new int[] { 1 + 1234 }, new float[] { 1.0f }, 1)).containsExactly("d1234");
      long postings = 0;
      for (final Index idx : ((TypeIndex) database.getSchema().getIndexByName("SpB[tokens,weights]")).getIndexesOnBuckets())
        postings += idx.countEntries();
      assertThat(postings).isEqualTo(2L * docs);
    }

    private void createType(final String type) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    }

    private LSMSparseVectorIndex sparseIndex(final String type) {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(type + "[tokens,weights]");
      return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    }

    private List<String> neighbors(final String type, final int[] tokens, final float[] weights, final int k) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
          type + "[tokens,weights]", tokens, weights, k)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          names.add(r.getProperty("name"));
        }
      }
      return names;
    }
  }
}
