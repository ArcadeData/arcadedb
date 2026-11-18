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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Vector searches inside an open transaction. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorTransactionSearchTest {

  /**
   * Regression for #7058: a search inside a transaction does not rebuild the graph synchronously.
   * <p>
   * Regression test for issue #7058: a similarity search issued inside an explicit transaction, after writes to the
   * index, rebuilds the graph synchronously on the calling thread, and the persist step of that rebuild used to commit
   * whatever transaction the thread held - the caller's - and to switch it to WAL-less first. Over Bolt that surfaced
   * as the client's own COMMIT failing with {@code Neo.ClientError.Transaction.TransactionNotFound: Transaction not
   * begun}. The persist now runs in a transaction of its own, nested when the caller holds one, and leaves the
   * caller's untouched.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SearchInsideTransaction {
    private static final String DB_PATH     = "./target/databases/SearchInsideTransaction";
    private static final int    DIMENSIONS  = 16;
    private static final int    NUM_VECTORS = 50;

    @AfterEach
    void cleanUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void searchInsideAnExplicitTransactionLeavesTheTransactionOpenAndUntouched() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      final Random rng = new Random(7058);

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          // Keep the background rebuild machinery out of the picture: the rebuild under test is the synchronous one
          // the first search after a batch of writes performs on the calling thread.
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

          db.transaction(() -> {
            final DocumentType t = db.getSchema().createDocumentType("Doc");
            t.createProperty("id", Type.INTEGER);
            t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          });
          db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
              + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");

          db.transaction(() -> {
            for (int i = 0; i < NUM_VECTORS; i++)
              db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
          });

          final LSMVectorIndex lsm = vectorIndex(db);
          assertThat(lsm.getStats().get("graphNodeCount")).as("precondition: no graph built yet, so the search below rebuilds it")
              .isEqualTo(0L);

          db.begin();
          // Uncommitted work in the caller's transaction, the same shape as the reporter's MERGE before the search.
          db.newDocument("Doc").set("id", NUM_VECTORS).set("embedding", randomVector(rng)).save();
          final boolean useWAL = ((DatabaseInternal) db).getTransaction().isUseWAL();

          assertThat(lsm.findNeighborsFromVector(randomVector(new Random(3)), 5, 64)).isNotEmpty();

          assertThat(db.isTransactionActive())
              .as("the graph rebuild the search triggered must not commit the caller's transaction")
              .isTrue();
          assertThat(((DatabaseInternal) db).getTransaction().isUseWAL())
              .as("the graph rebuild must not switch the caller's transaction to WAL-less either")
              .isEqualTo(useWAL);
          assertThatCode(db::commit).as("the caller's COMMIT must still find its transaction").doesNotThrowAnyException();

          assertThat(db.countType("Doc", true)).isEqualTo(NUM_VECTORS + 1);
          assertThat(lsm.getStats().get("graphNodeCount"))
              .as("the graph must have been built and persisted all the same, in a transaction of its own")
              .isEqualTo((long) NUM_VECTORS);
        } finally {
          db.drop();
        }
      }
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
    }

    private static float[] randomVector(final Random rng) {
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = (float) rng.nextGaussian();
      return v;
    }
  }

  /**
   * Regression for #7378: the PQ approximate search reads its own transaction writes.
   * <p>
   * The issue #7378 overlay on the zero-disk-I/O PQ path, which has its own graph walk, its own post-filter and its
   * own delta merge ({@code mergeWithDeltaScanApproximate}) and so cannot inherit the exact path's coverage. The
   * fixture is deliberately the one {@code VectorSearchCorrectnessTest.ApproximateDeltaScoringScale} uses - enough vectors, and
   * {@code quantization: PRODUCT} - because below that the method falls back to the exact search and the test would
   * pass without ever reaching the code it names.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ApproximateSearchReadsOwnWrites extends TestHelper {

    private static final int DIMENSIONS  = 32;
    private static final int VECTORS     = 400;
    private static final int PQ_CLUSTERS = 16;
    private static final int TOP_K       = 10;

    @BeforeEach
    void freezeTheGraph() {
      GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
      GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
      GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
    }

    @AfterEach
    void thawTheGraph() {
      GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
      GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
      GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
    }

    /** The PQ graph walk plus {@code mergeWithDeltaScanApproximate}. */
    @Test
    void theApproximateSearchScoresARowInsertedInTheOpenTransaction() {
      createSchemaAndData();
      final LSMVectorIndex index = vectorIndex();
      assertThat(index.isPQSearchAvailable()).as("the fixture is only meaningful while the PQ path is really taken")
          .isTrue();

      // A copy of an indexed vector: the pending row is then at the same point in space as its committed twin and
      // cannot fail to be inside a top-10 that already contains the twin.
      final float[] query = vector(17);

      database.begin();
      try {
        database.newDocument("Doc").set("id", "created-in-tx").set("embedding", query).save();

        assertThat(idsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null)))
            .as("the PQ path must resolve the caller's own uncommitted row too")
            .contains("created-in-tx");
      } finally {
        database.rollback();
      }
    }

    /** The issue #6514 pre-filter plan, the approximate path's second way of answering a query. */
    @Test
    void theApproximatePreFilterPlanScoresARowInsertedInTheOpenTransaction() {
      createSchemaAndData();
      final LSMVectorIndex index = vectorIndex();

      final float[] query = vector(17);

      database.begin();
      try {
        final RID pending = database.newDocument("Doc").set("id", "created-in-tx").set("embedding", query).save()
            .getIdentity();

        final Set<RID> allowed = new HashSet<>();
        allowed.add(pending);
        allowed.add(ridOf("doc17"));

        final long preFilterBefore = index.getStats().get("preFilterSearches");
        final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, TOP_K, allowed);
        assertThat(index.getStats().get("preFilterSearches")).as("the fixture has to reach the pre-filter plan")
            .isGreaterThan(preFilterBefore);

        assertThat(idsOf(results)).as("an allow-list of rows the caller just wrote must still resolve them")
            .contains("created-in-tx");
      } finally {
        database.rollback();
      }
    }

    /** The symmetric half on this path: the committed vector of a row the transaction deleted must not come back. */
    @Test
    void theApproximateSearchDropsARowDeletedInTheOpenTransaction() {
      createSchemaAndData();
      final LSMVectorIndex index = vectorIndex();

      final float[] query = vector(17);
      final RID doc17 = ridOf("doc17");
      assertThat(ridsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null))).as("precondition")
          .contains(doc17);

      database.begin();
      try {
        database.command("sql", "DELETE FROM Doc WHERE id = 'doc17'");

        assertThat(ridsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null)))
            .as("the PQ path must not return the committed vector of a row this transaction deleted")
            .doesNotContain(doc17);
      } finally {
        database.rollback();
      }
    }

    /**
     * The overlay superseding a row that is still in the delta buffer rather than in the graph - the one combination
     * of the two structures {@code TransactionVectorOverlay.augment()} handles that nothing else here reaches.
     * <p>
     * Every other test in this PR supersedes a graph-resident row, so {@code augment()}'s filter over the committed
     * buffer - the branch that drops a buffered entry whose RID this transaction has rewritten - was reachable but
     * never actually matched anything.
     * <p>
     * It lives on the approximate path on purpose. {@code findNeighborsFromVectorApproximate} is the one search that
     * never calls {@code rebuildGraphBeforeSearch()}, and on a graph below {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} that
     * method rebuilds synchronously and drains the buffer - so on the exact path the row is in the graph by the time
     * the assertion runs and the test would pass without ever reaching the branch it names. The delta count is
     * asserted on both sides of the search for exactly that reason.
     */
    @Test
    void aBufferedRowTheTransactionRewritesIsNotAlsoReturnedFromTheBuffer() {
      createSchemaAndData();
      final LSMVectorIndex index = vectorIndex();

      final float[] query = vector(17);

      // Committed after the graph was built, and the graph is frozen, so this row lives in the buffer and nowhere
      // else.
      //
      // Its committed vector is deliberately NEAR the query, not far from it: if it were far, a build that failed to
      // drop it from the merged buffer would simply rank it outside TOP_K and the "only once" assertion below would
      // hold for the wrong reason. Near enough to be inside TOP_K, and distinct enough from the pending vector to be
      // a second row rather than the same one, is what makes this test able to fail.
      database.transaction(() -> database.newDocument("Doc").set("id", "buffered").set("embedding", nearly(query))
          .save());
      assertThat(PendingDeltaVectors.of(index))
          .as("the fixture is only a regression test while the row is in the buffer rather than the graph")
          .isEqualTo(1L);

      database.begin();
      try {
        database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = 'buffered'", (Object) query);

        final List<String> ids = idsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null));

        assertThat(PendingDeltaVectors.of(index))
            .as("the search must not have drained the buffer, or this test proves nothing about augment()")
            .isEqualTo(1L);
        assertThat(ids).as("the rewritten row is scored on the vector this transaction wrote")
            .contains("buffered");
        assertThat(ids).as("and the buffer's own copy of it must not come back alongside the pending one")
            .containsOnlyOnce("buffered");
      } finally {
        database.rollback();
      }
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private void createSchemaAndData() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        for (int i = 0; i < VECTORS; i++)
          database.newDocument("Doc").set("id", "doc" + i).set("embedding", vector(i)).save();
      });

      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
            "dimensions": %d,
            "similarity": "COSINE",
            "quantization": "PRODUCT",
            "pqClusters": %d
          }""".formatted(DIMENSIONS, PQ_CLUSTERS));
    }

    /** A vector a short step from {@code v}: close enough to rank alongside it, far enough to be a distinct row. */
    private static float[] nearly(final float[] v) {
      final float[] near = new float[v.length];
      for (int i = 0; i < v.length; i++)
        near[i] = v[i] + 0.02f;
      return near;
    }

    private static float[] vector(final int seed) {
      final Random rnd = new Random(seed * 1_000_003L);
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = rnd.nextFloat() * 2 - 1;
      return v;
    }

    private static List<RID> ridsOf(final List<Pair<RID, Float>> results) {
      final List<RID> rids = new ArrayList<>(results.size());
      for (final Pair<RID, Float> r : results)
        rids.add(r.getFirst());
      return rids;
    }

    private List<String> idsOf(final List<Pair<RID, Float>> results) {
      final List<String> ids = new ArrayList<>(results.size());
      for (final Pair<RID, Float> r : results)
        ids.add(((Document) database.lookupByRID(r.getFirst(), true)).getString("id"));
      return ids;
    }

    private RID ridOf(final String id) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
        return rs.next().getIdentity().orElseThrow();
      }
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #7378: a dense vector search inside a transaction scores the rows it wrote.
   * <p>
   * Regression test for issue #7378: a dense vector search issued inside an open transaction did not score the rows
   * that transaction had written. Writes reach {@code LSMVectorIndex} through
   * {@code TransactionIndexContext.addIndexOperation} and are applied to the graph only at commit replay, so a caller
   * that inserted a row and searched for it in the same transaction got its own write back only after the commit -
   * while the equivalent full-text write was visible immediately, because {@code LSMTreeIndex.get()} merges the
   * transaction's queued keys into its answer.
   * <p>
   * The overlay this pins is deliberately the same shape as what {@code TransactionIndexContext.commit()} would
   * apply: an {@code ADD}/{@code REPLACE} contributes the vector it carries, a {@code REMOVE} supersedes whatever the
   * committed index holds for that RID. Every assertion below therefore has the same form - what the search returns
   * inside the transaction must be what it returns after the commit.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorSearchReadsOwnWrites extends TestHelper {

    private static final int DIMENSIONS = 8;
    private static final int SEEDED     = 40;

    /**
     * The direction the transaction's own row points in, and the query used throughout. It is orthogonal to every
     * seeded vector (which only ever populate the first four components), so the pending row is the nearest neighbour
     * by a wide margin whenever it is a candidate at all - the answer cannot be a coincidence of the fixture.
     */
    private static final float[] PENDING_DIRECTION = { 0, 0, 0, 0, 1, 0, 0, 0 };

    @BeforeEach
    void freezeTheGraph() {
      // Nothing may move the pending row into the graph behind the test's back: the whole point is that the row is
      // visible while it is still queued on the transaction and nowhere else.
      GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
      GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
      GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
    }

    @AfterEach
    void thawTheGraph() {
      GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
      GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
      GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
    }

    /** {@code findNeighborsFromVector} - the entry point every SQL, HTTP and gRPC dense search funnels into. */
    @Test
    void theExactSearchScoresARowInsertedInTheOpenTransaction() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
          .as("precondition: nothing in the committed index points this way yet")
          .doesNotContain("created-in-tx");

      database.begin();
      try {
        insertPendingRow();

        final List<Pair<RID, Float>> inTx = index.findNeighborsFromVector(PENDING_DIRECTION, 5);
        assertThat(idsOf(inTx))
            .as("the row this transaction just wrote must be a candidate, and the query vector IS its embedding")
            .isNotEmpty()
            .first().isEqualTo("created-in-tx");
      } finally {
        database.rollback();
      }
    }

    /**
     * The answer inside the transaction must be the answer after the commit. This is the assertion that would still
     * hold if the overlay contributed the row twice, or with a different vector, so it is paired with the ordering
     * check above rather than standing alone.
     */
    @Test
    void whatTheTransactionSeesIsWhatTheCommitLeavesBehind() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      final List<String> inTx;
      database.begin();
      insertPendingRow();
      inTx = idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5));
      database.commit();

      assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
          .as("a search inside the transaction must agree with the same search after the commit")
          .isEqualTo(inTx);
      assertThat(inTx).containsOnlyOnce("created-in-tx");
    }

    /** {@code get(keys, limit)} - the {@code IndexCursor} entry point, which delegates to the exact search. */
    @Test
    void theIndexCursorAlsoScoresARowInsertedInTheOpenTransaction() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        final RID pending = insertPendingRow();

        final List<RID> rids = new ArrayList<>();
        index.get(new Object[] { PENDING_DIRECTION }, 5).forEachRemaining(r -> rids.add(r.getIdentity()));

        assertThat(rids).as("IndexCursor.get() reads the same overlay the scored search does").contains(pending);
      } finally {
        database.rollback();
      }
    }

    /** {@code findNeighborsFromVectorGrouped} - the {@code groupBy} plan, with its own delta merge and group cap. */
    @Test
    void theGroupedSearchScoresARowInsertedInTheOpenTransaction() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        insertPendingRow();

        final List<Pair<RID, Float>> grouped = index.findNeighborsFromVectorGrouped(PENDING_DIRECTION, 5, 2, -1, null,
            rid -> ((Document) database.lookupByRID(rid, true)).getString("grp"));

        assertThat(idsOf(grouped))
            .as("the grouped plan merges the transaction overlay into the same rank-ordered stream as the delta buffer")
            .contains("created-in-tx");
      } finally {
        database.rollback();
      }
    }

    /**
     * The issue #6502 pre-filter plan, reached with an allow-list narrow enough to beat the graph walk. It is the
     * plan a caller takes when it searches among records it has just written, which is exactly this scenario.
     */
    @Test
    void thePreFilterPlanScoresARowInsertedInTheOpenTransaction() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        final RID pending = insertPendingRow();

        final Set<RID> allowed = new HashSet<>();
        allowed.add(pending);
        allowed.add(ridOf("doc0"));

        final long preFilterBefore = index.getStats().get("preFilterSearches");
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(PENDING_DIRECTION, 5, -1, allowed);
        assertThat(index.getStats().get("preFilterSearches"))
            .as("the fixture has to actually reach the pre-filter plan").isGreaterThan(preFilterBefore);

        assertThat(idsOf(results)).as("an allow-list of rows the caller just wrote must still resolve them")
            .contains("created-in-tx");
      } finally {
        database.rollback();
      }
    }

    /** The symmetric half: a row this transaction deleted must stop being a candidate immediately, not at commit. */
    @Test
    void aRowDeletedInTheOpenTransactionIsNoLongerACandidate() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      final float[] query = seedVector(0);
      final RID doc0 = ridOf("doc0");
      assertThat(ridsOf(index.findNeighborsFromVector(query, 5))).as("precondition").contains(doc0);

      database.begin();
      try {
        database.command("sql", "DELETE FROM Doc WHERE id = 'doc0'");

        // Asserted on RIDs, not on the ids read back from the records: inside this transaction the deleted record
        // can no longer be resolved at all, so a helper that dereferences the answer would abort the test with a
        // RecordNotFoundException instead of failing it with the assertion that names the defect.
        assertThat(ridsOf(index.findNeighborsFromVector(query, 5)))
            .as("the committed vector of a row this transaction deleted must not be returned")
            .doesNotContain(doc0);
      } finally {
        database.rollback();
      }

      assertThat(ridsOf(index.findNeighborsFromVector(query, 5)))
          .as("and the rollback must put it straight back").contains(doc0);
    }

    /**
     * An embedding rewritten inside the transaction. Before the overlay the search returned the OLD vector; a merge
     * that only added the pending row without superseding the committed one would return both, which is worse than
     * either. The row has to appear exactly once, scored on the vector the transaction wrote.
     */
    @Test
    void anEmbeddingRewrittenInTheOpenTransactionIsScoredOnceOnItsNewVector() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = 'doc0'", (Object) PENDING_DIRECTION);

        final List<String> ids = idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5));
        assertThat(ids).as("the rewritten row is scored on its new vector, so it is now the nearest neighbour")
            .isNotEmpty().first().isEqualTo("doc0");
        assertThat(ids).as("and it is ONE row, not the old vector alongside the new one").containsOnlyOnce("doc0");
      } finally {
        database.rollback();
      }
    }

    /**
     * The same record's embedding rewritten twice before the search. The overlay is a model of what
     * {@code TransactionIndexContext.commit()} replays, so it must give the same answer the commit does - and, since
     * issue #7971, that answer is pinned down rather than merely agreed on: <b>the LAST rewrite is the one that
     * survives</b>, and the record ends up with exactly ONE vector.
     * <p>
     * This test used to stop at the agreement, because it could not say more. Both adds stayed in the lane under
     * their own {@code ComparableKey} - a queued {@code REMOVE} carried a placeholder zero vector, so it never
     * coalesced with the add it retired - and the survivor was whichever the replay reached last in that map's
     * order, i.e. a hash of the vector rather than the order the statements ran in. #7971 made the {@code REMOVE}
     * carry the vector it retires and gave the commit replay the write order to pick by, so both halves of the claim
     * below now hold.
     */
    @Test
    void twoRewritesOfOneRowLeaveTheLastOneBehind() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      // The first rewrite aims the row somewhere else entirely; the second aims it at the query. Whichever survives
      // is therefore visible in the ranking, which is what makes "the last one wins" an assertion and not a wish.
      final float[] firstRewrite = seedVector(99);

      database.begin();
      database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = 'doc0'", (Object) firstRewrite);
      database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = 'doc0'", (Object) PENDING_DIRECTION);
      final List<String> inTx = idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5));
      final List<String> inTxAtFirst = idsOf(index.findNeighborsFromVector(firstRewrite, 5));
      database.commit();

      assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
          .as("two rewrites in one transaction must look the same before and after the commit")
          .isEqualTo(inTx);
      assertThat(inTx).as("the row the second rewrite aimed at the query must lead")
          .isNotEmpty().first().isEqualTo("doc0");
      assertThat(inTx).as("and appear once, not once per rewrite").containsOnlyOnce("doc0");

      assertThat(idsOf(index.findNeighborsFromVector(firstRewrite, 5)))
          .as("a query aimed at the SUPERSEDED embedding must rank the row the same way before and after the commit")
          .isEqualTo(inTxAtFirst);

      assertThat(index.countEntries())
          .as("the record carries one embedding, so the commit must leave one live vector for it - not one per rewrite")
          .isEqualTo(SEEDED);
    }

    /**
     * A row added and then deleted inside one transaction is gone from that transaction's own search, and from the
     * answer after the commit - even when the removal could not be keyed on the vector it retires, so the two never
     * collapsed in the queue. Raised in the review of PR #8001; the overlay and the commit reach the same answer by
     * the same rule (the last entry for a RID decides), and this is what holds them to it.
     */
    @Test
    void aRowAddedAndThenDeletedInTheSameTransactionIsGoneFromBothSides() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      final String pendingId;
      try {
        insertPendingRow();
        pendingId = "created-in-tx";
        assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
            .as("precondition: the transaction can see its own row before it deletes it")
            .contains(pendingId);

        // Delete it through the index with a key it cannot convert, so the REMOVE rides the placeholder and shares
        // no ComparableKey with the ADD it retires.
        final RID rid = ridOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5), pendingId);
        index.remove(new Object[] { "not a vector" }, rid);

        assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
            .as("a row this transaction deleted must be gone from its own search")
            .doesNotContain(pendingId);

        database.commit();
      } catch (final RuntimeException e) {
        database.rollback();
        throw e;
      }

      assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
          .as("...and must not come back after the commit either")
          .doesNotContain(pendingId);
    }

    /** The RID the search returned for {@code id}. */
    private RID ridOf(final List<Pair<RID, Float>> results, final String id) {
      for (final Pair<RID, Float> r : results)
        if (id.equals(r.getFirst().asDocument(true).get("id")))
          return r.getFirst();
      throw new IllegalStateException("no result for id " + id);
    }

    /**
     * The reason {@code TransactionIndexContext.getIndexKeys(IndexInternal)} exists, driven rather than argued.
     * <p>
     * An {@link LSMVectorIndex} is named after the component file it holds, and a compaction swaps that file in and
     * renames the index (issue #6105). Entries are queued on the transaction under the name the index answered to
     * when the lane was opened, so a search that looked its own lane up by {@code getName()} would miss it entirely
     * after a rename - and would miss it silently, which is the defect #7378 is about, reintroduced through the
     * back door. Resolving the lane by index identity is what closes that, and this is the test that reaches it:
     * without the identity fallback the search below returns the committed answer and the assertion fails.
     * <p>
     * The compaction runs on another thread because it refuses to run inside a transaction - which is also exactly
     * where production runs it, on the async executor after a commit. The shape is the read-side twin of
     * {@code VectorGraphStalenessTest.WriteAfterCompaction.aWriteQueuedBeforeACompactionOnAnotherThreadIsStillIndexed}.
     */
    @Test
    @Tag("slow")
    void aSearchFindsItsOwnPendingRowAfterACompactionRenamedTheIndex() throws Exception {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();
      final String nameWhenQueued = index.getName();

      database.begin();
      try {
        insertPendingRow();

        final Throwable[] failure = new Throwable[1];
        final Thread compactor = new Thread(() -> {
          try {
            database.command("sql", "COMPACT INDEX `" + nameWhenQueued + "`");
          } catch (final Throwable t) {
            failure[0] = t;
          }
        }, "issue7378-compactor");
        compactor.start();
        compactor.join(120_000);

        assertThat(compactor.isAlive()).as("the compaction must have finished").isFalse();
        assertThat(failure[0]).as("the compaction must not have failed").isNull();
        assertThat(index.getName())
            .as("the fixture is only a regression test once the compaction really renamed the index")
            .isNotEqualTo(nameWhenQueued);

        assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
            .as("the lane was opened under the pre-compaction name, so only an identity lookup still finds it")
            .isNotEmpty().first().isEqualTo("created-in-tx");
      } finally {
        database.rollback();
      }
    }

    /**
     * The second half of the rename case: a write, a compaction that renames the index, and then ANOTHER write to
     * the same index, all inside one transaction.
     * <p>
     * {@code addIndexKeyLock} keys a lane by the name the index answers to when the entry is queued and opens a new
     * lane for a name it has not seen, so this sequence leaves TWO lanes for one index - one under each name.
     * {@code TransactionIndexContext.commit()} replays both, because it resolves every lane through the index it
     * remembers rather than through its key. A reader that stopped at the first lane it matched would see one of the
     * two writes and answer the search with the other missing, which is the defect of #7378 relocated rather than
     * fixed. Both rows have to be candidates here.
     */
    @Test
    @Tag("slow")
    void aSearchAfterARenameSeesTheWritesFromBothSidesOfIt() throws Exception {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();
      final String nameWhenQueued = index.getName();

      database.begin();
      try {
        database.newDocument("Doc").set("id", "before-rename").set("grp", "g-tx")
            .set("embedding", PENDING_DIRECTION).save();

        final Throwable[] failure = new Throwable[1];
        final Thread compactor = new Thread(() -> {
          try {
            database.command("sql", "COMPACT INDEX `" + nameWhenQueued + "`");
          } catch (final Throwable t) {
            failure[0] = t;
          }
        }, "issue7378-compactor-2");
        compactor.start();
        compactor.join(120_000);

        assertThat(compactor.isAlive()).as("the compaction must have finished").isFalse();
        assertThat(failure[0]).as("the compaction must not have failed").isNull();
        assertThat(index.getName())
            .as("the fixture is only a regression test once the compaction really renamed the index")
            .isNotEqualTo(nameWhenQueued);

        // Queued under the NEW name, so it opens a second lane for the same index.
        database.newDocument("Doc").set("id", "after-rename").set("grp", "g-tx")
            .set("embedding", PENDING_DIRECTION).save();

        assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
            .as("both writes are this transaction's own, whichever name their lane was opened under")
            .contains("before-rename", "after-rename");
      } finally {
        database.rollback();
      }
    }

    /** A rolled back transaction leaves nothing behind: the overlay lives on the transaction, not on the index. */
    @Test
    void aRolledBackTransactionTakesItsPendingRowBackOut() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      insertPendingRow();
      assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5))).contains("created-in-tx");
      database.rollback();

      assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
          .as("a rolled back write must leave no trace in the answer").doesNotContain("created-in-tx");
      // Nodes the seeding build left unreachable are re-queued on purpose and are not a trace of the write (#7190, #8200)
      assertThat(PendingDeltaVectors.of(index))
          .as("and no trace in the index's own buffers either").isEqualTo(0L);
    }

    /**
     * End to end through SQL, which is the path the three HTTP routes and the three gRPC RPCs of issue #7326 take.
     * The engine-level assertions above prove the index resolves the overlay; this one proves the query layer above
     * it does not lose the row again.
     */
    @Test
    void sqlVectorNeighborsScoresARowInsertedInTheOpenTransaction() {
      seedAndBuildGraph();

      database.begin();
      try {
        insertPendingRow();

        final List<String> ids = new ArrayList<>();
        try (final ResultSet rs = database.query("sql",
            "SELECT id FROM (SELECT expand(vectorNeighbors('Doc[embedding]', ?, 5)))", (Object) PENDING_DIRECTION)) {
          while (rs.hasNext())
            ids.add(rs.next().getProperty("id"));
        }

        assertThat(ids).as("vectorNeighbors() inside the caller's transaction must see the caller's own write")
            .contains("created-in-tx");
      } finally {
        database.rollback();
      }
    }

    /**
     * A search on a thread holding no transaction must be untouched by any of this - including the fast path that
     * decides there is no overlay to build at all.
     */
    @Test
    void aSearchOutsideAnyTransactionIsUnaffected() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      final List<String> ids = idsOf(index.findNeighborsFromVector(seedVector(3), 5));
      assertThat(ids).isNotEmpty().first().isEqualTo("doc3");
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private RID insertPendingRow() {
      final Document doc = database.newDocument("Doc").set("id", "created-in-tx").set("grp", "g-tx")
          .set("embedding", PENDING_DIRECTION).save();
      return doc.getIdentity();
    }

    private void seedAndBuildGraph() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.grp STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        for (int i = 0; i < SEEDED; i++)
          database.newDocument("Doc").set("id", "doc" + i).set("grp", "g" + (i % 4)).set("embedding", seedVector(i))
              .save();
      });

      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
            "dimensions": %d,
            "similarity": "COSINE"
          }""".formatted(DIMENSIONS));

      // One search outside any transaction, so the seeded rows are in the graph rather than in the delta buffer when
      // the assertions run: the defect is about the graph not resolving uncommitted rows, and a fixture that left
      // everything in the delta buffer would never reach it.
      final LSMVectorIndex index = vectorIndex();
      index.findNeighborsFromVector(seedVector(0), 1);
      assertThat(index.getStats().get("graphNodeCount"))
          .as("the fixture is only a regression test once the seeded rows are really in the graph")
          .isEqualTo((long) SEEDED);
    }

    /**
     * Deterministic, distinct, and only faintly aligned with {@link #PENDING_DIRECTION} - the fifth component is
     * small but never zero and never repeated, so no two rows tie on cosine distance to the query. A fixture of
     * exact ties would leave the rank of everything below the pending row arbitrary and the assertions flaky.
     */
    private static float[] seedVector(final int i) {
      final float[] v = new float[DIMENSIONS];
      v[0] = 1.0f;
      v[1] = 0.02f * (i + 1);
      v[2] = 0.01f * ((i * 7) % 13);
      v[3] = 0.005f * ((i * 3) % 11);
      v[4] = 0.001f * (i + 1);
      return v;
    }

    private static List<RID> ridsOf(final List<Pair<RID, Float>> results) {
      final List<RID> rids = new ArrayList<>(results.size());
      for (final Pair<RID, Float> r : results)
        rids.add(r.getFirst());
      return rids;
    }

    private List<String> idsOf(final List<Pair<RID, Float>> results) {
      final List<String> ids = new ArrayList<>(results.size());
      for (final Pair<RID, Float> r : results)
        ids.add(((Document) database.lookupByRID(r.getFirst(), true)).getString("id"));
      return ids;
    }

    private RID ridOf(final String id) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
        return rs.next().getIdentity().orElseThrow();
      }
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #7931: an aborted transaction leaves nothing behind in the vector index.
   * <p>
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
  @Nested
  @Tag("vector")
  class AbortedTransactionVectorLeak {
    private static final String DB_ROOT     = "target/test-databases/AbortedTransactionVectorLeak";
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
     * pointing at an offset for a record that no longer exists. Without the check that the current generation still
     * holds the tombstone, this test fails.
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
     * Nothing is dropped by the re-anchor: a full rebuild carries the replay's tombstones over (issue #11099), so whether
     * one can be lifted is decided per id at undo time, against the generation then in force.
     */
    @Test
    void reAnchoringKeepsEveryRecordForTheUndoToDecideAgainstTheCurrentGeneration() throws Exception {
      withDatabase(db -> {
        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();

        final VectorLocationIndex before = index.residentLocationsForTest();
        final int live = before.getActiveVectorIds().findFirst().orElseThrow();
        final RID rid = before.getRid(live);

        final VectorIndexReplayUndo undo = new VectorIndexReplayUndo(index, before);
        undo.recordAllocated(4242);
        undo.recordTombstoned(live, before.getOffsetAndFlag(live), rid);
        before.markDeleted(live);
        undo.mutationsCharged = 2;
        undo.mutablePagesCreated = 1;

        index.buildVectorGraphNow();
        final VectorLocationIndex after = index.residentLocationsForTest();
        assertThat(after).as("precondition: the rebuild must have republished the location index").isNotSameAs(before);
        assertThat(after.isDeleted(live)).as("precondition: the rebuild carried the replay's tombstone over").isTrue();

        undo.rebaseTo(after);

        assertThat(undo.locationsAtReplay).as("the journal must now speak for the generation in force").isSameAs(after);
        assertThat(undo.tombstonedCount)
            .as("a tombstone the new generation carried over must still be liftable, so the re-anchor keeps it").isEqualTo(1);
        assertThat(undo.allocatedCount)
            .as("an id minted by this replay is illegitimate in EVERY generation, so it must survive the re-anchor "
                + "- dropping it is what would leave an uncommitted vector searchable")
            .isEqualTo(1);
        assertThat(undo.mutationsCharged).as("the counters are index-wide, not per generation").isEqualTo(2);
        assertThat(undo.mutablePagesCreated).isEqualTo(1);

        undo.undoIndexReplay();

        assertThat(after.isLive(live)).as("the abort lifts the carried tombstone").isTrue();
        assertThat(after.getRid(live)).isEqualTo(rid);
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

  /**
   * Regression for #7967: the transaction overlay is bounded and reused across searches.
   * <p>
   * Regression test for issue #7967: the dense vector transaction overlay of issue #7378 was rebuilt from scratch on
   * every search, kept a converted {@code VectorFloat} per pending row with nothing bounding how many, and reached
   * its lane through a walk of every OTHER index the transaction had touched.
   * <p>
   * Three separate costs in one per-search window, and all three are removed here without changing a single answer -
   * which is what the assertions below are built around: every one of them pairs the cheaper behaviour with the
   * property #7378 established, that a search inside the transaction returns what the same search returns after the
   * commit. A bound that dropped a pending row, or a cache that served a stale one, would be a correctness
   * regression rather than a saving.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OverlayIsBoundedAndReused extends TestHelper {

    private static final int DIMENSIONS = 8;
    private static final int SEEDED     = 20;

    @BeforeEach
    void freezeTheGraph() {
      // Nothing may move a pending row into the graph behind the test's back: what is under test is the overlay.
      GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
      GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
      GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
    }

    @AfterEach
    void thawTheGraph() {
      GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
      GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
      GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
      GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE.reset();
    }

    /**
     * The budget: past it a pending row keeps no converted payload, and is read back from the record the transaction
     * saved instead. Every row must still be scored, at its own vector.
     */
    @Test
    void aPendingRowPastThePayloadBudgetIsStillScoredAtItsOwnVector() {
      // One payload for the whole index - committed buffer and overlay alike - so every pending row but the first
      // takes the declined path. Anything that drops such a row shows up as a missing or misranked answer below.
      GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE.setValue(1);

      createSchema();
      seed();
      final LSMVectorIndex index = vectorIndex();

      final int pendingRows = 6;
      final List<String> expected = new ArrayList<>();
      final List<List<String>> inTx = new ArrayList<>();

      database.begin();
      for (int i = 0; i < pendingRows; i++) {
        final String id = "pending-" + i;
        database.newDocument("Doc").set("id", id).set("embedding", axis(i % DIMENSIONS)).save();
        expected.add(id);
      }
      // The fixture is only a test of the budget once the budget has actually declined something.
      final TransactionVectorOverlay overlay = TransactionVectorOverlay.open((DatabaseInternal) database, subIndex(),
          VectorizationProvider.getInstance().getVectorTypeSupport(), 1);
      assertThat(overlay).isNotNull();
      assertThat(overlay.pendingPayloadsDeclined())
          .as("with a budget of one, every pending row but the first must have been declined its payload")
          .isEqualTo(pendingRows - 1);

      // Each pending row is the exact query vector for its own axis, so it has to come back first for that query.
      for (int i = 0; i < pendingRows; i++)
        inTx.add(idsOf(index.findNeighborsFromVector(axis(i % DIMENSIONS), 3)));
      database.commit();

      for (int i = 0; i < pendingRows; i++) {
        assertThat(inTx.get(i))
            .as("row %s must be scored at its own vector even with no payload kept for it", expected.get(i))
            .isNotEmpty().first().isEqualTo(expected.get(i));
        assertThat(inTx.get(i))
            .as("and contributed once, not once per queued entry")
            .containsOnlyOnce(expected.get(i));
        // The nearest neighbour, not the whole ranking: every other row in this fixture is orthogonal to the query
        // and to each row, so ranks 2 and 3 are an arbitrary pick between ties on BOTH sides of the commit. What
        // the budget could break is a row being dropped or scored at the wrong vector, and that is what is asserted.
        // The full-ranking agreement is VectorSearchReadsOwnWrites's subject.
        assertThat(idsOf(index.findNeighborsFromVector(axis(i % DIMENSIONS), 3)))
            .as("and the search inside the transaction must agree with the same search after the commit on WHICH row "
                + "the query is nearest to")
            .isNotEmpty().first().isEqualTo(expected.get(i));
      }
    }

    /**
     * Rewriting a row the budget already DECLINED must not hand it a payload back for free.
     * <p>
     * The gate used to read "is this RID already a key in the map", which is true for a declined row as much as for a
     * resident one - so a transaction that outran the budget and then rewrote its declined rows crept back over it,
     * one row at a time, which is the one thing the budget exists to stop. Found in the review of PR #8001.
     */
    @Test
    void rewritingADeclinedRowDoesNotWinItsPayloadBack() {
      GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE.setValue(2);

      createSchema();
      seed();

      final int pendingRows = 6;
      database.begin();
      try {
        final List<RID> written = new ArrayList<>(pendingRows);
        for (int i = 0; i < pendingRows; i++)
          written.add(database.newDocument("Doc").set("id", "pending-" + i).set("embedding", axis(i % DIMENSIONS))
              .save().getIdentity());

        // Every row past the budget was declined. Rewriting them must not promote them.
        for (final RID rid : written)
          database.lookupByRID(rid, true).asDocument(true).modify().set("embedding", axis(1)).save();

        final TransactionVectorOverlay overlay = TransactionVectorOverlay.open((DatabaseInternal) database, subIndex(),
            VectorizationProvider.getInstance().getVectorTypeSupport(), 2);
        assertThat(overlay).isNotNull();
        assertThat(overlay.pendingCount() - overlay.pendingPayloadsDeclined())
            .as("the rows actually holding a payload must still be within the budget after the rewrites")
            .isLessThanOrEqualTo(2);

        // ...and the rows are all still answerable, which is the property the budget may never trade away. Asked for
        // one more than there are pending rows, so a row the budget dropped would show as a missing id rather than
        // being masked by the limit - which is why this contains rather than equals: the extra slot holds a seed row.
        assertThat(idsOf(index().findNeighborsFromVector(axis(1), pendingRows + 1)))
            .as("every rewritten row must still be scored at the vector it now holds, declined payload or not")
            .contains("pending-0", "pending-1", "pending-2", "pending-3", "pending-4", "pending-5");
      } finally {
        database.rollback();
      }
    }

    /**
     * The cache: several searches with no write between them build the overlay once. Measured through the lane
     * version the cache is keyed on, because the overlay itself is package-private state with no counter of its own -
     * a version that has not moved is precisely the condition under which a rebuild is not allowed to happen.
     */
    @Test
    void searchesWithNoWriteBetweenThemDoNotRebuildTheOverlay() {
      createSchema();
      seed();
      final LSMVectorIndex index = vectorIndex();
      final IndexInternal sub = subIndex();

      database.begin();
      try {
        database.newDocument("Doc").set("id", "pending").set("embedding", axis(4)).save();

        final TransactionIndexContext changes = ((DatabaseInternal) database).getTransaction().getIndexChanges();
        index.findNeighborsFromVector(axis(4), 3);

        final Object firstView = changes.cachedIndexView(sub);
        assertThat(firstView).as("the first search must leave its overlay cached on the transaction").isNotNull();

        for (int i = 0; i < 5; i++)
          assertThat(idsOf(index.findNeighborsFromVector(axis(4), 3)))
              .as("and every later search must give the same answer")
              .isNotEmpty().first().isEqualTo("pending");

        assertThat(changes.cachedIndexView(sub))
            .as("five more searches with no write between them must reuse the very same overlay, not rebuild it")
            .isSameAs(firstView);
      } finally {
        database.rollback();
      }
    }

    /** ...and a write between two searches must invalidate it, or the second search answers from stale lanes. */
    @Test
    void aWriteBetweenTwoSearchesInvalidatesTheCachedOverlay() {
      createSchema();
      seed();
      final LSMVectorIndex index = vectorIndex();
      final IndexInternal sub = subIndex();

      database.begin();
      try {
        database.newDocument("Doc").set("id", "first").set("embedding", axis(4)).save();
        final TransactionIndexContext changes = ((DatabaseInternal) database).getTransaction().getIndexChanges();
        index.findNeighborsFromVector(axis(4), 3);
        final Object firstView = changes.cachedIndexView(sub);

        database.newDocument("Doc").set("id", "second").set("embedding", axis(5)).save();
        assertThat(changes.cachedIndexView(sub))
            .as("a write must discard the view built before it")
            .isNotSameAs(firstView);

        assertThat(idsOf(index.findNeighborsFromVector(axis(5), 3)))
            .as("and the row written after the first search must be found by the second")
            .isNotEmpty().first().isEqualTo("second");
      } finally {
        database.rollback();
      }
    }

    /**
     * A row taken back by {@code undoRecordChanges} - the retraction a refused unique key triggers (issue #7467) -
     * must not reach a search, whether or not a view was cached over the lanes it was queued on.
     * <p>
     * The caching this issue added is what makes that worth asserting rather than assuming: a view is a snapshot of
     * the lanes, and a retraction changes them without queueing anything. It is invalidated here by the refused
     * record's own queued entries, which move the lane version a moment before the retraction removes them - so the
     * bump {@code undoRecordChanges} makes is belt to that braces rather than the only thing holding this up. The
     * assertion is on the answer either way, because that is the thing that must be true however it is arrived at.
     * <p>
     * Both records are written in the SAME transaction on purpose: that is what makes {@code addIndexKeyLock} raise
     * the duplicate at {@code save()} time, with the vector index's entry for the refused record already queued and
     * the retraction still to run. A key already committed by an earlier transaction is a different path - it is
     * refused at commit, not at save - and would exercise nothing here.
     */
    @Test
    void aRetractedRowDoesNotSurviveInACachedOverlay() {
      createSchema();
      database.transaction(() -> database.getSchema().getType("Doc").createProperty("code", Type.STRING)
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, true));
      seed();

      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        database.newDocument("Doc").set("id", "taken").set("code", "K").set("embedding", axis(6)).save();

        // A search BEFORE the refusal, so there is a cached view built over lanes the retraction is about to change.
        assertThat(idsOf(index.findNeighborsFromVector(axis(6), 3)))
            .as("precondition: the first record of this transaction is visible to it")
            .isNotEmpty().first().isEqualTo("taken");

        assertThatThrownBy(() -> database.newDocument("Doc").set("id", "refused").set("code", "K")
            .set("embedding", axis(7)).save())
            .as("the second record must be refused by the unique index")
            .isInstanceOf(DuplicatedKeyException.class);

        assertThat(idsOf(index.findNeighborsFromVector(axis(7), 3)))
            .as("the refused record was retracted, so no search - cached view or not - may rank it")
            .doesNotContain("refused");
        assertThat(idsOf(index.findNeighborsFromVector(axis(6), 3)))
            .as("and the record that WAS accepted must still be there")
            .isNotEmpty().first().isEqualTo("taken");
      } finally {
        database.rollback();
      }
    }

    /**
     * The lane lookup: a transaction that has written to OTHER indexes but not to this one must answer "nothing
     * queued" without looking at any of them. Asserted as the answer rather than as the cost - what a reader must
     * never get is a lane belonging to someone else, or a miss on one of its own.
     */
    @Test
    void anIndexNeverWrittenToAnswersNothingEvenWhenOtherIndexesWere() {
      createSchema();
      database.transaction(() -> {
        final DocumentType other = database.getSchema().createDocumentType("Other");
        other.createProperty("name", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
      });
      seed();

      final LSMVectorIndex index = vectorIndex();
      final List<String> committed = idsOf(index.findNeighborsFromVector(axis(4), 3));

      database.begin();
      try {
        for (int i = 0; i < 50; i++)
          database.newDocument("Other").set("name", "n" + i).save();

        assertThat(idsOf(index.findNeighborsFromVector(axis(4), 3)))
            .as("writes to an unrelated index must not change what this index answers")
            .isEqualTo(committed);

        assertThat(((DatabaseInternal) database).getTransaction().getIndexChanges().getIndexKeyLanes(subIndex()))
            .as("and the vector index must report no lane of its own")
            .isEmpty();
      } finally {
        database.rollback();
      }
    }

    // ---------- helpers ----------

    private void createSchema() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.STRING);
        type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      });
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
          + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");
    }

    private void seed() {
      database.transaction(() -> {
        for (int i = 0; i < SEEDED; i++) {
          final float[] v = new float[DIMENSIONS];
          v[0] = 1f;
          v[1] = 0.01f * (i + 1);
          database.newDocument("Doc").set("id", "seed-" + i).set("embedding", v).save();
        }
      });
    }

    /** A unit vector along one axis. Two of them share no direction, so each is unambiguously its own row's query. */
    private static float[] axis(final int axis) {
      final float[] v = new float[DIMENSIONS];
      v[axis] = 1f;
      return v;
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) subIndex();
    }

    private LSMVectorIndex index() {
      return vectorIndex();
    }

    private IndexInternal subIndex() {
      return ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }

    private static List<String> idsOf(final List<Pair<RID, Float>> results) {
      final List<String> ids = new ArrayList<>(results.size());
      for (final Pair<RID, Float> r : results)
        ids.add((String) r.getFirst().asDocument(true).get("id"));
      return ids;
    }
  }

  /**
   * Regression for #7971: the last rewrite of a record embedding in a transaction wins.
   * <p>
   * Regression test for issue #7971: when one transaction rewrites the same record's embedding more than once, the
   * commit kept EVERY rewrite as a live vector of that record, and which of them a search ranked the record by was
   * decided by {@code ComparableVector} order - a hash of the vector's contents - rather than by which write ran last.
   * <p>
   * Two things caused it. {@code LSMVectorIndex.remove()} queued an all-zero placeholder key because it "had no vector
   * to queue", so the {@code REMOVE} never landed on the same {@code ComparableKey} as the {@code ADD} it was meant to
   * retire and {@code TransactionIndexContext}'s per-key dedup never collapsed the pair; and the commit replay walked
   * the resulting {@code TreeMap} in KEY order, with nothing on an entry to say when it had been written. A dense
   * vector index holds one live vector per RID, so a record re-embedded twice inside a transaction came out of it
   * indexed under both embeddings at once - three live vectors after two rewrites, three graph nodes for one record,
   * and the same record returned several times by one search.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LastVectorWriteWins {
    private static final String DB_PATH    = "./target/databases/LastVectorWriteWins";
    private static final int    DIMENSIONS = 8;

    @AfterEach
    void cleanUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void rewritingAnEmbeddingTwiceLeavesOneVectorBehind() {
      withDatabase(db -> {
        final RID rid = insertWith(db, unit(0));

        // One transaction, three embeddings for the same record. Only the LAST one is what the record holds when the
        // transaction commits, so the index may hold exactly one vector for it - the last one.
        db.transaction(() -> {
          final MutableDocument doc = db.lookupByRID(rid, true).asDocument(true).modify();
          doc.set("embedding", unit(1)).save();
          doc.set("embedding", unit(2)).save();
          doc.set("embedding", unit(3)).save();
        });

        assertOneVectorPerRecord(db, rid);
      });
    }

    /**
     * The same rewrite sequence on a record CREATED inside the very transaction that rewrites it: the first
     * {@code ADD} has no committed vector behind it, which is the shape the re-embedding pipelines of issue #7971
     * actually run.
     */
    @Test
    void rewritingAnEmbeddingOfARecordCreatedInTheSameTransactionLeavesOneVectorBehind() {
      withDatabase(db -> {
        final RID[] holder = new RID[1];
        db.transaction(() -> {
          final MutableDocument doc = db.newDocument("Doc").set("embedding", unit(1));
          doc.save();
          doc.set("embedding", unit(2)).save();
          doc.set("embedding", unit(3)).save();
          holder[0] = doc.getIdentity();
        });

        assertOneVectorPerRecord(db, holder[0]);
      });
    }

    /**
     * The same guarantee without {@code DocumentIndexer} in the middle: two {@code put()}s for one RID inside one
     * transaction, with no {@code remove()} between them to coalesce with. Nothing but the write order the commit
     * replay carries can tell those two apart, so this is what pins it.
     */
    @Test
    void twoDirectPutsForOneRecordCommitAsOneVector() {
      withDatabase(db -> {
        final RID rid = insertWith(db, unit(0));
        final LSMVectorIndex idx = vectorIndex(db);

        db.transaction(() -> {
          idx.remove(new Object[] { unit(0) }, rid);
          idx.put(new Object[] { unit(1) }, new RID[] { rid });
          idx.put(new Object[] { unit(2) }, new RID[] { rid });
        });

        assertOneVectorPerRecord(db, rid);
      });
    }

    /**
     * The same guarantee across a COMPACTION that renames the index mid-transaction, which is what gives one index
     * two lanes (issue #6105). Raised by CodeRabbit on PR #8001: the sequence stamp alone proves nothing unless the
     * commit actually consults it across lanes, so this drives the whole path and looks at what the commit left.
     * <p>
     * The compaction runs on another thread because it refuses to run inside a transaction - which is also exactly
     * where production runs it, on the async executor after a commit.
     */
    @Test
    @Tag("slow")
    void aRewriteEitherSideOfACompactionStillLeavesOneVector() {
      withDatabase(db -> {
        final RID rid = insertWith(db, unit(0));
        final LSMVectorIndex idx = vectorIndex(db);
        final String nameWhenQueued = idx.getName();

        db.begin();
        try {
          final MutableDocument doc = db.lookupByRID(rid, true).asDocument(true).modify();
          doc.set("embedding", unit(1)).save();

          // The rename, on a thread of its own, with the first write already queued under the old name.
          final Throwable[] failure = new Throwable[1];
          final Thread compactor = new Thread(() -> {
            try {
              db.command("sql", "COMPACT INDEX `" + nameWhenQueued + "`");
            } catch (final Throwable t) {
              failure[0] = t;
            }
          }, "issue7971-compactor");
          compactor.start();
          try {
            compactor.join(120_000);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the compaction", e);
          }

          assertThat(compactor.isAlive()).as("the compaction must have finished").isFalse();
          assertThat(failure[0]).as("the compaction must not have failed").isNull();
          assertThat(idx.getName())
              .as("the fixture is only a regression test once the compaction really renamed the index")
              .isNotEqualTo(nameWhenQueued);

          // ...and the second write, which opens a lane under the new name.
          doc.set("embedding", unit(2)).save();
          db.commit();
        } catch (final RuntimeException e) {
          db.rollback();
          throw e;
        }

        assertThat(vectorIndex(db).countEntries())
            .as("a rewrite either side of a rename is still ONE record with one embedding, not one per lane")
            .isEqualTo(1L);
      });
    }

    /**
     * A record added and then DELETED in one transaction must not come back, even when the REMOVE could not be keyed
     * on the vector it retires.
     * <p>
     * Raised in the review of PR #8001. The commit replays every REMOVE before any ADD, so a REMOVE only cancels an
     * ADD by displacing it in the per-key map - which needs the two to share a {@code ComparableKey}. When
     * {@code removalKey()} falls back to the placeholder (a key the caller could not convert), they do not, and the
     * winner-per-RID pass looked at {@code ADD}/{@code REPLACE} alone, so it re-added a RID the transaction had
     * deleted.
     */
    @Test
    void aRecordDeletedAfterBeingAddedDoesNotComeBackWhenTheRemoveCarriesNoKey() {
      withDatabase(db -> {
        final RID rid = insertWith(db, unit(0));
        final LSMVectorIndex idx = vectorIndex(db);

        db.transaction(() -> {
          idx.remove(new Object[] { unit(0) }, rid);
          idx.put(new Object[] { unit(1) }, new RID[] { rid });
          // A removal the index cannot key on - the caller has no usable old value - so it rides the placeholder
          // and shares no ComparableKey with the ADD above.
          idx.remove(new Object[] { "not a vector" }, rid);
        });

        assertThat(vectorIndex(db).countEntries())
            .as("the record was deleted after being added, so the commit must leave nothing behind for it")
            .isZero();
      });
    }

    /**
     * What "one vector per record" means everywhere it is observable: the live count of the index, the number of nodes
     * a graph build produces, and how many times one search hands the record back.
     * <p>
     * Deliberately NOT asserted here: WHICH embedding a search scores the record at. The delta scan resolves a pending
     * RID's vector through the record when the payload is not resident, so a query that happens to be the record's
     * current property value scores 0 whatever the index holds - an assertion on the distance would pass on the broken
     * engine too.
     */
    private static void assertOneVectorPerRecord(final Database db, final RID rid) {
      final LSMVectorIndex idx = vectorIndex(db);

      assertThat(idx.countEntries())
          .as("a record carries one embedding, so the index must hold one live vector for it, not one per rewrite")
          .isEqualTo(1L);

      final List<Pair<RID, Float>> results = idx.findNeighborsFromVector(unit(0), 10);
      assertThat(results.stream().filter(r -> rid.equals(r.getFirst())).count())
          .as("one search must return the record once, not once per embedding it held during the transaction")
          .isEqualTo(1L);

      idx.buildVectorGraphNow();
      assertThat(idx.getStats().get("graphNodeCount"))
          .as("the graph must carry one node for the record, not one per embedding it held during the transaction")
          .isEqualTo(1L);
    }

    private static void withDatabase(final java.util.function.Consumer<Database> body) {
      FileUtils.deleteRecursively(new File(DB_PATH));
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          // The absorb machinery is irrelevant here: what is under test is what the COMMIT leaves in the index.
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          db.transaction(() -> {
            final DocumentType t = db.getSchema().createDocumentType("Doc");
            t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          });
          db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
              + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");

          body.accept(db);
        } finally {
          db.drop();
        }
      }
    }

    private static RID insertWith(final Database db, final float[] vector) {
      final RID[] holder = new RID[1];
      db.transaction(() -> {
        final MutableDocument doc = db.newDocument("Doc").set("embedding", vector);
        doc.save();
        holder[0] = doc.getIdentity();
      });
      return holder[0];
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }

    /** A unit vector along axis {@code axis}: any two of them are the same distance apart. */
    private static float[] unit(final int axis) {
      final float[] v = new float[DIMENSIONS];
      v[axis] = 1f;
      return v;
    }
  }

  /**
   * Regression for #7974: a rebuild inside a transaction reads through the right database.
   * <p>
   * Regression test for issue #7974: a similarity search issued inside an open transaction rebuilds the graph
   * synchronously when the resident graph is small, and everything that rebuild read used to come back through the
   * calling thread's transaction. An embedding written and not yet committed was therefore baked into the state the
   * index keeps, and the rollback could not take it back: the record reverted, the index did not, and it was left
   * describing a row by a vector no committed transaction ever wrote.
   * <p>
   * The two tests pin the two places that state lives, because they fail independently: the index-scoped vector cache
   * every later search on this session reads, and - once the index stores its vectors inline - the persisted graph
   * file, which survives a reopen.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RebuildInsideTransaction {
    private static final String DB_PATH     = "./target/databases/RebuildInsideTransaction";
    private static final int    DIMENSIONS  = 16;
    private static final int    NUM_VECTORS = 40;

    @AfterEach
    void cleanUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void aRolledBackEmbeddingMustNotSurviveInTheIndexScopedVectorCache() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      final Random rng = new Random(7974);

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          createSchema(db, false);

          final RID[] rids = insertSeedRows(db, rng);
          final LSMVectorIndex lsm = vectorIndex(db);

          // First search builds the graph over the committed rows.
          lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
          assertThat(lsm.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);

          // One more row, committed, so it sits in the delta buffer and leaves a mutation behind: that is what makes
          // the search inside the transaction below rebuild rather than just walk the graph.
          final float[] committedVector = randomVector(rng);
          final RID victim = insertRow(db, "buffered", committedVector);

          final float[] rolledBackVector = farAwayVector();

          db.begin();
          db.lookupByRID(victim, true).asDocument().modify().set("embedding", rolledBackVector).save();
          lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
          db.rollback();

          final Document afterRollback = db.lookupByRID(victim, true).asDocument();
          assertThat((float[]) afterRollback.get("embedding"))
              .as("precondition: the rollback took the record's embedding back")
              .containsExactly(committedVector);

          assertThat(ridsOf(lsm.findNeighborsFromVector(rolledBackVector, 5, 64)))
              .as("the index must not rank the row by an embedding no committed transaction ever wrote")
              .doesNotContain(victim);

          assertThat(ridsOf(lsm.findNeighborsFromVector(committedVector, 5, 64)))
              .as("the index must rank the row by the embedding the database actually holds")
              .contains(victim);

          assertThat(rids).hasSize(NUM_VECTORS);
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void aRolledBackEmbeddingMustNotSurviveInThePersistedGraph() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      final Random rng = new Random(7974);

      final RID victim;
      final float[] committedVector;
      final float[] rolledBackVector = farAwayVector();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          // storeVectorsInGraph is what makes the damage outlive the session: the rebuild writes each vector inline
          // into the graph file, and every later search scores from the file rather than from the record.
          createSchema(db, true);

          final RID[] rids = insertSeedRows(db, rng);
          victim = rids[0];
          committedVector = (float[]) db.lookupByRID(victim, true).asDocument().get("embedding");

          final LSMVectorIndex lsm = vectorIndex(db);
          lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
          assertThat(lsm.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);

          // The row whose rebuild is triggered is NOT the row being rewritten here: this one only leaves a mutation
          // in the delta buffer, so the rebuild has to re-read the victim's vector from its document.
          insertRow(db, "buffered", randomVector(rng));

          db.begin();
          db.lookupByRID(victim, true).asDocument().modify().set("embedding", rolledBackVector).save();
          lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
          db.rollback();
        } finally {
          db.close();
        }

        final Database reopened = factory.open();
        try {
          final LSMVectorIndex lsm = vectorIndex(reopened);

          assertThat((float[]) reopened.lookupByRID(victim, true).asDocument().get("embedding"))
              .as("precondition: the rollback took the record's embedding back")
              .containsExactly(committedVector);

          assertThat(ridsOf(lsm.findNeighborsFromVector(rolledBackVector, 5, 64)))
              .as("the persisted graph must not carry an embedding no committed transaction ever wrote")
              .doesNotContain(victim);

          assertThat(ridsOf(lsm.findNeighborsFromVector(committedVector, 5, 64)))
              .as("the persisted graph must carry the embedding the database actually holds")
              .contains(victim);
        } finally {
          reopened.drop();
        }
      }
    }

    @Test
    void rowsInsertedByARolledBackTransactionMustNotBeLeftInTheIndex() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      final Random rng = new Random(7974);

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          createSchema(db, false);

          insertSeedRows(db, rng);
          final LSMVectorIndex lsm = vectorIndex(db);
          lsm.findNeighborsFromVector(randomVector(rng), 5, 64);

          // The mutation that makes the search inside the transaction rebuild rather than just walk the graph.
          insertRow(db, "buffered", randomVector(rng));

          // Enough uncommitted rows that the index pages describe well under the bucket's document count: that is
          // what sends the rebuild down its document-scan recovery fallback, which used to sweep the bucket and take
          // whatever it found there - rows this transaction had not committed included, each one handed a vector id
          // and a place in the graph that the rollback below could not take back.
          final float[] uncommittedVector = farAwayVector();

          db.begin();
          for (int i = 0; i < NUM_VECTORS; i++)
            db.newDocument("Doc").set("name", "tx" + i).set("embedding", randomVector(rng)).save();
          db.newDocument("Doc").set("name", "txFar").set("embedding", uncommittedVector).save();

          lsm.findNeighborsFromVector(randomVector(rng), 5, 64);
          db.rollback();

          assertThat(db.countType("Doc", true))
              .as("precondition: the rollback took the transaction's own rows back")
              .isEqualTo(NUM_VECTORS + 1);
          assertThat(lsm.getStats().get("totalVectors"))
              .as("a rolled back insert must leave nothing behind in the index")
              .isEqualTo((long) NUM_VECTORS + 1);
          assertThat(ridsOf(lsm.findNeighborsFromVector(uncommittedVector, 5, 64)))
              .as("no result may be a row no committed transaction ever wrote")
              .allSatisfy(rid -> assertThat(db.lookupByRID(rid, true).asDocument().getString("name"))
                  .doesNotStartWith("tx"));
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void compactingInsideATransactionIsStillRefused() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      final Random rng = new Random(7974);

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          createSchema(db, false);
          insertSeedRows(db, rng);

          final LSMVectorIndex lsm = vectorIndex(db);
          assertThat(lsm.scheduleCompaction()).as("precondition: the compaction slot was taken").isTrue();

          // A compaction rewrites the data file outside transactional control, so it may not run under a caller's
          // transaction. The refusal is asked BEFORE the build suspends that transaction (issue #7974) - inside the
          // suspension it could only ever answer false - so this pins the question at its new evaluation point.
          db.begin();
          try {
            assertThatThrownBy(lsm::compact)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inside a transaction");
          } finally {
            db.rollback();
          }

          // The refusal hands the scheduling slot back, so ask for it again before the control run.
          assertThat(lsm.scheduleCompaction()).isTrue();
          assertThat(lsm.compact()).as("and outside a transaction it runs").isTrue();
        } catch (final IOException | InterruptedException e) {
          throw new IllegalStateException(e);
        } finally {
          db.drop();
        }
      }
    }

    private static void createSchema(final Database db, final boolean storeVectorsInGraph) {
      // Keep the background rebuild machinery out of the picture: the rebuild under test is the synchronous one the
      // small-graph arm performs on the calling thread.
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

      db.transaction(() -> {
        final DocumentType t = db.getSchema().createDocumentType("Doc");
        t.createProperty("name", Type.STRING);
        t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      });
      db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", \"storeVectorsInGraph\": " + storeVectorsInGraph + " }");
    }

    private static RID[] insertSeedRows(final Database db, final Random rng) {
      final RID[] rids = new RID[NUM_VECTORS];
      db.transaction(() -> {
        for (int i = 0; i < NUM_VECTORS; i++)
          rids[i] = db.newDocument("Doc").set("name", "doc" + i).set("embedding", randomVector(rng)).save().getIdentity();
      });
      return rids;
    }

    private static RID insertRow(final Database db, final String name, final float[] vector) {
      final RID[] rid = new RID[1];
      db.transaction(() -> rid[0] = db.newDocument("Doc").set("name", name).set("embedding", vector).save().getIdentity());
      return rid[0];
    }

    private static List<RID> ridsOf(final List<Pair<RID, Float>> neighbors) {
      return neighbors.stream().map(Pair::getFirst).toList();
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
    }

    /** Far enough from every seed vector that only the row actually carrying it can rank first for it. */
    private static float[] farAwayVector() {
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = 1000f + i;
      return v;
    }

    private static float[] randomVector(final Random rng) {
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = (float) rng.nextGaussian();
      return v;
    }
  }
}
