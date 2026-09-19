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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7378VectorSearchReadsOwnWritesTest extends TestHelper {

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
   * The same record's embedding rewritten twice before the search. Both rewrites queue their own {@code ADD}, and
   * {@code TransactionIndexContext.commit()} replays every one of them, so what the committed index ends up
   * holding for that RID is whatever those two adds make of it. The overlay is a model of that replay and must
   * therefore give the same answer as the commit here too - which is the only claim this test makes. Whether the
   * engine ought to collapse two adds of one RID into one vector is a separate question about the commit path,
   * not about the overlay, and this test is what would notice if the two ever stopped agreeing.
   */
  @Test
  void twoRewritesOfOneRowAgreeWithWhatTheCommitLeavesBehind() {
    seedAndBuildGraph();
    final LSMVectorIndex index = vectorIndex();

    database.begin();
    database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = 'doc0'", (Object) seedVector(99));
    database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = 'doc0'", (Object) PENDING_DIRECTION);
    final List<String> inTx = idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5));
    database.commit();

    assertThat(idsOf(index.findNeighborsFromVector(PENDING_DIRECTION, 5)))
        .as("two rewrites in one transaction must look the same before and after the commit")
        .isEqualTo(inTx);
    assertThat(inTx).as("and the row the second rewrite aimed at the query must lead either way")
        .isNotEmpty().first().isEqualTo("doc0");
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
    assertThat(index.getStats().get("deltaVectorsCount"))
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
