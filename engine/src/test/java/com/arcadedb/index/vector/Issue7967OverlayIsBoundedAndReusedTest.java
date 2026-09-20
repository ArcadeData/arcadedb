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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue7967OverlayIsBoundedAndReusedTest extends TestHelper {

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
      // The full-ranking agreement is Issue7378VectorSearchReadsOwnWritesTest's subject.
      assertThat(idsOf(index.findNeighborsFromVector(axis(i % DIMENSIONS), 3)))
          .as("and the search inside the transaction must agree with the same search after the commit on WHICH row "
              + "the query is nearest to")
          .isNotEmpty().first().isEqualTo(expected.get(i));
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
