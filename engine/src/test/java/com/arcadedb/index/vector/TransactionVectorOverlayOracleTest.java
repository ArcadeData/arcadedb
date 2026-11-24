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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.vector.LSMVectorIndex.DeltaVectorEntry;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the overlay of a transaction tells the search paths, asked directly: the rows it contributes, the committed rows it supersedes
 * (a rewrite, a removal, a delete, and the rows it adds, which hide their own RID), what it does to the buffer of delta vectors, and
 * when it is absent. The searches built on it are in {@link VectorTransactionSearchTest}; those cannot tell a wrong {@code supersedes},
 * {@code supersededRIDs} or {@code pendingCount} from the right one, because each of the three is covered by another of them.
 */
class TransactionVectorOverlayOracleTest extends TestHelper {
  private static final int               DIM = 8;
  private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

  private final List<RID> seeded = new ArrayList<>();

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

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE DOCUMENT TYPE Other BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Other.key STRING");
      database.command("sql", "CREATE INDEX ON Other (key) NOTUNIQUE");
    });
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + DIM + ", similarity: 'COSINE'}").close();
  }

  private void seed(final int rows) {
    database.transaction(() -> {
      for (int i = 0; i < rows; i++)
        seeded.add(database.newDocument("Doc").set("id", "d" + i).set("embedding", axis(i)).save().getIdentity());
    });
  }

  private LSMVectorIndex index() {
    return (LSMVectorIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];
  }

  private TransactionVectorOverlay overlay(final int budget) {
    return TransactionVectorOverlay.open((DatabaseInternal) database, index(), VTS, budget);
  }

  private static float[] axis(final int i) {
    final float[] v = new float[DIM];
    v[i % DIM] = 1f;
    return v;
  }

  private static DeltaVectorEntry buffered(final int vectorId, final RID rid) {
    return new DeltaVectorEntry(vectorId, rid, VTS.createFloatVector(axis(vectorId)));
  }

  @Test
  void theOverlayKnowsWhichCommittedRowsItSupersedesAndHowManyItContributes() {
    seed(6);

    database.begin();
    final MutableDocument rewritten = database.lookupByRID(seeded.get(0), true).asDocument().modify();
    rewritten.set("embedding", axis(6));
    rewritten.save();
    final MutableDocument removed = database.lookupByRID(seeded.get(1), true).asDocument().modify();
    removed.remove("embedding");
    removed.save();
    database.lookupByRID(seeded.get(2), true).asDocument().delete();
    final RID added = database.newDocument("Doc").set("id", "added").set("embedding", axis(7)).save().getIdentity();

    final TransactionVectorOverlay overlay = overlay(100);
    assertThat(overlay).isNotNull();
    assertThat(overlay.pendingCount()).as("the rewritten row and the added one, not the removed or the deleted ones").isEqualTo(2);
    assertThat(overlay.pendingPayloadsDeclined()).isZero();

    final Set<RID> expected = new HashSet<>(List.of(seeded.get(0), seeded.get(1), seeded.get(2), added));
    assertThat(overlay.supersededRIDs()).as("rewritten, removed, deleted, and the row the transaction added hides its own RID")
        .containsExactlyInAnyOrderElementsOf(expected);
    for (final RID rid : expected)
      assertThat(overlay.supersedes(rid)).as("supersedes %s", rid).isTrue();
    for (int i = 3; i < 6; i++)
      assertThat(overlay.supersedes(seeded.get(i))).as("a row the transaction never touched: %s", seeded.get(i)).isFalse();
    database.rollback();
  }

  @Test
  void theOverlayAppliesToTheBufferOfDeltaVectorsOnlyWhereItChangesIt() {
    seed(6);
    final DeltaVectorEntry committedRow3 = buffered(3, seeded.get(3));
    final DeltaVectorEntry committedRow4 = buffered(4, seeded.get(4));
    final DeltaVectorEntry committedRow0 = buffered(0, seeded.get(0));

    database.begin();
    final MutableDocument rewritten = database.lookupByRID(seeded.get(0), true).asDocument().modify();
    rewritten.set("embedding", axis(6));
    rewritten.save();
    final RID added = database.newDocument("Doc").set("id", "added").set("embedding", axis(7)).save().getIdentity();
    final TransactionVectorOverlay overlay = overlay(100);
    assertThat(overlay).isNotNull();

    // nothing buffered: the answer is the overlay's own rows, and the same list every time (no copy)
    final List<DeltaVectorEntry> alone = overlay.augment(List.of());
    assertThat(alone).extracting(e -> e.rid).containsExactlyInAnyOrder(seeded.get(0), added);
    assertThat(alone).allMatch(e -> e.vectorId == TransactionVectorOverlay.PENDING_VECTOR_ID);
    assertThat(overlay.augment(List.of())).isSameAs(alone);

    // a buffered row the transaction rewrote is dropped and its new version appended after the rows that stay
    final List<DeltaVectorEntry> committed = List.of(committedRow3, committedRow0, committedRow4);
    final List<DeltaVectorEntry> merged = overlay.augment(committed);
    assertThat(merged).hasSize(4);
    assertThat(merged.subList(0, 2)).containsExactly(committedRow3, committedRow4);
    assertThat(merged.subList(2, 4)).extracting(e -> e.rid).containsExactlyInAnyOrder(seeded.get(0), added);
    assertThat(merged).doesNotContain(committedRow0);

    // buffered rows the transaction left alone: its rows are appended to a copy, the buffer itself is never touched
    final List<DeltaVectorEntry> untouched = List.of(committedRow3, committedRow4);
    final List<DeltaVectorEntry> appended = overlay.augment(untouched);
    assertThat(appended).hasSize(4).isNotSameAs(untouched);
    assertThat(appended.subList(0, 2)).containsExactly(committedRow3, committedRow4);
    assertThat(untouched).hasSize(2);
    database.rollback();

    // a transaction that only deleted a row the buffer does not hold has nothing to add and nothing to drop: the buffer comes back as it is
    database.begin();
    database.lookupByRID(seeded.get(2), true).asDocument().delete();
    final TransactionVectorOverlay deletes = overlay(100);
    assertThat(deletes).isNotNull();
    assertThat(deletes.pendingCount()).isZero();
    assertThat(deletes.supersededRIDs()).containsExactly(seeded.get(2));
    assertThat(deletes.augment(untouched)).isSameAs(untouched);
    database.rollback();
  }

  @Test
  void aTransactionThatWroteToAnotherIndexOnlyHasNoOverlayAndSearchesWithoutOneRepeatedly() {
    seed(6);
    final List<String> before = ids(index().findNeighborsFromVector(axis(0), 3));

    database.begin();
    database.newDocument("Other").set("key", "x").save();
    assertThat(overlay(100)).as("it queued entries, none of them for the vector index").isNull();
    assertThat(overlay(100)).as("and the answer is remembered: asking again is the same null, not a failure").isNull();
    for (int i = 0; i < 3; i++)
      assertThat(ids(index().findNeighborsFromVector(axis(0), 3))).isEqualTo(before);
    database.rollback();
  }

  @Test
  void aTransactionThatWroteTheOnlyRowsOfAnEmptyIndexFindsThemAndLosesThemOnRollback() {
    assertThat(index().findNeighborsFromVector(axis(0), 5)).isEmpty();

    database.begin();
    final List<RID> written = new ArrayList<>();
    for (int i = 0; i < 3; i++)
      written.add(database.newDocument("Doc").set("id", "p" + i).set("embedding", axis(i)).save().getIdentity());

    final TransactionVectorOverlay overlay = overlay(100);
    assertThat(overlay).isNotNull();
    assertThat(overlay.pendingCount()).isEqualTo(3);
    final List<Pair<RID, Float>> found = index().findNeighborsFromVector(axis(1), 5);
    assertThat(found).as("k is not clamped to the (empty) committed index: the rows of the transaction count").hasSize(3);
    assertThat(found.get(0).getFirst()).isEqualTo(written.get(1));
    assertThat(index().findNeighborsFromVector(axis(1), 2)).hasSize(2);
    database.rollback();

    assertThat(index().findNeighborsFromVector(axis(1), 5)).isEmpty();
  }

  private List<String> ids(final List<Pair<RID, Float>> results) {
    final List<String> ids = new ArrayList<>();
    for (final Pair<RID, Float> r : results)
      ids.add((String) database.lookupByRID(r.getFirst(), true).asDocument().get("id"));
    return ids;
  }
}
