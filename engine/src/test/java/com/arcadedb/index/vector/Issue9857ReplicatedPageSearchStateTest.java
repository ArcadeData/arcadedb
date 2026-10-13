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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9857, at the level of one index: a page that reaches {@link LSMVectorIndex#applyReplicatedPageUpdate} from a PEER
 * changes the search state as a local write does - a tombstone over a live id counts as a mutation and turns the graph
 * MUTABLE, once, however many times the page is applied - while the same page replayed from this database's own WAL at
 * open only moves the locations, because the first search reconciles the persisted graph against the pages and charges
 * the tombstones it finds there itself (issue #7842).
 * <p>
 * The peer's delete is simulated with a tombstone appended to the pages only, which the location index has not seen:
 * exactly the state a replicated page arrives in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9857ReplicatedPageSearchStateTest extends TestHelper {
  private static final String TYPE_NAME = "Probe";
  private static final int    RECORDS   = 20;

  private LSMVectorIndex index;

  @BeforeEach
  void createIndexAndBuildGraph() {
    final VertexType type = database.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("id", Integer.class);
    type.createProperty("vector", float[].class);
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "vector" })
        .withLSMVectorType();
    builder.withDimensions(2);
    final TypeIndex typeIndex = builder.create();
    index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex(TYPE_NAME).set("id", i, "vector", new float[] { i + 1f, 1f }).save();
    });

    // The first search builds the graph: from here on only a mutation makes the next search rebuild it
    assertThat(index.findNeighborsFromVector(new float[] { 1f, 1f }, 5)).hasSize(5);
    assertThat(index.getStats().get("mutationsSinceRebuild")).as("precondition: the build paid every write off").isZero();
    assertThat(index.getStats().get("graphState")).as("precondition: the graph is IMMUTABLE (1)").isEqualTo(1L);
  }

  @Test
  void aPeerTombstoneIsChargedOnceAndTurnsTheGraphMutable() {
    final int vectorId = tombstoneOnThePagesOnly(3);

    index.applyReplicatedPageUpdate(lastPage(), true);

    assertThat(index.residentLocationsForTest().isDeleted(vectorId)).isTrue();
    assertThat(index.getStats().get("mutationsSinceRebuild"))
        .as("the tombstone is one mutation, and the live entries the page also carries are none").isEqualTo(1L);
    assertThat(index.getStats().get("graphState")).as("the graph owes a rebuild: MUTABLE (2)").isEqualTo(2L);

    // The same page applied again (a torn-write repair, a node's own commit echoed back) changes nothing
    index.applyReplicatedPageUpdate(lastPage(), true);
    assertThat(index.getStats().get("mutationsSinceRebuild")).isEqualTo(1L);
  }

  @Test
  void theSamePageReplayedByRecoveryMovesTheLocationsOnly() {
    final int vectorId = tombstoneOnThePagesOnly(3);

    index.applyReplicatedPageUpdate(lastPage(), false);

    assertThat(index.residentLocationsForTest().isDeleted(vectorId)).as("the locations still follow the page").isTrue();
    assertThat(index.getStats().get("mutationsSinceRebuild")).as("recovery charges nothing").isZero();
    assertThat(index.getStats().get("graphState")).as("and leaves the graph as it was: IMMUTABLE (1)").isEqualTo(1L);
  }

  /** Appends a tombstone for the record with this {@code id} to the pages, leaving the location index untouched. */
  private int tombstoneOnThePagesOnly(final int id) {
    final RID rid;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE_NAME + " WHERE id = ?", id)) {
      rid = rs.next().getProperty("rid");
    }
    final int[] vectorIds = index.residentLocationsForTest().getVectorIdsForRid(rid);
    assertThat(vectorIds).hasSize(1);
    database.transaction(() -> index.persistTombstoneForTest(vectorIds[0], rid));
    assertThat(index.residentLocationsForTest().isLive(vectorIds[0])).as("precondition: only the pages know of the delete")
        .isTrue();
    return vectorIds[0];
  }

  private MutablePage lastPage() {
    final DatabaseInternal db = (DatabaseInternal) database;
    try {
      final ImmutablePage page = db.getPageManager()
          .getImmutablePage(new PageId(db, index.getFileId(), index.getTotalPages() - 1), index.getPageSize(), false, false);
      return page.modify();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
