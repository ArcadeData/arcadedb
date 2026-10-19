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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10338: transactions committed right after {@code CREATE GRAPH ANALYTICAL VIEW} (status BUILDING) were replayed over
 * the CSR the build published, and the replay of a vertex that took the slot of a deleted one read that node's edges from the
 * CSR with its absolute offsets passed to accessors that add the node's offset themselves. Past the first node that was an
 * {@code ArrayIndexOutOfBoundsException}, logged as "Cannot deliver a committed change" with the change dropped, or a wrong
 * count of the edges to subtract; a build that hit it never published.
 * <p>
 * Two things follow whatever the cause, and are pinned too: a committed change a SYNCHRONOUS view cannot merge makes it rebuild
 * instead of being dropped with a warning, and so does a buffered change the build cannot replay.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10338CommitRightAfterCreateViewTest extends TestHelper {
  private static final String EDGE_TYPE = "L";
  private static final RID    SECOND    = new RID(1, 1);
  private static final RID    THIRD     = new RID(1, 2);

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
  }

  // The three tests below replay a delta over hand-made CSRs: the RIDs are synthetic (one bucket, the position being the node id),
  // no database is involved

  /** The tail of a chain sits on the slot that is replayed: its in-edge starts past the first entry of the reverse index. */
  @Test
  void replayOfAReusedSlotWhoseInEdgeIsNotTheFirstEntry() {
    // 0 -> 1 -> 2, the node 2 (offset 1 in the reverse index) is the slot that was deleted and taken by a new vertex
    final NodeIdMapping mapping = mapping(3);
    final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 2, 2 }, new int[] { 1, 2 }, new int[] { 0, 0, 1, 2 },
        new int[] { 0, 1 }, 3, 2);

    final DeltaOverlay overlay = replayReuse(mapping, csr, THIRD);

    assertThat(overlay.isDeleted(2)).as("the base node stays masked").isTrue();
    assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("the edge 1 -> 2 of the scan is not counted").isEqualTo(1);
  }

  /** A slot in the middle of a ring has an in-edge and an out-edge, both past the start of their arrays. */
  @Test
  void replayOfAReusedSlotCountsItsOwnEdgesAndNotTheNeighboursOf() {
    // the ring 0 -> 1 -> 2 -> 3 -> 0: the node 1 is replayed, and the two edges it holds (0 -> 1, 1 -> 2) stop counting
    final NodeIdMapping mapping = mapping(4);
    final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 2, 3, 4 }, new int[] { 1, 2, 3, 0 },
        new int[] { 0, 1, 2, 3, 4 }, new int[] { 3, 0, 1, 2 }, 4, 4);

    final DeltaOverlay overlay = replayReuse(mapping, csr, SECOND);

    assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("2 -> 3 and 3 -> 0 are left").isEqualTo(2);
  }

  /** A self-loop is one edge, not one on each side. */
  @Test
  void replayOfAReusedSlotWithASelfLoopCountsItOnce() {
    // 0 -> 1, 1 -> 1, 1 -> 2: the node 1 holds three edges, one of them a loop
    final NodeIdMapping mapping = mapping(3);
    final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 3, 3 }, new int[] { 1, 1, 2 }, new int[] { 0, 0, 2, 3 },
        new int[] { 0, 1, 1 }, 3, 3);

    final DeltaOverlay overlay = replayReuse(mapping, csr, SECOND);

    assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("nothing is left").isZero();
  }

  /** The scenario of the issue, made deterministic: the commits land while the build waits for a permit, and are replayed. */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void commitsRightAfterCreateAreReplayedOnThePublishedBuild(final String mode) {
    final MutableVertex[] v = new MutableVertex[7];
    database.transaction(() -> {
      // an edge before the slot that is reused: its node does not start at the first offset of the CSR
      database.newVertex("P").set("id", 1L).save().newEdge("L", database.newVertex("P").set("id", 1L).save()).save();
      v[0] = database.newVertex("P").set("id", 2L).save();
      v[1] = database.newVertex("Q").set("id", 6L).save();
      v[2] = database.newVertex("P").set("id", 6L).save();
      v[0].newEdge("L", v[2]).save();
      v[1].newEdge("K", v[1]).save();
      v[5] = database.newVertex("Q").set("id", 6L).save();
      v[6] = database.newVertex("P").set("id", 5L).save();
      v[5].newEdge("K", v[6]).save();
    });

    final MutableVertex[] created = new MutableVertex[2];
    final int held = GraphAnalyticalView.BUILD_PERMITS.drainPermits();
    final GraphAnalyticalView view;
    try {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE " + mode)
          .close();
      view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.getStatus()).as("the build waits for a permit").isEqualTo(GraphAnalyticalView.Status.BUILDING);

      database.transaction(() -> v[0].delete());
      database.transaction(() -> created[0] = database.newVertex("P").set("id", 2L).save());
      database.transaction(() -> v[6].delete());
      database.transaction(() -> {
        created[1] = database.newVertex("P").set("id", 4L).save();
        created[0].newEdge("L", created[1]).save();
      });
    } finally {
      GraphAnalyticalView.BUILD_PERMITS.release(held);
    }

    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).as("the build publishes").isTrue();
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(edgesAnsweredByCypher("L")).as("the two L edges that are left").isEqualTo(2);
    assertThat(edgesAnsweredByCypher("K")).as("the K self-loop that is left").isEqualTo(1);
  }

  @Test
  void aCommittedChangeTheViewCannotMergeMakesItRebuildInsteadOfBeingDropped() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("id", 1L).save();
      a.newEdge("K", database.newVertex("P").set("id", 2L).save()).save();
    });
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K) PROPERTIES (id) UPDATE MODE SYNCHRONOUS")
        .close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

    // a change the overlay cannot take: a vertex without an identity
    final TxDelta unmergeable = new TxDelta();
    unmergeable.addedVertices.add(new TxDelta.VertexDelta(null, Map.of()));
    // The rebuild waits for a permit, so the view can be seen leaving READY: a change dropped with a warning would leave it there
    final int held = GraphAnalyticalView.BUILD_PERMITS.drainPermits();
    try {
      view.onCommittedDelta(unmergeable);
      assertThat(view.getStatus()).as("the unmergeable change asks for a rebuild").isNotEqualTo(GraphAnalyticalView.Status.READY);
    } finally {
      GraphAnalyticalView.BUILD_PERMITS.release(held);
    }

    // the graph is unchanged, so the rebuild the failure asked for publishes the same view
    awaitReadyAgain(view);
    assertThat(view.getEdgeCount()).isEqualTo(1);
    assertThat(view.getNodeCount()).isEqualTo(2);
  }

  /** A change buffered while the first build ran that the build cannot replay must not leave the view NOT_BUILT. */
  @Test
  void aBufferedChangeTheBuildCannotReplayDoesNotFailTheBuild() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("id", 1L).save();
      a.newEdge("K", database.newVertex("P").set("id", 2L).save()).save();
    });

    final int held = GraphAnalyticalView.BUILD_PERMITS.drainPermits();
    final GraphAnalyticalView view;
    try {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K) PROPERTIES (id) UPDATE MODE SYNCHRONOUS")
          .close();
      view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.getStatus()).as("the build waits for a permit").isEqualTo(GraphAnalyticalView.Status.BUILDING);

      final TxDelta unmergeable = new TxDelta();
      unmergeable.addedVertices.add(new TxDelta.VertexDelta(null, Map.of()));
      view.onCommittedDelta(unmergeable);
    } finally {
      GraphAnalyticalView.BUILD_PERMITS.release(held);
    }

    awaitReadyAgain(view);
    assertThat(view.getEdgeCount()).isEqualTo(1);
    assertThat(view.getNodeCount()).isEqualTo(2);
  }

  /** The view rebuilds after the failure: it is READY again, once the rebuild it asked for published. */
  private static void awaitReadyAgain(final GraphAnalyticalView view) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> view.getStatus() == GraphAnalyticalView.Status.READY);
  }

  /** The delete of the old vertex and the add of the new one that took its slot, replayed over a fresh base that holds both. */
  private DeltaOverlay replayReuse(final NodeIdMapping mapping, final CSRAdjacencyIndex csr, final RID reused) {
    final Map<String, CSRAdjacencyIndex> fresh = Map.of(EDGE_TYPE, csr);
    final TxDelta deleteOld = new TxDelta();
    deleteOld.deletedVertices.add(reused);
    final TxDelta addNew = new TxDelta();
    addNew.addedVertices.add(new TxDelta.VertexDelta(reused, Map.of()));

    return new DeltaOverlay(mapping.size()).merge(deleteOld, mapping, fresh, (type, src, tgt) -> 0)
        .merge(addNew, mapping, fresh, (type, src, tgt) -> 0);
  }

  private static NodeIdMapping mapping(final int nodes) {
    final NodeIdMapping mapping = new NodeIdMapping(1);
    final int bucket = mapping.registerBucket(1, "P", nodes);
    for (int i = 0; i < nodes; i++)
      mapping.addNode(bucket, i);
    mapping.compact();
    return mapping;
  }

  private long edgesAnsweredByCypher(final String edgeType) {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:" + edgeType + "]->() RETURN count(r) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
