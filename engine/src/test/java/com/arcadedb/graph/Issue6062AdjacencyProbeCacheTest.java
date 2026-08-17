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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6062: {@code CHECK DATABASE} paid an unconditional O(degree) linear walk of a
 * neighbour's adjacency list PER EDGE, so a super-node of degree D cost O(D²) and a real database was measured at
 * 80 hours.
 * <p>
 * Two independent probes produced that shape and both are covered here: {@code checkEdges} asked both endpoints of
 * every edge whether their list names the record ({@code EdgeLinkedList.containsEdge}), and {@code checkVertices}
 * asked the far vertex of every adjacency entry whether it points back ({@code isConnectedTo} →
 * {@code EdgeLinkedList.containsVertex}). Neither remembered anything between two probes of the SAME list, so a hub
 * referenced by D edges had its list walked D times.
 * <p>
 * The assertions are on counters the check now publishes, never on elapsed time: {@code adjacencyProbes} is how many
 * back-reference questions the run asked, {@code adjacencyProbeListWalks} how many of them had to touch an adjacency
 * list to answer, and {@code adjacencyEntriesScanned} how many list entries that cost in total. The fix makes the
 * second grow with the number of DISTINCT lists instead of with the number of edges, and the third linear in the size
 * of the graph instead of quadratic in the degree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6062AdjacencyProbeCacheTest extends TestHelper {
  private static final String VERTEX_TYPE = "Issue6062Node";
  private static final String EDGE_TYPE   = "Issue6062Link";
  private static final int    DEGREE      = 400;

  /**
   * One test deliberately leaves a half-linked edge behind - that is the state under measurement - and asserts the
   * check result itself. The blanket teardown run would only re-assert the same database with the opposite
   * expectation.
   */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().buildEdgeType().withName(EDGE_TYPE).withBidirectional(true).create();
    });
  }

  @AfterEach
  void restoreDefaults() {
    GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.reset();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.reset();
  }

  /**
   * The headline case, in its purest form: TWO vertices joined by many parallel edges, so every probe in the run
   * asks about one of exactly two adjacency lists. The number of lists the run has to walk must therefore stay in
   * the single digits however many edges there are - before the fix it was one walk per probe, which is one per
   * edge per endpoint.
   */
  @Test
  void theSameAdjacencyListIsNotRewalkedOncePerEdge() {
    createParallelEdgesBetweenTwoVertices();

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    final long probes = (Long) result.get("adjacencyProbes");
    final long walks = (Long) result.get("adjacencyProbeListWalks");

    assertThat(probes).as("both passes probe both endpoints of every edge").isGreaterThanOrEqualTo(2L * DEGREE);
    assertThat(walks)
        .as("only two adjacency lists exist in this graph, so a run must not walk one per probe")
        .isLessThanOrEqualTo(16L);
  }

  /**
   * The control: with the cache disabled the run falls back to the pre-fix behaviour, and the counter the test
   * above reads goes back to one walk per probe. Without this a cache that never engaged - or a counter that
   * measured something else - would satisfy the assertion above on its own.
   */
  @Test
  void disablingTheCacheRestoresOneListWalkPerProbe() {
    createParallelEdgesBetweenTwoVertices();

    GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    final long probes = (Long) result.get("adjacencyProbes");
    final long walks = (Long) result.get("adjacencyProbeListWalks");

    assertThat(probes).isGreaterThanOrEqualTo(2L * DEGREE);
    assertThat(walks).as("with the cache off every probe walks a list, which is the shape #6062 reports")
        .isEqualTo(probes);
  }

  /**
   * The super-node shape the issue actually describes: one hub with many DISTINCT neighbours. Here the number of
   * lists is proportional to the edge count, so the walk counter cannot discriminate - the cost is in how many
   * ENTRIES each walk visits, which is what makes a hub quadratic. Linear in the graph after the fix; before it,
   * every one of the {@code DEGREE} probes of the hub scanned ~{@code DEGREE/2} of its entries.
   */
  @Test
  void aHubIsScannedLinearlyRatherThanOncePerIncidentEdge() {
    createHubWithDistinctSpokes();

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    final long entriesScanned = (Long) result.get("adjacencyEntriesScanned");

    assertThat(entriesScanned)
        .as("the hub's list must be materialised once per pass, not re-scanned per incident edge")
        .isGreaterThan(0L)
        .isLessThanOrEqualTo(10L * DEGREE);
    assertThat((Long) result.get("adjacencyProbeListWalks"))
        .as("and the probes of the hub after the first must be answered without touching its list at all")
        .isLessThan((Long) result.get("adjacencyProbes"));
  }

  /**
   * The cache must not change a single finding. A HALF-LINKED edge - present in its source's OUT list, missing
   * from its target's IN list - is the finding the back-reference probe exists to make, and it is reported
   * identically whether the probe reads a materialised list or walks the chain.
   */
  @Test
  void theCachedAndTheDirectProbeAgreeOnAHalfLinkedEdge() {
    final RID target = createHubWithDistinctSpokes();

    // Drop the hub's IN list: every spoke's edge is now missing from its target's IN list.
    database.transaction(() -> {
      final MutableVertex hub = target.asVertex(true).modify();
      hub.setInEdgesHeadChunk(null);
      hub.save();
    });

    final Map<String, Object> cached = new DatabaseChecker(database).setVerboseLevel(0).check();

    GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);
    final Map<String, Object> direct = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(cached.get("edgesMissingInReference")).isEqualTo((long) DEGREE);
    assertThat(cached.get("edgesMissingInReference")).isEqualTo(direct.get("edgesMissingInReference"));
    assertThat(cached.get("edgesMissingOutReference")).isEqualTo(direct.get("edgesMissingOutReference"));
    assertThat(cached.get("unreachableEdgeRecords")).isEqualTo(direct.get("unreachableEdgeRecords"));
    assertThat(cached.get("missingReferenceBack")).isEqualTo(direct.get("missingReferenceBack"));
    assertThat(cached.get("totalCorruptedRecords")).isEqualTo(direct.get("totalCorruptedRecords"));
    assertThat(warnings(cached)).containsExactlyInAnyOrderElementsOf(warnings(direct));
  }

  /**
   * The PROMOTED super-node, where getting the memoisation wrong is not merely slow but wrong. A neighbour-keyed
   * probe of a {@link StripedEdgeList} reads only the stripes that can hold that neighbour and is strict about a
   * stripe head it cannot resolve; an image built from the whole list would skip such a chain silently. The cache
   * therefore memoises per CHAIN and leaves the selection to
   * {@link StripedEdgeList#chainsForNeighbourProbe(RID)} - this test is what reaches that override, and it fails if
   * the two paths ever disagree about a healthy striped hub.
   */
  @Test
  void aPromotedSuperNodeIsProbedThroughItsOwnStripeSelection() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    final RID hub = createHubWithDistinctSpokes();

    // PRECONDITION: the hub really is striped, or the test proves nothing about the striped path.
    database.transaction(() -> {
      final RID head = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
      assertThat(database.lookupByRID(head, true)).isInstanceOf(StripeDirectory.class);
    });

    final Map<String, Object> cached = new DatabaseChecker(database).setVerboseLevel(0).check();

    GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);
    final Map<String, Object> direct = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(warnings(cached)).isEmpty();
    assertThat(warnings(cached)).containsExactlyInAnyOrderElementsOf(warnings(direct));
    assertThat(cached.get("edgesMissingInReference")).isEqualTo(direct.get("edgesMissingInReference"));
    assertThat(cached.get("edgesMissingOutReference")).isEqualTo(direct.get("edgesMissingOutReference"));
    assertThat(cached.get("missingReferenceBack")).isEqualTo(direct.get("missingReferenceBack"));
    assertThat((Long) cached.get("adjacencyProbeListWalks"))
        .as("the striped hub's chains must still be memoised, not re-walked per incident edge")
        .isLessThan((Long) direct.get("adjacencyProbeListWalks"));
  }

  /**
   * The FALLBACK path: a budget smaller than the hub's degree. That list cannot be represented at all, so it is
   * walked to the budget, abandoned, and answered by the original {@code EdgeLinkedList} probe - and the verdict is
   * remembered, so the abandoned walk happens once rather than once per probe. What must not change is a single
   * finding, which is what this asserts against both the fully cached and the fully disabled run.
   */
  @Test
  void aListLargerThanTheWholeBudgetFallsBackWithoutChangingAFinding() {
    createHubWithDistinctSpokes();

    final Map<String, Object> cached = new DatabaseChecker(database).setVerboseLevel(0).check();

    // Below the hub's degree, above a spoke's: the hub falls back, the spokes still cache.
    GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(10);
    final Map<String, Object> partial = new DatabaseChecker(database).setVerboseLevel(0).check();

    GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);
    final Map<String, Object> direct = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(warnings(partial)).isEmpty();
    assertThat(partial.get("edgesMissingInReference")).isEqualTo(cached.get("edgesMissingInReference"));
    assertThat(partial.get("edgesMissingOutReference")).isEqualTo(cached.get("edgesMissingOutReference"));
    assertThat(partial.get("missingReferenceBack")).isEqualTo(direct.get("missingReferenceBack"));
    assertThat(partial.get("unreachableEdgeRecords")).isEqualTo(direct.get("unreachableEdgeRecords"));
    assertThat(partial.get("totalCorruptedRecords")).isEqualTo(direct.get("totalCorruptedRecords"));

    // The verdict on the hub is REMEMBERED: its list is abandoned once, not once per incident edge, so the run
    // stays far below the "walk the hub per probe" volume the fully disabled run pays.
    assertThat((Long) partial.get("adjacencyEntriesScanned"))
        .as("the over-budget list must be walked to the budget once, not once per probe")
        .isLessThanOrEqualTo(5L * DEGREE);
  }

  /**
   * A CHUNK POINTING AT ITSELF must end the walk, not feed it forever.
   * <p>
   * {@code EdgeLinkedList.containsEdge}/{@code containsVertex} have always broken out of such a chain; the iterator
   * behind {@code entryIterator()} did not - and the checker walks a vertex's own edge list through it, so this
   * shape of corruption meant a {@code CHECK DATABASE} that never returned. This PR gives the same iterator a second
   * consumer (the probe cache materialises through it), which is why the guard belongs here now.
   * <p>
   * Asserted by walking under a HARD CAP rather than by running the check under a timeout: an unterminating walk
   * does not fail a test, it hangs the build, and a regression has to come back as a red test. Without the guard the
   * walk reaches the cap and the assertion fails in milliseconds.
   */
  @Test
  void aSelfReferencingChunkEndsTheWalkInsteadOfFeedingItForever() {
    final RID hub = createHubWithDistinctSpokes();

    // Point the hub's IN head chunk at itself.
    database.transaction(() -> {
      final RID head = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
      final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(head, true);
      segment.setPrevious(segment);
      ((DatabaseInternal) database).updateRecord(segment);
    });

    final int cap = 8 * DEGREE;
    final int[] walked = new int[1];
    database.transaction(() -> {
      final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
          .getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
      final Iterator<Pair<RID, RID>> entries = list.entryIterator();
      while (walked[0] <= cap && entries.hasNext()) {
        entries.next();
        ++walked[0];
      }
    });

    assertThat(walked[0])
        .as("the self-loop must end the walk; without the guard it yields the chunk's entries forever")
        .isLessThanOrEqualTo(DEGREE);
  }

  /** A healthy graph stays healthy: the cache must not invent a finding either. */
  @Test
  void aHealthyGraphReportsNothing() {
    createHubWithDistinctSpokes();

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(warnings(result)).isEmpty();
    assertThat((Long) result.get("totalCorruptedRecords")).isZero();
    assertThat((Long) result.get("unreachableEdgeRecords")).isZero();
    assertThat((Long) result.get("edgesMissingInReference")).isZero();
    assertThat((Long) result.get("edgesMissingOutReference")).isZero();
  }

  /** Two vertices, {@code DEGREE} parallel edges between them: exactly two adjacency lists in the whole database. */
  private void createParallelEdgesBetweenTwoVertices() {
    database.transaction(() -> {
      final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
      final MutableVertex destination = database.newVertex(VERTEX_TYPE).set("name", "destination").save();
      for (int i = 0; i < DEGREE; i++)
        source.newEdge(EDGE_TYPE, destination);
    });
  }

  /** One hub with {@code DEGREE} distinct spokes, each pointing at it: the super-node shape from the issue. */
  private RID createHubWithDistinctSpokes() {
    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
    database.transaction(() -> {
      final MutableVertex target = hub[0].asVertex(true).modify();
      for (int i = 0; i < DEGREE; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
    });
    return hub[0];
  }

  @SuppressWarnings("unchecked")
  private Collection<String> warnings(final Map<String, Object> result) {
    return (Collection<String>) result.get("warnings");
  }
}
