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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.ResettableIterator;
import com.sun.management.ThreadMXBean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression tests, one nested class per fixed issue.
 */
class EdgeIteratorWalkRegressionTest {

  /**
   * Regression for #10148: {@code GraphEngine.getConnectedVertexEntries} walks the adjacency entries of a vertex on the RIDs they hold, as
   * {@code getConnectedVertexRIDs} does, and names the edge of the entry it last returned without reading a record. Over every list
   * layout (a plain list, with and without an edge-type filter, and the striped layout of a promoted super-node) it must return
   * the vertices in the order {@code getConnectedVertexRIDs} does, each with the RID of the edge that links it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeEntryRIDIteratorTest extends TestHelper {
    private int savedThreshold;
    private int savedStripes;

    @BeforeEach
    void saveConfig() {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    }

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("Hub");
      database.getSchema().createVertexType("Src");
      database.getSchema().createEdgeType("LINK");
      database.getSchema().createEdgeType("OTHER");
    }

    @Test
    void aPlainListNamesTheEdgeOfEveryEntry() {
      final RID hubId = buildHub(40);

      database.transaction(() -> {
        assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT);
        assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "LINK");
        assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "OTHER");
        assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "LINK", "OTHER");
      });
    }

    @Test
    void theIncomingListNamesTheEdgeOfEveryEntry() {
      final RID hubId = buildHub(40);

      database.transaction(() -> {
        final Vertex hub = hubId.asVertex();
        final List<RID> neighbors = new ArrayList<>();
        for (final Edge edge : hub.getEdges(Vertex.DIRECTION.OUT, "LINK"))
          neighbors.add(edge.getIn());
        // the far end sees the same edge in its incoming list
        for (final RID neighbor : neighbors)
          assertEveryEntryNamesItsEdge(neighbor, Vertex.DIRECTION.IN, "LINK");
      });
    }

    @Test
    void aPromotedSuperNodeNamesTheEdgeOfEveryEntryInTheOrderOfTheRidWalk() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(4);
      final RID hubId = buildHub(600);

      // PRECONDITION: THE HUB MUST REALLY HAVE BEEN PROMOTED, OTHERWISE THE TEST PROVES NOTHING
      database.transaction(() -> {
        final RID head = ((VertexInternal) hubId.asVertex(true)).getOutEdgesHeadChunk();
        assertThat(database.lookupByRID(head, true)).isInstanceOf(StripeDirectory.class);
        assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT);
        assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "LINK");
      });
    }

    @Test
    void aVertexWithoutEdgesHasNoEntries() {
      final RID[] lone = new RID[1];
      database.transaction(() -> lone[0] = database.newVertex("Src").save().getIdentity());
      final RID loneId = lone[0];

      database.transaction(() -> {
        final EdgeEntryRIDIterator entries = engine().getConnectedVertexEntries((VertexInternal) loneId.asVertex(), Vertex.DIRECTION.OUT);
        assertThat(entries.hasNext()).isFalse();
        assertThatThrownBy(entries::next).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(entries::getEdgeRID).isInstanceOf(IllegalStateException.class);
      });
    }

    @Test
    void theEdgeIsAskedAfterAnEntryWasReturned() {
      final RID hubId = buildHub(3);

      database.transaction(() -> {
        final EdgeEntryRIDIterator entries = engine().getConnectedVertexEntries((VertexInternal) hubId.asVertex(), Vertex.DIRECTION.OUT);
        assertThatThrownBy(entries::getEdgeRID).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> engine().getConnectedVertexEntries((VertexInternal) hubId.asVertex(), Vertex.DIRECTION.BOTH))
            .isInstanceOf(IllegalArgumentException.class);
      });
    }

    /** The hub points to {@code count} vertices over LINK (with a property, so a record each) and to a few over OTHER. */
    private RID buildHub(final int count) {
      final RID[] hubId = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub").save();
        for (int i = 0; i < count; i++) {
          final MutableVertex src = database.newVertex("Src").set("i", i).save();
          hub.newEdge("LINK", src).set("n", i).save();
        }
        for (int i = 0; i < 5; i++)
          hub.newEdge("OTHER", database.newVertex("Src").set("i", -i).save()).set("n", -i).save();
        hubId[0] = hub.getIdentity();
      });
      return hubId[0];
    }

    /** The entries come in the order of the RID walk, and the edge each names is the edge that links the vertex it returned. */
    private void assertEveryEntryNamesItsEdge(final RID vertexId, final Vertex.DIRECTION direction, final String... types) {
      final VertexInternal vertex = (VertexInternal) vertexId.asVertex();
      final List<RID> expected = new ArrayList<>();
      for (final RID neighbor : engine().getConnectedVertexRIDs(vertex, direction, types))
        expected.add(neighbor);

      // The edges as the edge walk sees them: the far end each links
      final Map<RID, RID> edgeOfNeighbor = new HashMap<>();
      for (final Edge edge : vertex.getEdges(direction, types))
        edgeOfNeighbor.put(direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut(), edge.getIdentity());

      final List<RID> seen = new ArrayList<>();
      final EdgeEntryRIDIterator entries = engine().getConnectedVertexEntries(vertex, direction, types);
      while (entries.hasNext()) {
        final RID neighbor = entries.next();
        seen.add(neighbor);
        assertThat(entries.getEdgeRID()).as("the edge of the entry for %s", neighbor).isEqualTo(edgeOfNeighbor.get(neighbor));
        // asking twice is the same answer, and asking does not move the walk
        assertThat(entries.getEdgeRID()).isEqualTo(edgeOfNeighbor.get(neighbor));
      }

      assertThat(seen).as("%s %s", direction, List.of(types)).isEqualTo(expected);
      assertThat(seen).isNotEmpty();
    }

    private GraphEngine engine() {
      return ((DatabaseInternal) database).getGraphEngine();
    }
  }

  /**
   * Regression for #10151: the {@link ResettableIterator} contract, driven over EVERY adjacency iterator the engine builds rather than
   * over one class. A {@code hasNext()} peeks the next entry and keeps it; a {@code reset()} that forgets to drop it hands
   * it out once more before the rewound walk hands it out again, so the ordinary "is there any neighbour?" check, followed
   * by a loop over the same {@code Iterable} (whose {@code iterator()} resets every source), yields the first neighbour
   * twice. {@code IteratorFilterBase} and {@code RIDIteratorFilter} both had that hole, each in its own copy of the peek.
   * <p>
   * For every walk - vertices, edges, neighbour RIDs and the raw edge-list iterators, OUT/IN/BOTH, with and without a type
   * filter and a neighbour filter, on the classic and on the striped super-node layout - {@code peek; reset; drain} and
   * {@code take k; peek; reset; drain} must both equal a fresh drain.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AdjacencyIteratorResetContractTest {
    private static final String DB_PATH   = "./target/databases/issue10151-reset-contract";
    private static final int    RECORD_E  = 40;
    private static final int    RECORD_F  = 10;
    private static final int    LIGHT_L   = 10;
    private static final int    INCOMING  = 12;
    private static final int    THRESHOLD = 16;

    private Database database;
    private int      savedThreshold;
    private int      savedStripes;

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    }

    @ParameterizedTest(name = "striped={0}")
    @ValueSource(booleans = { false, true })
    void peekResetDrainEqualsAFreshDrainOnEveryAdjacencyIterator(final boolean striped) {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
      // 0 DISABLES THE PROMOTION, SO THE CLASSIC RUN NEVER BECOMES A STRIPED ONE WHATEVER THE DEFAULT THRESHOLD IS
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(striped ? THRESHOLD : 0);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(4);

      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      final RID[] hubAndNeighbour = new RID[2];
      database.transaction(() -> {
        database.getSchema().createVertexType("V");
        database.getSchema().createEdgeType("E");
        database.getSchema().createEdgeType("F");
        database.getSchema().createEdgeType("L");

        final MutableVertex hub = database.newVertex("V").set("name", "hub").save();
        final MutableVertex neighbour = database.newVertex("V").set("name", "neighbour").save();
        hubAndNeighbour[0] = hub.getIdentity();
        hubAndNeighbour[1] = neighbour.getIdentity();
        for (int i = 0; i < RECORD_E; i++)
          hub.newEdge("E", i % 7 == 0 ? neighbour : database.newVertex("V").set("n", i).save());
        for (int i = 0; i < RECORD_F; i++)
          hub.newEdge("F", database.newVertex("V").set("f", i).save());
        for (int i = 0; i < LIGHT_L; i++)
          hub.newLightEdge("L", i % 3 == 0 ? neighbour : database.newVertex("V").set("l", i).save());
        for (int i = 0; i < INCOMING; i++)
          (i % 4 == 0 ? neighbour : database.newVertex("V").set("in", i).save()).newEdge(i % 2 == 0 ? "E" : "F", hub);
        for (int i = 0; i < 2; i++) {
          neighbour.newEdge("F", hub);
          neighbour.newLightEdge("L", hub);
        }
      });

      final VertexInternal hub = (VertexInternal) hubAndNeighbour[0].asVertex();
      final RID neighbour = hubAndNeighbour[1];
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();

      // PRECONDITION: THE LAYOUT UNDER TEST IS THE ONE ASKED FOR, OTHERWISE HALF OF THE MATRIX PROVES NOTHING
      assertThat(engine.getEdgeHeadChunk(hub, Vertex.DIRECTION.OUT) instanceof StripedEdgeList).isEqualTo(striped);

      final Map<String, Supplier<Iterator<?>>> walks = new LinkedHashMap<>();
      final String[][] filters = { {}, { "E" }, { "E", "L" }, { "F", "E" } };
      for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
        for (final String[] types : filters) {
          final String label = direction + " " + String.join(",", types);
          walks.put("getVertices " + label, () -> hub.getVertices(direction, types).iterator());
          walks.put("getEdges " + label, () -> hub.getEdges(direction, types).iterator());
          walks.put("getConnectedVertexRIDs " + label, () -> engine.getConnectedVertexRIDs(hub, direction, types).iterator());
          walks.put("getEdgesConnectedTo " + label, () -> engine.getEdgesConnectedTo(hub, direction, neighbour, types));
          if (direction == Vertex.DIRECTION.BOTH)
            continue;
          final Supplier<EdgeLinkedList> list = () -> engine.getEdgeHeadChunk(hub, direction);
          walks.put("entryIterator " + label, () -> list.get().entryIterator(types));
          walks.put("edgeIterator " + label, () -> list.get().edgeIterator(types));
          walks.put("edgeIteratorKnowingEndpoints " + label, () -> list.get().edgeIteratorKnowingEndpoints(types));
          walks.put("edgeIteratorConnectedTo " + label, () -> list.get().edgeIteratorConnectedTo(neighbour, types));
          walks.put("vertexIterator " + label, () -> list.get().vertexIterator(types));
          walks.put("ridIterator " + label, () -> list.get().ridIterator(types));
          walks.put("entryRidIterator " + label, () -> list.get().entryRidIterator(types));
        }

      int resettable = 0;
      for (final Map.Entry<String, Supplier<Iterator<?>>> walk : walks.entrySet()) {
        final List<Object> fresh = drain(walk.getValue().get());
        // A WALK WITH NOTHING TO HAND OUT CANNOT SHOW A DUPLICATED PEEK: EVERY FILTER ABOVE MATCHES AT LEAST TWO ENTRIES
        assertThat(fresh).as(walk.getKey()).hasSizeGreaterThan(1);

        for (int taken = 0; taken <= 2; taken++) {
          final Iterator<?> it = walk.getValue().get();
          if (!(it instanceof ResettableIterator<?> rewindable))
            break;
          for (int i = 0; i < taken; i++)
            it.next();
          assertThat(it.hasNext()).as(walk.getKey()).isTrue();
          rewindable.reset();
          assertThat(drain(it)).as(walk.getKey() + ", " + taken + " taken, peeked, reset").isEqualTo(fresh);
          if (taken == 0)
            ++resettable;
        }
      }
      // THE CONTRACT IS ONLY WORTH SOMETHING IF IT REACHED THE ITERATORS: EVERY CLASSIC ONE IS RESETTABLE
      assertThat(resettable).isGreaterThanOrEqualTo(walks.size() * 3 / 4);
    }

    private static List<Object> drain(final Iterator<?> it) {
      final List<Object> out = new ArrayList<>();
      while (it.hasNext())
        out.add(keyOf(it.next()));
      return out;
    }

    private static Object keyOf(final Object element) {
      if (element instanceof Edge edge)
        // A LIGHTWEIGHT EDGE HAS NO RECORD IDENTITY OF ITS OWN: ITS ENDPOINTS TELL IT APART
        return edge.getIdentity() + ":" + edge.getOut() + "->" + edge.getIn();
      if (element instanceof Identifiable identifiable)
        return identifiable.getIdentity();
      return element;
    }
  }

  /**
   * Regression for #10308: a walk over the edge list of a vertex that is still going on while the same transaction changes that list. Before
   * the Gremlin neighbor expansion went lazy, nothing walked a list while writing to it: the whole list was read first. A walk now
   * has to hand out the entries its segment held when it reached it, whatever the transaction does to the segment afterwards.
   * <p>
   * The segment a transaction has written is the transaction's own cached instance, which a lookup hands to a walk as it is: an
   * edge removed from it moved the entries behind the removed one back under the walk's position, so every second entry was never
   * reached (and one added at the head moved the entries forward, so some were reached twice).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeListWalkWhileWritingTest {
    private static final String DB_PATH = "./target/databases/issue10308-walk-while-writing";
    private static final int    EDGES   = 3_000;

    private Database database;
    private RID      root;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen()) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    /** The root and its edges, created by the CURRENT transaction when it is called inside one. */
    private void createRootWithEdges() {
      final MutableVertex vertex = database.newVertex("V").set("name", "root").save();
      root = vertex.getIdentity();
      for (int i = 0; i < EDGES; i++)
        vertex.newEdge("E", database.newVertex("V").set("n", i).save());
    }

    private long edgesOfRoot() {
      long total = 0;
      for (final Edge ignored : root.asVertex().getEdges(Vertex.DIRECTION.OUT, "E"))
        ++total;
      return total;
    }

    @Test
    void deletingTheNeighborsOfAListTheTransactionCreatedWhileWalkingReachesEveryOne() {
      database.begin();
      createRootWithEdges();

      final long visited = deleteNeighborsWhileWalking();

      assertThat(visited).isEqualTo(EDGES);
      assertThat(edgesOfRoot()).isZero();
      database.commit();
      assertThat(edgesOfRoot()).isZero();
    }

    @Test
    void deletingTheEdgesOfAListTheTransactionCreatedWhileWalkingReachesEveryOne() {
      database.begin();
      createRootWithEdges();

      final long visited = deleteEdgesWhileWalking();

      assertThat(visited).isEqualTo(EDGES);
      assertThat(edgesOfRoot()).isZero();
      database.commit();
      assertThat(edgesOfRoot()).isZero();
    }

    @Test
    void deletingTheNeighborsOfACommittedListAfterTouchingItWhileWalkingReachesEveryOne() {
      database.transaction(this::createRootWithEdges);

      database.begin();
      // THE LIST IS THE TRANSACTION'S OWN FROM HERE: ONE MORE EDGE WRITES ITS HEAD SEGMENT, AND A DELETE EVERY OTHER SEGMENT IT HITS
      root.asVertex().modify().newEdge("E", database.newVertex("V").set("n", -1).save());

      final long visited = deleteNeighborsWhileWalking();

      assertThat(visited).isEqualTo(EDGES + 1);
      assertThat(edgesOfRoot()).isZero();
      database.commit();
      assertThat(edgesOfRoot()).isZero();
    }

    @Test
    void deletingTheNeighborsOfACommittedListWhileWalkingReachesEveryOne() {
      database.transaction(this::createRootWithEdges);

      database.begin();
      final long visited = deleteNeighborsWhileWalking();

      assertThat(visited).isEqualTo(EDGES);
      assertThat(edgesOfRoot()).isZero();
      database.commit();
      assertThat(edgesOfRoot()).isZero();
    }

    @Test
    void addingEdgesWhileWalkingAListTheTransactionCreatedReachesEveryOneOnce() {
      database.begin();
      createRootWithEdges();

      final MutableVertex vertex = root.asVertex().modify();
      final List<RID> seen = new ArrayList<>();
      final Iterator<Vertex> neighbors = vertex.getVertices(Vertex.DIRECTION.OUT, "E").iterator();
      int guard = 0;
      while (neighbors.hasNext() && ++guard < 10 * EDGES) {
        seen.add(neighbors.next().getIdentity());
        // A NEW ENTRY GOES IN AT THE BEGINNING OF THE HEAD SEGMENT, MOVING EVERY ENTRY OF IT FORWARD
        vertex.newEdge("E", database.newVertex("V").set("n", -1).save());
      }

      assertThat(seen).hasSize(EDGES);
      assertThat(seen.stream().distinct().count()).isEqualTo(EDGES);
      database.commit();
    }

    private long deleteNeighborsWhileWalking() {
      long visited = 0;
      final Iterator<Vertex> neighbors = root.asVertex().getVertices(Vertex.DIRECTION.OUT, "E").iterator();
      // THE GUARD ONLY BOUNDS A WALK THAT NEVER ENDS
      while (neighbors.hasNext() && visited < 10L * EDGES) {
        final Vertex neighbor = neighbors.next();
        ++visited;
        if (database.existsRecord(neighbor.getIdentity()))
          neighbor.delete();
      }
      return visited;
    }

    private long deleteEdgesWhileWalking() {
      long visited = 0;
      final Iterator<Edge> edges = root.asVertex().getEdges(Vertex.DIRECTION.OUT, "E").iterator();
      while (edges.hasNext() && visited < 10L * EDGES) {
        final Edge edge = edges.next();
        ++visited;
        if (database.existsRecord(edge.getIdentity()))
          edge.delete();
      }
      return visited;
    }
  }

  /**
   * Regression for #10375: {@code Iterator.remove()} on an adjacency iterator took the entry out of the segment by
   * position and wrote the segment without excluding its page from the edge-append merge. When the same transaction had
   * also appended to that segment, a concurrent commit on the page made the merge rebase it - committed image plus this
   * transaction's tracked appends - and the removal vanished, while the edge record it named was deleted for good: the
   * list was left with an entry pointing at a freed slot.
   * <p>
   * Every case runs the reporter's window: the transaction appends to the vertex's OUT chunk and removes an entry of it
   * through an iterator, a second thread commits an append to the same chunk, then the transaction commits. The commit
   * must fail with the retryable conflict every other removal site gives, and the graph must be left as it was. That every other
   * segment write goes through {@link EdgeLinkedList#updateRewrittenSegment} is held by {@code EdgeSegmentRewriteCensusTest} (census
   * module).
   */
  @Nested
  class IteratorRemoveEdgeAppendMergeTest extends TestHelper {
    private static final int COMMITTED_EDGES = 5;

    private RID   source;
    private RID[] targets;
    private RID[] edges;

    /** One way to take the entry of {@code edges[0]} out of the list, each through its own iterator. */
    enum Walk {
      EDGES {
        @Override
        Iterator<?> open(final EdgeLinkedList list) {
          return list.edgeIterator();
        }
      }, EDGES_FILTERED {
        @Override
        Iterator<?> open(final EdgeLinkedList list) {
          return list.edgeIterator("E");
        }
      }, VERTICES {
        @Override
        Iterator<?> open(final EdgeLinkedList list) {
          return list.vertexIterator();
        }
      }, VERTICES_FILTERED {
        @Override
        Iterator<?> open(final EdgeLinkedList list) {
          return list.vertexIterator("E");
        }
      }, ENTRIES {
        @Override
        Iterator<?> open(final EdgeLinkedList list) {
          return list.entryIterator();
        }
      }, ENTRIES_FILTERED {
        @Override
        Iterator<?> open(final EdgeLinkedList list) {
          return list.entryIterator("E");
        }
      };

      abstract Iterator<?> open(EdgeLinkedList list);
    }

    @ParameterizedTest
    @EnumSource(Walk.class)
    void anIteratorRemovalNextToAnAppendIsNotRebasedAway(final Walk walk) throws Exception {
      createGraph();

      runWindow(() -> {
        final Iterator<?> it = walk.open(outList());
        advanceTo(it, targets[0]);
        it.remove();
      });

      // THE EDGE THE REFUSED TRANSACTION DELETED IS STILL THERE, AND SO IS ITS ENTRY
      assertThat(edges[0].asEdge(true)).isNotNull();
      assertThat(outEntries()).contains(edges[0]);
      assertNoDanglingEntry();
    }

    /**
     * The {@code CHECK DATABASE FIX} prune, {@link EdgeVertexIterator#removeEntryKeepingEdge}: the same unpoisoned write,
     * so the repair pass could lose its own repair the same way.
     */
    @Test
    void thePruneOfTheRepairPassIsNotRebasedAway() throws Exception {
      createGraph();

      runWindow(() -> {
        final EdgeVertexIterator it = (EdgeVertexIterator) outList().entryIterator();
        advanceTo(it, targets[0]);
        it.removeEntryKeepingEdge();
      });

      assertThat(outEntries()).contains(edges[0]);
      assertNoDanglingEntry();
    }

    /**
     * A delete listener that appends to the walked list makes {@code remove()} find the entry by identity instead of by
     * position: the other unpoisoned write. Here the listener's append alone is what the merge tracks.
     */
    @Test
    void aRemovalByIdentityAfterAListenerAppendIsNotRebasedAway() throws Exception {
      createGraph();

      final boolean[] inListener = { false };
      final AfterRecordDeleteListener listener = record -> {
        if (inListener[0] || !(record instanceof Edge))
          return;
        inListener[0] = true;
        try {
          source.asVertex().newEdge("E", targets[1].asVertex());
        } finally {
          inListener[0] = false;
        }
      };

      database.getEvents().registerListener(listener);
      try {
        runWindow(() -> {
          final Iterator<?> it = outList().edgeIterator();
          advanceTo(it, targets[0]);
          it.remove();
        });
      } finally {
        database.getEvents().unregisterListener(listener);
      }

      assertThat(edges[0].asEdge(true)).isNotNull();
      assertThat(outEntries()).contains(edges[0]);
      assertNoDanglingEntry();
    }

    /** The conflict is retryable: the retry reads the list again and the removal goes through, leaving nothing behind. */
    @Test
    void theRetryOfTheRefusedRemovalSucceeds() throws Exception {
      createGraph();

      final ExecutorService other = Executors.newSingleThreadExecutor();
      final int[] attempts = { 0 };
      try {
        database.transaction(() -> {
          ++attempts[0];
          source.asVertex().newEdge("E", targets[COMMITTED_EDGES].asVertex());
          final Iterator<?> it = outList().edgeIterator();
          advanceTo(it, targets[0]);
          it.remove();
          if (attempts[0] == 1)
            appendInOwnTransaction(other);
        }, false, 3);
      } finally {
        other.shutdownNow();
      }

      assertThat(attempts[0]).isEqualTo(2);
      assertThatThrownBy(() -> edges[0].asEdge(true)).isInstanceOf(RecordNotFoundException.class);
      assertThat(outEntries()).doesNotContain(edges[0]).hasSize(COMMITTED_EDGES + 1);
      assertNoDanglingEntry();
    }

    /**
     * The reporter's window: this transaction appends to the OUT chunk of {@code source} and runs {@code removal} on it, a
     * second thread commits an append to the same chunk, then this transaction commits - which must be refused.
     */
    private void runWindow(final Runnable removal) throws Exception {
      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        database.begin();
        // AN IN-CHUNK APPEND, TRACKED BY THE EDGE-APPEND MERGE
        source.asVertex().newEdge("E", targets[COMMITTED_EDGES].asVertex());
        removal.run();

        // BUMPS THE VERSION OF THE CHUNK'S PAGE
        appendInOwnTransaction(other);

        assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        other.shutdownNow();
      }
    }

    private void appendInOwnTransaction(final ExecutorService other) {
      try {
        // BOUNDED, SO A STALLED COMMIT FAILS THE TEST INSTEAD OF HANGING THE SUITE
        other.submit(() -> database.transaction(() -> source.asVertex().newEdge("E", targets[COMMITTED_EDGES + 1].asVertex())))
            .get(60, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void createGraph() {
      database.command("sql", "CREATE VERTEX TYPE V BUCKETS 1").close();
      database.command("sql", "CREATE EDGE TYPE E BUCKETS 1").close();
      targets = new RID[COMMITTED_EDGES + 2];
      edges = new RID[COMMITTED_EDGES];
      database.transaction(() -> {
        source = database.newVertex("V").save().getIdentity();
        for (int i = 0; i < targets.length; i++)
          targets[i] = database.newVertex("V").set("id", i).save().getIdentity();
        for (int i = 0; i < COMMITTED_EDGES; i++)
          edges[i] = source.asVertex().newEdge("E", targets[i].asVertex()).getIdentity();
      });
    }

    private EdgeLinkedList outList() {
      final Vertex vertex = source.asVertex();
      return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.OUT);
    }

    /** Moves {@code it} onto the entry whose far end is {@code target}. */
    private static void advanceTo(final Iterator<?> it, final RID target) {
      while (it.hasNext()) {
        final Object item = it.next();
        final RID farEnd = switch (item) {
          case Edge edge -> edge.getIn();
          case Vertex vertex -> vertex.getIdentity();
          case Pair<?, ?> pair -> (RID) pair.getSecond();
          default -> throw new IllegalStateException("Unexpected item " + item);
        };
        if (farEnd.equals(target))
          return;
      }
      throw new IllegalStateException("No entry to " + target);
    }

    /** The edge RIDs the OUT list of {@code source} names, read without loading the edges. */
    private List<RID> outEntries() {
      final List<RID> entries = new ArrayList<>();
      database.transaction(() -> {
        final Iterator<Pair<RID, RID>> it = outList().entryIterator();
        while (it.hasNext())
          entries.add(it.next().getFirst());
      });
      return entries;
    }

    private void assertNoDanglingEntry() {
      final List<RID> dangling = new ArrayList<>();
      database.transaction(() -> {
        for (final RID edge : outEntries())
          try {
            edge.asEdge(true);
          } catch (final RecordNotFoundException e) {
            dangling.add(edge);
          }
      });
      assertThat(dangling).as("entries of the OUT list naming a deleted edge record").isEmpty();
    }
  }

  /**
   * Regression for #10929: a {@link MutableVertex} handed out by {@code modify()} is a copy of the record with the edge-list heads it
   * was copied with. An edge appended in the same transaction through another handle of the vertex moves the head on the
   * instance the transaction holds, not on that copy, so the copy
   * <ul>
   * <li>answered the neighbours it had before the append (its accessors did not read the heads from the transaction's
   * instance, as {@link ImmutableVertex}'s do), and</li>
   * <li>wrote its stale heads back on {@code save()}, unlinking every edge appended since from the vertex while the edge
   * record and the far endpoint's entry survived.</li>
   * </ul>
   * A {@code save()} is a property write: it must never change which edges a vertex has.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StaleMutableVertexHeadsTest extends TestHelper {
    private RID a;
    private RID b;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
      database.transaction(() -> {
        a = database.newVertex("V").set("id", "a").save().getIdentity();
        b = database.newVertex("V").set("id", "b").save().getIdentity();
      });
    }

    @Test
    void staleModifyHandleReadsTheEdgesAppendedThroughAnotherHandle() {
      database.transaction(() -> {
        final Vertex immutableA = database.lookupByRID(a, true).asVertex();
        final MutableVertex staleA = immutableA.modify();

        database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);

        assertThat(immutableA.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(1L);
        assertThat(staleA.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(1L);
        assertThat(staleA.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1L);
        assertThat(rids(staleA.getVertices(Vertex.DIRECTION.OUT))).containsExactly(b);
        assertThat(rids(staleA.getVertices(Vertex.DIRECTION.OUT, "E"))).containsExactly(b);
        assertThat(rids(staleA.getVertices())).containsExactly(b);
        assertThat(count(staleA.getEdges())).isEqualTo(1);
        assertThat(count(staleA.getEdges(Vertex.DIRECTION.OUT, "E"))).isEqualTo(1);
        assertThat(staleA.getConnectedVertexRIDs(Vertex.DIRECTION.OUT)).containsExactly(b);
        assertThat(staleA.isConnectedTo(b)).isTrue();
        assertThat(staleA.isConnectedTo(b, Vertex.DIRECTION.OUT)).isTrue();
        assertThat(staleA.isConnectedTo(b, Vertex.DIRECTION.OUT, "E")).isTrue();
      });
    }

    @Test
    void savingAStaleModifyHandleKeepsTheEdgesAppendedThroughAnotherHandle() {
      database.transaction(() -> {
        final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();
        final MutableVertex staleB = database.lookupByRID(b, true).asVertex().modify();

        database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);

        staleA.set("x", 1).save();
        staleB.set("y", 2).save();

        assertThat(database.lookupByRID(a, true).asVertex().countEdges(Vertex.DIRECTION.OUT)).isEqualTo(1L);
        assertThat(database.lookupByRID(b, true).asVertex().countEdges(Vertex.DIRECTION.IN)).isEqualTo(1L);
      });

      assertConnectedOnBothSides(1);
      assertThat(database.lookupByRID(a, true).asVertex().getInteger("x")).isEqualTo(1);
      assertThat(database.lookupByRID(b, true).asVertex().getInteger("y")).isEqualTo(2);
      assertCheckDatabaseIsClean();
    }

    @Test
    void savingAStaleModifyHandleKeepsTheEdgesAppendedThroughTheEngineApi() {
      database.transaction(() -> {
        final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();

        database.lookupByRID(a, true).asVertex().newEdge("E", b);

        staleA.set("x", 1).save();
      });

      assertConnectedOnBothSides(1);
      assertCheckDatabaseIsClean();
    }

    /**
     * Enough edges for the head of each list to move to a new segment more than once while the stale handle is held: every
     * move is a head the copy does not know.
     */
    @Test
    void savingAStaleModifyHandleKeepsEveryHeadTheListMovedTo() {
      final int edges = 500;
      database.transaction(() -> {
        final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();
        final MutableVertex staleB = database.lookupByRID(b, true).asVertex().modify();
        final RID headBefore = staleA.getOutEdgesHeadChunk();

        for (int i = 0; i < edges; i++)
          database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);

        final VertexInternal held = (VertexInternal) database.lookupByRID(a, true).asVertex();
        assertThat(held.getOutEdgesHeadChunk()).isNotEqualTo(headBefore);

        staleA.set("x", 1).save();
        staleB.set("y", 2).save();
        assertThat(staleA.getOutEdgesHeadChunk()).isEqualTo(held.getOutEdgesHeadChunk());
      });

      assertConnectedOnBothSides(edges);
      assertCheckDatabaseIsClean();
    }

    /** The handle stays usable after the save: a further append goes to it and a further save keeps it. */
    @Test
    void aStaleHandleSavedTwiceAroundFurtherAppendsLosesNothing() {
      database.transaction(() -> {
        final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();

        database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);
        staleA.set("x", 1).save();
        database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);
        staleA.newEdge("E", b);
        staleA.set("x", 2).save();
      });

      assertConnectedOnBothSides(3);
      assertThat(database.lookupByRID(a, true).asVertex().getInteger("x")).isEqualTo(2);
      assertCheckDatabaseIsClean();
    }

    /** A head the handle sets ITSELF is a deliberate edge-list write, and the save keeps it. */
    @Test
    void anExplicitHeadAssignmentOnTheHandleIsStillWritten() {
      database.transaction(() -> database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b));

      database.begin();
      try {
        final MutableVertex handle = database.lookupByRID(a, true).asVertex().modify();
        database.command("sql", "UPDATE ? SET z = 1", a);
        handle.setOutEdgesHeadChunk(null);
        handle.save();
        assertThat(((VertexInternal) database.lookupByRID(a, true).asVertex()).getOutEdgesHeadChunk()).isNull();
      } finally {
        database.rollback();
      }

      assertConnectedOnBothSides(1);
    }

    private void assertConnectedOnBothSides(final int expected) {
      database.transaction(() -> {
        final Vertex reloadedA = database.lookupByRID(a, true).asVertex();
        final Vertex reloadedB = database.lookupByRID(b, true).asVertex();
        assertThat(reloadedA.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(expected);
        assertThat(count(reloadedA.getEdges(Vertex.DIRECTION.OUT, "E"))).isEqualTo(expected);
        assertThat(reloadedB.countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(expected);
        assertThat(count(reloadedB.getEdges(Vertex.DIRECTION.IN, "E"))).isEqualTo(expected);
        assertThat(database.countType("E", false)).isEqualTo(expected);
      });
    }

    private void assertCheckDatabaseIsClean() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Result report = rs.next();
        assertThat(report.<Long>getProperty("edgesMissingOutReference")).isZero();
        final Object warnings = report.getProperty("warnings");
        if (warnings != null)
          assertThat((Iterable<?>) warnings).isEmpty();
        assertThat(report.<Long>getProperty("totalErrors")).isZero();
      }
    }

    private static List<RID> rids(final Iterable<Vertex> vertices) {
      final List<RID> result = new ArrayList<>();
      for (final Vertex v : vertices)
        result.add(v.getIdentity());
      return result;
    }

    private static int count(final Iterable<?> iterable) {
      int n = 0;
      for (final Object ignored : iterable)
        n++;
      return n;
    }
  }

  /**
   * Regression for #11007: the per-hop and per-record work of a fixed-length expand. A type-filtered walk built its bucket mask on every
   * vertex it expanded, every edge-list walk hashed its segment into the transaction's written records even in a transaction
   * that wrote nothing, and every record read asked the transaction's page maps twice. The mask is now kept by the type and the
   * two lookups are skipped when they cannot answer: what the walks return must not change, above all when the type's buckets do.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeWalkHotPathTest extends TestHelper {

    @Test
    void theBucketMaskOfATypeIsKeptAndRebuiltWhenItsBucketsChange() {
      database.command("sql", "CREATE EDGE TYPE KNOWS").close();
      final String[] knows = { "KNOWS" };

      final EdgeBucketMask first = EdgeBucketMask.of((DatabaseInternal) database, knows);
      assertThat(EdgeBucketMask.of((DatabaseInternal) database, knows)).isSameAs(first);

      // A subtype brings its buckets
      database.command("sql", "CREATE EDGE TYPE KNOWS_WELL EXTENDS KNOWS").close();
      final EdgeBucketMask withSubType = EdgeBucketMask.of((DatabaseInternal) database, knows);
      assertThat(withSubType).isNotSameAs(first);
      for (final Bucket bucket : database.getSchema().getType("KNOWS_WELL").getBuckets(false))
        assertThat(withSubType.matches(bucket.getFileId())).isTrue();

      // A bucket added to the type
      database.command("sql", "CREATE BUCKET knows_extra").close();
      database.command("sql", "ALTER TYPE KNOWS BUCKET +knows_extra").close();
      final int extra = database.getSchema().getBucketByName("knows_extra").getFileId();
      assertThat(EdgeBucketMask.of((DatabaseInternal) database, knows).matches(extra)).isTrue();

      // ... and removed from it
      database.command("sql", "ALTER TYPE KNOWS BUCKET -knows_extra").close();
      assertThat(EdgeBucketMask.of((DatabaseInternal) database, knows).matches(extra)).isFalse();

      // Several types, or a name that is not an edge type, are answered as before
      assertThat(EdgeBucketMask.of((DatabaseInternal) database, new String[] { "KNOWS", "KNOWS_WELL" }).matches(extra)).isFalse();
      assertThat(EdgeBucketMask.of((DatabaseInternal) database, new String[] { "NOPE" })).isNull();
    }

    @Test
    void aFilteredHopSeesTheEdgesOfASubTypeCreatedAfterAnEarlierHop() {
      database.command("sql", "CREATE VERTEX TYPE Person").close();
      database.command("sql", "CREATE EDGE TYPE KNOWS").close();
      final RID[] people = new RID[3];
      database.transaction(() -> {
        for (int i = 0; i < people.length; i++)
          people[i] = database.newVertex("Person").set("id", i).save().getIdentity();
        people[0].asVertex().newEdge("KNOWS", people[1].asVertex());
      });
      assertThat(count("MATCH (a:Person {id: 0})-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(1L);

      database.command("sql", "CREATE EDGE TYPE KNOWS_WELL EXTENDS KNOWS").close();
      database.transaction(() -> people[0].asVertex().newEdge("KNOWS_WELL", people[2].asVertex()));

      assertThat(count("MATCH (a:Person {id: 0})-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(2L);
      assertThat(people[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(2L);
      final Iterator<Vertex> neighbours = people[0].asVertex().getVertices(Vertex.DIRECTION.OUT, "KNOWS").iterator();
      int seen = 0;
      while (neighbours.hasNext()) {
        neighbours.next();
        ++seen;
      }
      assertThat(seen).isEqualTo(2);
    }

    @Test
    void theWrittenRecordsAreAskedOnlyOfATransactionThatWroteOne() {
      database.command("sql", "CREATE VERTEX TYPE Person").close();
      database.command("sql", "CREATE EDGE TYPE KNOWS").close();
      final RID[] people = new RID[2];
      database.transaction(() -> {
        people[0] = database.newVertex("Person").set("id", 0).save().getIdentity();
        people[1] = database.newVertex("Person").set("id", 1).save().getIdentity();
        people[0].asVertex().newEdge("KNOWS", people[1].asVertex());
      });

      database.transaction(() -> {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        assertThat(tx.hasWrittenRecords()).isFalse();
        assertThat(tx.getWrittenRecord(people[0])).isNull();
        assertThat(people[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "KNOWS")).isEqualTo(1L);

        // A walk over a list the transaction is writing sees the transaction's own copy
        people[0].asVertex().newEdge("KNOWS", people[1].asVertex());
        assertThat(tx.hasWrittenRecords()).isTrue();
        assertThat(count("MATCH (a:Person {id: 0})-[:KNOWS]->(b) RETURN count(*) AS n")).isEqualTo(2L);
      });
    }

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Regression for #11012: a full scan of the edge lists (the build of a Graph Analytical View) read every segment as a record and handed
   * out every entry as a pair of RIDs, about 400 bytes of garbage per edge. {@link EdgeListScanner} reads the segments where they
   * lie and hands the entries out as primitives: the same entries in the same order, without allocating per entry.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeListScannerTest extends TestHelper {
    private static final int VERTICES = 200;

    @Test
    void theScannerHandsOutTheEntriesTheRecordWalkDoesInTheSameOrder() {
      final List<RID> vertices = createGraph();

      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
      database.transaction(() -> assertSameEntriesAsTheRecordWalk(scanner, vertices));

      // Not vacuous: the lists span several segments, all read in place
      assertThat(scanner.getSegmentsReadInPlace()).isGreaterThan(2L * VERTICES);
      assertThat(scanner.getSegmentsReadAsRecords()).isZero();
    }

    @Test
    void aSegmentTheTransactionWroteIsReadAsTheTransactionHoldsIt() {
      final List<RID> vertices = createGraph();

      database.transaction(() -> {
        final Vertex first = vertices.getFirst().asVertex();
        for (int i = 0; i < 30; i++)
          first.newEdge("KNOWS", vertices.get(1 + i).asVertex());
        // The edge appended last is not committed: only the transaction's copy of the segment holds it
        final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
        assertSameEntriesAsTheRecordWalk(scanner, vertices);
        assertThat(scanner.getSegmentsReadAsRecords()).isPositive();
      });
    }

    @Test
    void aReadListenerSeesTheSegmentsAsTheRecordWalkReadsThem() {
      final List<RID> vertices = createGraph();

      final AtomicInteger reads = new AtomicInteger();
      final AfterRecordReadListener listener = record -> {
        reads.incrementAndGet();
        return record;
      };
      database.getEvents().registerListener(listener);
      try {
        final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
        database.transaction(() -> assertSameEntriesAsTheRecordWalk(scanner, vertices));
        assertThat(scanner.getSegmentsReadInPlace()).isZero();
        assertThat(reads.get()).isPositive();
      } finally {
        database.getEvents().unregisterListener(listener);
      }
    }

    @Test
    void aStripedSuperNodeListIsWalkedWhole() {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
      final List<RID> vertices = createGraph();

      final VertexInternal hub = (VertexInternal) vertices.getFirst().asVertex();
      assertThat(database.lookupByRID(hub.getOutEdgesHeadChunk(), true)).isInstanceOf(StripeDirectory.class);

      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
      database.transaction(() -> assertSameEntriesAsTheRecordWalk(scanner, vertices));
    }

    /**
     * Under REPEATABLE_READ the scanner reads the pages the transaction pinned, as the record walk does: an edge committed by
     * another thread after the transaction read the list is seen by neither, and a list read after the commit is seen alike by
     * both.
     */
    @Test
    void underRepeatableReadTheScannerReadsThePinnedPagesAsTheRecordWalkDoes() throws Exception {
      final List<RID> vertices = createGraph();
      final RID hub = vertices.get(1);
      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        final List<String> before = recordWalk(hub);
        assertThat(scan(scanner, hub)).isEqualTo(before);

        // ANOTHER THREAD APPENDS TO THE SAME LIST AND COMMITS
        final Thread writer = new Thread(() -> database.transaction(() -> {
          for (int i = 0; i < 5; i++)
            hub.asVertex().newEdge("KNOWS", vertices.get(10 + i).asVertex());
        }));
        writer.start();
        writer.join();

        // THE PINNED PAGES STILL ANSWER: THE SCANNER SEES WHAT THE RECORD WALK SEES, AND WHAT IT SAW BEFORE
        assertThat(recordWalk(hub)).isEqualTo(before);
        assertThat(scan(scanner, hub)).isEqualTo(before);
      } finally {
        database.rollback();
      }

      database.transaction(() -> {
        final List<String> after = recordWalk(hub);
        assertThat(after).hasSize(41 + 5);
        assertThat(scan(scanner, hub)).isEqualTo(after);
      });
    }

    private List<String> recordWalk(final RID rid) {
      final List<String> entries = new ArrayList<>();
      final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
          .getEdgeHeadChunk((VertexInternal) database.lookupByRID(rid, true), Vertex.DIRECTION.OUT);
      final Iterator<Pair<RID, RID>> it = list.entryIterator();
      while (it.hasNext()) {
        final Pair<RID, RID> entry = it.next();
        entries.add(entry.getFirst() + ">" + entry.getSecond());
      }
      return entries;
    }

    private static List<String> scan(final EdgeListScanner scanner, final RID rid) {
      final List<String> entries = new ArrayList<>();
      scanner.scan(rid.getBucketId(), rid.getPosition(), Vertex.DIRECTION.OUT,
          (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> entries.add(
              "#" + edgeBucketId + ":" + edgePosition + ">#" + vertexBucketId + ":" + vertexPosition));
      return entries;
    }

    /** A visitor that walks the far end's list with the same scanner does not disturb the walk it is called from. */
    @Test
    void aVisitorMayScanAnotherListWithTheSameScanner() {
      final List<RID> vertices = createGraph();
      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);

      database.transaction(() -> {
        for (final RID rid : vertices.subList(0, 20)) {
          final List<String> expected = new ArrayList<>();
          scanner.scan(rid.getBucketId(), rid.getPosition(), Vertex.DIRECTION.OUT,
              (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> expected.add(vertexBucketId + ":" + vertexPosition));

          final List<String> scanned = new ArrayList<>();
          final long[] nested = new long[1];
          scanner.scan(rid.getBucketId(), rid.getPosition(), Vertex.DIRECTION.OUT,
              (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> {
                scanned.add(vertexBucketId + ":" + vertexPosition);
                scanner.scan(vertexBucketId, vertexPosition, Vertex.DIRECTION.IN, (a, b, c, d) -> ++nested[0]);
              });

          assertThat(scanned).isEqualTo(expected);
          assertThat(nested[0]).isPositive();
        }
      });
    }

    @Test
    void aPositionThatHoldsNoVertexVisitsNothing() {
      final List<RID> vertices = createGraph();
      final RID deleted = vertices.get(5);
      database.transaction(() -> deleted.asVertex().delete());

      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
      final AtomicInteger visited = new AtomicInteger();
      database.transaction(() -> {
        scanner.scan(deleted.getBucketId(), deleted.getPosition(), Vertex.DIRECTION.OUT, (a, b, c, d) -> visited.incrementAndGet());
        scanner.scan(deleted.getBucketId(), 1_000_000L, Vertex.DIRECTION.OUT, (a, b, c, d) -> visited.incrementAndGet());
      });
      assertThat(visited.get()).isZero();
    }

    @Test
    void theRecordPositionsOfABucketAreTheRecordsAScanHandsOut() {
      final List<RID> vertices = createGraph();
      database.transaction(() -> {
        // A deleted record leaves a hole, a new one of this transaction is on a page only the transaction holds
        vertices.get(3).asVertex().delete();
        vertices.get(77).asVertex().delete();
        database.newVertex("Person").set("id", -1).save();

        for (final Bucket bucket : database.getSchema().getType("Person").getBuckets(false)) {
          final List<Long> expected = new ArrayList<>();
          final Iterator<Record> records = bucket.iterator();
          while (records.hasNext())
            expected.add(records.next().getIdentity().getPosition());

          final List<Long> positions = new ArrayList<>();
          ((LocalBucket) bucket).forEachRecordPosition(positions::add);
          assertThat(positions).isEqualTo(expected);
        }
      });
    }

    /**
     * The point of the issue, measured on the thread's allocation counter rather than on time: a scan of every list allocates
     * per list, not per entry, so the bytes per entry are a small fraction of the record walk's.
     */
    @Test
    void aFullScanAllocatesAFractionOfTheRecordWalkPerEntry() {
      final List<RID> vertices = createGraph();
      final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
      // THE THREAD'S ALLOCATION COUNTER IS A HOTSPOT FEATURE: A JVM WITHOUT IT HAS NOTHING TO MEASURE
      assumeTrue(threads.isThreadAllocatedMemorySupported() && threads.isThreadAllocatedMemoryEnabled());
      final long thread = Thread.currentThread().threadId();

      database.transaction(() -> {
        final List<VertexInternal> loaded = new ArrayList<>();
        for (final RID rid : vertices)
          loaded.add((VertexInternal) rid.asVertex());

        final long[] sink = new long[1];
        final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
        final EdgeListScanner.EntryVisitor visitor = (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> sink[0] +=
            vertexPosition + edgeBucketId;

        long recordWalkBytes = Long.MAX_VALUE;
        long scannerBytes = Long.MAX_VALUE;
        long entries = 0;
        // The smallest of a few rounds, so a JIT compilation in the middle of one does not count
        for (int round = 0; round < 5; round++) {
          long before = threads.getThreadAllocatedBytes(thread);
          long count = 0;
          for (final VertexInternal v : loaded) {
            final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk(v, Vertex.DIRECTION.OUT);
            if (list == null)
              continue;
            final Iterator<Pair<RID, RID>> it = list.entryIterator();
            while (it.hasNext()) {
              final Pair<RID, RID> entry = it.next();
              sink[0] += entry.getSecond().getPosition();
              ++count;
            }
          }
          recordWalkBytes = Math.min(recordWalkBytes, threads.getThreadAllocatedBytes(thread) - before);
          entries = count;

          before = threads.getThreadAllocatedBytes(thread);
          for (final VertexInternal v : loaded)
            scanner.scan(v, Vertex.DIRECTION.OUT, visitor);
          scannerBytes = Math.min(scannerBytes, threads.getThreadAllocatedBytes(thread) - before);
        }

        assertThat(entries).isGreaterThan(8_000L);
        assertThat(scannerBytes).as("scanner %d bytes, record walk %d bytes, over %d entries", scannerBytes, recordWalkBytes, entries)
            .isLessThan(recordWalkBytes / 10);
        assertThat(sink[0]).isNotZero();
      });
    }

    /** A graph whose out-lists span several segments, with regular and lightweight edges of two types and self loops. */
    private List<RID> createGraph() {
      database.command("sql", "CREATE VERTEX TYPE Person").close();
      database.command("sql", "CREATE EDGE TYPE KNOWS").close();
      database.command("sql", "CREATE EDGE TYPE LIKES LIGHTWEIGHT").close();
      final List<RID> vertices = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          vertices.add(database.newVertex("Person").set("id", i).save().getIdentity());
      });
      final Random random = new Random(11012);
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++) {
          final Vertex from = vertices.get(i).asVertex();
          for (int k = 0; k < 40; k++)
            from.newEdge(k % 3 == 0 ? "LIKES" : "KNOWS", vertices.get(random.nextInt(VERTICES)).asVertex());
          from.newEdge("KNOWS", from);
        }
        // One hub with many more edges than the others
        final Vertex hub = vertices.getFirst().asVertex();
        for (int k = 0; k < 300; k++)
          hub.newEdge("KNOWS", vertices.get(random.nextInt(VERTICES)).asVertex());
      });
      return vertices;
    }

    private void assertSameEntriesAsTheRecordWalk(final EdgeListScanner scanner, final List<RID> vertices) {
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      for (final RID rid : vertices) {
        final VertexInternal vertex = (VertexInternal) rid.asVertex();
        for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
          final List<String> expected = new ArrayList<>();
          final EdgeLinkedList list = engine.getEdgeHeadChunk(vertex, direction);
          if (list != null) {
            final Iterator<Pair<RID, RID>> it = list.entryIterator();
            while (it.hasNext()) {
              final Pair<RID, RID> entry = it.next();
              expected.add(entry.getFirst().getBucketId() + ":" + entry.getFirst().getPosition() + ">" + entry.getSecond().getBucketId() + ":"
                  + entry.getSecond().getPosition());
            }
          }

          final List<String> scanned = new ArrayList<>();
          scanner.scan(vertex, direction,
              (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> scanned.add(
                  edgeBucketId + ":" + edgePosition + ">" + vertexBucketId + ":" + vertexPosition));

          assertThat(scanned).as("%s list of %s", direction, rid).isEqualTo(expected);

          // The same list, from the vertex's position: its head is read where the vertex record lies
          scanned.clear();
          scanner.scan(rid.getBucketId(), rid.getPosition(), direction,
              (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> scanned.add(
                  edgeBucketId + ":" + edgePosition + ">" + vertexBucketId + ":" + vertexPosition));
          assertThat(scanned).as("%s list of %s by position", direction, rid).isEqualTo(expected);
        }
      }
    }
  }

  /**
   * Regression for #11104: the adjacency the {@code algo.*} procedures fall back to without a Graph Analytical View
   * ({@link GraphEngine#buildAdjacencyList}) loaded every edge record and resolved its far end through a boxed
   * {@code Map<RID, Integer>}, twice per edge. It now reads the edge-list entries in place ({@link EdgeListScanner}) and resolves
   * the far end through a primitive vertex index.
   * <p>
   * The contract is the answer of the record walk, row by row and in the same order, which {@link #recordWalk} restates as it was
   * written; the one deliberate difference is a ghost entry, which is followed as the view build follows it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AdjacencyFromEntriesTest {
    private static final String DB_PATH = "./target/databases/issue-11104";

    private static final Vertex.DIRECTION[] DIRECTIONS = { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN, Vertex.DIRECTION.BOTH };

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("AdjNode");
      database.getSchema().createVertexType("AdjOther");
      database.getSchema().createEdgeType("KNOWS");
      database.getSchema().buildEdgeType().withName("LIKES").create();
      database.getSchema().buildEdgeType().withName("LOVES").withSuperType("LIKES").create();
      database.getSchema().buildEdgeType().withName("LIGHT").withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("ONEWAY").withBidirectional(false).create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void theRowsAreTheRecordWalksRowsInTheSameOrder() {
      final List<Vertex> vertices = randomGraph(120, 900, new String[] { "KNOWS", "LIKES", "LOVES", "LIGHT" });
      final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);

      for (final String[] types : new String[][] { null, {}, { "KNOWS" }, { "LIKES" }, { "LOVES" }, { "LIGHT" }, { "KNOWS", "LOVES" },
          { "NoSuchType" } })
        for (final Vertex.DIRECTION dir : DIRECTIONS)
          assertThat(GraphEngine.buildAdjacencyList(vertices, index, dir, types))
              .as("%s %s", dir, Arrays.toString(types))
              .isDeepEqualTo(recordWalk(vertices, index, dir, types));
    }

    @Test
    void aSubsetOfTheVerticesOnlyListsNeighboursOfTheSubset() {
      final List<Vertex> all = randomGraph(80, 500, new String[] { "KNOWS", "LIKES" });
      // Every other vertex, in reverse: the rows follow the list, the neighbours the map, and a vertex left out is no neighbour
      final List<Vertex> subset = new ArrayList<>();
      for (int i = all.size() - 1; i >= 0; i -= 2)
        subset.add(all.get(i));
      final Map<RID, Integer> index = GraphEngine.buildRidIndex(subset);

      for (final Vertex.DIRECTION dir : DIRECTIONS)
        assertThat(GraphEngine.buildAdjacencyList(subset, index, dir, null)).as(dir.name())
            .isDeepEqualTo(recordWalk(subset, index, dir, null));
    }

    @Test
    void aUnidirectionalTypeIsTransposedIntoTheIncomingRows() {
      final List<Vertex> vertices = randomGraph(60, 400, new String[] { "ONEWAY", "KNOWS" });
      final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);

      for (final String[] types : new String[][] { null, { "ONEWAY" }, { "KNOWS" }, { "ONEWAY", "KNOWS" } })
        for (final Vertex.DIRECTION dir : DIRECTIONS)
          assertThat(GraphEngine.buildAdjacencyList(vertices, index, dir, types)).as("%s %s", dir, Arrays.toString(types))
              .isDeepEqualTo(transposedRecordWalk(vertices, index, dir, types));
    }

    @Test
    void theEntryCountIsHandedOverBeforeTheRowsAreAllocated() {
      final List<Vertex> vertices = randomGraph(40, 300, new String[] { "KNOWS" });
      final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);
      final AtomicLong counted = new AtomicLong(-1);

      final int[][] adj = GraphEngine.buildAdjacencyList(vertices, index, Vertex.DIRECTION.BOTH, null, counted::set);
      assertThat(counted.get()).isEqualTo(Arrays.stream(adj).mapToLong(row -> row.length).sum()).isPositive();

      counted.set(-1);
      assertThat(GraphEngine.buildAdjacencyList(List.of(), Map.of(), Vertex.DIRECTION.OUT, null, counted::set)).isEmpty();
      assertThat(counted.get()).isZero();
    }

    @Test
    void theEdgesOfTheTransactionInProgressAreListed() {
      final List<Vertex> vertices = randomGraph(30, 100, new String[] { "KNOWS" });
      database.begin();
      try {
        final Vertex a = vertices.get(3).asVertex(true);
        final Vertex b = vertices.get(7).asVertex(true);
        a.modify().newEdge("KNOWS", b).save();
        final List<Vertex> reloaded = reload(vertices);
        final Map<RID, Integer> index = GraphEngine.buildRidIndex(reloaded);
        for (final Vertex.DIRECTION dir : DIRECTIONS)
          assertThat(GraphEngine.buildAdjacencyList(reloaded, index, dir, null)).as(dir.name())
              .isDeepEqualTo(recordWalk(reloaded, index, dir, null));
      } finally {
        database.rollback();
      }
    }

    @Test
    void aGhostEntryIsFollowedAsTheViewBuildFollowsIt() {
      final MutableVertex[] v = new MutableVertex[3];
      final Edge[] edges = new Edge[2];
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          v[i] = database.newVertex("AdjNode").set("i", i).save();
        edges[0] = v[0].newEdge("KNOWS", v[1]).save();
        edges[1] = v[1].newEdge("KNOWS", v[2]).save();
      });
      // The edge record goes, the entry stays: a ghost (see GhostEdgeReporter)
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, edges[0].getIdentity()));
      final List<Vertex> vertices = reload(List.of(v[0], v[1], v[2]));
      final Map<RID, Integer> index = GraphEngine.buildRidIndex(vertices);

      // No edge record is read, so the ghost cannot be told from a live entry: it is followed, as the view build follows it
      assertThat(GraphEngine.buildAdjacencyList(vertices, index, Vertex.DIRECTION.OUT, null)).isDeepEqualTo(new int[][] { { 1 }, { 2 }, {} });
      assertThat(GraphEngine.buildAdjacencyList(vertices, index, Vertex.DIRECTION.IN, null)).isDeepEqualTo(new int[][] { {}, { 0 }, { 1 } });

      // CHECK DATABASE FIX prunes it, after which every walk agrees
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
        rs.hasNext();
      }
      final List<Vertex> repaired = reload(vertices);
      assertThat(GraphEngine.buildAdjacencyList(repaired, index, Vertex.DIRECTION.OUT, null)).isDeepEqualTo(new int[][] { {}, { 2 }, {} });
      assertThat(GraphEngine.buildAdjacencyList(repaired, index, Vertex.DIRECTION.OUT, null))
          .isDeepEqualTo(recordWalk(repaired, index, Vertex.DIRECTION.OUT, null));
    }

    // ---------------------------------------------------------------------------------------------------------------------------

    /** Vertices of two types, edges of the given types between random pairs, self-loops and parallel edges included. */
    private List<Vertex> randomGraph(final int vertexCount, final int edgeCount, final String[] edgeTypes) {
      final Random random = new Random(11104);
      final List<Vertex> vertices = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < vertexCount; i++)
          vertices.add(database.newVertex(i % 3 == 0 ? "AdjOther" : "AdjNode").set("i", i).save());
        for (int e = 0; e < edgeCount; e++) {
          final MutableVertex from = vertices.get(random.nextInt(vertexCount)).modify();
          final Vertex to = vertices.get(random.nextInt(vertexCount));
          from.newEdge(edgeTypes[random.nextInt(edgeTypes.length)], to).save();
        }
      });
      return reload(vertices);
    }

    private List<Vertex> reload(final List<? extends Vertex> vertices) {
      final List<Vertex> reloaded = new ArrayList<>(vertices.size());
      for (final Vertex vertex : vertices)
        reloaded.add(vertex.getIdentity().asVertex(true));
      return reloaded;
    }

    /** The record walk {@code GraphEngine.buildAdjacencyList} was before #11104, for the bidirectional edge types. */
    private static int[][] recordWalk(final List<Vertex> vertices, final Map<RID, Integer> index, final Vertex.DIRECTION dir,
        final String[] types) {
      final int[][] adj = new int[vertices.size()][];
      for (int i = 0; i < vertices.size(); i++) {
        final Vertex v = vertices.get(i);
        final List<Integer> row = new ArrayList<>();
        for (final Edge e : types != null && types.length > 0 ? v.getEdges(dir, types) : v.getEdges(dir)) {
          try {
            final Integer j = index.get(GraphEngine.neighborRid(e, v.getIdentity(), dir));
            if (j != null)
              row.add(j);
          } catch (final RecordNotFoundException ghost) {
            // skipped by the record walk
          }
        }
        adj[i] = row.stream().mapToInt(Integer::intValue).toArray();
      }
      return adj;
    }

    /** The record walk over a set of types that includes a unidirectional one, as it was before #11104 (issue #8629). */
    private int[][] transposedRecordWalk(final List<Vertex> vertices, final Map<RID, Integer> index, final Vertex.DIRECTION dir,
        final String[] types) {
      final boolean typed = types != null && types.length > 0;
      final String[] unidirectional = dir == Vertex.DIRECTION.OUT ? new String[0] :
          IncomingEdgeLookup.getUnidirectionalTypes(database.getSchema(), types);
      if (unidirectional.length == 0)
        return recordWalk(vertices, index, dir, types);
      final List<String> unidirectionalTypes = List.of(unidirectional);

      final List<List<Integer>> rows = new ArrayList<>();
      for (int i = 0; i < vertices.size(); i++)
        rows.add(new ArrayList<>());
      final boolean both = dir == Vertex.DIRECTION.BOTH;
      for (int i = 0; i < vertices.size(); i++) {
        final Vertex v = vertices.get(i);
        for (final Edge e : both ? (typed ? v.getEdges(Vertex.DIRECTION.OUT, types) : v.getEdges(Vertex.DIRECTION.OUT)) :
            v.getEdges(Vertex.DIRECTION.OUT, unidirectional)) {
          final Integer j = index.get(e.getIn());
          if (j == null)
            continue;
          if (both)
            rows.get(i).add(j);
          if (unidirectionalTypes.contains(e.getTypeName()))
            rows.get(j).add(i);
        }
        for (final Edge e : typed ? v.getEdges(Vertex.DIRECTION.IN, types) : v.getEdges(Vertex.DIRECTION.IN)) {
          if (unidirectionalTypes.contains(e.getTypeName()))
            continue;
          final Integer j = index.get(e.getOut());
          if (j != null)
            rows.get(i).add(j);
        }
      }
      final int[][] adj = new int[rows.size()][];
      for (int i = 0; i < rows.size(); i++)
        adj[i] = rows.get(i).stream().mapToInt(Integer::intValue).toArray();
      return adj;
    }
  }

  /**
   * Regression for #11142: a dropped edge type left adjacency entries pointing at it.
   *
   * https://github.com/ArcadeData/arcadedb/issues/11142
   * <p>
   * An edge type dropped (UNSAFE, or by a version before the #10923 guard) while lightweight edges were still in the
   * vertices' adjacency lists leaves entries naming a bucket id the schema no longer resolves. CHECK DATABASE must report
   * them and CHECK DATABASE FIX must prune them, so the vertices can be deleted again.
   */
  @Nested
  class DroppedEdgeTypeAdjacencyEntriesTest extends TestHelper {

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // The stranded entries are injected on purpose.
      return false;
    }

    private MutableVertex[] strandEntries(final String edgeDdl, final String edgeType) {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", edgeDdl);
      final MutableVertex[] v = new MutableVertex[2];
      database.transaction(() -> {
        v[0] = database.newVertex("V").save();
        v[1] = database.newVertex("V").save();
        v[0].newLightEdge(edgeType, v[1]);
      });
      database.command("sql", "DROP TYPE " + edgeType + " UNSAFE");
      return v;
    }

    private Map<String, Object> check(final boolean fix) {
      return new GraphDatabaseChecker((DatabaseInternal) database).checkVertices("V", fix, 0);
    }

    @Test
    void checkReportsAndFixPrunesEntriesOfADroppedEdgeType() {
      final MutableVertex[] v = strandEntries("CREATE EDGE TYPE EL LIGHTWEIGHT", "EL");

      final Map<String, Object> report = check(false);
      assertThat(((Number) report.get("invalidLinks")).longValue()).as("report: %s", report).isGreaterThan(0L);

      final Map<String, Object> fixed = check(true);
      assertThat(((Number) fixed.get("prunedDanglingEntries")).longValue()).as("report: %s", fixed).isGreaterThan(0L);

      final Map<String, Object> after = check(false);
      assertThat(((Number) after.get("invalidLinks")).longValue()).as("report: %s", after).isZero();

      database.transaction(() -> database.lookupByRID(v[0].getIdentity(), true).asVertex().delete());
      database.transaction(() -> database.lookupByRID(v[1].getIdentity(), true).asVertex().delete());
      assertThat(database.countType("V", false)).isZero();
    }

    @Test
    void sqlCheckDatabaseFixPrunesUnidirectionalEntries() {
      final MutableVertex[] v = strandEntries("CREATE EDGE TYPE EU UNIDIRECTIONAL LIGHTWEIGHT", "EU");

      try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("prunedDanglingEntries")).longValue()).as("result: %s", r.toJSON()).isGreaterThan(0L);
      }

      database.transaction(() -> database.lookupByRID(v[0].getIdentity(), true).asVertex().delete());
      assertThat(database.countType("V", false)).isEqualTo(1);
    }

    @Test
    void fixKeepsLightweightEdgesOfTheTypesThatStillExist() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE KEEP LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE GONE LIGHTWEIGHT");
      final MutableVertex[] v = new MutableVertex[2];
      database.transaction(() -> {
        v[0] = database.newVertex("V").save();
        v[1] = database.newVertex("V").save();
        v[0].newLightEdge("KEEP", v[1]);
        v[0].newLightEdge("GONE", v[1]);
      });
      database.command("sql", "DROP TYPE GONE UNSAFE");

      final Map<String, Object> fixed = check(true);
      // one stale entry in the OUT list of v0 and one in the IN list of v1
      assertThat(((Number) fixed.get("prunedDanglingEntries")).longValue()).as("report: %s", fixed).isEqualTo(2L);

      assertThat(database.lookupByRID(v[0].getIdentity(), true).asVertex().countEdges(Vertex.DIRECTION.OUT, "KEEP")).isEqualTo(1);
      assertThat(((Number) check(false).get("invalidLinks")).longValue()).isZero();
    }
  }

  /**
   * Regression for #11234: a walk of an edge list that is still going when its own transaction empties and deletes a segment AHEAD of it
   * (an older one) must go on where the chain now goes. The relink is written on the transaction's copy of the segment in front,
   * and the walk held another one, whose pointer named the deleted segment: {@code RecordNotFoundException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WalkPastDeletedSegmentsTest extends TestHelper {
    private static final int EDGES = 3000;

    private Logger          previousLogger;
    private CapturingLogger captured;
    private RID             hub;
    private final List<RID> targets = new ArrayList<>();

    @BeforeEach
    void installCapturingLogger() {
      previousLogger = LogManager.instance().getLogger();
      captured = new CapturingLogger();
      LogManager.instance().setLogger(captured);
    }

    @AfterEach
    void restoreLogger() {
      LogManager.instance().setLogger(previousLogger);
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Entity");
        database.getSchema().createEdgeType("RELATES_TO");
      });
      // ONE EDGE PER TARGET, OLDEST FIRST, IN SEVERAL TRANSACTIONS AS A LOAD WOULD
      database.transaction(() -> hub = database.newVertex("Entity").set("id", "HUB").save().getIdentity());
      for (int from = 0; from < EDGES; from += 500) {
        final int begin = from;
        database.transaction(() -> {
          final MutableVertex h = hub.asVertex().modify();
          for (int i = begin; i < begin + 500; i++) {
            final MutableVertex target = database.newVertex("Entity").set("id", "E" + i).save();
            targets.add(target.getIdentity());
            h.newEdge("RELATES_TO", target);
          }
        });
      }
    }

    @Test
    void aFilteredWalkGoesOnPastTheOldSegmentsItsTransactionDeleted() {
      walkWhileDeletingTheOldest(true);
    }

    @Test
    void anUnfilteredWalkGoesOnPastTheOldSegmentsItsTransactionDeleted() {
      walkWhileDeletingTheOldest(false);
    }

    /**
     * The walk reaches the newest edge the transaction is going to keep of the old ones (to E1000), so it reads the segment in front
     * of those the deletes empty. The transaction deletes the 1000 oldest edges, oldest first, and the walk goes on: it meets none of
     * them, and does not step onto a segment that is gone.
     */
    private void walkWhileDeletingTheOldest(final boolean filtered) {
      database.transaction(() -> {
        final Vertex h = hub.asVertex();
        final Iterator<Edge> walk = (filtered ? h.getEdges(Vertex.DIRECTION.OUT, "RELATES_TO") : h.getEdges(Vertex.DIRECTION.OUT)).iterator();
        final Set<RID> seen = new HashSet<>();
        final RID keptOldest = targets.get(1000);
        while (!seen.contains(keptOldest))
          seen.add(walk.next().getIn());

        deleteEdgesTo(targets.subList(0, 1000));

        while (walk.hasNext())
          assertThat(seen.add(walk.next().getIn())).isTrue();

        assertThat(seen).hasSize(EDGES - 1000);
        assertThat(seen).doesNotContainAnyElementsOf(targets.subList(0, 1000));
      });
      assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.OUT, "RELATES_TO")).isEqualTo(EDGES - 1000);
      assertThat(captured.loadErrors).isEmpty();
    }

    @Test
    void aWalkThatMeetsEdgesItsTransactionDeletedSkipsThemWithoutCallingThemCorrupted() {
      database.transaction(() -> {
        final Iterator<Edge> walk = hub.asVertex().getEdges(Vertex.DIRECTION.OUT, "RELATES_TO").iterator();
        final Edge first = walk.next();

        // EVERY OTHER EDGE GOES, THE ONES OF THE SEGMENT THE WALK IS READING TOO: IT STILL LISTS THEM
        final List<RID> others = new ArrayList<>(targets);
        others.remove(first.getIn());
        deleteEdgesTo(others);

        assertThat(walk.hasNext()).isFalse();
      });
      assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.OUT, "RELATES_TO")).isEqualTo(1);
      assertThat(captured.loadErrors).isEmpty();
    }

    @Test
    void aFilteredWalkDoesNotHandOutTheEdgeItPeekedOnceItsTransactionDeletedIt() {
      peekThenDelete(true);
    }

    @Test
    void anUnfilteredWalkDoesNotHandOutTheEdgeItPeekedOnceItsTransactionDeletedIt() {
      peekThenDelete(false);
    }

    /** hasNext() has already read the next entry when the transaction deletes its edge: next() goes on to the one after it. */
    private void peekThenDelete(final boolean filtered) {
      database.transaction(() -> {
        final Vertex h = hub.asVertex();
        final List<Edge> order = new ArrayList<>();
        walkOf(h, filtered).forEachRemaining(order::add);

        final Iterator<Edge> walk = walkOf(h, filtered);
        assertThat(walk.next().getIdentity()).isEqualTo(order.get(0).getIdentity());
        assertThat(walk.hasNext()).isTrue();

        order.get(1).delete();

        assertThat(walk.hasNext()).isTrue();
        assertThat(walk.next().getIdentity()).isEqualTo(order.get(2).getIdentity());
      });
      assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.OUT, "RELATES_TO")).isEqualTo(EDGES - 1);
    }

    private static Iterator<Edge> walkOf(final Vertex vertex, final boolean filtered) {
      return (filtered ? vertex.getEdges(Vertex.DIRECTION.OUT, "RELATES_TO") : vertex.getEdges(Vertex.DIRECTION.OUT)).iterator();
    }

    @Test
    void aWalkOfAListWhoseVertexItsTransactionDeletedEnds() {
      database.transaction(() -> {
        final Iterator<Edge> walk = hub.asVertex().getEdges(Vertex.DIRECTION.OUT, "RELATES_TO").iterator();
        walk.next();

        hub.asVertex().delete();

        while (walk.hasNext())
          walk.next();
      });
      assertThat(database.countType("RELATES_TO", false)).isZero();
      assertThat(captured.loadErrors).isEmpty();
    }

    private void deleteEdgesTo(final List<RID> targetsToDisconnect) {
      final Set<RID> wanted = new HashSet<>(targetsToDisconnect);
      final List<Edge> toDelete = new ArrayList<>();
      for (final Edge e : hub.asVertex().getEdges(Vertex.DIRECTION.OUT, "RELATES_TO"))
        if (wanted.contains(e.getIn()))
          toDelete.add(e);
      // OLDEST FIRST, AS THE STATEMENT OF THE ISSUE DOES
      for (int i = toDelete.size() - 1; i >= 0; i--)
        toDelete.get(i).delete();
    }

    /** Keeps the "Error on loading edge" records the edge iterators log when they take an entry for corruption. */
    private static final class CapturingLogger implements Logger {
      final List<String> loadErrors = new CopyOnWriteArrayList<>();

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        capture(level, message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        capture(level, message);
      }

      private void capture(final Level level, final String message) {
        if (message != null && message.startsWith("Error on loading edge"))
          loadErrors.add(level + ":" + message);
      }

      @Override
      public void flush() {
        // NOTHING TO FLUSH
      }
    }
  }
}
