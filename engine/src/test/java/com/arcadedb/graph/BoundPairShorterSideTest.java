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
import com.arcadedb.engine.PageManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link GraphEngine#getEdgesConnectedTo} reads the edges joining two bound vertices off whichever end has the shorter
 * edge list (issue #11699). It used to walk the list of the vertex it was called on, so the same question about one
 * edge cost the degree of a hub or one entry depending only on which end the caller named first.
 * <p>
 * The answers are checked against the walk the method replaces (every edge of the vertex, kept when its far end is the
 * target), from both ends and in every direction: parallel edges, a self loop, both directions, the type filter,
 * lightweight twins, a unidirectional type (whose incoming side no vertex stores, so only the source end can answer) and
 * a striped super-node list.
 */
class BoundPairShorterSideTest extends TestHelper {
  private static final int FILLERS = 200;

  @Test
  void readsThePairOffTheShorterList() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      final MutableVertex leaf = database.newVertex("N").save();
      for (int i = 0; i < FILLERS; i++)
        hub.newEdge("L", database.newVertex("N").save());
      hub.newEdge("L", leaf);
      ends[0] = hub.getIdentity();
      ends[1] = leaf.getIdentity();
    });

    database.transaction(() -> {
      final VertexInternal hub = (VertexInternal) ends[0].asVertex();
      final VertexInternal leaf = (VertexInternal) ends[1].asVertex();
      final RID leafList = leaf.getInEdgesHeadChunk();

      // A LIGHTWEIGHT EDGE REMEMBERS THE LIST ENTRY IT WAS READ FROM: ASKED FROM THE HUB, IT COMES OFF THE LEAF'S LIST
      for (final Edge edge : drain(engine().getEdgesConnectedTo(hub, Vertex.DIRECTION.OUT, leaf, "L"))) {
        assertThat(((LightEdgeRID) edge.getIdentity()).getOriginSegment()).isEqualTo(leafList);
        assertThat(edge.getOut()).isEqualTo(ends[0]);
        assertThat(edge.getIn()).isEqualTo(ends[1]);
      }
      for (final Edge edge : drain(engine().getEdgesConnectedTo(leaf, Vertex.DIRECTION.IN, hub, "L")))
        assertThat(((LightEdgeRID) edge.getIdentity()).getOriginSegment()).isEqualTo(leafList);
    });
  }

  /**
   * The cost is the shorter list's, whatever the longer one holds: the pages read to answer the probe from a hub of
   * 32,000 edges are the pages read from a hub of 2,000. Before the fix the hub's whole list was walked, so the larger
   * hub read about 30 more chunk pages.
   */
  @Test
  void costDoesNotGrowWithTheLongerList() {
    // ONE CLASSIC CHAIN PER LIST: A STRIPED SUPER-NODE LIST ADDS A CONSTANT PER STRIPE THAT WOULD BLUR THE COMPARISON
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 0);
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE T");
    final RID[] small = hubWithLeaf(2_000);
    final RID[] large = hubWithLeaf(32_000);

    final long smallPages = probePages(small);
    final long largePages = probePages(large);
    assertThat(largePages).as("pages read probing a 32,000-edge hub, against %d for a 2,000-edge one", smallPages)
        .isLessThanOrEqualTo(smallPages + 2);
  }

  /**
   * The probe is made from the end that is already the shorter one, and that list is longer than the head start: the
   * race runs out on the named end first and the walk goes back over it. The cost stays a small multiple of the
   * named end's own list, whatever the other end holds.
   */
  @Test
  void costDoesNotGrowWithTheLongerListWhenTheNamedEndIsTheShorterOne() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 0);
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE T");
    final RID[] small = leafWithOwnEdgesTowardHub(2_000);
    final RID[] large = leafWithOwnEdgesTowardHub(32_000);

    final long smallPages = probeFromLeaf(small);
    final long largePages = probeFromLeaf(large);
    assertThat(largePages).as("pages read probing from the shorter end towards a 32,000-edge hub, against %d for a 2,000-edge one",
        smallPages).isLessThanOrEqualTo(smallPages + 2);
  }

  private RID[] leafWithOwnEdgesTowardHub(final int hubDegree) {
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      final MutableVertex leaf = database.newVertex("N").save();
      // THE LEAF'S OUTGOING LIST HOLDS MORE THAN THE HEAD START, THE HUB'S INCOMING LIST FAR MORE
      for (int i = 0; i < 100; i++)
        leaf.newEdge("T", database.newVertex("N").save());
      leaf.newEdge("T", hub);
      for (int i = 0; i < hubDegree; i++)
        database.newVertex("N").save().newEdge("T", hub);
      ends[0] = hub.getIdentity();
      ends[1] = leaf.getIdentity();
    });
    return ends;
  }

  private long probeFromLeaf(final RID[] ends) {
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final long[] pages = new long[1];
    for (int round = 0; round < 2; round++) {
      final int r = round;
      database.transaction(() -> {
        final VertexInternal hub = (VertexInternal) ends[0].asVertex();
        final VertexInternal leaf = (VertexInternal) ends[1].asVertex();
        final PageManager.PPageManagerStats before = pageManager.getStats();
        assertThat(drain(engine().getEdgesConnectedTo(leaf, Vertex.DIRECTION.OUT, hub, "T"))).hasSize(1);
        final PageManager.PPageManagerStats after = pageManager.getStats();
        if (r == 1)
          pages[0] = after.cacheHits + after.cacheMiss - before.cacheHits - before.cacheMiss;
      });
    }
    return pages[0];
  }

  @Test
  void answersAsTheWalkOfEitherEnd() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE T");
    database.command("sql", "CREATE EDGE TYPE S");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      final MutableVertex leaf = database.newVertex("N").save();
      for (int i = 0; i < FILLERS; i++) {
        final MutableVertex filler = database.newVertex("N").save();
        hub.newEdge("T", filler);
        hub.newEdge("L", filler);
        hub.newEdge("U", filler);
        filler.newEdge("T", hub);
      }
      for (int i = 0; i < 3; i++)
        hub.newEdge("T", leaf, "p", i);
      leaf.newEdge("T", hub, "p", 9);
      hub.newEdge("S", leaf);
      hub.newEdge("T", hub, "p", 7);
      hub.newEdge("L", leaf);
      hub.newEdge("L", leaf);
      hub.newEdge("U", leaf);
      leaf.newEdge("U", hub);
      ends[0] = hub.getIdentity();
      ends[1] = leaf.getIdentity();
    });

    assertSameAsWalk(ends[0], ends[1]);
  }

  @Test
  void answersAsTheWalkOfEitherEndOnAStripedList() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 100);
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE T");
    database.command("sql", "CREATE EDGE TYPE S");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      final MutableVertex leaf = database.newVertex("N").save();
      for (int i = 0; i < 3; i++)
        hub.newEdge("T", leaf, "p", i);
      hub.newEdge("L", leaf);
      for (int i = 0; i < 2_000; i++) {
        final MutableVertex filler = database.newVertex("N").save();
        hub.newEdge(i % 2 == 0 ? "T" : "L", filler);
        filler.newEdge("T", hub);
      }
      hub.newEdge("T", leaf, "p", 3);
      hub.newEdge("L", leaf);
      leaf.newEdge("T", hub, "p", 9);
      hub.newEdge("T", hub, "p", 7);
      hub.newEdge("U", leaf);
      ends[0] = hub.getIdentity();
      ends[1] = leaf.getIdentity();
    });

    assertSameAsWalk(ends[0], ends[1]);
  }

  /** Inside the transaction that writes them, the edges just added are found from either end too. */
  @Test
  void seesTheEdgesTheTransactionAdds() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE T");
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      final MutableVertex leaf = database.newVertex("N").save();
      for (int i = 0; i < FILLERS; i++)
        hub.newEdge("T", database.newVertex("N").save());
      ends[0] = hub.getIdentity();
      ends[1] = leaf.getIdentity();
    });

    database.transaction(() -> {
      final VertexInternal hub = (VertexInternal) ends[0].asVertex();
      final VertexInternal leaf = (VertexInternal) ends[1].asVertex();
      assertThat(drain(engine().getEdgesConnectedTo(hub, Vertex.DIRECTION.OUT, leaf, "T"))).isEmpty();
      hub.modify().newEdge("T", leaf, "p", 1);
      hub.modify().newEdge("T", leaf, "p", 2);
      // THE HANDLES WERE TAKEN BEFORE THE APPENDS: THE ENGINE READS THE INSTANCES THE TRANSACTION HOLDS
      assertThat(drain(engine().getEdgesConnectedTo(hub, Vertex.DIRECTION.OUT, leaf, "T"))).hasSize(2);
      assertThat(drain(engine().getEdgesConnectedTo(leaf, Vertex.DIRECTION.IN, hub, "T"))).hasSize(2);
      assertThat(drain(engine().getEdgesConnectedTo(hub, Vertex.DIRECTION.BOTH, leaf.getIdentity()))).hasSize(2);
    });
  }

  private void assertSameAsWalk(final RID hubRID, final RID leafRID) {
    database.transaction(() -> {
      final VertexInternal hub = (VertexInternal) hubRID.asVertex();
      final VertexInternal leaf = (VertexInternal) leafRID.asVertex();
      final String[][] typeFilters = { {}, { "T" }, { "S" }, { "L" }, { "T", "S" }, { "U" } };
      for (final String[] types : typeFilters)
        for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values()) {
          for (final VertexInternal[] pair : new VertexInternal[][] { { hub, leaf }, { leaf, hub }, { hub, hub }, { leaf, leaf } }) {
            final List<String> expected = walk(pair[0], direction, pair[1].getIdentity(), types);
            final List<String> actual = describe(engine().getEdgesConnectedTo(pair[0], direction, pair[1], types));
            assertThat(actual).as("%s %s %s types %s", pair[0].getIdentity(), direction, pair[1].getIdentity(), List.of(types))
                .containsExactlyInAnyOrderElementsOf(expected);
          }
        }
    });
  }

  /** What callers did before getEdgesConnectedTo existed: every edge of the vertex, kept when its far end is the target. */
  private static List<String> walk(final Vertex vertex, final Vertex.DIRECTION direction, final RID target, final String[] types) {
    final List<String> result = new ArrayList<>();
    final RID self = vertex.getIdentity();
    for (final Edge edge : vertex.getEdges(direction, types)) {
      final boolean reaches = switch (direction) {
        case OUT -> edge.getIn().equals(target);
        case IN -> edge.getOut().equals(target);
        case BOTH -> (edge.getOut().equals(self) && edge.getIn().equals(target)) || (edge.getIn().equals(self) && edge.getOut()
            .equals(target));
      };
      if (reaches)
        result.add(describe(edge));
    }
    return result;
  }

  private static List<String> describe(final Iterator<Edge> edges) {
    final List<String> result = new ArrayList<>();
    while (edges.hasNext())
      result.add(describe(edges.next()));
    return result;
  }

  private static String describe(final Edge edge) {
    return edge.getTypeName() + " " + edge.getIdentity() + " " + edge.getOut() + "->" + edge.getIn() + " " + edge.get("p");
  }

  private static List<Edge> drain(final Iterator<Edge> edges) {
    final List<Edge> result = new ArrayList<>();
    while (edges.hasNext())
      result.add(edges.next());
    return result;
  }

  private RID[] hubWithLeaf(final int degree) {
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      final MutableVertex leaf = database.newVertex("N").save();
      hub.newEdge("T", leaf);
      for (int i = 0; i < degree; i++)
        hub.newEdge("T", database.newVertex("N").save());
      ends[0] = hub.getIdentity();
      ends[1] = leaf.getIdentity();
    });
    return ends;
  }

  /** The page-cache lookups of one probe from the hub towards its leaf, after a warm-up probe. */
  private long probePages(final RID[] ends) {
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final long[] pages = new long[1];
    database.transaction(() -> {
      final VertexInternal hub = (VertexInternal) ends[0].asVertex();
      final VertexInternal leaf = (VertexInternal) ends[1].asVertex();
      assertThat(drain(engine().getEdgesConnectedTo(hub, Vertex.DIRECTION.OUT, leaf, "T"))).hasSize(1);
    });
    database.transaction(() -> {
      final VertexInternal hub = (VertexInternal) ends[0].asVertex();
      final VertexInternal leaf = (VertexInternal) ends[1].asVertex();
      final PageManager.PPageManagerStats before = pageManager.getStats();
      assertThat(drain(engine().getEdgesConnectedTo(hub, Vertex.DIRECTION.OUT, leaf, "T"))).hasSize(1);
      final PageManager.PPageManagerStats after = pageManager.getStats();
      pages[0] = after.cacheHits + after.cacheMiss - before.cacheHits - before.cacheMiss;
    });
    return pages[0];
  }

  private GraphEngine engine() {
    return ((DatabaseInternal) database).getGraphEngine();
  }
}
