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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10548: the depth-bounded breadth-first search costs what it reaches. On a long chain, one level finds one node, the
 * search stops where the caller stops it, and a new search from another start forgets everything the previous one found.
 */
class BoundedBreadthFirstSearchTest extends TestHelper {
  private static final int CHAIN = 3000;

  private final List<RID> chain = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE NEXT");
    database.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < CHAIN; i++) {
        final MutableVertex vertex = database.newVertex("N").set("id", i).save();
        if (previous != null)
          previous.newEdge("NEXT", vertex).save();
        chain.add(vertex.getIdentity());
        previous = vertex;
      }
    });
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW chain10548 VERTEX TYPES (N) EDGE TYPES (NEXT)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "chain10548");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();
  }

  @Test
  void eachLevelFindsOnlyWhatItReaches() {
    final GraphTraversalProvider view = GraphAnalyticalViewRegistry.get(database, "chain10548").pinned();
    final BoundedBreadthFirstSearch search = new BoundedBreadthFirstSearch(view, Vertex.DIRECTION.OUT, new String[] { "NEXT" },
        WorkCheckpoint.NONE);

    final int first = view.getNodeId(chain.getFirst());
    search.start(first, 1);
    assertThat(search.size()).isEqualTo(1);
    assertThat(search.depth()).isEqualTo(0);

    assertThat(search.expandLevel(null)).isTrue();
    assertThat(search.size()).isEqualTo(2);
    assertThat(search.node(1)).isEqualTo(view.getNodeId(chain.get(1)));

    for (int i = 0; i < 3; i++)
      search.expandLevel(null);
    assertThat(search.depth()).isEqualTo(4);
    assertThat(search.size()).isEqualTo(5);
    assertThat(search.isVisited(view.getNodeId(chain.get(4)))).isTrue();
    assertThat(search.isVisited(view.getNodeId(chain.get(5)))).isFalse();

    // a new search forgets the previous one
    final int middle = view.getNodeId(chain.get(CHAIN / 2));
    search.start(middle, 1);
    assertThat(search.size()).isEqualTo(1);
    assertThat(search.isVisited(first)).isFalse();
    assertThat(search.isVisited(view.getNodeId(chain.get(1)))).isFalse();

    // run to the end: the last node has nowhere to go, and the search stops by itself
    while (search.expandLevel(null))
      ;
    assertThat(search.size()).isEqualTo(CHAIN - CHAIN / 2);
    assertThat(search.hasFrontier()).isFalse();
    assertThat(search.expandLevel(null)).isFalse();
  }

  @Test
  void theListenerSeesEveryEntryAndTheCheckpointCanAbort() {
    final GraphTraversalProvider view = GraphAnalyticalViewRegistry.get(database, "chain10548").pinned();
    final int[] seen = new int[2];
    final BoundedBreadthFirstSearch both = new BoundedBreadthFirstSearch(view, Vertex.DIRECTION.BOTH, new String[] { "NEXT" },
        WorkCheckpoint.NONE);
    both.start(view.getNodeId(chain.get(10)), 1);
    final BoundedBreadthFirstSearch.EntryListener listener = new BoundedBreadthFirstSearch.EntryListener() {
      @Override
      public void discovered(final int from, final int to) {
        seen[0]++;
      }

      @Override
      public void revisited(final int from, final int to) {
        seen[1]++;
      }
    };
    both.expandLevel(listener);
    both.expandLevel(listener);
    // depth 1 finds both neighbours; depth 2 finds two more and meets the start twice, once from each side
    assertThat(seen[0]).isEqualTo(4);
    assertThat(seen[1]).isEqualTo(2);

    final BoundedBreadthFirstSearch aborted = new BoundedBreadthFirstSearch(view, Vertex.DIRECTION.OUT, new String[] { "NEXT" },
        () -> {
          throw new IllegalStateException("stop");
        });
    aborted.start(view.getNodeId(chain.getFirst()), 1);
    assertThatThrownBy(() -> {
      while (aborted.expandLevel(null))
        ;
    }).isInstanceOf(IllegalStateException.class).hasMessage("stop");
  }
}
