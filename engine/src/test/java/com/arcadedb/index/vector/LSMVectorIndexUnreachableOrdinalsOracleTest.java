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
import com.arcadedb.index.TypeIndex;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.util.Bits;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The nodes of a graph that no path from the entry node reaches (issue #5615) are the ones the delta scan has to serve, because a
 * beam search follows edges forward from the entry only. {@link LSMVectorIndex#findUnreachableOrdinals} is checked against a plain
 * breadth-first search over random graphs of several levels, written as adjacency lists: a node present at a level has a list of
 * neighbours (some of them out of range), a node absent from a level has none, and an entry node, a level or a node can be missing,
 * throw or answer null. The answer is every present node the search over the edges of every level does not reach, ascending.
 */
class LSMVectorIndexUnreachableOrdinalsOracleTest extends TestHelper {

  /** adjacency[level][node]: neighbours, or null when the node is not at that level; {@code throwing} lists (level, node) views that throw */
  private static final class StubGraph implements ImmutableGraphIndex {
    final int[][][] adjacency;
    final int       entry;
    final int       upper;
    final int[]     throwing;

    StubGraph(final int[][][] adjacency, final int entry, final int upper, final int[] throwing) {
      this.adjacency = adjacency;
      this.entry = entry;
      this.upper = upper;
      this.throwing = throwing;
    }

    @Override
    public int size() {
      return upper;
    }

    @Override
    public int getIdUpperBound() {
      return upper;
    }

    @Override
    public boolean containsNode(final int node) {
      return node >= 0 && node < upper && adjacency[0][node] != null;
    }

    @Override
    public NodesIterator getNodes(final int level) {
      throw new UnsupportedOperationException();
    }

    @Override
    public View getView() {
      final StubGraph g = this;
      return new View() {
        @Override
        public NodesIterator getNeighborsIterator(final int level, final int node) {
          if (g.throwing != null && g.throwing[node] == 1)
            throw new IllegalStateException("view refused node " + node);
          if (g.throwing != null && g.throwing[node] == 2)
            return null;
          if (level >= g.adjacency.length || node >= g.adjacency[level].length || g.adjacency[level][node] == null)
            return NodesIterator.EMPTY_NODE_ITERATOR;
          final int[] n = g.adjacency[level][node];
          return NodesIterator.fromPrimitiveIterator(Arrays.stream(n).iterator(), n.length);
        }

        @Override
        public void processNeighbors(final int level, final int node, final ScoreFunction scoreFunction, final IntMarker visited,
            final NeighborProcessor processor) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
          return g.upper;
        }

        @Override
        public NodeAtLevel entryNode() {
          return g.entry < 0 ? null : new NodeAtLevel(0, g.entry);
        }

        @Override
        public Bits liveNodes() {
          return Bits.ALL;
        }

        @Override
        public int getIdUpperBound() {
          return g.upper;
        }

        @Override
        public boolean contains(final int level, final int node) {
          return level < g.adjacency.length && node < g.adjacency[level].length && g.adjacency[level][node] != null;
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public int maxDegree() {
      return 8;
    }

    @Override
    public List<Integer> maxDegrees() {
      return List.of(8);
    }

    @Override
    public int getDimension() {
      return 4;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isHierarchical() {
      return adjacency.length > 1;
    }

    @Override
    public int getMaxLevel() {
      return adjacency.length - 1;
    }

    @Override
    public int getDegree(final int level) {
      return 8;
    }

    @Override
    public double getAverageDegree(final int level) {
      return 4;
    }

    @Override
    public int size(final int level) {
      return upper;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  private LSMVectorIndex index() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\": 4, \"similarity\": \"EUCLIDEAN\"}");
    });
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
  }

  private static int[] oracle(final StubGraph g) {
    final boolean[] reached = new boolean[g.upper];
    final ArrayDeque<Integer> queue = new ArrayDeque<>();
    reached[g.entry] = true;
    queue.add(g.entry);
    while (!queue.isEmpty()) {
      final int node = queue.poll();
      if (g.throwing != null && g.throwing[node] != 0)
        continue;
      for (int level = 0; level < g.adjacency.length; level++) {
        if (node >= g.adjacency[level].length || g.adjacency[level][node] == null)
          continue;
        for (final int next : g.adjacency[level][node])
          if (next >= 0 && next < g.upper && !reached[next]) {
            reached[next] = true;
            queue.add(next);
          }
      }
    }
    final List<Integer> out = new ArrayList<>();
    for (int node = 0; node < g.upper; node++)
      if (!reached[node] && g.containsNode(node))
        out.add(node);
    return out.stream().mapToInt(Integer::intValue).toArray();
  }

  @Test
  void theUnreachableOrdinalsAreWhatABreadthFirstSearchOverEveryLevelLeaves() {
    final LSMVectorIndex index = index();
    final Random rnd = new Random(5615);
    int withOrphans = 0;
    int withoutOrphans = 0;
    for (int round = 0; round < 400; round++) {
      final int upper = 1 + rnd.nextInt(40);
      final int levels = 1 + rnd.nextInt(3);
      final int[][][] adjacency = new int[levels][upper][];
      for (int node = 0; node < upper; node++) {
        // level 0 holds the node unless a hole is punched; a node of a higher level is at every level below it
        final boolean present = rnd.nextInt(12) != 0;
        final int top = rnd.nextInt(levels);
        for (int level = 0; level <= top && present; level++) {
          final int degree = rnd.nextInt(4);
          final int[] n = new int[degree];
          for (int i = 0; i < degree; i++)
            n[i] = rnd.nextInt(10) == 0 ? (rnd.nextBoolean() ? -1 : upper + rnd.nextInt(5)) : rnd.nextInt(upper);
          adjacency[level][node] = n;
        }
      }
      final int[] throwing = rnd.nextInt(4) == 0 ? new int[upper] : null;
      if (throwing != null)
        throwing[rnd.nextInt(upper)] = 1 + rnd.nextInt(2);
      final int entry = rnd.nextInt(upper);
      final StubGraph g = new StubGraph(adjacency, entry, upper, throwing);
      final int[] expected = oracle(g);
      assertThat(index.findUnreachableOrdinals(g)).as("round %d: %d nodes, %d levels, entry %d", round, upper, levels, entry)
          .containsExactly(expected);
      if (expected.length > 0)
        withOrphans++;
      else
        withoutOrphans++;
    }
    assertThat(withOrphans).as("the generator leaves orphans in some graphs").isGreaterThan(50);
    assertThat(withoutOrphans).as("and none in others").isGreaterThan(20);
  }

  @Test
  void aGraphWithoutAnEntryNodeOrAnEntryOutsideItsRangeReportsNothing() {
    final LSMVectorIndex index = index();
    final int[][][] adjacency = { { { 1 }, { 0 }, null, { } } };
    assertThat(index.findUnreachableOrdinals(new StubGraph(adjacency, -1, 4, null))).isEmpty();
    assertThat(index.findUnreachableOrdinals(new StubGraph(adjacency, 4, 4, null))).isEmpty();
    assertThat(index.findUnreachableOrdinals(new StubGraph(new int[][][] { { } }, 0, 0, null))).isEmpty();
    // a graph with no entry node answers nothing, as does an entry past its range
    // node 3 is present and reached by nobody, node 2 is absent
    assertThat(index.findUnreachableOrdinals(new StubGraph(adjacency, 0, 4, null))).containsExactly(3);
  }
}
