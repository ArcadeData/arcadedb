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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unreachable-node detection, the primitive behind the fix for issue #5615.
 * <p>
 * A graph build occasionally emits a node carrying a full set of outgoing edges and no incoming ones. Beam search
 * only ever walks edges forward from the entry node, so such a node can never be returned at any {@code efSearch},
 * however good its score. The engine detects those nodes after a build and keeps their vectors reachable through
 * the delta scan; this pins the detection itself against graphs whose shape is known exactly, which the
 * concurrency reproducer cannot do.
 *
 * @see LSMVectorIndexConcurrentRebuildVisibilityTest
 */
class LSMVectorIndexGraphConnectivityTest extends TestHelper {

  @Test
  void everyNodeReachableFromEntryReportsNoOrphans() {
    // 0 -> 1 -> 2 -> 3, entry 0. Every node is on the path.
    final int[][] edges = { { 1 }, { 2 }, { 3 }, {} };

    assertThat(index().findUnreachableOrdinals(new FixedGraph(edges, 0))).isEmpty();
  }

  @Test
  void nodeWithOutgoingEdgesButNoIncomingIsReportedUnreachable() {
    // 2 points at 0 and 1 but nothing points at 2 - exactly the shape observed on issue #5615.
    final int[][] edges = { { 1 }, { 0 }, { 0, 1 } };

    assertThat(index().findUnreachableOrdinals(new FixedGraph(edges, 0))).containsExactly(2);
  }

  @Test
  void severalOrphansAreAllReported() {
    final int[][] edges = { { 1 }, { 0 }, { 0 }, { 1 }, {} };

    assertThat(index().findUnreachableOrdinals(new FixedGraph(edges, 0))).containsExactly(2, 3, 4);
  }

  @Test
  void aDisconnectedCycleIsUnreachableEvenThoughEveryNodeHasIncomingEdges() {
    // 2 <-> 3 is a closed pair: both have an in-edge, neither is reachable from entry 0. An in-degree count
    // would call this healthy, which is why reachability is walked from the entry node instead.
    final int[][] edges = { { 1 }, { 0 }, { 3 }, { 2 } };

    assertThat(index().findUnreachableOrdinals(new FixedGraph(edges, 0))).containsExactly(2, 3);
  }

  @Test
  void anEmptyGraphReportsNothing() {
    assertThat(index().findUnreachableOrdinals(new FixedGraph(new int[0][], 0))).isEmpty();
  }

  /**
   * An unreadable ordinal must not be re-queued into the delta buffer. {@code getVector} never returns null - it
   * hands back a sentinel - so the recovery path has to ask whether the vector is real, otherwise it pairs a
   * genuine RID with a meaningless distance and the delta scan reports a hit at the wrong distance.
   */
  @Test
  void theSentinelIsDistinguishableFromARealVector() {
    final VectorLocationIndex locations = new VectorLocationIndex();
    final ArcadePageVectorValues values = new ArcadePageVectorValues((DatabaseInternal) database, 4, "vector", locations, new int[] { 7 });

    // Ordinal 0 maps to vectorId 7, which has no location at all, so the read cannot succeed.
    final VectorFloat<?> unreadable = values.getVector(0);
    assertThat(unreadable).as("getVector never returns null").isNotNull();
    assertThat(values.isDeletedSentinel(unreadable)).as("an unreadable ordinal yields the sentinel").isTrue();

    // Anything out of range is the sentinel too, and a genuine vector is not.
    assertThat(values.isDeletedSentinel(values.getVector(99))).isTrue();
    assertThat(values.isDeletedSentinel(
        VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(new float[] { 1, 2, 3, 4 })))
        .as("a real vector is not the sentinel").isFalse();
  }

  private LSMVectorIndex index() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("Probe")) {
        database.getSchema().createDocumentType("Probe");
        database.getSchema().getType("Probe").createProperty("vector", Type.ARRAY_OF_FLOATS);
        database.command("sql", """
            CREATE INDEX ON Probe (vector) LSM_VECTOR
            METADATA { "dimensions": 4, "similarity": "EUCLIDEAN" }""");
      }
    });
    final var typeIndex = database.getSchema().getIndexByName("Probe[vector]");
    return (LSMVectorIndex) ((TypeIndex) typeIndex).getIndexesOnBuckets()[0];
  }

  /**
   * A flat graph whose adjacency is fixed by the test. Only the members the reachability walk touches are
   * implemented; anything else throws so an unnoticed dependency cannot pass silently.
   */
  private record FixedGraph(int[][] edges, int entry) implements ImmutableGraphIndex {

    @Override
    public ImmutableGraphIndex.View getView() {
      return new View() {
        @Override
        public NodesIterator getNeighborsIterator(final int level, final int node) {
          return new NodesIterator.ArrayNodesIterator(edges[node]);
        }

        @Override
        public void processNeighbors(final int a, final int b, final io.github.jbellis.jvector.graph.similarity.ScoreFunction f,
            final IntMarker m, final NeighborProcessor p) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
          return edges.length;
        }

        @Override
        public NodeAtLevel entryNode() {
          return edges.length == 0 ? null : new NodeAtLevel(0, entry);
        }

        @Override
        public Bits liveNodes() {
          return Bits.ALL;
        }

        @Override
        public boolean contains(final int level, final int node) {
          return level == 0 && node >= 0 && node < edges.length;
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public int size() {
      return edges.length;
    }

    @Override
    public int getIdUpperBound() {
      return edges.length;
    }

    @Override
    public boolean containsNode(final int node) {
      return node >= 0 && node < edges.length;
    }

    @Override
    public int getMaxLevel() {
      return 0;
    }

    @Override
    public boolean isHierarchical() {
      return false;
    }

    @Override
    public int getDegree(final int node) {
      return edges[node].length;
    }

    @Override
    public NodesIterator getNodes(final int level) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int maxDegree() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Integer> maxDegrees() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDimension() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getAverageDegree(final int level) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size(final int level) {
      return edges.length;
    }

    @Override
    public long ramBytesUsed() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
  }
}
