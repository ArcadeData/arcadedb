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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex.DIRECTION;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6719: three cheap O(n) sequential loops in {@link GraphAlgorithms} - {@code pageRank}'s dangling-node sum
 * accumulation, {@code lccBuildAndIntersect}'s {@code offsets}/{@code pos} array-fill loops and its final
 * {@code lcc[u]} coefficient-materialization loop - had no {@link WorkCheckpoint#check()} call at all, unlike every
 * other sequential pass in this file. Each is cheap per element (a double add or an int assign), so on a huge graph
 * it is not a correctness bug, just a small abortability gap: a caller that armed the checkpoint to stop a runaway
 * call could still be made to wait out one of these loops in full.
 * <p>
 * Both tests below rely on precise checkpoint-call accounting rather than timing: a graph shape and node count are
 * chosen so the checkpoint calls contributed by phases that already had a checkpoint before this fix are known
 * exactly, and the assertion is set just past that known baseline - so it can only be satisfied if the previously
 * unchecked loop calls the checkpoint too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6719CheckpointGapsTest extends TestHelper {

  /**
   * {@code pageRank}'s dangling-sum loop, isolated with an all-dangling graph (no edges at all, so every node's
   * out-degree is zero) run for a single iteration. Without the loop's own checkpoint, one iteration makes exactly
   * 4 checkpoint calls (top-of-iteration, the contrib phase, the pull phase, the dangling-distribute phase - each
   * of the two {@code parallelForRangeCheckpointed} phases collapses to a single batch since {@code n} is below
   * {@code PARALLEL_THRESHOLD}), and {@code iterations == 1} means the loop never comes back around for a 5th. A
   * checkpoint that throws on its 5th call therefore only fires if the dangling-sum loop calls it too - which,
   * with 5000 dangling nodes and a periodic check every 1024 of them, it does well before the 4-call baseline
   * would otherwise finish the iteration.
   */
  @Test
  void danglingSumLoopIsCheckpointed() {
    final int n = 5000;
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    for (int i = 0; i < n; i++)
      database.newVertex("Node").save();
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node").withEdgeTypes("LINK").build();
    try {
      final AtomicInteger calls = new AtomicInteger();
      final WorkCheckpoint abortOn5thCall = () -> {
        if (calls.incrementAndGet() == 5)
          throw new RuntimeException("checkpoint reached - dangling-sum loop is checkpointed");
      };

      assertThatThrownBy(() -> GraphAlgorithms.pageRank(gav, 0.85, 1, DIRECTION.OUT, abortOn5thCall, "LINK"))
          .as("the dangling-sum loop must call the checkpoint before an all-dangling, single-iteration pageRank "
              + "run would otherwise finish (only 4 checkpoint calls without it)")
          .hasMessageContaining("dangling-sum loop is checkpointed");
    } finally {
      gav.drop();
    }
  }

  /**
   * The three previously-unchecked {@code lccBuildAndIntersect} loops (offsets build, pos build, final {@code
   * lcc[]} materialization), isolated together against the loops that already had a checkpoint before this fix.
   * <p>
   * One edge on an otherwise edgeless 5000-node graph keeps the CSR non-null (avoiding a null CSR on the
   * single-edge-type path) while leaving every degree at 0 or 1 - checkpoint call counts depend only on the node
   * count crossing the 1024-node periodic-check boundary, not on degree, so the single edge does not change the
   * count. For n = 5000 that boundary is crossed 4 times (at node indices 1023, 2047, 3071, 4095) in each of:
   * degree computation, the single-edge-type merge loop and the compact-in-place loop - all three checkpointed
   * before this fix, 12 calls total - plus one more from the triangle-counting phase's single batch, 13 without
   * the fix. The three newly-checkpointed loops each add 4 more, so a fixed run makes 25 calls; asserting at least
   * 25 fails against the pre-fix 13 and passes only once all three loops are checkpointed.
   */
  @Test
  void lccPrepAndFinalMaterializationLoopsAreCheckpointed() {
    final int n = 5000;
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.begin();
    MutableVertex first = null, second = null;
    for (int i = 0; i < n; i++) {
      final MutableVertex v = database.newVertex("Node").save();
      if (i == 0)
        first = v;
      else if (i == 1)
        second = v;
    }
    first.newEdge("LINK", second);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node").withEdgeTypes("LINK").build();
    try {
      final AtomicInteger calls = new AtomicInteger();
      final WorkCheckpoint counting = calls::incrementAndGet;

      final double[] lcc = GraphAlgorithms.localClusteringCoefficient(gav, counting, "LINK");

      assertThat(lcc).hasSize(n);
      assertThat(calls.get())
          .as("13 checkpoint calls come from the phases already checkpointed before issue #6719; the three "
              + "newly-checkpointed loops must contribute at least 4 more each")
          .isGreaterThanOrEqualTo(25);
    } finally {
      gav.drop();
    }
  }
}
