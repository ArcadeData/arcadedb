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
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance comparison test: OLTP vs Graph OLAP CSR for multi-hop traversal.
 * <p>
 * Tagged {@code benchmark} (#7220): a comparison run is what that lane exists for, and this one is both the
 * slowest kind of test and the one most sensitive to what else the JVM is doing, so it has no business in the
 * default lane of a 14 000-test single-JVM build. The class name ends in {@code Benchmark} because the lane
 * selects on both the tag and {@code -Dsurefire.includes=**&#47;*Benchmark.java}; a tag on its own would route it
 * to no lane at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class CSRPerformanceBenchmark extends TestHelper {

  private static final int NODE_COUNT = 5_000;
  private static final int EDGE_COUNT = 20_000;

  /** The headroom the comment on the old {@code > 1.0} bound always claimed, now actually encoded. */
  private static final double MIN_SPEEDUP = 2.0;

  @Test
  void compareTraversalPerformance() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Build a random graph
    final MutableVertex[] nodes = new MutableVertex[NODE_COUNT];
    database.begin();
    for (int i = 0; i < NODE_COUNT; i++)
      nodes[i] = database.newVertex("Node").set("idx", i).save();

    final Random rnd = new Random(42);
    for (int i = 0; i < EDGE_COUNT; i++) {
      final int from = rnd.nextInt(NODE_COUNT);
      int to = rnd.nextInt(NODE_COUNT);
      while (to == from)
        to = rnd.nextInt(NODE_COUNT);
      nodes[from].newLightEdge("LINK", nodes[to]);
    }
    database.commit();

    // Build CSR
    final GraphAnalyticalView gav = new GraphAnalyticalView(database);
    gav.build(null, null);

    assertThat(gav.getNodeCount()).isEqualTo(NODE_COUNT);

    // BENCHMARK: 2-hop traversal via CSR (count total 2-hop reachable nodes)
    final CSRAdjacencyIndex linkCSR = gav.getCSRIndex("LINK");
    long csrTotal = 0;
    final long csrStart = System.nanoTime();
    final long csrStartStall = StallAwareStopwatch.jvmStallNanos();
    for (int src = 0; src < NODE_COUNT; src++) {
      final int outDeg = linkCSR.outDegree(src);
      for (int i = 0; i < outDeg; i++) {
        final int neighbor = linkCSR.outNeighbor(src, i);
        csrTotal += linkCSR.outDegree(neighbor);
      }
    }
    final long csrElapsed = effectiveNanos(csrStart, csrStartStall);

    // BENCHMARK: 2-hop traversal via OLTP iterators
    long oltpTotal = 0;
    final long oltpStart = System.nanoTime();
    final long oltpStartStall = StallAwareStopwatch.jvmStallNanos();
    database.begin();
    for (final MutableVertex node : nodes) {
      for (final Vertex neighbor : node.getVertices(Vertex.DIRECTION.OUT)) {
        for (final Vertex hop2 : neighbor.getVertices(Vertex.DIRECTION.OUT))
          oltpTotal++;
      }
    }
    database.commit();
    final long oltpElapsed = effectiveNanos(oltpStart, oltpStartStall);

    // Both should compute the same total
    assertThat(csrTotal).isEqualTo(oltpTotal);

    final double speedup = (double) oltpElapsed / csrElapsed;
    LogManager.instance().log(this, Level.INFO,
        "2-hop traversal: CSR=%.1f ms, OLTP=%.1f ms, speedup=%.1fx, 2-hop-reachable=%d",
        csrElapsed / 1_000_000.0, oltpElapsed / 1_000_000.0, speedup, csrTotal);

    // #7220. Two separate things were wrong, and only the second is load-related:
    //  - the bound was `> 1.0` while the comment above it claimed "at least 2x", so the headroom the author
    //    intended was never actually encoded. Idle, CSR wins this by 25-40x, so 2.0 is an honest floor with the
    //    order of magnitude of headroom JvmStallMonitor asks of a bound that is itself the assertion.
    //  - the ratio was taken over two RAW wall-clock windows measured at different points of a shared-JVM run,
    //    which #6260 forbids. A ratio is worse off than a single bound, not better: a pause charged to one
    //    window and not the other decides the comparison on its own, whatever the kernels did.
    // What the discount below does and does not buy, measured rather than assumed: it recovers the one long
    // stop-the-world pause #6260 is about, because that stops the sampler too. It does NOT rescue this test
    // under sustained load - a starved thread and a long series of sub-20ms GC pauses both leave the sampler
    // ticking, so almost nothing is charged as stall and the ratio still collapses. That half is what
    // @Tag("benchmark") fixes, by getting the class out of the 14 000-test lane rather than by arithmetic.
    assertThat(speedup)
        .as("CSR 2-hop traversal should be at least %.0fx faster than OLTP; measured %.1fx "
                + "(CSR %,d ns, OLTP %,d ns, both stall-discounted). This ratio IS the assertion: loosening it "
                + "deletes the test. Red on an idle machine means the kernel regressed; red on a loaded one "
                + "means this ran outside the benchmark lane.",
            MIN_SPEEDUP, speedup, csrElapsed, oltpElapsed)
        .isGreaterThan(MIN_SPEEDUP);
  }

  /**
   * The window's wall-clock span with the JVM-wide stall observed inside it discounted (#6260), in nanoseconds.
   * This is the {@link StallAwareStopwatch#jvmStallNanos()} form of the discount a {@link StallAwareStopwatch}
   * applies, used instead of the stopwatch itself because the stopwatch reports whole milliseconds and the CSR
   * window is only a couple of them: a 1ms quantum on a ~2ms denominator would leave the ratio dominated by
   * rounding. Floored at 1ns so the ratio stays finite if a stall ever swallows a whole window.
   */
  private static long effectiveNanos(final long startNanos, final long startStallNanos) {
    final long spanNanos = System.nanoTime() - startNanos;
    final long stalledNanos = StallAwareStopwatch.jvmStallNanos() - startStallNanos;
    return Math.max(1L, spanNanos - stalledNanos);
  }
}
