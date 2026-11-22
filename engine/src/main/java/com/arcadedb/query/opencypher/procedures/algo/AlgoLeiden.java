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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.leiden(relTypes?, maxIterations?, resolution?)
 * <p>
 * Detects communities by modularity optimisation, then refines them so that every community is connected: the local-moving
 * passes of {@link AlgoLouvain#detect} (a node moves to the neighbouring community that gains most, staying included, until a
 * pass moves nothing or {@code maxIterations} passes ran), followed by a refinement that splits each community into its
 * connected components, which is the guarantee that sets Leiden apart from Louvain (a community that is not connected cannot
 * be well connected). The resolution parameter γ controls community granularity: a lower value favours fewer, larger
 * communities and a large one leaves every vertex alone. A low value does not promise one community per connected
 * component: a node moves one at a time and never leaves a community it has more edges into, so two dense groups joined by
 * an edge can stay apart even at 0.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.leiden('KNOWS', 10, 1.0)
 * YIELD nodeId, community
 * RETURN nodeId, community
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLeiden extends AbstractAlgoProcedure {
  public static final String NAME = "algo.leiden";

  /** The modularity gain below which a pass is no longer worth another: rounding noise, not a better partition. */
  private static final double NOISE = 1e-12;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Detects communities using the Leiden algorithm with refinement phase";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "community");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);
    final int maxIterations = extractInt(optionalNumberArg(args, 1, "maxIterations", 10), "maxIterations", 1);
    final double resolution = optionalNumberArg(args, 2, "resolution", 1.0).doubleValue();

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final GraphData.WeightedAdjacency adjacency = graph.weightedAdjacency(guard, Vertex.DIRECTION.BOTH, null, relTypes);
    final int[][] neighbors = adjacency.neighbors();

    // doubles: weighted degree, community total degree, candidate weights and the two modularity accumulators; ints: the
    // community, the candidate list, the refined label and the stack of the component walk; the touched flags
    graph.memory().reserve(saturatingProduct(n, 5 * DOUBLE_BYTES + 4 * INT_BYTES + BOOLEAN_BYTES), "the community working set",
        n + " nodes");

    final int[] community = AlgoLouvain.detect(neighbors, adjacency.weights(), maxIterations, NOISE, resolution, guard).community();
    final int[] refined = splitIntoConnectedComponents(neighbors, community, guard);

    return IntStream.range(0, n).mapToObj(i -> {
      // a node id a view keeps for a deleted vertex is no vertex: it is isolated, and it has no row (as in algo.louvain)
      final RID rid = graph.getRID(i);
      if (rid == null)
        return null;
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", rid);
      r.setProperty("community", refined[i]);
      return (Result) r;
    }).filter(Objects::nonNull);
  }

  /**
   * The refinement: every community becomes one community per connected component of the edges inside it. The labels are
   * numbered from 0 in order of first appearance.
   */
  private static int[] splitIntoConnectedComponents(final int[][] neighbors, final int[] community, final WorkGuard guard) {
    final int n = community.length;
    final int[] label = new int[n];
    Arrays.fill(label, -1);
    final int[] stack = new int[n];
    int next = 0;
    int work = 0; // adjacency entries walked: a vertex of high degree is many of them, so the checkpoint counts these
    for (int start = 0; start < n; start++) {
      guard.checkPeriodically(start);
      if (label[start] >= 0)
        continue;
      label[start] = next;
      int top = 0;
      stack[top++] = start;
      while (top > 0) {
        final int node = stack[--top];
        for (final int other : neighbors[node]) {
          guard.checkPeriodically(++work);
          if (label[other] < 0 && community[other] == community[node]) {
            label[other] = next;
            stack[top++] = other;
          }
        }
      }
      next++;
    }
    return label;
  }
}
