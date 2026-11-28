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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.eigenvector(relTypes?, direction?, maxIterations?, tolerance?)
 * <p>
 * Computes eigenvector centrality for every vertex using the power iteration method.
 * A vertex's score is proportional to the sum of the scores of its neighbors.
 * Scores are normalized by the L∞ (max) norm keeping values in [0, 1].
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.eigenvector('KNOWS', 'BOTH', 50, 1e-8)
 * YIELD node, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoEigenvectorCentrality extends AbstractAlgoProcedure {
  public static final String NAME = "algo.eigenvector";

  // states of the walk in verticesReachingNoCycle
  private static final byte UNVISITED              = 0;
  private static final byte ON_STACK               = 1;
  private static final byte ON_STACK_REACHES_CYCLE = 2;
  private static final byte NO_CYCLE               = 3;
  private static final byte REACHES_CYCLE          = 4;

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
    return 4;
  }

  @Override
  public String getDescription() {
    return "Compute eigenvector centrality for all vertices using power iteration";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes    = extractRelTypes(argOrDefault(args, 0, null), context);
    final Vertex.DIRECTION dir = parseDirection(extractString(argOrDefault(args, 1, "BOTH"), "direction"));
    final int maxIterations    = extractInt(optionalNumberArg(args, 2, "maxIterations", 20), "maxIterations", 1);
    final double tolerance     = optionalNumberArg(args, 3, "tolerance", 1e-6).doubleValue();
    requireNonNegativeFinite("tolerance", tolerance);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adj = graph.adjacency(dir, relTypes);

    final boolean[] reachesNoCycle = verticesReachingNoCycle(adj, guard);

    double[] scores    = new double[n];
    double[] newScores = new double[n];
    for (int i = 0; i < n; i++)
      scores[i] = reachesNoCycle[i] ? 0.0 : 1.0;

    for (int iteration = 0; iteration < maxIterations; iteration++) {
      // maxIterations is a caller-supplied knob and the tolerance break only fires if the graph converges, so the
      // outer loop carries the checkpoint. One iteration is O(n + m), which swallows a flag test whole.
      guard.check();
      // newScores[v] = scores[v] + the sum of scores[u] for all neighbours u of v, that is power iteration on A + I. A alone does not
      // converge on a bipartite graph (every tree, star, grid or even cycle): its extreme eigenvalues are +l and -l, the iterate
      // alternates between two vectors and the answer depends on the parity of maxIterations. Adding the identity shifts every
      // eigenvalue by one, keeps the eigenvectors, and makes the dominant one the only one of largest magnitude. A vertex that
      // reaches no cycle along adj scores zero in every eigenvector of A with a nonzero eigenvalue, and is kept at zero: iterated
      // on A + I it would not drain (see verticesReachingNoCycle).
      for (int v = 0; v < n; v++) {
        // A single iteration walks the whole graph, so on a large one the checkpoint belongs inside the pass too.
        guard.checkPeriodically(v);
        if (reachesNoCycle[v]) {
          newScores[v] = 0.0;
          continue;
        }
        double sum = scores[v];
        for (final int u : adj[v])
          sum += scores[u];
        newScores[v] = sum;
      }

      // Normalize by L-infinity (max) norm to keep values in [0, 1]
      double maxVal = 0.0;
      for (int i = 0; i < n; i++)
        if (newScores[i] > maxVal) maxVal = newScores[i];
      if (maxVal > 0)
        for (int i = 0; i < n; i++) newScores[i] /= maxVal;

      // Check convergence: max change across all vertices
      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        final double d = Math.abs(newScores[i] - scores[i]);
        if (d > maxChange) maxChange = d;
      }

      // Swap arrays
      final double[] tmp = scores;
      scores = newScores;
      newScores = tmp;

      if (maxChange < tolerance)
        break;
    }

    final double[] finalScores = scores;
    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("score", finalScores[i]);
      return (Result) r;
    });
  }

  /**
   * Marks every vertex from which no walk along {@code adj} reaches a cycle. On those vertices A is nilpotent: plain power iteration
   * drains them to zero within as many passes as their longest walk, but A + I has eigenvalue 1 there with a Jordan block, so a
   * vertex whose successors all reach no cycle keeps its raw score while its predecessors grow linearly. On a DAG followed OUT or IN
   * (a citation graph, a hierarchy) the scores would never meet the tolerance and would depend on {@code maxIterations}. Their
   * entry of the dominant eigenvector is zero, which is what the iteration on A answered, so they are pinned there. An undirected
   * adjacency is symmetric, so there every vertex with a neighbour lies on a 2-cycle and only the isolated ones are marked.
   * <p>
   * One iterative depth-first walk, O(n + m), with three int/byte arrays of n entries and no copy of the edges: reaching a vertex
   * still on the stack closes a cycle, and reaching one already known to reach a cycle reaches it too.
   */
  static boolean[] verticesReachingNoCycle(final int[][] adj, final WorkGuard guard) {
    final int n = adj.length;
    final byte[] state = new byte[n];
    final int[] stack = new int[n];
    final int[] next = new int[n];
    int steps = 0;
    for (int root = 0; root < n; root++) {
      if (state[root] != UNVISITED)
        continue;
      int top = 0;
      stack[0] = root;
      next[0] = 0;
      state[root] = ON_STACK;
      while (top >= 0) {
        guard.checkPeriodically(steps++);
        final int v = stack[top];
        final int[] row = adj[v];
        if (next[top] < row.length) {
          final int u = row[next[top]++];
          final byte s = state[u];
          if (s == UNVISITED) {
            ++top;
            stack[top] = u;
            next[top] = 0;
            state[u] = ON_STACK;
          } else if (s != NO_CYCLE)
            // ON_STACK closes a cycle, ON_STACK_REACHES_CYCLE and REACHES_CYCLE lead to one
            state[v] = ON_STACK_REACHES_CYCLE;
        } else {
          final boolean reaches = state[v] == ON_STACK_REACHES_CYCLE;
          state[v] = reaches ? REACHES_CYCLE : NO_CYCLE;
          --top;
          if (reaches && top >= 0)
            state[stack[top]] = ON_STACK_REACHES_CYCLE;
        }
      }
    }
    final boolean[] noCycle = new boolean[n];
    for (int v = 0; v < n; v++)
      noCycle[v] = state[v] == NO_CYCLE;
    return noCycle;
  }
}
