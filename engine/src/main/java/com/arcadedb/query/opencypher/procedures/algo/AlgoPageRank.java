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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.pagerank([config])
 * <p>
 * Computes the PageRank score for all nodes in the graph. PageRank measures the importance of
 * nodes based on the link structure: a node is more important if it receives links from other
 * important nodes.
 * </p>
 * <p>
 * Config map parameters (all optional):
 * <ul>
 *   <li>dampingFactor (double, default 0.85): probability of following a link</li>
 *   <li>maxIterations (int, default 20): maximum number of iterations</li>
 *   <li>tolerance (double, default 0.0001): convergence threshold</li>
 *   <li>weightProperty (string, default null): edge property to use as weight; every weight must be finite and
 *       non-negative, or the call is refused</li>
 *   <li>direction (string, default "OUT"): edges rank is pushed along - "OUT", "IN", or "BOTH".
 *       Anything else is rejected rather than coerced, since the three answer different questions</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20})
 * YIELD node, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoPageRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.pagerank";

  /** The keys the config map may hold: any other is refused, so a misspelt one cannot fall back to its default (issue #11145). */
  private static final Set<String> CONFIG_KEYS = Set.of("dampingFactor", "direction", "maxIterations", "tolerance", "weightProperty");

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
    return 1;
  }

  @Override
  public String getDescription() {
    return "Computes PageRank scores for all nodes in the graph";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = extractConfig(argOrDefault(args, 0, null), "config", CONFIG_KEYS);

    final double dampingFactor = config != null && config.get("dampingFactor") instanceof Number n ?
        n.doubleValue() : 0.85;
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        extractInt(n, "maxIterations", 1) : 20;
    final double tolerance = config != null && config.get("tolerance") instanceof Number n ?
        n.doubleValue() : 0.0001;
    final String weightProperty = config != null ? (String) config.get("weightProperty") : null;
    // OUT, not the BOTH most algorithms default to: rank is pushed along the stored edges
    final Vertex.DIRECTION direction = extractDirection(config != null ? config.get("direction") : null, Vertex.DIRECTION.OUT);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Try CSR-accelerated path (only for unweighted PageRank)
    final GraphTraversalProvider provider = weightProperty == null ? findProvider(db, null, context) : null;
    // Only while the view is not serving pending changes: the GraphAlgorithms kernel below reads its base CSR
    // arrays directly and sizes its result from the base node mapping, which is neither the current graph nor
    // as wide as the id space the view now reports - see GraphTraversalProvider#hasPendingChanges (issue #6792).
    if (provider instanceof GraphAnalyticalView.Pinned gav && !gav.hasPendingChanges()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      return executeWithCSR(gav, dampingFactor, maxIterations, tolerance, direction, guard);
    }

    // Fall back to OLTP path
    return executeWithOLTP(db, dampingFactor, maxIterations, tolerance, weightProperty, direction, guard, context);
  }

  private Stream<Result> executeWithCSR(final GraphAnalyticalView.Pinned gav,
      final double dampingFactor, final int maxIterations, final double tolerance, final Vertex.DIRECTION direction,
      final WorkGuard guard) {
    final int n = gav.getNodeCount();
    if (n == 0)
      return Stream.empty();

    // The kernel stops on the same convergence test as the OLTP path below, so the two answer the same scores whether a
    // view serves the call or not (issue #9791). A graph may never converge below the tolerance, so maxIterations is
    // what ends the run, and the guard is the only thing that can end one the caller no longer wants.
    final double[] scores = GraphAlgorithms.pageRank(gav, dampingFactor, maxIterations, tolerance, direction, guard::check);

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", gav.getRID(i));
      result.setProperty("score", scores[i]);
      return (Result) result;
    });
  }

  private Stream<Result> executeWithOLTP(final Database db, final double dampingFactor,
      final int maxIterations, final double tolerance, final String weightProperty,
      final Vertex.DIRECTION direction, final WorkGuard guard, final CommandContext context) {
    final GraphData graph = loadGraphFromRecords(db, null, context);
    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // Build adjacency once: for each node, the dense ids it pushes rank to, and the matching weights.
    //
    // `direction` names the edges rank travels ALONG, so it selects which stored directions to walk rather than
    // which to report: OUT pushes along stored edges, IN pushes along their reverse, and BOTH pushes both ways,
    // which is what makes it undirected. Before PR #6956 the OUT walk was unconditional and only BOTH added
    // the reverse one, so an IN request was answered with OUT's adjacency.
    //
    // The unweighted rows are read from the edge lists without loading an edge, and the weighted ones from the edge records, which
    // carry the weight: both are priced against the call's budget as they are built (issue #12554). The heap this method used to
    // take for them, a record object per vertex and a boxed map beside them, was not priced at all.
    final int[][] outNeighbors;
    final double[][] outWeights;
    if (weightProperty != null) {
      final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, direction, weightProperty);
      // one NaN or infinite weight made every score NaN through the totals it divides by (issue #12957)
      requireFiniteNonNegativeWeights(weighted, weightProperty, guard);
      outNeighbors = weighted.neighbors();
      outWeights = weighted.weights();
    } else {
      outNeighbors = graph.adjacency(guard, direction);
      outWeights = null;
    }

    // The scores, which the rows are read from, and the vector the next iteration is summed in
    graph.memory().reserve(saturatingProduct(n, 2L * DOUBLE_BYTES), "the rank vectors", n + " nodes");
    final double[] scores = new double[n];
    final double[] newScores = new double[n];
    Arrays.fill(scores, 1.0 / n);

    for (int iter = 0; iter < maxIterations; iter++) {
      // maxIterations is a caller-supplied knob and the tolerance break only fires if the graph converges, so the
      // outer loop carries the checkpoint. One iteration is O(n + m), which swallows a flag test whole.
      guard.check();
      Arrays.fill(newScores, 0.0);
      double dangling = 0.0;

      for (int i = 0; i < n; i++) {
        if (outNeighbors[i].length == 0)
          dangling += scores[i];
      }

      for (int i = 0; i < n; i++) {
        // A single iteration walks the whole graph, so on a large one the checkpoint belongs inside the pass too.
        guard.checkPeriodically(i);
        final int[] neighbors = outNeighbors[i];
        if (neighbors.length == 0)
          continue;
        if (outWeights != null) {
          double totalWeight = 0;
          for (final double w : outWeights[i])
            totalWeight += w;
          if (totalWeight == 0)
            continue;
          for (int j = 0; j < neighbors.length; j++)
            newScores[neighbors[j]] += scores[i] * outWeights[i][j] / totalWeight;
        } else {
          final double contribution = scores[i] / neighbors.length;
          for (final int neighbor : neighbors)
            newScores[neighbor] += contribution;
        }
      }

      final double danglingContribution = dampingFactor * dangling / n;
      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        newScores[i] = (1.0 - dampingFactor) / n + dampingFactor * newScores[i] + danglingContribution;
        maxChange = Math.max(maxChange, Math.abs(newScores[i] - scores[i]));
        scores[i] = newScores[i];
      }

      if (maxChange < tolerance)
        break;
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", graph.getRID(i));
      result.setProperty("score", scores[i]);
      return (Result) result;
    });
  }
}
