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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
 *   <li>weightProperty (string, default null): edge property to use as weight</li>
 *   <li>direction (string, default "OUT"): edge direction for push - "OUT", "IN", or "BOTH"</li>
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

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;

    final double dampingFactor = config != null && config.get("dampingFactor") instanceof Number n ?
        n.doubleValue() : 0.85;
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        extractInt(n, "maxIterations", 1) : 20;
    final double tolerance = config != null && config.get("tolerance") instanceof Number n ?
        n.doubleValue() : 0.0001;
    final String weightProperty = config != null ? (String) config.get("weightProperty") : null;
    final String dirStr = config != null && config.get("direction") instanceof String s ? s : "OUT";
    final Vertex.DIRECTION direction = "BOTH".equalsIgnoreCase(dirStr) ? Vertex.DIRECTION.BOTH :
        "IN".equalsIgnoreCase(dirStr) ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Try CSR-accelerated path (only for unweighted PageRank)
    final GraphTraversalProvider provider = weightProperty == null ? findProvider(db, null) : null;
    // Only while the view is not serving pending changes: the GraphAlgorithms kernel below reads its base CSR
    // arrays directly and sizes its result from the base node mapping, which is neither the current graph nor
    // as wide as the id space the view now reports - see GraphTraversalProvider#hasPendingChanges (issue #6792).
    if (provider instanceof GraphAnalyticalView gav && !gav.hasPendingChanges()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      return executeWithCSR(context, gav, dampingFactor, maxIterations, direction, guard);
    }

    // Fall back to OLTP path
    return executeWithOLTP(db, dampingFactor, maxIterations, tolerance, weightProperty, direction, guard);
  }

  private Stream<Result> executeWithCSR(final CommandContext context, final GraphAnalyticalView gav,
      final double dampingFactor, final int maxIterations, final Vertex.DIRECTION direction, final WorkGuard guard) {
    final int n = gav.getNodeCount();
    if (n == 0)
      return Stream.empty();

    // The CSR kernel has no convergence test at all, so maxIterations alone decides when it stops: the guard is
    // the only thing that can end a run the caller no longer wants.
    final double[] scores = GraphAlgorithms.pageRank(gav, dampingFactor, maxIterations, direction, guard::check);

    // Set result count hint for CallStep count-only optimization
    context.setVariable(CommandContext.RESULT_COUNT_HINT_VAR, (long) n);

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", gav.getRID(i));
      result.setProperty("score", scores[i]);
      return (Result) result;
    });
  }

  private Stream<Result> executeWithOLTP(final Database db, final double dampingFactor,
      final int maxIterations, final double tolerance, final String weightProperty,
      final Vertex.DIRECTION direction, final WorkGuard guard) {
    final List<Vertex> vertices = loadVertices(db, null, newMemoryBudget(db));
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // Build adjacency once: for each node, store (neighborIdx, weight) pairs.
    // When direction is BOTH, edges are treated as bidirectional (undirected graphs).
    final int[][] outNeighbors = new int[n][];
    final double[][] outWeights = weightProperty != null ? new double[n][] : null;
    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      final List<int[]> nbrs = new ArrayList<>();
      final List<Double> wts = weightProperty != null ? new ArrayList<>() : null;

      // OUT and BOTH traverse edges in their stored direction.
      if (direction != Vertex.DIRECTION.IN) {
        for (final Edge edge : v.getEdges(Vertex.DIRECTION.OUT)) {
          try {
            final Integer neighborIdx = ridToIdx.get(edge.getInVertex().getIdentity());
            if (neighborIdx == null)
              continue;
            nbrs.add(new int[]{ neighborIdx });
            if (wts != null) {
              final Object w = edge.get(weightProperty);
              wts.add(w instanceof Number num ? num.doubleValue() : 1.0);
            }
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
          }
        }
      }

      // IN reverses every edge; BOTH also traverses the reverse direction to treat the graph as undirected.
      if (direction != Vertex.DIRECTION.OUT) {
        for (final Edge edge : v.getEdges(Vertex.DIRECTION.IN)) {
          try {
            final Integer neighborIdx = ridToIdx.get(edge.getOutVertex().getIdentity());
            if (neighborIdx == null)
              continue;
            nbrs.add(new int[]{ neighborIdx });
            if (wts != null) {
              final Object w = edge.get(weightProperty);
              wts.add(w instanceof Number num ? num.doubleValue() : 1.0);
            }
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
          }
        }
      }

      outNeighbors[i] = new int[nbrs.size()];
      for (int j = 0; j < nbrs.size(); j++)
        outNeighbors[i][j] = nbrs.get(j)[0];
      if (outWeights != null) {
        outWeights[i] = new double[wts.size()];
        for (int j = 0; j < wts.size(); j++)
          outWeights[i][j] = wts.get(j);
      }
    }

    // Iterate purely in-memory
    final double[] scores = new double[n];
    final double initialScore = 1.0 / n;
    for (int i = 0; i < n; i++)
      scores[i] = initialScore;

    for (int iter = 0; iter < maxIterations; iter++) {
      // maxIterations is a caller-supplied knob and the tolerance break only fires if the graph converges, so the
      // outer loop carries the checkpoint. One iteration is O(n + m), which swallows a flag test whole.
      guard.check();
      final double[] newScores = new double[n];
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
      result.setProperty("node", vertices.get(i).getIdentity());
      result.setProperty("score", scores[i]);
      return (Result) result;
    });
  }
}
