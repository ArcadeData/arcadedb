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

import java.util.Arrays;
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

  /** Starting size of the per-node adjacency buffer the OLTP fallback reuses; it doubles on demand. */
  private static final int INITIAL_ADJACENCY_CAPACITY = 16;

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
    final Vertex.DIRECTION direction = extractDirection(config != null ? config.get("direction") : null);

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

  /**
   * Resolves the {@code direction} config value, rejecting anything that is neither absent nor one of the three
   * supported values.
   * <p>
   * The three directions answer genuinely different questions - OUT pushes rank along stored edges, IN along
   * their reverse, BOTH along both - so coercing an unrecognised value to OUT, as this did before, answers a
   * question the caller did not ask and returns a plausible-looking result rather than an error. {@code
   * 'INCOMING'} is the case that matters: it is what someone reaching for {@code IN} types, and it used to
   * silently produce OUT's scores. That was harmless only while IN was broken anyway (see the direction fix in
   * PR #6956); now that IN works, the silence is the bug.
   * <p>
   * Absent and explicitly null both mean "use the default", which is OUT - not BOTH, whatever the shared
   * {@code direction} note in the docs says about most algorithms.
   * <p>
   * Deliberately local rather than routed through {@link com.arcadedb.graph.GraphEngine#parseDirection}: that
   * helper coerces unknown values to BOTH and is shared by around twenty other {@code algo.*} procedures, so
   * tightening it is a far wider behaviour change than this procedure's own bug fix should carry.
   */
  private Vertex.DIRECTION extractDirection(final Object value) {
    if (value == null)
      return Vertex.DIRECTION.OUT;
    if (!(value instanceof String s))
      throw new IllegalArgumentException(
          getName() + "(): direction must be a string, one of OUT, IN or BOTH, got " + value);
    if ("OUT".equalsIgnoreCase(s))
      return Vertex.DIRECTION.OUT;
    if ("IN".equalsIgnoreCase(s))
      return Vertex.DIRECTION.IN;
    if ("BOTH".equalsIgnoreCase(s))
      return Vertex.DIRECTION.BOTH;
    throw new IllegalArgumentException(
        getName() + "(): unknown direction '" + s + "', expected one of OUT, IN or BOTH");
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

    // Build adjacency once: for each node, the dense ids it pushes rank to, and the matching weights.
    //
    // `direction` names the edges rank travels ALONG, so it selects which stored directions to walk rather than
    // which to report: OUT pushes along stored edges, IN pushes along their reverse, and BOTH pushes both ways,
    // which is what makes it undirected. Before PR #6956 the OUT walk was unconditional and only BOTH added
    // the reverse one, so an IN request was answered with OUT's adjacency.
    final int[][] outNeighbors = new int[n][];
    final double[][] outWeights = weightProperty != null ? new double[n][] : null;
    final Vertex.DIRECTION[] walks = direction == Vertex.DIRECTION.BOTH ?
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN } :
        new Vertex.DIRECTION[] { direction };
    // One growable primitive buffer, reused across every node and copied out at its exact size, rather than a
    // List<int[]> whose every entry was a one-element array and a List<Double> that boxed every weight: that is
    // two allocations per EDGE on a path that walks the whole graph.
    int[] nbrBuf = new int[INITIAL_ADJACENCY_CAPACITY];
    double[] wtBuf = weightProperty != null ? new double[INITIAL_ADJACENCY_CAPACITY] : null;
    int edgeStep = 0;
    for (int i = 0; i < n; i++) {
      final Vertex v = vertices.get(i);
      int count = 0;

      for (final Vertex.DIRECTION walk : walks) {
        for (final Edge edge : v.getEdges(walk)) {
          // This build walks and deserialises every edge in the graph and had no checkpoint at all: the first
          // one a call reached was the iteration loop below, so a deadline could not be seen until the whole
          // adjacency was already materialised. Throttled by EDGE rather than by vertex for the reason
          // AbstractAlgoProcedure.RecordRowReader gives for the same walk - one supernode can hold millions of
          // them, and a per-vertex checkpoint would leave that whole node unabortable.
          guard.checkPeriodically(edgeStep++);
          try {
            final Integer neighborIdx = ridToIdx.get(
                walk == Vertex.DIRECTION.OUT ? edge.getInVertex().getIdentity() : edge.getOutVertex().getIdentity());
            if (neighborIdx == null)
              continue;
            if (count == nbrBuf.length) {
              nbrBuf = Arrays.copyOf(nbrBuf, count << 1);
              if (wtBuf != null)
                wtBuf = Arrays.copyOf(wtBuf, count << 1);
            }
            nbrBuf[count] = neighborIdx;
            if (wtBuf != null) {
              final Object w = edge.get(weightProperty);
              wtBuf[count] = w instanceof Number num ? num.doubleValue() : 1.0;
            }
            count++;
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
          }
        }
      }

      outNeighbors[i] = Arrays.copyOf(nbrBuf, count);
      if (outWeights != null)
        outWeights[i] = Arrays.copyOf(wtBuf, count);
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
