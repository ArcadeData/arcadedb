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
import com.arcadedb.graph.DenseNodeIdProvider;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.personalizedPageRank(sourceNodes, relTypes?, dampingFactor?, maxIterations?, tolerance?)
 * <p>
 * Computes Personalized PageRank (PPR) scores relative to one or more source nodes. Unlike standard PageRank,
 * the teleportation probability is concentrated at the source nodes (personalization vector = 0 for every
 * other node). This measures the structural importance/proximity of all nodes relative to the sources.
 * </p>
 * <p>
 * The first argument is either:
 * <ul>
 *   <li>a single node: all the teleportation probability is on it;</li>
 *   <li>a list of nodes: the teleportation probability is split uniformly between them (a duplicated node
 *   counts once per occurrence);</li>
 *   <li>a list of {@code [node, weight]} pairs: a personalization vector, where weights are non-negative numbers
 *   normalized to sum to 1 (so only their ratios matter). Plain nodes and pairs can be mixed, a plain node
 *   having weight 1.</li>
 * </ul>
 * Source nodes that are not part of the graph being analyzed are ignored; if none is, no row is returned. When a
 * positively weighted source is missing from the accelerated (CSR) view, the whole call is answered by the OLTP path
 * over the full graph instead, where that node is analyzed like any other.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (s:Person {name:'Alice'})
 * CALL algo.personalizedPageRank(s, 'KNOWS', 0.85, 20, 0.000001)
 * YIELD nodeId, score
 * RETURN nodeId, score ORDER BY score DESC
 *
 * MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'})
 * CALL algo.personalizedPageRank([[a, 3.0], [b, 1.0]], 'KNOWS')
 * YIELD nodeId, score
 * RETURN nodeId, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoPersonalizedPageRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.personalizedPageRank";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Computes Personalized PageRank scores relative to a source node, a list of nodes, or a weighted personalization vector";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<RID, Double> sources = extractSources(args[0]);
    final String[] relTypes = args.length > 1 ? extractRelTypes(args[1]) : null;
    final double dampingFactor = args.length > 2 && args[2] instanceof Number n ? n.doubleValue() : 0.85;
    final int maxIterations = args.length > 3 && args[3] instanceof Number n ? extractInt(n, "maxIterations", 1) : 20;
    final double tolerance = args.length > 4 && args[4] instanceof Number n ? n.doubleValue() : 1e-6;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Try CSR-accelerated path
    // Renumbered when the provider's id space has holes in it, so that n below is both the array size and the
    // exclusive id bound - the two have to be the same number for a rank array indexed by neighbour id
    // (issue #6792). wrap() is a no-op for a compact id space.
    final GraphTraversalProvider provider = DenseNodeIdProvider.wrap(findProvider(db, relTypes, context));
    if (provider != null) {
      final double[] personalization = buildPersonalization(sources, provider.getNodeCount(), provider::getNodeId, false);
      if (personalization != null) {
        context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
        return executeWithCSR(provider, personalization, relTypes, dampingFactor, maxIterations, tolerance, guard);
      }
    }

    // Fall back to OLTP path
    return executeWithOLTP(db, sources, relTypes, dampingFactor, maxIterations, tolerance, guard);
  }

  /**
   * Parses the first argument into RID -> weight (insertion ordered, duplicated nodes accumulate their weight).
   */
  private Map<RID, Double> extractSources(final Object arg) {
    final Map<RID, Double> sources = new LinkedHashMap<>();
    // Not extractVertexList(): the [node, weight] pairs need their own parsing
    if (arg instanceof Collection<?> items) {
      if (items.isEmpty())
        throw new IllegalArgumentException(getName() + "(): sourceNodes cannot be an empty list");
      for (final Object item : items) {
        if (item instanceof List<?> pair) {
          if (pair.size() != 2)
            throw new IllegalArgumentException(getName() + "(): each weighted source must be a [node, weight] pair");
          if (!(pair.get(1) instanceof Number weight))
            throw new IllegalArgumentException(getName() + "(): the weight of a [node, weight] pair must be a number");
          addSource(sources, extractVertex(pair.get(0), "sourceNodes[*]"), weight.doubleValue());
        } else if (item == null || item instanceof Vertex) // null goes to extractVertex() for its "cannot be null" message
          addSource(sources, extractVertex(item, "sourceNodes[*]"), 1.0);
        else
          throw new IllegalArgumentException(getName() + "(): sourceNodes[*] must be a node or a [node, weight] pair, got "
              + item.getClass().getSimpleName() + " (use [[node, weight], ...] for weighted sources)");
      }
    } else
      addSource(sources, extractVertex(arg, "sourceNodes"), 1.0);

    double total = 0.0;
    for (final double w : sources.values())
      total += w;
    if (!(total > 0.0) || Double.isInfinite(total))
      throw new IllegalArgumentException(getName() + "(): the source weights must sum to a positive finite number");
    return sources;
  }

  private void addSource(final Map<RID, Double> sources, final Vertex vertex, final double weight) {
    if (Double.isNaN(weight) || Double.isInfinite(weight) || weight < 0.0)
      throw new IllegalArgumentException(getName() + "(): source weights must be finite and non-negative, got " + weight);
    sources.merge(vertex.getIdentity(), weight, Double::sum);
  }

  /**
   * Builds the normalized personalization vector (sums to 1) of size {@code n}. Sources missing from the graph
   * (index &lt; 0) are dropped and the rest renormalized when {@code lenient}; otherwise null is returned as soon as
   * a source with a positive weight is unknown to the caller's id space, so the caller can fall back to the OLTP
   * path. An all-zero vector is never returned (null instead).
   */
  private static double[] buildPersonalization(final Map<RID, Double> sources, final int n,
      final ToIntFunction<RID> indexOf, final boolean lenient) {
    // Resolve the (few) sources first, so the O(n) vector is only allocated when one is going to be returned
    final int[] indexes = new int[sources.size()];
    final double[] weights = new double[indexes.length];
    int known = 0;
    double total = 0.0;
    for (final Map.Entry<RID, Double> e : sources.entrySet()) {
      final int idx = indexOf.applyAsInt(e.getKey());
      if (idx < 0) {
        if (!lenient && e.getValue() > 0.0)
          return null;
        continue;
      }
      indexes[known] = idx;
      weights[known++] = e.getValue();
      total += e.getValue();
    }
    if (!(total > 0.0))
      return null;

    final double[] personal = new double[n];
    for (int k = 0; k < known; k++)
      personal[indexes[k]] += weights[k] / total;
    return personal;
  }

  private Stream<Result> executeWithCSR(final GraphTraversalProvider provider, final double[] personal,
      final String[] relTypes, final double dampingFactor, final int maxIterations, final double tolerance,
      final WorkGuard guard) {
    final int n = provider.getNodeCount();
    if (n == 0)
      return Stream.empty();

    // Zero-allocation neighbor access via NeighborView
    final NeighborView outView = provider.getNeighborView(Vertex.DIRECTION.OUT, relTypes);
    final NeighborView inView = provider.getNeighborView(Vertex.DIRECTION.IN, relTypes);
    final boolean hasView = outView != null && inView != null;

    final int[][] outAdjFallback = hasView ? null : buildAdjacencyFromProvider(provider, Vertex.DIRECTION.OUT, relTypes);
    final int[][] inAdjFallback = hasView ? null : buildAdjacencyFromProvider(provider, Vertex.DIRECTION.IN, relTypes);

    final int[] outDegree = new int[n];
    for (int i = 0; i < n; i++)
      outDegree[i] = hasView ? outView.degree(i) : outAdjFallback[i].length;

    final double[] rank = personal.clone();

    for (int iter = 0; iter < maxIterations; iter++) {
      // maxIterations is a caller-supplied knob and the tolerance break only fires if the graph converges, so the
      // outer loop carries the checkpoint. One iteration is O(n + m), which swallows a flag test whole.
      guard.check();
      final double[] newRank = new double[n];
      double dangling = 0.0;
      for (int i = 0; i < n; i++)
        if (outDegree[i] == 0)
          dangling += rank[i];

      if (hasView) {
        final int[] inNbrs = inView.neighbors();
        for (int i = 0; i < n; i++) {
          // A single iteration walks the whole graph, so on a large one the checkpoint belongs inside the pass too.
          guard.checkPeriodically(i);
          double incoming = 0.0;
          for (int k = inView.offset(i), end = inView.offsetEnd(i); k < end; k++) {
            final int j = inNbrs[k];
            if (outDegree[j] > 0)
              incoming += rank[j] / outDegree[j];
          }
          newRank[i] = (1.0 - dampingFactor) * personal[i] + dampingFactor * incoming + dampingFactor * dangling * personal[i];
        }
      } else {
        for (int i = 0; i < n; i++) {
          // The fallback branch of the same pass - the checkpoint belongs in whichever one runs.
          guard.checkPeriodically(i);
          double incoming = 0.0;
          for (final int j : inAdjFallback[i])
            if (outDegree[j] > 0)
              incoming += rank[j] / outDegree[j];
          newRank[i] = (1.0 - dampingFactor) * personal[i] + dampingFactor * incoming + dampingFactor * dangling * personal[i];
        }
      }

      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        maxChange = Math.max(maxChange, Math.abs(newRank[i] - rank[i]));
        rank[i] = newRank[i];
      }

      if (maxChange < tolerance)
        break;
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", provider.getRID(i));
      r.setProperty("score", rank[i]);
      return (Result) r;
    });
  }

  private Stream<Result> executeWithOLTP(final Database db, final Map<RID, Double> sources, final String[] relTypes,
      final double dampingFactor, final int maxIterations, final double tolerance, final WorkGuard guard) {

    final GraphData graph = loadGraph(db, null, relTypes);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // There is no other path to fall back to, so sources missing from the graph are ignored
    final double[] personal = buildPersonalization(sources, n, graph::indexOf, true);
    if (personal == null)
      return Stream.empty();

    final int[][] outAdj = graph.adjacency(Vertex.DIRECTION.OUT, relTypes);
    final int[] outDegree = new int[n];
    for (int i = 0; i < n; i++)
      outDegree[i] = outAdj[i].length;

    final int[][] inAdj = graph.adjacency(Vertex.DIRECTION.IN, relTypes);

    final double[] rank = personal.clone();

    for (int iter2 = 0; iter2 < maxIterations; iter2++) {
      // Same knob and same checkpoint as the CSR path above: the tolerance break only fires if the graph
      // converges, so maxIterations is what ends the run and the guard is what can abort it.
      guard.check();
      final double[] newRank = new double[n];
      double dangling = 0.0;
      for (int i = 0; i < n; i++)
        if (outDegree[i] == 0)
          dangling += rank[i];

      for (int i = 0; i < n; i++) {
        // A single iteration walks the whole graph, so on a large one the checkpoint belongs inside the pass too.
        guard.checkPeriodically(i);
        double incoming = 0.0;
        for (final int j : inAdj[i])
          if (outDegree[j] > 0)
            incoming += rank[j] / outDegree[j];
        newRank[i] = (1.0 - dampingFactor) * personal[i] + dampingFactor * incoming + dampingFactor * dangling * personal[i];
      }

      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        maxChange = Math.max(maxChange, Math.abs(newRank[i] - rank[i]));
        rank[i] = newRank[i];
      }

      if (maxChange < tolerance)
        break;
    }

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", graph.getRID(i));
      r.setProperty("score", rank[i]);
      return (Result) r;
    });
  }
}
