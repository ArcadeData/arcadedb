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
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.personalizedPageRank(sourceNodes, relTypes?, dampingFactor?, maxIterations?, tolerance?)
 * or algo.personalizedPageRank(sourceNodes, config)
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
 * The second argument is either the relationship types, followed by the positional knobs, or a config map (any other key is
 * refused):
 * <ul>
 *   <li>relTypes (a name, a list of names or null, default every type)</li>
 *   <li>dampingFactor (double, default 0.85), maxIterations (int, default 20), tolerance (double, default 0.000001)</li>
 *   <li>direction (string, default "OUT"): the edges rank travels along, with the values of {@code algo.pagerank}. OUT walks the
 *   stored edges, so a vertex nothing points at yet scores 0 from every other source; IN walks them backwards, and BOTH treats
 *   the graph as undirected, which is what a just-written vertex that only links to existing ones needs to be reached</li>
 *   <li>weightProperty (string, default none): the edge property holding the edge's weight. A vertex hands its score to its
 *   neighbours in proportion to the weights of the edges it walks, a missing or non-numeric value counting as 1, and one whose
 *   edges all weigh 0 hands it back through teleportation like a vertex with no edge. Weights must be finite and
 *   non-negative</li>
 * </ul>
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
 *
 * MATCH (s:Doc {id: 42})
 * CALL algo.personalizedPageRank(s, {relTypes: 'SIMILAR', direction: 'BOTH', weightProperty: 'score'})
 * YIELD nodeId, score
 * RETURN nodeId, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoPersonalizedPageRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.personalizedPageRank";

  /** The keys the config map may hold: any other is refused, so a misspelt one cannot fall back to its default (issue #11145). */
  private static final Set<String> CONFIG_KEYS = Set.of("relTypes", "dampingFactor", "maxIterations", "tolerance", "direction",
      "weightProperty");

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
    final String[] relTypes;
    final double dampingFactor;
    final int maxIterations;
    final double tolerance;
    final Vertex.DIRECTION direction;
    final String weightProperty;
    if (argOrDefault(args, 1, null) instanceof Map<?, ?>) {
      // The options-map form (issue #11146): the knobs the positional form has no room for, under algo.pagerank's names
      if (args.length > 2)
        throw new IllegalArgumentException(getName() + "(): with a config map as second argument there is no third one,"
            + " put dampingFactor, maxIterations and tolerance in the map");
      final Map<String, Object> config = extractConfig(args[1], "config", CONFIG_KEYS);
      relTypes = extractRelTypes(config.get("relTypes"), context);
      dampingFactor = configNumber(config, "dampingFactor", 0.85).doubleValue();
      maxIterations = extractInt(configNumber(config, "maxIterations", 20), "maxIterations", 1);
      tolerance = configNumber(config, "tolerance", 1e-6).doubleValue();
      direction = extractDirection(config.get("direction"), Vertex.DIRECTION.OUT);
      final Object weightConfig = config.get("weightProperty");
      weightProperty = weightConfig != null ? extractWeightProperty(weightConfig, context) : null;
    } else {
      relTypes = extractRelTypes(argOrDefault(args, 1, null), context);
      dampingFactor = optionalNumberArg(args, 2, "dampingFactor", 0.85).doubleValue();
      maxIterations = extractInt(optionalNumberArg(args, 3, "maxIterations", 20), "maxIterations", 1);
      tolerance = optionalNumberArg(args, 4, "tolerance", 1e-6).doubleValue();
      direction = Vertex.DIRECTION.OUT;
      weightProperty = null;
    }

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Served by a view when one covers the types. Its node ids are renumbered compact by loadGraph(), so that n below is
    // both the array size and the exclusive id bound - the two have to be the same number for a rank array indexed by
    // neighbour id (issue #6792).
    GraphData graph = loadGraph(db, null, relTypes, context);
    double[] personal = buildPersonalization(sources, graph.nodeCount, graph::indexOf, !graph.isCSRBacked());
    if (personal == null && graph.isCSRBacked()) {
      // A positively weighted source the view does not hold (created after its last build): the whole call is answered
      // over the records instead, where that node is analyzed like any other
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, false);
      graph = loadGraphFromRecords(db, null, context);
      personal = buildPersonalization(sources, graph.nodeCount, graph::indexOf, true);
    }
    if (personal == null || graph.nodeCount == 0)
      return Stream.empty();

    final double[] rank = weightProperty == null ?
        iterate(graph, personal, direction, relTypes, dampingFactor, maxIterations, tolerance, guard) :
        iterateWeighted(graph, personal, direction, weightProperty, relTypes, dampingFactor, maxIterations, tolerance, guard);

    final GraphData answered = graph;
    return IntStream.range(0, graph.nodeCount).mapToObj(i -> {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", answered.getRID(i));
      r.setProperty("score", rank[i]);
      return (Result) r;
    });
  }

  /** A number of the config map, {@code defaultValue} when absent or null; anything else than a number is refused. */
  private Number configNumber(final Map<String, Object> config, final String key, final Number defaultValue) {
    final Object value = config.get(key);
    if (value == null)
      return defaultValue;
    if (value instanceof Number n)
      return n;
    throw new IllegalArgumentException(getName() + "(): " + key + " must be a number, got " + value.getClass().getSimpleName());
  }

  /**
   * Unweighted power iteration. Every node hands its score, split evenly, to the neighbours its edges in {@code direction}
   * reach; a node with none hands it back to the sources through teleportation. Pushed rather than pulled, so a node holding no
   * score yet - in a personalized run, every node the walk has not reached - costs a flag test instead of a read of its edges.
   */
  private static double[] iterate(final GraphData graph, final double[] personal, final Vertex.DIRECTION direction,
      final String[] relTypes, final double dampingFactor, final int maxIterations, final double tolerance, final WorkGuard guard) {
    final int n = graph.nodeCount;
    // Zero-allocation neighbour access when a view serves the call, a record-built adjacency otherwise
    final NeighborView view = graph.neighborView(direction, relTypes);
    final int[][] adjacency = view == null ? graph.adjacency(direction, relTypes) : null;
    final int[] neighbors = view != null ? view.neighbors() : null;
    final int[] degree = new int[n];
    for (int i = 0; i < n; i++)
      degree[i] = view != null ? view.degree(i) : adjacency[i].length;

    final double[] rank = personal.clone();
    final double[] next = new double[n];
    for (int iter = 0; iter < maxIterations; iter++) {
      // maxIterations is a caller-supplied knob and the tolerance break only fires if the graph converges, so the
      // outer loop carries the checkpoint. One iteration is O(n + m), which swallows a flag test whole.
      guard.check();
      Arrays.fill(next, 0.0);
      double dangling = 0.0;
      for (int j = 0; j < n; j++) {
        // A single iteration walks the whole graph, so on a large one the checkpoint belongs inside the pass too.
        guard.checkPeriodically(j);
        final double score = rank[j];
        if (score == 0.0)
          continue;
        if (degree[j] == 0) {
          dangling += score;
          continue;
        }
        final double share = score / degree[j];
        if (view != null) {
          for (int k = view.offset(j), end = view.offsetEnd(j); k < end; k++)
            next[neighbors[k]] += share;
        } else
          for (final int neighbor : adjacency[j])
            next[neighbor] += share;
      }
      if (settle(rank, next, personal, dampingFactor, dangling) < tolerance)
        break;
    }
    return rank;
  }

  /**
   * Weighted power iteration (issue #11146): a node hands its score to its neighbours in proportion to the weights of the
   * edges it walks. The weights are read with their edges, from the view's edge columns when it holds the property and from
   * the edge records otherwise, so each weight is the one of its own edge whichever path serves the call.
   */
  private double[] iterateWeighted(final GraphData graph, final double[] personal, final Vertex.DIRECTION direction,
      final String weightProperty, final String[] relTypes, final double dampingFactor, final int maxIterations,
      final double tolerance, final WorkGuard guard) {
    final int n = graph.nodeCount;
    final GraphData.WeightedAdjacency adjacency = graph.weightedAdjacency(guard, direction, weightProperty, relTypes);
    final int[][] neighbors = adjacency.neighbors();
    final double[][] weights = adjacency.weights();
    final double[] totalWeight = new double[n];
    for (int i = 0; i < n; i++) {
      double total = 0.0;
      for (final double w : weights[i]) {
        // A negative weight would hand a neighbour a negative score, and an infinite one all of it: neither is a walk
        if (!(w >= 0.0) || Double.isInfinite(w))
          throw new IllegalArgumentException(
              getName() + "(): the edge weights of '" + weightProperty + "' must be finite and non-negative, got " + w);
        total += w;
      }
      totalWeight[i] = total;
    }

    final double[] rank = personal.clone();
    final double[] next = new double[n];
    for (int iter = 0; iter < maxIterations; iter++) {
      // Same knob and same checkpoints as the unweighted iteration above
      guard.check();
      Arrays.fill(next, 0.0);
      double dangling = 0.0;
      for (int j = 0; j < n; j++) {
        guard.checkPeriodically(j);
        final double score = rank[j];
        if (score == 0.0)
          continue;
        // Edges that all weigh 0 lead nowhere: the score goes back to the sources, as for a node with no edge
        if (!(totalWeight[j] > 0.0)) {
          dangling += score;
          continue;
        }
        final double perUnit = score / totalWeight[j];
        final int[] row = neighbors[j];
        final double[] rowWeights = weights[j];
        for (int k = 0; k < row.length; k++)
          next[row[k]] += perUnit * rowWeights[k];
      }
      if (settle(rank, next, personal, dampingFactor, dangling) < tolerance)
        break;
    }
    return rank;
  }

  /**
   * Completes one iteration: what was walked is damped, and teleportation - plus the score of the nodes with nowhere to go -
   * returns to the sources in their personalization share. Writes the new scores into {@code rank} and answers the largest
   * change.
   */
  private static double settle(final double[] rank, final double[] walked, final double[] personal, final double dampingFactor,
      final double dangling) {
    double maxChange = 0.0;
    for (int i = 0; i < rank.length; i++) {
      final double updated = (1.0 - dampingFactor) * personal[i] + dampingFactor * walked[i] + dampingFactor * dangling * personal[i];
      maxChange = Math.max(maxChange, Math.abs(updated - rank[i]));
      rank[i] = updated;
    }
    return maxChange;
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
}
