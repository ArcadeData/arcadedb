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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;

import com.arcadedb.utility.IntIntHashMap;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.labelpropagation([config])
 * <p>
 * Detects communities using the Label Propagation Algorithm (LPA). Each node adopts the
 * most common label among its neighbors in each iteration. Very fast and scalable; useful
 * for real-time community detection. Results may be non-deterministic due to tie-breaking.
 * </p>
 * <p>
 * When a Graph Analytical View (GAV) is available, delegates to the CSR-native
 * implementation in {@link GraphAlgorithms#labelPropagation} for maximum performance.
 * </p>
 * <p>
 * Config map parameters (all optional, any other key is refused):
 * <ul>
 *   <li>maxIterations (int, default 10): maximum number of propagation iterations</li>
 *   <li>direction (string, default "BOTH"): edge direction to follow (IN, OUT, BOTH)</li>
 *   <li>tieBreakProperty (string, optional): vertex property used to break ties between equally frequent labels, the
 *   smallest value wins (LDBC Graphalytics CDLP breaks ties by the smallest vertex id). Without it the smallest
 *   internal node index wins, which follows load order. Values must be mutually comparable (numbers with numbers, or
 *   the same class); a vertex without the property loses every tie</li>
 *   <li>seedProperty (string, optional): vertex property holding a pre-assigned integer label. Every vertex carrying the
 *   same value starts in the same community, and a vertex without the property starts with a label of its own. The
 *   communityId of a community that grew from a seed is the seed value itself; any other community is numbered past the
 *   largest seed, so the two never collide (the convention of Neo4j GDS {@code seedProperty})</li>
 *   <li>fixedSeeds (boolean, default false): keeps every seeded vertex on its seed label for the whole run while it still
 *   spreads that label to its neighbours, which is semi-supervised classification: the reviewed vertices hold their class
 *   and the graph classifies the rest. Requires seedProperty</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.labelpropagation({maxIterations: 10})
 * YIELD node, communityId
 * RETURN communityId, count(*) AS size ORDER BY size DESC
 *
 * CALL algo.labelpropagation({seedProperty: 'label', fixedSeeds: true})
 * YIELD node, communityId
 * RETURN node.name, communityId
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLabelPropagation extends AbstractAlgoProcedure {
  public static final String NAME = "algo.labelpropagation";

  /** The keys the config map may hold: any other is refused, so a misspelt one cannot fall back to its default (issue #11145). */
  private static final Set<String> CONFIG_KEYS = Set.of("maxIterations", "direction", "tieBreakProperty", "seedProperty",
      "fixedSeeds");

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
    return "Detects communities using the Label Propagation Algorithm, optionally from pre-assigned seed labels";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "communityId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = extractConfig(argOrDefault(args, 0, null), "config", CONFIG_KEYS);
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        extractInt(n, "maxIterations", 1) : 10;

    final Object tieBreakConfig = config != null ? config.get("tieBreakProperty") : null;
    final String tieBreakProperty = tieBreakConfig != null ? extractString(tieBreakConfig, "tieBreakProperty") : null;
    final Object seedConfig = config != null ? config.get("seedProperty") : null;
    final String seedProperty = seedConfig != null ? extractString(seedConfig, "seedProperty") : null;
    final Object fixedSeedsConfig = config != null ? config.get("fixedSeeds") : null;
    if (fixedSeedsConfig != null && !(fixedSeedsConfig instanceof Boolean))
      throw new IllegalArgumentException(getName() + "(): fixedSeeds must be a boolean, got " + fixedSeedsConfig);
    final boolean fixedSeeds = Boolean.TRUE.equals(fixedSeedsConfig);
    if (fixedSeeds && seedProperty == null)
      throw new IllegalArgumentException(getName() + "(): fixedSeeds needs seedProperty, the vertex property holding the seed labels");
    // Every option is read before the dispatch below and handed to both paths: one read after it reached only the OLTP
    // path, and a view covering the types silently answered BOTH for OUT and IN (issue #9767)
    final Vertex.DIRECTION direction = extractDirection(config != null ? config.get("direction") : null, Vertex.DIRECTION.BOTH);

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Try CSR-accelerated path: delegate to native label propagation on CSR arrays
    final GraphTraversalProvider provider = findProvider(db, null, context);
    // Only while the view is not serving pending changes: the GraphAlgorithms kernel below reads its base CSR
    // arrays directly and sizes its result from the base node mapping, which is neither the current graph nor
    // as wide as the id space the view now reports - see GraphTraversalProvider#hasPendingChanges (issue #6792).
    if (provider instanceof GraphAnalyticalView.Pinned gav && !gav.hasPendingChanges()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      final int n = gav.getNodeCount();
      if (n == 0)
        return Stream.empty();
      final Labelling labelling = initialLabelling(n,
          property -> i -> context.getDatabase().lookupByRID(gav.getRID(i), true).asVertex().get(property), tieBreakProperty,
          seedProperty, fixedSeeds);
      // The kernel's "nothing moved" break only fires if the labelling settles - a graph that oscillates between two
      // labellings never converges - so maxIterations is what ends the run, and the guard is what can abort it.
      final int[] labels = GraphAlgorithms.labelPropagation(gav, maxIterations, labelling.initial, labelling.fixed, direction,
          guard::check);
      return results(n, gav::getRID, labels, labelling);
    }

    // Fall back to OLTP path
    final List<Vertex> vertices = loadVertices(db, null, newMemoryBudget(db));
    if (vertices.isEmpty())
      return Stream.empty();
    final int n = vertices.size();
    final Labelling labelling = initialLabelling(n, property -> i -> vertices.get(i).get(property), tieBreakProperty, seedProperty,
        fixedSeeds);
    // Build adjacency once to avoid repeated OLTP traversal
    final int[][] adj = buildAdjacencyList(vertices, buildRidIndex(vertices), direction, null);
    final int[] labels = propagate(adj, labelling, maxIterations, guard);
    return results(n, i -> vertices.get(i).getIdentity(), labels, labelling);
  }

  /**
   * The starting labels of a run, in the label space the propagation works in: dense ints where the smallest label wins a
   * tie, so the order of the label ids IS the tie-break order. {@code valueOfLabel} turns a final label back into the
   * communityId a caller reads.
   *
   * @param initial      starting label of every dense node
   * @param fixed        the nodes that keep their label for the whole run, or null for none
   * @param valueOfLabel the communityId of every label id
   * @param seeded       whether the run was seeded, which makes communityId the seed value (a long) rather than the dense
   *                     index of the node whose label was adopted (an int, the shape of an unseeded run)
   */
  private record Labelling(int[] initial, boolean[] fixed, long[] valueOfLabel, boolean seeded) {
  }

  /**
   * Builds the starting labels. Without a seed property each node starts with its own label, ordered by the tie-break
   * property when one is given, by node index otherwise: exactly the labelling this procedure always started from.
   * <p>
   * With one (issue #11145), a node carrying the property starts on that value and a node without it on
   * {@code largestSeed + 1 + nodeIndex}, which can never be a seed: the label values are what communityId reports, the
   * convention of Neo4j GDS. Equal values share one label id, so every seed of a class pulls the same way. The label ids are
   * ordered by value, so a tie goes to the smallest value; with a tie-break property they are ordered instead by the
   * smallest tie-break rank among the nodes that start on them, which for an unseeded node is its own rank, as before.
   */
  private Labelling initialLabelling(final int n, final Function<String, IntFunction<Object>> reader, final String tieBreakProperty,
      final String seedProperty, final boolean fixedSeeds) {
    final int[] rank = tieBreakProperty != null ? computeTieBreakRank(n, reader.apply(tieBreakProperty), tieBreakProperty) : null;

    if (seedProperty == null) {
      final int[] initial = new int[n];
      final long[] valueOfLabel = new long[n];
      for (int i = 0; i < n; i++) {
        final int label = rank != null ? rank[i] : i;
        initial[i] = label;
        valueOfLabel[label] = i;
      }
      return new Labelling(initial, null, valueOfLabel, false);
    }

    // The value every node starts on: its seed, or a label of its own past the largest seed
    final IntFunction<Object> seedOf = reader.apply(seedProperty);
    final long[] value = new long[n];
    final boolean[] hasSeed = new boolean[n];
    long largestSeed = -1L;
    for (int i = 0; i < n; i++) {
      final Object seed = seedOf.apply(i);
      if (seed == null)
        continue;
      if (!(seed instanceof Number num) || !isIntegral(num))
        throw new IllegalArgumentException(getName() + "(): seedProperty '" + seedProperty + "' must hold an integer, node " + i
            + " has " + (seed instanceof Number ? seed : seed.getClass().getSimpleName() + " '" + seed + "'"));
      value[i] = num.longValue();
      hasSeed[i] = true;
      if (value[i] > largestSeed)
        largestSeed = value[i];
    }
    if (largestSeed > Long.MAX_VALUE - n)
      throw new IllegalArgumentException(getName() + "(): seedProperty '" + seedProperty + "' holds " + largestSeed
          + ", which leaves no room to number the " + n + " nodes past the largest seed");
    for (int i = 0; i < n; i++)
      if (!hasSeed[i])
        value[i] = largestSeed + 1 + i;

    // One label id per distinct value, in value order: a sort of primitives, no boxing per node
    final long[] sorted = value.clone();
    Arrays.sort(sorted);
    int distinct = 0;
    for (int i = 0; i < n; i++)
      if (i == 0 || sorted[i] != sorted[i - 1])
        sorted[distinct++] = sorted[i];
    final int[] initial = new int[n];
    for (int i = 0; i < n; i++)
      initial[i] = Arrays.binarySearch(sorted, 0, distinct, value[i]);
    long[] valueOfLabel = Arrays.copyOf(sorted, distinct);

    if (rank != null) {
      // Reorder the label ids by the smallest tie-break rank among the nodes starting on each. Those minima are distinct,
      // since every rank belongs to one node, so they index a dense array: O(n), no comparator.
      final int[] smallestRank = new int[distinct];
      Arrays.fill(smallestRank, Integer.MAX_VALUE);
      for (int i = 0; i < n; i++)
        if (rank[i] < smallestRank[initial[i]])
          smallestRank[initial[i]] = rank[i];
      final int[] labelAtRank = new int[n];
      Arrays.fill(labelAtRank, -1);
      for (int label = 0; label < distinct; label++)
        labelAtRank[smallestRank[label]] = label;
      final int[] reordered = new int[distinct];
      final long[] reorderedValue = new long[distinct];
      int next = 0;
      for (int r = 0; r < n; r++) {
        final int label = labelAtRank[r];
        if (label >= 0) {
          reordered[label] = next;
          reorderedValue[next++] = valueOfLabel[label];
        }
      }
      for (int i = 0; i < n; i++)
        initial[i] = reordered[initial[i]];
      valueOfLabel = reorderedValue;
    }

    return new Labelling(initial, fixedSeeds ? hasSeed : null, valueOfLabel, true);
  }

  /** Synchronous label propagation over a record-built adjacency: compute every new label, then apply them all. */
  private static int[] propagate(final int[][] adj, final Labelling labelling, final int maxIterations, final WorkGuard guard) {
    final int n = adj.length;
    final boolean[] fixed = labelling.fixed;
    int[] label = labelling.initial.clone();

    for (int iter = 0; iter < maxIterations; iter++) {
      // maxIterations is a caller-supplied knob and the "nothing moved" break only fires if the labelling settles,
      // so the outer loop carries the checkpoint. One iteration is O(n + m), which swallows a flag test whole.
      guard.check();
      final int[] newLabel = new int[n];
      boolean changed = false;

      for (int i = 0; i < n; i++) {
        // A single iteration walks the whole graph, so on a large one the checkpoint belongs inside the pass too.
        guard.checkPeriodically(i);
        final int[] neighbors = adj[i];
        // A seed keeps its label for the whole run, and so does a node with nobody to listen to
        if (neighbors.length == 0 || (fixed != null && fixed[i])) {
          newLabel[i] = label[i];
          continue;
        }

        // Count labels of neighbors
        final IntIntHashMap labelCount = new IntIntHashMap();
        for (final int neighborIdx : neighbors)
          labelCount.increment(label[neighborIdx]);

        // Find most frequent label (ties broken by smallest label)
        final int[] best = { label[i], 0 }; // [bestLabel, bestCount]
        labelCount.forEach((lbl, cnt) -> {
          if (cnt > best[1] || (cnt == best[1] && lbl < best[0])) {
            best[1] = cnt;
            best[0] = lbl;
          }
        });
        final int bestLabel = best[0];

        newLabel[i] = bestLabel;
        if (bestLabel != label[i])
          changed = true;
      }

      label = newLabel;
      if (!changed)
        break;
    }
    return label;
  }

  private static Stream<Result> results(final int n, final IntFunction<RID> ridOf, final int[] labels, final Labelling labelling) {
    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", ridOf.apply(i));
      final long communityId = labelling.valueOfLabel[labels[i]];
      // An unseeded run keeps reporting the dense node index as an int, the shape it always had
      result.setProperty("communityId", labelling.seeded ? (Object) communityId : (Object) (int) communityId);
      return (Result) result;
    });
  }

  private static boolean isIntegral(final Number n) {
    if (isFixedIntegral(n))
      return true;
    if (n instanceof BigDecimal d)
      return d.stripTrailingZeros().scale() <= 0 && d.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
          && d.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0;
    final double d = n.doubleValue();
    return Double.isFinite(d) && d == Math.rint(d) && Math.abs(d) < 0x1p63;
  }

  /**
   * Ranks the {@code n} dense nodes by the value of {@code property}: {@code rank[i]} is the position of node {@code i}
   * in ascending property order. Nodes with a missing value sort last, and equal values fall back to the dense index so
   * the result is always a permutation. Reads one vertex record per node, so the cost is O(n) record loads on top of
   * the algorithm itself.
   * <p>
   * Every value is normalised to a single key type before sorting, so the comparator is a consistent total order whatever
   * mix of numeric classes the property holds: fixed-width integrals sort on a {@code long}, any other numeric mix on
   * (NaN/Infinity class, exact {@link BigDecimal}), and non-numbers must all share one {@link Comparable} class.
   */
  @SuppressWarnings("unchecked")
  private static int[] computeTieBreakRank(final int n, final IntFunction<Object> valueOf, final String property) {
    final Object[] values = new Object[n];
    Object first = null;
    boolean allNumbers = true;
    boolean allFixedIntegral = true;
    for (int i = 0; i < n; i++) {
      final Object value = valueOf.apply(i);
      if (value != null) {
        if (!(value instanceof Comparable))
          throw new IllegalArgumentException("Property '" + property + "' of node " + i + " is not comparable, cannot be used as tieBreakProperty");
        if (first == null)
          first = value;
        if (!(value instanceof Number num)) {
          allNumbers = false;
          allFixedIntegral = false;
        } else if (!isFixedIntegral(num))
          allFixedIntegral = false;
        if ((first instanceof Number) != (value instanceof Number) || (!(first instanceof Number) && first.getClass() != value.getClass()))
          throw new IllegalArgumentException("Property '" + property + "' of node " + i + " is a " + value.getClass().getSimpleName()
              + " but others are " + first.getClass().getSimpleName() + ", cannot be used as tieBreakProperty");
      }
      values[i] = value;
    }

    // One normalised key per node, computed once: no per-comparison parsing or allocation
    final long[] longKeys = allNumbers && allFixedIntegral ? new long[n] : null;
    final byte[] numClass = allNumbers && !allFixedIntegral ? new byte[n] : null;
    final BigDecimal[] decKeys = numClass != null ? new BigDecimal[n] : null;
    for (int i = 0; i < n; i++) {
      if (values[i] == null)
        continue;
      if (longKeys != null)
        longKeys[i] = ((Number) values[i]).longValue();
      else if (numClass != null) {
        final Number num = (Number) values[i];
        if (isFinite(num)) {
          numClass[i] = 1;
          decKeys[i] = new BigDecimal(num.toString());
        } else {
          final double d = num.doubleValue();
          numClass[i] = (byte) (Double.isNaN(d) ? 3 : d > 0 ? 2 : 0);
        }
      }
    }

    final Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++)
      order[i] = i;
    Arrays.sort(order, (a, b) -> {
      final Object va = values[a];
      final Object vb = values[b];
      if (va == null || vb == null) {
        if (va != vb)
          return va == null ? 1 : -1;
      } else {
        final int cmp;
        if (longKeys != null)
          cmp = Long.compare(longKeys[a], longKeys[b]);
        else if (numClass != null)
          cmp = numClass[a] != numClass[b] ? Byte.compare(numClass[a], numClass[b]) :
              numClass[a] == 1 ? decKeys[a].compareTo(decKeys[b]) : 0;
        else
          cmp = ((Comparable<Object>) va).compareTo(vb);
        if (cmp != 0)
          return cmp;
      }
      return Integer.compare(a, b);
    });

    final int[] rank = new int[n];
    for (int r = 0; r < n; r++)
      rank[order[r]] = r;
    return rank;
  }

  private static boolean isFixedIntegral(final Number n) {
    return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
  }

  private static boolean isFinite(final Number n) {
    return !(n instanceof Double d && !Double.isFinite(d)) && !(n instanceof Float f && !Float.isFinite(f));
  }
}
