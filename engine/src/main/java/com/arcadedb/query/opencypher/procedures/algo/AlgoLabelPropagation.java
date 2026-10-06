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
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
 * Config map parameters (all optional):
 * <ul>
 *   <li>maxIterations (int, default 10): maximum number of propagation iterations</li>
 *   <li>direction (string, default "BOTH"): edge direction to follow (IN, OUT, BOTH)</li>
 *   <li>tieBreakProperty (string, optional): vertex property used to break ties between equally frequent labels, the
 *   smallest value wins (LDBC Graphalytics CDLP breaks ties by the smallest vertex id). Without it the smallest
 *   internal node index wins, which follows load order. Values must be mutually comparable (numbers with numbers, or
 *   the same class); a vertex without the property loses every tie</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL algo.labelpropagation({maxIterations: 10})
 * YIELD node, communityId
 * RETURN communityId, count(*) AS size ORDER BY size DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoLabelPropagation extends AbstractAlgoProcedure {
  public static final String NAME = "algo.labelpropagation";

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
    return "Detects communities using the Label Propagation Algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "communityId");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Map<String, Object> config = args.length > 0 ? extractMap(args[0], "config") : null;
    final int maxIterations = config != null && config.get("maxIterations") instanceof Number n ?
        extractInt(n, "maxIterations", 1) : 10;

    final Object tieBreakConfig = config != null ? config.get("tieBreakProperty") : null;
    final String tieBreakProperty = tieBreakConfig != null ? extractString(tieBreakConfig, "tieBreakProperty") : null;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Try CSR-accelerated path: delegate to native label propagation on CSR arrays
    final GraphTraversalProvider provider = findProvider(db, null, context);
    // Only while the view is not serving pending changes: the GraphAlgorithms kernel below reads its base CSR
    // arrays directly and sizes its result from the base node mapping, which is neither the current graph nor
    // as wide as the id space the view now reports - see GraphTraversalProvider#hasPendingChanges (issue #6792).
    if (provider instanceof GraphAnalyticalView gav && !gav.hasPendingChanges()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      return executeWithCSR(context, gav, maxIterations, tieBreakProperty, guard);
    }

    // Fall back to OLTP path
    final String directionStr = config != null && config.get("direction") instanceof String s ? s : "BOTH";
    final Vertex.DIRECTION direction = parseDirection(directionStr);
    return executeWithOLTP(db, maxIterations, direction, tieBreakProperty, guard);
  }

  private Stream<Result> executeWithCSR(final CommandContext context, final GraphAnalyticalView gav,
      final int maxIterations, final String tieBreakProperty, final WorkGuard guard) {
    final int n = gav.getNodeCount();
    if (n == 0)
      return Stream.empty();

    final int[] rank = tieBreakProperty != null ? computeTieBreakRank(n,
        i -> context.getDatabase().lookupByRID(gav.getRID(i), true).asVertex().get(tieBreakProperty), tieBreakProperty) : null;

    // The kernel's "nothing moved" break only fires if the labelling settles - a graph that oscillates between two
    // labellings never converges - so maxIterations is what ends the run, and the guard is what can abort it.
    final int[] labels = GraphAlgorithms.labelPropagation(gav, maxIterations, rank, guard::check);
    context.setVariable(CommandContext.RESULT_COUNT_HINT_VAR, (long) n);

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", gav.getRID(i));
      result.setProperty("communityId", labels[i]);
      return (Result) result;
    });
  }

  private Stream<Result> executeWithOLTP(final Database db, final int maxIterations,
      final Vertex.DIRECTION direction, final String tieBreakProperty, final WorkGuard guard) {
    final List<Vertex> vertices = loadVertices(db, null, newMemoryBudget(db));
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final int[] rank = tieBreakProperty != null ? computeTieBreakRank(n,
        i -> vertices.get(i).get(tieBreakProperty), tieBreakProperty) : null;
    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // Build adjacency once to avoid repeated OLTP traversal
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, direction, null);

    // Initialize: each node gets its own index as label. With a tie-break property labels live in rank space, so the
    // "smallest label wins" rule below follows the property order; they are translated back to indexes at the end.
    int[] label = new int[n];
    for (int i = 0; i < n; i++)
      label[i] = rank != null ? rank[i] : i;

    // Synchronous label propagation: compute all new labels, then apply
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
        if (neighbors.length == 0) {
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
        int bestLabel = best[0];

        newLabel[i] = bestLabel;
        if (bestLabel != label[i])
          changed = true;
      }

      label = newLabel;
      if (!changed)
        break;
    }

    // Return raw label values (no sequential remapping)
    if (rank != null) {
      final int[] nodeOfRank = GraphAlgorithms.invertRank(rank);
      for (int i = 0; i < n; i++)
        label[i] = nodeOfRank[label[i]];
    }
    final int[] finalLabel = label;
    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", vertices.get(i).getIdentity());
      result.setProperty("communityId", finalLabel[i]);
      return (Result) result;
    });
  }

  private static boolean isFixedIntegral(final Number n) {
    return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
  }

  private static boolean isFinite(final Number n) {
    return !(n instanceof Double d && !Double.isFinite(d)) && !(n instanceof Float f && !Float.isFinite(f));
  }

  /**
   * Ranks the {@code n} dense nodes by the value of {@code property}: {@code rank[i]} is the position of node {@code i}
   * in ascending property order. Nodes with a missing value sort last, and equal values fall back to the dense index so
   * the result is always a permutation. Reads one vertex record per node, so the cost is O(n) record loads on top of
   * the algorithm itself.
   */
  @SuppressWarnings("unchecked")
  private static int[] computeTieBreakRank(final int n, final IntFunction<Object> valueOf, final String property) {
    final Object[] values = new Object[n];
    Object first = null;
    for (int i = 0; i < n; i++) {
      final Object value = valueOf.apply(i);
      if (value != null) {
        if (!(value instanceof Comparable))
          throw new IllegalArgumentException("Property '" + property + "' of node " + i + " is not comparable, cannot be used as tieBreakProperty");
        if (first == null)
          first = value;
        else if (!(first instanceof Number && value instanceof Number) && first.getClass() != value.getClass())
          throw new IllegalArgumentException("Property '" + property + "' of node " + i + " is a " + value.getClass().getSimpleName()
              + " but others are " + first.getClass().getSimpleName() + ", cannot be used as tieBreakProperty");
      }
      values[i] = value;
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
        if (va instanceof Number na && vb instanceof Number nb) {
          // Long.compare only for fixed-width integrals; anything else finite is compared exactly (a double cannot hold
          // every long, longValue() drops fractions and overflows BigInteger). NaN/Infinity fall back to Double.compare.
          if (isFixedIntegral(na) && isFixedIntegral(nb))
            cmp = Long.compare(na.longValue(), nb.longValue());
          else if (isFinite(na) && isFinite(nb) && (na.getClass() != nb.getClass() || na instanceof BigDecimal || na instanceof BigInteger))
            cmp = new BigDecimal(na.toString()).compareTo(new BigDecimal(nb.toString()));
          else
            cmp = Double.compare(na.doubleValue(), nb.doubleValue());
        } else if (va.getClass() == vb.getClass())
          cmp = ((Comparable<Object>) va).compareTo(vb);
        else
          throw new IllegalArgumentException("Property '" + property + "' has values of different types, cannot be used as tieBreakProperty");
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
}
