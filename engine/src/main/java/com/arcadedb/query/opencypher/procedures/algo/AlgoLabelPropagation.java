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

import java.util.List;
import java.util.Map;
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

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    // Try CSR-accelerated path: delegate to native label propagation on CSR arrays
    final GraphTraversalProvider provider = findProvider(db, null);
    // Only while the view is not serving pending changes: the GraphAlgorithms kernel below reads its base CSR
    // arrays directly and sizes its result from the base node mapping, which is neither the current graph nor
    // as wide as the id space the view now reports - see GraphTraversalProvider#hasPendingChanges (issue #6792).
    if (provider instanceof GraphAnalyticalView gav && !gav.hasPendingChanges()) {
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      return executeWithCSR(context, gav, maxIterations, guard);
    }

    // Fall back to OLTP path
    final String directionStr = config != null && config.get("direction") instanceof String s ? s : "BOTH";
    final Vertex.DIRECTION direction = parseDirection(directionStr);
    return executeWithOLTP(db, maxIterations, direction, guard);
  }

  private Stream<Result> executeWithCSR(final CommandContext context, final GraphAnalyticalView gav,
      final int maxIterations, final WorkGuard guard) {
    final int n = gav.getNodeCount();
    if (n == 0)
      return Stream.empty();

    // The kernel's "nothing moved" break only fires if the labelling settles - a graph that oscillates between two
    // labellings never converges - so maxIterations is what ends the run, and the guard is what can abort it.
    final int[] labels = GraphAlgorithms.labelPropagation(gav, maxIterations, guard::check);
    context.setVariable(CommandContext.RESULT_COUNT_HINT_VAR, (long) n);

    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", gav.getRID(i));
      result.setProperty("communityId", labels[i]);
      return (Result) result;
    });
  }

  private Stream<Result> executeWithOLTP(final Database db, final int maxIterations,
      final Vertex.DIRECTION direction, final WorkGuard guard) {
    final List<Vertex> vertices = loadVertices(db, null, newMemoryBudget(db));
    if (vertices.isEmpty())
      return Stream.empty();

    final int n = vertices.size();
    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // Build adjacency once to avoid repeated OLTP traversal
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, direction, null);

    // Initialize: each node gets its own index as label
    int[] label = new int[n];
    for (int i = 0; i < n; i++)
      label[i] = i;

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
    final int[] finalLabel = label;
    return IntStream.range(0, n).mapToObj(i -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", vertices.get(i).getIdentity());
      result.setProperty("communityId", finalLabel[i]);
      return (Result) result;
    });
  }
}
