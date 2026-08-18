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

import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.apsp(weightProperty?, relTypes?)
 * <p>
 * Computes All-Pairs Shortest Paths (APSP) using the Floyd-Warshall algorithm O(V³).
 * Returns one result per reachable (source, target) pair (i ≠ j). When no weight property
 * is specified, all edges have unit weight 1.0.
 * </p>
 * <p>
 * When a Graph Analytical View with edge properties is available, the distance matrix
 * initialization uses CSR adjacency and columnar edge weights, avoiding OLTP edge deserialization.
 * </p>
 * <p>
 * Note: this algorithm is O(V²) in memory and O(V³) in time. Only suitable for graphs
 * with up to a few thousand vertices - a bound the distance matrix's reservation through
 * {@link AbstractAlgoProcedure.MemoryBudget} now enforces rather than merely advises. The time half of that
 * guarantee is a {@link WorkGuard} checkpoint inside the Floyd-Warshall loop: the budget's own floor admits about
 * 2890 nodes, which is ~2.4e10 iterations, so a graph it accepts must at least be abortable (issue #6302).
 * </p>
 * <p>
 * The rows are produced lazily from the completed distance matrix, so the O(V²) figure above is the matrix and
 * nothing else: a caller that reads one row holds one row. This is the only {@code algo.*} procedure whose row
 * count is quadratic in the graph rather than bounded by a top-k, a component count or one neighbourhood, which
 * is what made materialising it eagerly the largest allocation of the call (issue #6296).
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.apsp('distance', 'ROAD')
 * YIELD source, target, distance
 * RETURN source.name, target.name, distance ORDER BY distance ASC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoAPSP extends AbstractAlgoProcedure {
  public static final String NAME = "algo.apsp";

  private static final double INF = Double.MAX_VALUE / 2.0;

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Compute all-pairs shortest paths using Floyd-Warshall algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("source", "target", "distance");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String weightProperty = args.length > 0 ? extractString(args[0], "weightProperty") : null;
    final String[] relTypes     = args.length > 1 ? extractRelTypes(args[1]) : null;

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);
    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // The distance matrix is nodeCount x nodeCount: 800 MB at 10 000 nodes, 80 GB at 100 000, with no knob
    // involved - the graph alone sizes it. Reserved before it is allocated so that a graph too large for
    // Floyd-Warshall is a client error naming the node count and the budget, rather than an OutOfMemoryError
    // that takes the rest of the JVM's work down with it.
    graph.memory().reserve(matrixBytes(n, n, DOUBLE_BYTES), "the distance matrix", n + " x " + n + " nodes");

    // Allocate distance matrix: one large contiguous allocation is GC-friendly
    final double[][] dist = new double[n][n];
    for (int i = 0; i < n; i++) {
      // One row is already O(n) writes, so the check costs nothing beside it and needs no throttle.
      guard.check();
      for (int j = 0; j < n; j++)
        dist[i][j] = i == j ? 0.0 : INF;
    }

    // Fill direct edges. Neighbours and weights are built together - from the columnar edge properties when
    // the view has them, from the edge records otherwise - so a weight always belongs to the neighbour it is
    // written against, whatever the edge-type filter and whichever backing answered (issue #6301).
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, weightProperty,
        relTypes);
    final int[][] adj = weighted.neighbors();
    final double[][] edgeWts = weighted.weights();
    for (int i = 0; i < n; i++) {
      guard.checkPeriodically(i);
      for (int j = 0; j < adj[i].length; j++) {
        final double w = edgeWts[i][j];
        if (w < dist[i][adj[i][j]])
          dist[i][adj[i][j]] = w;
      }
    }

    // Floyd-Warshall.
    //
    // The triple loop is O(V³) with no knob anywhere in it - the graph alone sizes it - and until issue #6302
    // nothing inside it could end a run: not a thread interrupt, not arcadedb.command.timeout, not a client
    // cancelling the query. The memory budget above caps the matrix at arcadedb.cypher.algoMaxWorkingMemory,
    // whose 64 MB floor admits n ≈ 2890, and 2890³ is ~2.4e10 iterations - minutes of CPU on one query thread,
    // on an input the budget explicitly accepts. The two halves of the guarantee have to agree: a call the
    // budget refuses fails immediately, so a call it admits must at least be abortable.
    //
    // The checkpoint sits on the `i` loop rather than the `k` loop: `k` runs only V times while its body is
    // O(V²), so a check per `k` leaves abort latency quadratic. One `i` iteration is O(V) writes, which is
    // already more than a flag test, so it needs no throttle either.
    for (int k = 0; k < n; k++) {
      for (int i = 0; i < n; i++) {
        guard.check();
        if (dist[i][k] >= INF)
          continue;  // Skip unreachable intermediate
        for (int j = 0; j < n; j++) {
          final double through = dist[i][k] + dist[k][j];
          if (through < dist[i][j])
            dist[i][j] = through;
        }
      }
    }

    // Emit the reachable pairs (i != j) lazily, one row at a time.
    //
    // The rows are the largest allocation this procedure makes, and until issue #6296 they were the one thing in
    // it that no budget saw: the matrix reserved above is n x n DOUBLES, the result is up to n² - n ROW OBJECTS,
    // each a ResultInternal with a three-entry property map. At the 64 MB floor of
    // arcadedb.cypher.algoMaxWorkingMemory the matrix check admits n ≈ 2890, and a connected graph of that size
    // then produced ~8.3M rows - well over a gigabyte, held all at once, against the 64 MB the budget had just
    // finished enforcing beside it. The call died of the allocation the budget was not looking at.
    //
    // Nothing required them to exist together: the matrix is complete before the first row is emitted, so the
    // rows are a pure projection of it. Streaming them makes the row-side footprint O(1) and hands the decision
    // of how many to hold to the consumer, which is where it belongs - CallStep keeps the iterator rather than
    // collecting it (see CallStep#executeProcedure), so a LIMIT upstream now costs what it says it costs.
    return IntStream.range(0, n).mapToObj(i -> {
      final double[] distances = dist[i];
      final RID source = graph.getRID(i);  // one lookup per SOURCE rather than one per pair
      return IntStream.range(0, n)
          // Identical to the eager loop's `if (i == j || dist[i][j] >= INF) continue`, NaN included: a NaN
          // distance would pass both forms of this test differently, but none can reach the matrix - every
          // write to it above, the edge fill and the Floyd-Warshall relaxation alike, is guarded by `<`,
          // which a NaN fails.
          .filter(j -> j != i && distances[j] < INF)
          .mapToObj(j -> {
            final ResultInternal r = new ResultInternal();
            r.setProperty("source", source);
            r.setProperty("target", graph.getRID(j));
            r.setProperty("distance", distances[j]);
            return (Result) r;
          });
    }).flatMap(Function.identity());
  }

}
