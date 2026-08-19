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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Stream;

/**
 * Procedure: algo.steinerTree(terminalNodes, relTypes?, weightProperty?)
 *
 * <p>Steiner Tree approximation (Kou, Markowsky &amp; Berman, 1981) with approximation
 * ratio 2(1 − 1/t) where t is the number of terminals. The algorithm:
 * <ol>
 *   <li>Runs Dijkstra from each terminal to all other reachable nodes.</li>
 *   <li>Constructs a complete distance graph on the terminal nodes.</li>
 *   <li>Finds the MST of that terminal distance graph (Kruskal's).</li>
 *   <li>Expands each MST edge back to its shortest path in the original graph.</li>
 *   <li>Prunes non-terminal leaves iteratively from the resulting subgraph.</li>
 * </ol>
 * Returns one row per edge of the Steiner tree subgraph.</p>
 *
 * <p>Example:
 * <pre>
 * MATCH (a:City), (b:City), (c:City)
 * WHERE a.name='Rome' AND b.name='Milan' AND c.name='Naples'
 * WITH [a,b,c] AS terminals
 * CALL algo.steinerTree(terminals, 'ROAD', 'distance')
 * YIELD source, target, weight, totalWeight
 * RETURN source.name, target.name, weight
 * </pre>
 * </p>
 *
 * <p>Working set: a {@code terminals x nodeCount} pair of Dijkstra tables plus one entry per terminal pair -
 * about {@code t²/2} of them - in five parallel primitive arrays, both reserved through
 * {@link AbstractAlgoProcedure.MemoryBudget} before anything is allocated. Quadratic in a list the caller
 * supplies and nothing bounds, which is why the pair count is also computed in {@code long}. The time it buys is
 * bounded the other way, by a {@link WorkGuard} checkpoint in each of the Dijkstra, Kruskal and pruning loops
 * (issue #6302).</p>
 *
 * <p>Edge weights are read through {@code GraphData.weightedAdjacency}, which produces the neighbour list and its
 * weights from one walk of the same edges. Deriving them from a second, independently ordered traversal is what
 * made a {@code relTypes} filter and the mere presence of a Graph Analytical View each change the tree this
 * procedure returned (issue #6301).</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoSteinerTree extends AbstractAlgoProcedure {

  @Override
  public String getName() {
    return "algo.steinerTree";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Steiner Tree 2-approximation (Kou-Markowsky-Berman): finds a minimum-weight subgraph connecting a given set of terminal nodes";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("source", "target", "weight", "totalWeight");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final List<Vertex> terminals = extractVertexList(args[0], "terminalNodes");
    final String[] relTypes = args.length > 1 ? extractRelTypes(args[1]) : null;
    final String weightProperty = args.length > 2 ? extractString(args[2], "weightProperty") : null;

    if (terminals.size() < 2)
      return Stream.empty();

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int t = terminals.size();

    // The terminal list is the knob here, and it is the one knob a caller supplies as data rather than as a
    // number: nothing bounds its length, and it need not even be bounded by the node count, since repeating the
    // same vertex is accepted. It sizes two working sets, both reserved before the adjacency build so that a
    // list too long to serve costs nothing beyond the reservation:
    //   - the Dijkstra tables, terminals x nodeCount of doubles plus the same of ints;
    //   - one entry per terminal PAIR, about t²/2 of them, in four parallel arrays.
    // The pair count is computed in long. In int, `t * (t - 1)` wraps just past 46341 terminals - the division
    // by 2 happens after the product, so the wrap is not avoided by the result fitting an int - and a negative
    // pairCount reached `new int[pairCount]` as a bare NegativeArraySizeException naming nothing.
    final long pairCount = (long) t * (t - 1) / 2;
    final MemoryBudget memory = graph.memory();
    memory.reserve(saturatingSum(matrixBytes(t, n, DOUBLE_BYTES), matrixBytes(t, n, INT_BYTES)),
        "the per-terminal Dijkstra tables", t + " terminals x " + n + " nodes");
    // Per pair: the endpoints pU/pV, the weight pW, and the two int arrays the index sort works over (the
    // order and the merge scratch). Four ints and a double, all primitive - which is what
    // sortedIndexesByWeight() is for: through Arrays.sort(Integer[], Comparator) the sort alone used to cost
    // 24 bytes per pair, more than everything else here put together (issue #6289).
    memory.reserve(saturatingProduct(pairCount, 4 * INT_BYTES + DOUBLE_BYTES),
        "the terminal-pair arrays", pairCount + " pairs over " + t + " terminals");
    if (pairCount > Integer.MAX_VALUE)
      throw new IllegalArgumentException(getName() + "(): " + t + " terminalNodes make " + pairCount
          + " terminal pairs, more than the " + Integer.MAX_VALUE + " entries a Java array can hold");

    // Build weighted adjacency lists (undirected for shortest paths). Neighbours and weights come out of one
    // walk of the same edges, which is what keeps `adjW[i][j]` the weight of the edge to `adj[i][j]`: the
    // hand-rolled second traversal this replaced filled the weights positionally from an unfiltered
    // getEdges(BOTH) walk and attached them to whatever neighbour happened to sit at that index, so a relTypes
    // filter or a Graph Analytical View each silently changed the tree Dijkstra went on to find (issue #6301).
    final GraphData.WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.BOTH, weightProperty,
        relTypes);
    final int[][] adj = weighted.neighbors();
    final double[][] adjW = weighted.weights();

    // Map terminals to their indices
    final int[] termIdx = new int[t];
    for (int i = 0; i < t; i++) {
      final int idx = graph.indexOf(terminals.get(i).getIdentity());
      if (idx < 0)
        return Stream.empty();
      termIdx[i] = idx;
    }

    // ── Step 1: Dijkstra from each terminal ───────────────────────────────
    final double[][] dist = new double[t][n]; // dist[ti][v] = shortest path from terminal ti to v
    final int[][] prev = new int[t][n];       // prev[ti][v] = predecessor of v in Dijkstra from ti
    for (int ti = 0; ti < t; ti++) {
      // One Dijkstra per terminal over the whole graph: O(t x (V + E) log V), sized by a terminal list the
      // caller supplies and by the graph, with no knob to cap either (issue #6302).
      guard.check();
      Arrays.fill(dist[ti], Double.MAX_VALUE);
      Arrays.fill(prev[ti], -1);
      dijkstra(termIdx[ti], adj, adjW, dist[ti], prev[ti], guard);
    }

    // ── Step 2: Build complete graph on terminals, run MST (Kruskal's) ────
    // Number of terminal pairs, computed in long and bounded above.
    final int pairs = (int) pairCount;
    final int[] pU = new int[pairs];
    final int[] pV = new int[pairs];
    final double[] pW = new double[pairs];
    int pi = 0;
    for (int i = 0; i < t; i++)
      for (int j = i + 1; j < t; j++) {
        pU[pi] = i;
        pV[pi] = j;
        pW[pi] = dist[i][termIdx[j]];
        pi++;
      }

    // Sort by weight for Kruskal's, over primitive indices: `pairs` is quadratic in a terminal list the caller
    // supplies, so a boxed Integer[] here is ~2M objects at 2000 terminals.
    final int[] sortIdx = sortedIndexesByWeight(pW, pairs);

    // Union-Find on terminal indices
    final int[] parent = new int[t];
    final int[] rank = new int[t];
    for (int i = 0; i < t; i++)
      parent[i] = i;

    // Kruskal's on terminal graph
    final int[] mstU = new int[t - 1];
    final int[] mstV = new int[t - 1];
    int mstSize = 0;
    for (int k = 0; k < pairs && mstSize < t - 1; k++) {
      // `pairs` is quadratic in the terminal list, and one iteration is two union-find finds - small enough
      // that the check is throttled rather than paid per pair.
      guard.checkPeriodically(k);
      final int idx = sortIdx[k];
      if (pW[idx] == Double.MAX_VALUE)
        continue; // unreachable pair
      final int ru = find(parent, pU[idx]);
      final int rv = find(parent, pV[idx]);
      if (ru != rv) {
        union(parent, rank, ru, rv);
        mstU[mstSize] = pU[idx];
        mstV[mstSize] = pV[idx];
        mstSize++;
      }
    }

    // ── Step 3: Expand each MST edge to its shortest path ─────────────────
    // Collect all edges of the Steiner subgraph (using boolean edge set)
    final boolean[][] steinerEdges = new boolean[n][]; // steinerEdges[i] per adj slot
    for (int i = 0; i < n; i++)
      steinerEdges[i] = new boolean[adj[i].length];

    final boolean[] steinerNodes = new boolean[n];
    for (int mi = 0; mi < mstSize; mi++) {
      // Expand path from terminal mstU[mi] to terminal mstV[mi]
      final int srcTerm = mstU[mi];
      final int dstTerm = mstV[mi];
      int cur = termIdx[dstTerm];
      while (cur != termIdx[srcTerm] && prev[srcTerm][cur] != -1) {
        final int p = prev[srcTerm][cur];
        steinerNodes[p] = true;
        steinerNodes[cur] = true;
        // Mark edge (p, cur) in steiner edges
        markEdge(adj, steinerEdges, p, cur);
        markEdge(adj, steinerEdges, cur, p);
        cur = p;
      }
      steinerNodes[termIdx[srcTerm]] = true;
      steinerNodes[termIdx[dstTerm]] = true;
    }

    // ── Step 4: Prune non-terminal leaves ─────────────────────────────────
    // Build steiner degree
    final int[] steinerDeg = new int[n];
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < adj[i].length; j++)
        if (steinerEdges[i][j])
          steinerDeg[i]++;
    }

    // Iteratively remove non-terminal leaves
    boolean changed = true;
    while (changed) {
      // Leaf pruning repeats until a whole pass changes nothing, so it is O(V x (V + E)) in the worst case -
      // one more graph-driven loop with nothing to bound it.
      guard.check();
      changed = false;
      for (int i = 0; i < n; i++) {
        guard.checkPeriodically(i);
        if (!steinerNodes[i] || isTerminal(i, termIdx))
          continue;
        if (steinerDeg[i] == 1) {
          // Remove leaf i
          steinerNodes[i] = false;
          changed = true;
          for (int j = 0; j < adj[i].length; j++) {
            if (steinerEdges[i][j]) {
              steinerEdges[i][j] = false;
              steinerDeg[i]--;
              final int nb = adj[i][j];
              // Remove reverse edge
              for (int k = 0; k < adj[nb].length; k++) {
                if (adj[nb][k] == i && steinerEdges[nb][k]) {
                  steinerEdges[nb][k] = false;
                  steinerDeg[nb]--;
                  break;
                }
              }
            }
          }
        }
      }
    }

    // ── Step 5: Collect result edges ──────────────────────────────────────
    double total = 0.0;
    final List<int[]> edgeList = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < adj[i].length; j++) {
        if (steinerEdges[i][j] && adj[i][j] > i) { // each edge once
          edgeList.add(new int[]{ i, adj[i][j], j });
          total += adjW[i][j];
        }
      }
    }
    if (edgeList.isEmpty())
      return Stream.empty();

    final double totalWeight = total;
    final List<Result> results = new ArrayList<>(edgeList.size());
    for (final int[] e : edgeList) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("source", graph.getRID(e[0]));
      r.setProperty("target", graph.getRID(e[1]));
      r.setProperty("weight", adjW[e[0]][e[2]]);
      r.setProperty("totalWeight", totalWeight);
      results.add(r);
    }
    return results.stream();
  }

  // ── Dijkstra ─────────────────────────────────────────────────────────────

  private static void dijkstra(final int src, final int[][] adj, final double[][] adjW,
      final double[] dist, final int[] prev, final WorkGuard guard) {
    dist[src] = 0.0;
    // PriorityQueue entry: [cost, node]
    final PriorityQueue<double[]> pq = new PriorityQueue<>(Comparator.comparingDouble(e -> e[0]));
    pq.offer(new double[]{ 0.0, src });
    while (!pq.isEmpty()) {
      // Settling one node costs its degree, which on a supernode is the whole edge list.
      guard.check();
      final double[] top = pq.poll();
      final double d = top[0];
      final int u = (int) top[1];
      if (d > dist[u])
        continue;
      for (int j = 0; j < adj[u].length; j++) {
        final int v = adj[u][j];
        final double nd = dist[u] + adjW[u][j];
        if (nd < dist[v]) {
          dist[v] = nd;
          prev[v] = u;
          pq.offer(new double[]{ nd, v });
        }
      }
    }
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static int find(final int[] parent, int x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  }

  private static void union(final int[] parent, final int[] rank, final int a, final int b) {
    if (rank[a] < rank[b])
      parent[a] = b;
    else if (rank[a] > rank[b])
      parent[b] = a;
    else {
      parent[b] = a;
      rank[a]++;
    }
  }

  private static boolean isTerminal(final int node, final int[] termIdx) {
    for (final int t : termIdx)
      if (t == node)
        return true;
    return false;
  }

  private static void markEdge(final int[][] adj, final boolean[][] steinerEdges,
      final int from, final int to) {
    for (int j = 0; j < adj[from].length; j++) {
      if (adj[from][j] == to) {
        steinerEdges[from][j] = true;
        return;
      }
    }
  }
}
