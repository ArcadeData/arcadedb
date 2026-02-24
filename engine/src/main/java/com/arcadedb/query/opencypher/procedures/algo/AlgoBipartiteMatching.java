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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.bipartiteMatching(relTypes?)
 * <p>
 * Computes a maximum bipartite matching on the graph using the Hopcroft-Karp algorithm
 * (O(E √V)). The graph is first 2-coloured via BFS to identify the two partitions; if the
 * graph is not bipartite the procedure returns an empty result.
 * </p>
 * <p>
 * Returns each matched pair as a row and a {@code matchingSize} equal to the total number of
 * matched edges.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.bipartiteMatching()
 * YIELD node1, node2, matchingSize
 * RETURN node1.name, node2.name, matchingSize
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoBipartiteMatching extends AbstractAlgoProcedure {
  public static final String NAME = "algo.bipartiteMatching";

  private static final int INF = Integer.MAX_VALUE / 2;
  private static final int NIL = 0; // sentinel for unmatched (arrays are 1-indexed)

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
    return "Maximum bipartite matching using Hopcroft-Karp algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node1", "node2", "matchingSize");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adj = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.BOTH, relTypes);

    // 2-colour the graph to identify bipartite partitions
    final int[] color = new int[n];
    Arrays.fill(color, -1);
    boolean bipartite = true;
    final int[] bfsQ = new int[n];

    for (int root = 0; root < n && bipartite; root++) {
      if (color[root] != -1)
        continue;
      color[root] = 0;
      int head = 0, tail = 0;
      bfsQ[tail++] = root;
      outer:
      while (head < tail) {
        final int v = bfsQ[head++];
        for (final int u : adj[v]) {
          if (color[u] == -1) {
            color[u] = 1 - color[v];
            bfsQ[tail++] = u;
          } else if (color[u] == color[v]) {
            bipartite = false;
            break outer;
          }
        }
      }
    }

    if (!bipartite)
      return Stream.empty();

    // Collect left (partition 0) and right (partition 1) nodes
    final int[] leftGlobal  = new int[n]; // leftGlobal[i] = global index of left node i+1
    final int[] rightGlobal = new int[n];
    final int[] leftOf      = new int[n]; // leftOf[global]  = 1-based left index (0 = not left)
    final int[] rightOf     = new int[n]; // rightOf[global] = 1-based right index (0 = not right)
    int L = 0, R = 0;

    for (int i = 0; i < n; i++) {
      if (color[i] == 0) {
        leftGlobal[L] = i;
        leftOf[i] = ++L;
      } else if (color[i] == 1) {
        rightGlobal[R] = i;
        rightOf[i] = ++R;
      }
    }

    // Hopcroft-Karp: pairU[u] / pairV[v] are 1-based; 0 = unmatched (NIL)
    final int[] pairU = new int[L + 1];
    final int[] pairV = new int[R + 1];
    final int[] dist  = new int[L + 1];

    int matching = 0;
    while (hopcroftBFS(pairU, pairV, dist, L, leftGlobal, rightOf, adj)) {
      for (int u = 1; u <= L; u++) {
        if (pairU[u] == NIL && hopcroftDFS(u, pairU, pairV, dist, leftGlobal, rightOf, adj))
          matching++;
      }
    }

    final List<Result> results = new ArrayList<>(matching);
    for (int u = 1; u <= L; u++) {
      if (pairU[u] != NIL) {
        final ResultInternal r = new ResultInternal();
        r.setProperty("node1", vertices.get(leftGlobal[u - 1]));
        r.setProperty("node2", vertices.get(rightGlobal[pairU[u] - 1]));
        r.setProperty("matchingSize", matching);
        results.add(r);
      }
    }
    return results.stream();
  }

  private boolean hopcroftBFS(final int[] pairU, final int[] pairV, final int[] dist,
      final int L, final int[] leftGlobal, final int[] rightOf, final int[][] adj) {
    final int[] queue = new int[L + 1];
    int head = 0, tail = 0;
    for (int u = 1; u <= L; u++) {
      if (pairU[u] == NIL) {
        dist[u] = 0;
        queue[tail++] = u;
      } else {
        dist[u] = INF;
      }
    }
    dist[NIL] = INF;
    while (head < tail) {
      final int u = queue[head++];
      if (dist[u] < dist[NIL]) {
        for (final int globalV : adj[leftGlobal[u - 1]]) {
          final int v = rightOf[globalV];
          if (v == 0) continue;
          final int w = pairV[v]; // left partner of right v (0 if unmatched)
          if (dist[w] == INF) {
            dist[w] = dist[u] + 1;
            if (w != NIL)
              queue[tail++] = w;
          }
        }
      }
    }
    return dist[NIL] != INF;
  }

  private boolean hopcroftDFS(final int u, final int[] pairU, final int[] pairV, final int[] dist,
      final int[] leftGlobal, final int[] rightOf, final int[][] adj) {
    if (u == NIL)
      return true;
    for (final int globalV : adj[leftGlobal[u - 1]]) {
      final int v = rightOf[globalV];
      if (v == 0) continue;
      final int w = pairV[v];
      if (dist[w] == dist[u] + 1 && hopcroftDFS(w, pairU, pairV, dist, leftGlobal, rightOf, adj)) {
        pairV[v] = u;
        pairU[u] = v;
        return true;
      }
    }
    dist[u] = INF;
    return false;
  }
}
