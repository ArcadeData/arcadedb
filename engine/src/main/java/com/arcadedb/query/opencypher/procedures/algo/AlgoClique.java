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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.clique(relTypes?, minSize?)
 * <p>
 * Finds all maximal cliques in the graph using the Bron-Kerbosch algorithm with
 * Tomita pivoting. The algorithm is implemented iteratively using an explicit stack
 * to avoid StackOverflow on large graphs.
 * </p>
 * <p>
 * Uses BitSet for candidate and excluded sets to minimize object allocation.
 * Returns one result row per maximal clique with size >= minSize.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.clique('KNOWS', 3)
 * YIELD clique, size
 * RETURN size, clique ORDER BY size DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoClique extends AbstractAlgoProcedure {
  public static final String NAME = "algo.clique";

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
    return "Finds all maximal cliques using Bron-Kerbosch algorithm with Tomita pivoting";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("clique", "size");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final int minSize       = args.length > 1 ? ((Number) args[1]).intValue() : 3;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);

    // Build undirected adjacency as BitSet[] for fast set operations
    // Treat graph as undirected: merge OUT and IN edges
    final int[][] adjOut = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);
    final int[][] adjIn  = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.IN,  relTypes);

    final BitSet[] adj = new BitSet[n];
    for (int i = 0; i < n; i++) {
      adj[i] = new BitSet(n);
      for (final int j : adjOut[i])
        adj[i].set(j);
      for (final int j : adjIn[i])
        adj[i].set(j);
      adj[i].clear(i); // no self-loops
    }

    final List<Result> results = new ArrayList<>();

    // Iterative Bron-Kerbosch with pivoting
    // Stack frames: (R as BitSet, P as BitSet, X as BitSet)
    // We use a Deque of StackFrame objects
    final Deque<StackFrame> stack = new ArrayDeque<>();

    final BitSet initialP = new BitSet(n);
    initialP.set(0, n);
    final BitSet initialR = new BitSet(n);
    final BitSet initialX = new BitSet(n);

    stack.push(new StackFrame(initialR, initialP, initialX));

    while (!stack.isEmpty()) {
      final StackFrame frame = stack.peek();

      if (frame.P.isEmpty() && frame.X.isEmpty()) {
        // Maximal clique found in frame.R
        final int size = frame.R.cardinality();
        if (size >= minSize) {
          final List<RID> clique = new ArrayList<>(size);
          for (int i = frame.R.nextSetBit(0); i >= 0; i = frame.R.nextSetBit(i + 1))
            clique.add(vertices.get(i).getIdentity());
          final ResultInternal r = new ResultInternal();
          r.setProperty("clique", clique);
          r.setProperty("size", size);
          results.add(r);
        }
        stack.pop();
        continue;
      }

      if (frame.nextVertex < 0) {
        // Choose pivot u from P ∪ X that maximizes |N(u) ∩ P|
        final BitSet pUnionX = (BitSet) frame.P.clone();
        pUnionX.or(frame.X);
        int pivot = -1;
        int maxNeighbors = -1;
        for (int u = pUnionX.nextSetBit(0); u >= 0; u = pUnionX.nextSetBit(u + 1)) {
          final BitSet intersect = (BitSet) adj[u].clone();
          intersect.and(frame.P);
          final int cnt = intersect.cardinality();
          if (cnt > maxNeighbors) {
            maxNeighbors = cnt;
            pivot = u;
          }
        }
        frame.pivot = pivot;
        // Candidates = P \ N(pivot)
        final BitSet candidates = (BitSet) frame.P.clone();
        if (pivot >= 0)
          candidates.andNot(adj[pivot]);
        frame.candidates = candidates;
        frame.nextVertex = candidates.nextSetBit(0);
      }

      final int v = frame.nextVertex;
      if (v < 0) {
        stack.pop();
        continue;
      }

      // Advance to next candidate for this frame
      frame.nextVertex = frame.candidates.nextSetBit(v + 1);

      // New R = frame.R ∪ {v}
      final BitSet newR = (BitSet) frame.R.clone();
      newR.set(v);

      // New P = frame.P ∩ N(v)
      final BitSet newP = (BitSet) frame.P.clone();
      newP.and(adj[v]);

      // New X = frame.X ∩ N(v)
      final BitSet newX = (BitSet) frame.X.clone();
      newX.and(adj[v]);

      // Update current frame: remove v from P, add to X
      frame.P.clear(v);
      frame.X.set(v);

      stack.push(new StackFrame(newR, newP, newX));
    }

    return results.stream();
  }

  private static final class StackFrame {
    BitSet R;
    BitSet P;
    BitSet X;
    BitSet candidates;
    int    pivot      = -1;
    int    nextVertex = -1;

    StackFrame(final BitSet r, final BitSet p, final BitSet x) {
      this.R = r;
      this.P = p;
      this.X = x;
    }
  }
}
