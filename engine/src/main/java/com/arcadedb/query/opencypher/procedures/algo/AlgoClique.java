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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.clique(relTypes?, minSize?)
 * <p>
 * Finds all maximal cliques in the graph using the Bron-Kerbosch algorithm with
 * Tomita pivoting. The algorithm is implemented iteratively using an explicit stack
 * to avoid StackOverflow on large graphs.
 * </p>
 * <p>
 * Candidate and excluded sets are sorted arrays of node ids, subsets of one node's neighbours.
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

  /**
   * Heap cost of a {@code StackFrame} object and the deque slot holding it, on top of the arrays it references.
   */
  private static final long STACK_FRAME_OVERHEAD_BYTES = 64L;

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

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);
    final int minSize       = extractInt(optionalNumberArg(args, 1, "minSize", 3), "minSize");

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    // The undirected simple graph: OUT and IN edges merged, as sorted neighbour sets without self-loops or repeats. Every
    // set the search handles is a subset of one of them, so it needs memory linear in the graph, not an n x n bit matrix
    // and n-bit sets per frame (issue #12173).
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    toSimpleNeighbourSets(adj, guard);
    int maxDegree = 0;
    for (final int[] row : adj)
      maxDegree = Math.max(maxDegree, row.length);

    final List<Result> results = new ArrayList<>();

    // Iterative Bron-Kerbosch with pivoting. The root frame has P = every node and X = {}: its pivot is the first node
    // of highest degree, its candidates the nodes not adjacent to it in ascending order, and as each is expanded it
    // moves from P to X. That frame is kept as a flag per node rather than as two sets of n nodes.
    graph.memory().reserve(saturatingProduct(n, BOOLEAN_BYTES), "the expanded flags of the root frame", n + " nodes");
    final boolean[] expanded = new boolean[n];
    int pivot = -1;
    for (int u = 0; u < n; u++)
      if (pivot < 0 || adj[u].length > adj[pivot].length)
        pivot = u;

    // Below the root a frame holds P, X and the candidates, each a subset of the neighbours of the node that opened
    // it, so at most maxDegree nodes each; the stack is as deep as the largest clique the search has reached. Priced
    // at the peak rather than per push: a frame is popped, and the budget never releases (issue #6375).
    final long frameBytes = saturatingSum(saturatingProduct(4L * maxDegree + 1, INT_BYTES), STACK_FRAME_OVERHEAD_BYTES);
    int deepest = 0;
    final Deque<StackFrame> stack = new ArrayDeque<>();
    int[] clique = new int[16];
    int step = 0;

    final int[] pivotNeighbors = adj[pivot];
    int skip = 0;
    for (int v = 0; v < n; v++) {
      // Candidates of the root = P \ N(pivot)
      if (skip < pivotNeighbors.length && pivotNeighbors[skip] == v) {
        skip++;
        continue;
      }

      // New P = P ∩ N(v) and new X = X ∩ N(v), P being the nodes not expanded yet and X the ones expanded
      final int[] neighbors = adj[v];
      int pSize = 0;
      for (final int w : neighbors)
        if (!expanded[w])
          pSize++;
      final int[] newP = new int[pSize];
      final int[] newX = new int[neighbors.length - pSize];
      int p = 0;
      int x = 0;
      for (final int w : neighbors) {
        if (expanded[w])
          newX[x++] = w;
        else
          newP[p++] = w;
      }
      expanded[v] = true;
      clique[0] = v;
      stack.push(new StackFrame(1, newP, newP.length, newX, newX.length));

      while (!stack.isEmpty()) {
        if (stack.size() > deepest) {
          graph.memory().reserve(saturatingProduct(stack.size() - deepest, frameBytes), "the clique search stack",
              stack.size() + " frames of up to " + maxDegree + " nodes");
          deepest = stack.size();
        }
        // Maximal-clique enumeration is exponential in the graph in the worst case, and neither `minSize` nor anything
        // else bounds the search - minSize only filters what is reported (issue #6302).
        guard.checkPeriodically(step++);
        final StackFrame frame = stack.peek();

        if (frame.pSize == 0 && frame.xSize == 0) {
          // Maximal clique found in frame.R
          final int size = frame.depth;
          if (size >= minSize) {
            final int[] members = Arrays.copyOf(clique, size);
            Arrays.sort(members);
            final List<RID> rids = new ArrayList<>(size);
            for (final int member : members)
              rids.add(graph.getRID(member));
            final ResultInternal r = new ResultInternal();
            r.setProperty("clique", rids);
            r.setProperty("size", size);
            results.add(r);
          }
          stack.pop();
          continue;
        }

        if (frame.candidates == null)
          frame.chooseCandidates(adj);

        if (frame.nextCandidate == frame.candidates.length) {
          stack.pop();
          continue;
        }
        final int candidate = frame.candidates[frame.nextCandidate++];

        // New R = frame.R ∪ {candidate}, New P = frame.P ∩ N(candidate), New X = frame.X ∩ N(candidate)
        if (frame.depth == clique.length)
          clique = Arrays.copyOf(clique, clique.length * 2);
        clique[frame.depth] = candidate;
        final int[] childP = intersect(frame.P, frame.pSize, adj[candidate]);
        final int[] childX = intersect(frame.X, frame.xSize, adj[candidate]);

        // Update current frame: remove the candidate from P, add it to X
        frame.moveToX(candidate);

        stack.push(new StackFrame(frame.depth + 1, childP, childP.length, childX, childX.length));
      }
    }

    return results.stream();
  }

  /** The ids of the first {@code size} entries of {@code set} that are also in {@code row}, both sorted, in order. */
  private static int[] intersect(final int[] set, final int size, final int[] row) {
    final int[] out = new int[Math.min(size, row.length)];
    final int count = size <= row.length ?
        intersectInto(set, size, row, row.length, out) :
        intersectInto(row, row.length, set, size, out);
    return count == out.length ? out : Arrays.copyOf(out, count);
  }

  /** The size of the intersection of two sorted sets. */
  private static int intersectionSize(final int[] a, final int aSize, final int[] b, final int bSize) {
    return aSize <= bSize ? intersectInto(a, aSize, b, bSize, null) : intersectInto(b, bSize, a, aSize, null);
  }

  /**
   * Walks the smaller sorted set and looks each id up in the larger one, writing the common ids in order to {@code out}
   * when it is not null: O(small x log(large)), which a node of high degree next to a small set needs.
   */
  private static int intersectInto(final int[] small, final int smallSize, final int[] large, final int largeSize,
      final int[] out) {
    int count = 0;
    int from = 0;
    for (int i = 0; i < smallSize && from < largeSize; i++) {
      final int found = Arrays.binarySearch(large, from, largeSize, small[i]);
      if (found >= 0) {
        if (out != null)
          out[count] = small[i];
        count++;
        from = found + 1;
      } else
        from = -found - 1;
    }
    return count;
  }

  /** One Bron-Kerbosch frame below the root: R is the first {@code depth} entries of the shared clique path. */
  private static final class StackFrame {
    final int depth;
    final int[] P;
    int pSize;
    int[] X;
    int xSize;
    int[] candidates;
    int nextCandidate;

    StackFrame(final int depth, final int[] p, final int pSize, final int[] x, final int xSize) {
      this.depth = depth;
      this.P = p;
      this.pSize = pSize;
      this.X = x;
      this.xSize = xSize;
    }

    /**
     * Picks the pivot u in P ∪ X that maximizes |N(u) ∩ P|, the first in ascending order among equals, and takes
     * P \ N(pivot) as the candidates, in ascending order.
     */
    void chooseCandidates(final int[][] adj) {
      int pivot = -1;
      int maxNeighbors = -1;
      int p = 0;
      int x = 0;
      while (p < pSize || x < xSize) {
        final int u = x == xSize || (p < pSize && P[p] < X[x]) ? P[p++] : X[x++];
        final int count = intersectionSize(adj[u], adj[u].length, P, pSize);
        if (count > maxNeighbors) {
          maxNeighbors = count;
          pivot = u;
        }
      }
      final int[] pivotNeighbors = adj[pivot];
      final int[] out = new int[pSize];
      int count = 0;
      int from = 0;
      for (int i = 0; i < pSize; i++) {
        final int found = Arrays.binarySearch(pivotNeighbors, from, pivotNeighbors.length, P[i]);
        if (found >= 0)
          from = found + 1;
        else {
          from = -found - 1;
          out[count++] = P[i];
        }
      }
      candidates = count == pSize ? out : Arrays.copyOf(out, count);
      // X gains one node per candidate
      X = Arrays.copyOf(X, xSize + count);
    }

    /** Removes {@code v}, which is in P, from P and adds it to X, keeping both sorted. */
    void moveToX(final int v) {
      final int inP = Arrays.binarySearch(P, 0, pSize, v);
      System.arraycopy(P, inP + 1, P, inP, pSize - inP - 1);
      pSize--;
      final int inX = -Arrays.binarySearch(X, 0, xSize, v) - 1;
      System.arraycopy(X, inX, X, inX + 1, xSize - inX);
      X[inX] = v;
      xSize++;
    }
  }
}
