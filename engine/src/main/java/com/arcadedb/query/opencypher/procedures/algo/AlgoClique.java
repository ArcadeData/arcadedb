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
 * One search per node in degeneracy order (Eppstein, Loffler and Strash), over sorted neighbour
 * sets: the candidate and excluded sets are sorted id arrays bounded by the starting node's
 * neighbourhood, so the memory is linear in the graph and a set operation costs the sizes of the
 * sets, not the node count (issue #12173). The graph is read as undirected and simple.
 * Returns one result row per maximal clique with size >= minSize, its members in node order.
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
   * Heap cost of a {@code Frame} object and the deque slot holding it, on top of the id arrays it references.
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

    // The graph as undirected and simple: sorted neighbour sets, OUT and IN merged, no self-loop, no repeated neighbour (the
    // representation algo.triangleCount reads), O(V + E) where the bit matrix was O(V^2) (issue #12173)
    final int[][] adj = graph.adjacency(Vertex.DIRECTION.BOTH, relTypes);
    toSimpleNeighbourSets(adj, guard);

    // Eppstein, Loffler and Strash: one search per node in degeneracy order, over its neighbours peeled after it (the candidates)
    // and before it (the excluded). Every maximal clique is found exactly once, from its earliest member, and each search starts
    // from at most degeneracy-many candidates however large the graph
    final int[] order = degeneracyOrder(adj, null, graph.memory());
    graph.memory().reserve(saturatingProduct(n, INT_BYTES), "the degeneracy ranks", n + " nodes");
    final int[] rank = new int[n];
    for (int i = 0; i < n; i++)
      rank[order[i]] = i;

    final CliqueSearch search = new CliqueSearch(graph, adj, minSize, guard);
    for (final int v : order) {
      guard.check();
      final int[] row = adj[v];
      final int[] p = new int[row.length];
      final int[] x = new int[row.length];
      int pLen = 0;
      int xLen = 0;
      for (final int w : row)
        if (rank[w] > rank[v])
          p[pLen++] = w;
        else
          x[xLen++] = w;
      search.run(v, p, pLen, x, xLen);
    }
    return search.results.stream();
  }

  /**
   * Bron-Kerbosch with Tomita pivoting from one node, iterative over an explicit stack so that a deep clique cannot overflow the
   * thread's stack. A frame holds its candidate set P and excluded set X as sorted node ids, never more than the starting node's
   * neighbourhood, and the members R of the clique being grown are a stack shared by the frames. The footprint of the live frames is
   * charged to the call's budget at its peak, as a reservation is never given back (issue #6375).
   */
  private final class CliqueSearch {
    private final GraphData    graph;
    private final int[][]      adj;
    private final int          minSize;
    private final WorkGuard    guard;
    private final List<Result> results = new ArrayList<>();
    private final Deque<Frame> stack   = new ArrayDeque<>();
    private       int[]        members = new int[16];
    private       int          size;
    private       long         liveBytes;
    private       long         peakBytes;
    private       int          steps;

    private CliqueSearch(final GraphData graph, final int[][] adj, final int minSize, final WorkGuard guard) {
      this.graph = graph;
      this.adj = adj;
      this.minSize = minSize;
      this.guard = guard;
    }

    private void run(final int start, final int[] p, final int pLen, final int[] x, final int xLen) {
      size = 0;
      push(start, p, pLen, x, xLen);
      while (!stack.isEmpty()) {
        // Maximal-clique enumeration is exponential in the worst case, and minSize only filters what is reported (issue #6302)
        guard.checkPeriodically(steps++);
        final Frame frame = stack.peek();

        if (frame.candidates == null) {
          if (frame.pLen == 0) {
            if (frame.xLen == 0)
              report();
            pop();
            continue;
          }
          // Pivot u in P ∪ X with the most neighbours in P: only the candidates outside N(u) need a branch of their own
          int pivot = -1;
          int best = -1;
          int ip = 0;
          int ix = 0;
          while (ip < frame.pLen || ix < frame.xLen) {
            final int u = ix >= frame.xLen || (ip < frame.pLen && frame.p[ip] < frame.x[ix]) ? frame.p[ip++] : frame.x[ix++];
            final int count = countCommon(frame.p, frame.pLen, adj[u]);
            if (count > best) {
              best = count;
              pivot = u;
            }
          }
          frame.candidates = difference(frame.p, frame.pLen, adj[pivot]);
          grow(frame, (long) frame.candidates.length * INT_BYTES);
        }

        if (frame.next == frame.candidates.length) {
          pop();
          continue;
        }
        final int v = frame.candidates[frame.next++];
        final int[] nv = adj[v];
        final int[] childP = new int[Math.min(frame.pLen, nv.length)];
        final int childPLen = intersect(frame.p, frame.pLen, nv, childP);
        final int[] childX = new int[Math.min(frame.xLen, nv.length) + childPLen];
        final int childXLen = intersect(frame.x, frame.xLen, nv, childX);

        // v leaves P for X: the branches after this one must not grow a clique this branch grows
        frame.pLen = remove(frame.p, frame.pLen, v);
        frame.xLen = insert(frame, v);

        push(v, childP, childPLen, childX, childXLen);
      }
    }

    private void report() {
      if (size < minSize)
        return;
      final int[] sorted = Arrays.copyOf(members, size);
      Arrays.sort(sorted);
      final List<RID> clique = new ArrayList<>(size);
      for (final int node : sorted)
        clique.add(graph.getRID(node));
      final ResultInternal r = new ResultInternal();
      r.setProperty("clique", clique);
      r.setProperty("size", size);
      results.add(r);
    }

    private void push(final int member, final int[] p, final int pLen, final int[] x, final int xLen) {
      if (size == members.length)
        members = Arrays.copyOf(members, size * 2);
      members[size++] = member;
      final Frame frame = new Frame(p, x);
      frame.pLen = pLen;
      frame.xLen = xLen;
      stack.push(frame);
      grow(frame, STACK_FRAME_OVERHEAD_BYTES + (long) (p.length + x.length) * INT_BYTES);
    }

    /** Charges {@code bytes} more held by {@code frame}, reserving whatever takes the live frames past their peak. */
    private void grow(final Frame frame, final long bytes) {
      frame.bytes += bytes;
      liveBytes += bytes;
      if (liveBytes > peakBytes) {
        graph.memory().reserve(liveBytes - peakBytes, "the clique search stack",
            stack.size() + " frames, " + liveBytes + " bytes of candidate sets");
        peakBytes = liveBytes;
      }
    }

    private void pop() {
      final Frame frame = stack.pop();
      liveBytes -= frame.bytes;
      size--;
    }

    /** Adds {@code v} to the sorted excluded set of {@code frame}, growing it when full; returns the new length. */
    private int insert(final Frame frame, final int v) {
      int at = Arrays.binarySearch(frame.x, 0, frame.xLen, v);
      if (at >= 0)
        return frame.xLen;
      at = -at - 1;
      if (frame.xLen == frame.x.length) {
        // Not reached from a frame sized as run() sizes them (X has room for every candidate of P), kept for safety
        final int[] grown = Arrays.copyOf(frame.x, Math.max(4, frame.xLen * 2));
        grow(frame, (long) (grown.length - frame.x.length) * INT_BYTES);
        frame.x = grown;
      }
      System.arraycopy(frame.x, at, frame.x, at + 1, frame.xLen - at);
      frame.x[at] = v;
      return frame.xLen + 1;
    }
  }

  /** Removes {@code v} from the sorted {@code set} of {@code len} ids; returns the new length. */
  private static int remove(final int[] set, final int len, final int v) {
    final int at = Arrays.binarySearch(set, 0, len, v);
    if (at < 0)
      return len;
    System.arraycopy(set, at + 1, set, at, len - at - 1);
    return len - 1;
  }

  /**
   * Writes the ids of the sorted {@code set} (its first {@code len}) that the sorted {@code row} also holds into {@code out}; returns
   * how many. A merge when the two are of a size, a binary search per id when the row is much longer (a hub's row against a small
   * candidate set).
   */
  private static int intersect(final int[] set, final int len, final int[] row, final int[] out) {
    int count = 0;
    if ((long) len * 8 < row.length) {
      for (int i = 0; i < len; i++)
        if (Arrays.binarySearch(row, set[i]) >= 0)
          out[count++] = set[i];
      return count;
    }
    int i = 0;
    int j = 0;
    while (i < len && j < row.length) {
      final int a = set[i];
      final int b = row[j];
      if (a < b)
        i++;
      else if (a > b)
        j++;
      else {
        out[count++] = a;
        i++;
        j++;
      }
    }
    return count;
  }

  /** How many ids of the sorted {@code set} (its first {@code len}) the sorted {@code row} also holds. */
  private static int countCommon(final int[] set, final int len, final int[] row) {
    int count = 0;
    if ((long) len * 8 < row.length) {
      for (int i = 0; i < len; i++)
        if (Arrays.binarySearch(row, set[i]) >= 0)
          count++;
      return count;
    }
    int i = 0;
    int j = 0;
    while (i < len && j < row.length) {
      final int a = set[i];
      final int b = row[j];
      if (a < b)
        i++;
      else if (a > b)
        j++;
      else {
        count++;
        i++;
        j++;
      }
    }
    return count;
  }

  /** The ids of the sorted {@code set} (its first {@code len}) that the sorted {@code row} does not hold, sorted. */
  private static int[] difference(final int[] set, final int len, final int[] row) {
    final int[] out = new int[len];
    int count = 0;
    if ((long) len * 8 < row.length) {
      // A few candidates against a long row (a hub as pivot): a binary search each instead of a walk of the whole row
      for (int i = 0; i < len; i++)
        if (Arrays.binarySearch(row, set[i]) < 0)
          out[count++] = set[i];
      return count == len ? out : Arrays.copyOf(out, count);
    }
    int j = 0;
    for (int i = 0; i < len; i++) {
      final int a = set[i];
      while (j < row.length && row[j] < a)
        j++;
      if (j == row.length || row[j] != a)
        out[count++] = a;
    }
    return count == len ? out : Arrays.copyOf(out, count);
  }

  private static final class Frame {
    private final int[] p;
    private       int   pLen;
    private       int[] x;
    private       int   xLen;
    private       int[] candidates;
    private       int   next;
    private       long  bytes;

    private Frame(final int[] p, final int[] x) {
      this.p = p;
      this.x = x;
    }
  }
}
