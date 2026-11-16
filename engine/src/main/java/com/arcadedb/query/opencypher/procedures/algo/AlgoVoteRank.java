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
import com.arcadedb.utility.NumberUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.voteRank(relTypes?, topK?)
 * <p>
 * Identifies the most influential spreaders in a network using the VoteRank algorithm.
 * Nodes are elected iteratively: the node with the highest vote score is elected as a
 * spreader, and its neighbors have their voting ability reduced by 1/degree.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.voteRank('KNOWS', 10)
 * YIELD nodeId, rank
 * RETURN nodeId, rank ORDER BY rank ASC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoVoteRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.voteRank";

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
    return "Identifies the most influential spreaders using the VoteRank algorithm";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "rank");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = extractRelTypes(argOrDefault(args, 0, null), context);
    final int topK          = NumberUtils.saturateToInt(optionalNumberArg(args, 1, "topK", Integer.MAX_VALUE));

    final Database db = context.getDatabase();
    final WorkGuard guard = newWorkGuard(context);

    final GraphData graph = loadGraph(db, null, relTypes, context);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();
    final int[][] adjOut = graph.adjacency(Vertex.DIRECTION.OUT, relTypes);

    // The in-neighbours of every node, each listed once per edge in ascending order: summing a node's votes over them
    // adds the same terms in the same order as a pass over the out-lists of every node, so a score recomputed for one
    // node is the score a full recomputation would give it, to the last bit.
    long entries = 0;
    final int[] inDegree = new int[n];
    for (int i = 0; i < n; i++) {
      entries += adjOut[i].length;
      for (final int j : adjOut[i])
        inDegree[j]++;
    }
    graph.memory().reserve(saturatingSum(saturatingProduct(n, MATRIX_ROW_OVERHEAD_BYTES), saturatingProduct(entries, INT_BYTES)),
        "the incoming adjacency list", n + " nodes, " + entries + " edge entries");
    final int[][] adjIn = new int[n][];
    for (int i = 0; i < n; i++)
      adjIn[i] = new int[inDegree[i]];
    final int[] fill = new int[n];
    for (int u = 0; u < n; u++)
      for (final int v : adjOut[u])
        adjIn[v][fill[v]++] = u;

    // voteScore[i] = initial votes = in-degree[i]
    final double[] voteScore    = new double[n];
    final double[] votingAbility = new double[n];
    for (int i = 0; i < n; i++) {
      voteScore[i]     = inDegree[i];
      votingAbility[i] = 1.0;
    }

    final boolean[] elected = new boolean[n];
    final int limit = Math.min(topK, n);
    final int[] electedOrder = new int[limit];
    int electedCount = 0;

    // The next node to elect is the one of highest score, the lowest index among equals, which is the node a scan of
    // every node picked: it comes from a heap of (score, index) entries (issue #12170).
    graph.memory().reserve(saturatingProduct(n, DOUBLE_BYTES + INT_BYTES + BOOLEAN_BYTES), "the election heap",
        n + " nodes");
    final ScoreMaxHeap heap = new ScoreMaxHeap(n);
    for (int i = 0; i < n; i++)
      heap.push(voteScore[i], i);
    // An election lowers some scores. Their nodes are only marked here; a marked node's score is summed again when its
    // entry reaches the top of the heap. A score never rises, so the entry of a marked node is an upper bound of its
    // score, and a node popped unmarked is the one a scan of the exact scores would pick. A node is summed
    // again once per time it surfaces, not once per election that touches it: many elections can lower the score of a
    // node with a large fan-in before it is read again. Every node not elected has exactly one entry (a summed node's
    // entry is popped before its new one is pushed), so the heap never holds more than n entries.
    final boolean[] stale = new boolean[n];

    while (electedCount < limit) {
      guard.checkPeriodically(electedCount);
      // Find node with highest vote score among non-elected
      int best = -1;
      double bestScore = -1.0;
      int step = 0;
      while (heap.size() > 0) {
        guard.checkPeriodically(step++);
        final double score = heap.topScore();
        final int node = heap.pop();
        if (stale[node]) {
          stale[node] = false;
          voteScore[node] = sumVotes(node, adjIn, elected, votingAbility);
          heap.push(voteScore[node], node);
          continue;
        }
        best = node;
        bestScore = score;
        break;
      }

      if (best == -1 || bestScore <= 0.0)
        break;

      elected[best] = true;
      electedOrder[electedCount++] = best;

      // Reduce voting ability of out-neighbors of best by 1/degree(best)
      final int deg = adjOut[best].length;
      if (deg > 0) {
        final double reduction = 1.0 / deg;
        for (final int neighbor : adjOut[best]) {
          if (!elected[neighbor])
            votingAbility[neighbor] = Math.max(0.0, votingAbility[neighbor] - reduction);
        }
      }

      // Only two kinds of score changed: those best voted for, since best no longer votes, and those its out-neighbours
      // vote for, since their voting ability dropped. Marking them costs the two-hop neighbourhood of the elected node,
      // instead of a pass over every edge (issue #12170).
      for (final int neighbor : adjOut[best]) {
        stale[neighbor] = true;
        if (!elected[neighbor])
          for (final int w : adjOut[neighbor])
            stale[w] = true;
      }
    }

    final List<Result> results = new ArrayList<>(electedCount);
    for (int i = 0; i < electedCount; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", graph.getRID(electedOrder[i]));
      r.setProperty("rank", i + 1);
      results.add(r);
    }
    return results.stream();
  }

  /**
   * The vote score of {@code node}: the voting abilities of its in-neighbours that are not elected, added in ascending
   * order and once per edge, which is the order a full recomputation over every node's out-list added them in, so the
   * sum is the same double.
   */
  private static double sumVotes(final int node, final int[][] adjIn, final boolean[] elected,
      final double[] votingAbility) {
    double score = 0.0;
    for (final int u : adjIn[node])
      if (!elected[u])
        score += votingAbility[u];
    return score;
  }

  /**
   * A binary heap of (score, node) entries, highest score first and the lowest node among equal scores, unboxed. Sized
   * once: it holds at most one entry per node.
   */
  private static final class ScoreMaxHeap {
    private final double[] scores;
    private final int[]    nodes;
    private int            size;

    ScoreMaxHeap(final int capacity) {
      scores = new double[capacity];
      nodes = new int[capacity];
    }

    int size() {
      return size;
    }

    double topScore() {
      return scores[0];
    }

    void push(final double score, final int node) {
      int i = size++;
      while (i > 0) {
        final int parent = (i - 1) >>> 1;
        if (!before(score, node, scores[parent], nodes[parent]))
          break;
        scores[i] = scores[parent];
        nodes[i] = nodes[parent];
        i = parent;
      }
      scores[i] = score;
      nodes[i] = node;
    }

    /** Removes the first entry and returns its node. */
    int pop() {
      final int top = nodes[0];
      final double lastScore = scores[--size];
      final int lastNode = nodes[size];
      int i = 0;
      while (true) {
        int child = 2 * i + 1;
        if (child >= size)
          break;
        if (child + 1 < size && before(scores[child + 1], nodes[child + 1], scores[child], nodes[child]))
          child++;
        if (!before(scores[child], nodes[child], lastScore, lastNode))
          break;
        scores[i] = scores[child];
        nodes[i] = nodes[child];
        i = child;
      }
      if (size > 0) {
        scores[i] = lastScore;
        nodes[i] = lastNode;
      }
      return top;
    }

    private static boolean before(final double scoreA, final int nodeA, final double scoreB, final int nodeB) {
      return scoreA > scoreB || (scoreA == scoreB && nodeA < nodeB);
    }
  }
}
