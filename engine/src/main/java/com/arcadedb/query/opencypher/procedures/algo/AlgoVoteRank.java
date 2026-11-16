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

    long entries = 0;
    for (final int[] row : adjOut)
      entries += row.length;
    // ints: the voters of each node (offsets and ids), the affected list, the change stamps and the heap's nodes;
    // doubles: the score and the ability of each node and the heap's scores; the elected flags
    graph.memory().reserve(saturatingSum(saturatingProduct(saturatingSum(4L * n + 1, entries), INT_BYTES),
            saturatingProduct(n, 3 * DOUBLE_BYTES + BOOLEAN_BYTES)),
        "the vote-rank working set", n + " nodes, " + entries + " edge entries");

    final int[] electedOrder = elect(adjOut, Math.min(topK, n), guard);

    final List<Result> results = new ArrayList<>(electedOrder.length);
    for (int i = 0; i < electedOrder.length; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", graph.getRID(electedOrder[i]));
      r.setProperty("rank", i + 1);
      results.add(r);
    }
    return results.stream();
  }

  /**
   * Runs the elections over the outgoing adjacency and returns the elected nodes in election order, at most
   * {@code limit} of them. A node's score is the sum of the voting abilities of the nodes not elected yet that point at
   * it; each round elects the node of highest score (the lowest index among equals) while that score is positive, and
   * lowers the ability of each of its out-neighbours by {@code 1 / outDegree(elected)}, never below zero.
   * <p>
   * An election changes only the scores within two hops of the elected node (issue #12170): the out-neighbours lose its
   * vote, and the out-neighbours of each out-neighbour whose ability dropped lose part of that one. Those scores alone
   * are re-summed, and the next winner comes from a max-heap with lazy deletion (a score that changed is pushed again,
   * an entry whose score is no longer current is dropped when it surfaces), instead of a full recompute and a scan of
   * every node per round, which made the default call (every node electable) O(V x (V + E)).
   * <p>
   * A score is re-summed over its voters rather than patched by subtracting the change, and the voters are listed in
   * ascending order, which is the order the full recompute added them in: every score is therefore bit for bit the one
   * the full recompute produced, and so is every tie and every election. Patching would have let rounding pick a
   * different winner between two equal scores.
   */
  static int[] elect(final int[][] adjOut, final int limit, final WorkGuard guard) {
    final int n = adjOut.length;

    // The voters of each node, in ascending order: a CSR built by walking the voters in ascending order
    final int[] voterStart = new int[n + 1];
    for (final int[] row : adjOut)
      for (final int v : row)
        voterStart[v + 1]++;
    for (int i = 0; i < n; i++)
      voterStart[i + 1] += voterStart[i];
    final int[] voters = new int[voterStart[n]];
    final int[] cursor = Arrays.copyOf(voterStart, n);
    for (int u = 0; u < n; u++)
      for (final int v : adjOut[u])
        voters[cursor[v]++] = u;

    final double[] score = new double[n];
    final double[] ability = new double[n];
    final ScoreHeap heap = new ScoreHeap(n);
    for (int i = 0; i < n; i++) {
      score[i] = voterStart[i + 1] - voterStart[i];
      ability[i] = 1.0;
      if (score[i] > 0.0)
        heap.push(score[i], i);
    }

    final boolean[] elected = new boolean[n];
    final int[] affectedStamp = new int[n];
    final int[] affected = cursor; // reused: the cursors are spent
    final int[] electedOrder = new int[limit];
    int electedCount = 0;

    while (electedCount < limit) {
      guard.checkPeriodically(electedCount);

      // The winner: the top entry whose score is still current. A node whose score fell to zero is never pushed, so an
      // empty heap means no node left has a positive score
      int best = -1;
      while (heap.size() > 0) {
        final int top = heap.topNode();
        final double topScore = heap.topScore();
        heap.pop();
        if (!elected[top] && topScore == score[top]) {
          best = top;
          break;
        }
      }
      if (best < 0)
        break;

      elected[best] = true;
      electedOrder[electedCount++] = best;
      final int stamp = electedCount; // never 0, the initial stamp
      int affectedCount = 0;

      // Its out-neighbours lose its vote
      for (final int v : adjOut[best])
        if (!elected[v] && affectedStamp[v] != stamp) {
          affectedStamp[v] = stamp;
          affected[affectedCount++] = v;
        }

      // Each out-neighbour votes less, once per edge from the elected node, and so do the scores it contributes to
      final int degree = adjOut[best].length;
      if (degree > 0) {
        final double reduction = 1.0 / degree;
        for (final int neighbor : adjOut[best]) {
          if (elected[neighbor])
            continue;
          final double lowered = Math.max(0.0, ability[neighbor] - reduction);
          if (lowered == ability[neighbor])
            continue;
          ability[neighbor] = lowered;
          for (final int w : adjOut[neighbor])
            if (!elected[w] && affectedStamp[w] != stamp) {
              affectedStamp[w] = stamp;
              affected[affectedCount++] = w;
            }
        }
      }

      for (int k = 0; k < affectedCount; k++) {
        final int w = affected[k];
        double sum = 0.0;
        for (int p = voterStart[w]; p < voterStart[w + 1]; p++) {
          final int u = voters[p];
          if (!elected[u])
            sum += ability[u];
        }
        if (sum != score[w]) {
          score[w] = sum;
          if (sum > 0.0)
            heap.push(sum, w);
        }
      }
    }
    return electedCount == limit ? electedOrder : Arrays.copyOf(electedOrder, electedCount);
  }

  /** A binary max-heap of (score, node) entries in two primitive arrays: higher score first, lower node among equals. */
  private static final class ScoreHeap {
    private double[] scores;
    private int[]    nodes;
    private int      size;

    ScoreHeap(final int capacity) {
      scores = new double[Math.max(16, capacity)];
      nodes = new int[scores.length];
    }

    int size() {
      return size;
    }

    double topScore() {
      return scores[0];
    }

    int topNode() {
      return nodes[0];
    }

    private static boolean before(final double scoreA, final int nodeA, final double scoreB, final int nodeB) {
      return scoreA > scoreB || (scoreA == scoreB && nodeA < nodeB);
    }

    void push(final double score, final int node) {
      if (size == scores.length) {
        final int grown = scores.length + (scores.length >> 1);
        scores = Arrays.copyOf(scores, grown);
        nodes = Arrays.copyOf(nodes, grown);
      }
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

    void pop() {
      final int last = --size;
      if (last == 0)
        return;
      final double score = scores[last];
      final int node = nodes[last];
      int i = 0;
      while (true) {
        int child = 2 * i + 1;
        if (child >= last)
          break;
        if (child + 1 < last && before(scores[child + 1], nodes[child + 1], scores[child], nodes[child]))
          child++;
        if (!before(scores[child], nodes[child], score, node))
          break;
        scores[i] = scores[child];
        nodes[i] = nodes[child];
        i = child;
      }
      scores[i] = score;
      nodes[i] = node;
    }
  }
}
