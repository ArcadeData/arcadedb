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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final int topK          = args.length > 1 ? ((Number) args[1]).intValue() : Integer.MAX_VALUE;

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final Map<RID, Integer> ridToIdx = buildRidIndex(vertices);
    final int[][] adjOut = buildAdjacencyList(vertices, ridToIdx, Vertex.DIRECTION.OUT, relTypes);

    // Compute in-degree for each node (votes received)
    final int[] inDegree = new int[n];
    for (int i = 0; i < n; i++)
      for (final int j : adjOut[i])
        inDegree[j]++;

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

    while (electedCount < limit) {
      // Find node with highest vote score among non-elected
      int best = -1;
      double bestScore = -1.0;
      for (int i = 0; i < n; i++) {
        if (!elected[i] && voteScore[i] > bestScore) {
          bestScore = voteScore[i];
          best = i;
        }
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
          if (!elected[neighbor]) {
            votingAbility[neighbor] = Math.max(0.0, votingAbility[neighbor] - reduction);
            // Recompute voteScore for neighbor's in-neighbors
          }
        }
      }

      // Recompute vote scores: for each node, score = sum of votingAbility of its in-neighbors
      // (only update nodes affected by the change — their out-neighbors of the elected node)
      for (final int neighbor : adjOut[best]) {
        if (!elected[neighbor]) {
          // neighbor's voteScore changes because votingAbility[neighbor] changed — but
          // voteScore[v] = sum_{u in inNeighbors(v)} votingAbility[u]
          // We need to find all nodes v that have neighbor as in-neighbor, i.e., adjOut[neighbor] contains v
          // Actually voteScore[v] depends on votingAbility of those who vote FOR v.
          // votingAbility[neighbor] changed → affects voteScore[w] for all w in adjOut[neighbor]
          for (final int w : adjOut[neighbor]) {
            if (!elected[w]) {
              // Recompute from scratch for w
              double sum = 0.0;
              // We need in-neighbors of w; use adjIn
              // Since we only have adjOut, rebuild from scratch for w
              // This is O(n*E) in worst case; use IN adjacency list instead
              // Mark for full recompute below
            }
          }
        }
      }

      // Rebuild all vote scores from scratch using votingAbility (simpler, correct approach)
      // voteScore[v] = sum_{u: u->v in edges} votingAbility[u]
      for (int v = 0; v < n; v++)
        voteScore[v] = 0.0;
      for (int u = 0; u < n; u++) {
        if (!elected[u]) {
          for (final int v : adjOut[u])
            if (!elected[v])
              voteScore[v] += votingAbility[u];
        }
      }
    }

    final List<Result> results = new ArrayList<>(electedCount);
    for (int i = 0; i < electedCount; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", vertices.get(electedOrder[i]).getIdentity());
      r.setProperty("rank", i + 1);
      results.add(r);
    }
    return results.stream();
  }
}
