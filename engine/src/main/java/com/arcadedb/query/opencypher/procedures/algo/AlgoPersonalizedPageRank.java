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
import com.arcadedb.graph.NeighborView;
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
 * Procedure: algo.personalizedPageRank(sourceNode, relTypes?, dampingFactor?, maxIterations?, tolerance?)
 * <p>
 * Computes Personalized PageRank (PPR) scores relative to a source node. Unlike standard PageRank,
 * the teleportation probability is concentrated at the source node (personalization vector = 1 at
 * source, 0 elsewhere). This measures the structural importance/proximity of all nodes relative
 * to the source.
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (s:Person {name:'Alice'})
 * CALL algo.personalizedPageRank(s, 'KNOWS', 0.85, 20, 0.000001)
 * YIELD nodeId, score
 * RETURN nodeId, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoPersonalizedPageRank extends AbstractAlgoProcedure {
  public static final String NAME = "algo.personalizedPageRank";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Computes Personalized PageRank scores relative to a source node";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeId", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex sourceVertex = extractVertex(args[0], "sourceNode");
    final String[] relTypes = args.length > 1 ? extractRelTypes(args[1]) : null;
    final double dampingFactor = args.length > 2 && args[2] instanceof Number n ? n.doubleValue() : 0.85;
    final int maxIterations = args.length > 3 && args[3] instanceof Number n ? n.intValue() : 20;
    final double tolerance = args.length > 4 && args[4] instanceof Number n ? n.doubleValue() : 1e-6;

    final Database db = context.getDatabase();

    // Try CSR-accelerated path
    final GraphTraversalProvider provider = findProvider(db, relTypes);
    if (provider != null) {
      final int sourceIdx = provider.getNodeId(sourceVertex.getIdentity());
      if (sourceIdx >= 0) {
        context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
        return executeWithCSR(provider, sourceIdx, relTypes, dampingFactor, maxIterations, tolerance);
      }
    }

    // Fall back to OLTP path
    return executeWithOLTP(db, sourceVertex, relTypes, dampingFactor, maxIterations, tolerance);
  }

  private Stream<Result> executeWithCSR(final GraphTraversalProvider provider, final int sourceIdx,
      final String[] relTypes, final double dampingFactor, final int maxIterations, final double tolerance) {
    final int n = provider.getNodeCount();
    if (n == 0)
      return Stream.empty();

    // Zero-allocation neighbor access via NeighborView
    final NeighborView outView = provider.getNeighborView(Vertex.DIRECTION.OUT, relTypes);
    final NeighborView inView = provider.getNeighborView(Vertex.DIRECTION.IN, relTypes);
    final boolean hasView = outView != null && inView != null;

    final int[][] outAdjFallback = hasView ? null : buildAdjacencyFromProvider(provider, Vertex.DIRECTION.OUT, relTypes);
    final int[][] inAdjFallback = hasView ? null : buildAdjacencyFromProvider(provider, Vertex.DIRECTION.IN, relTypes);

    final int[] outDegree = new int[n];
    for (int i = 0; i < n; i++)
      outDegree[i] = hasView ? outView.degree(i) : outAdjFallback[i].length;

    final double[] rank = new double[n];
    rank[sourceIdx] = 1.0;

    for (int iter = 0; iter < maxIterations; iter++) {
      final double[] newRank = new double[n];
      double dangling = 0.0;
      for (int i = 0; i < n; i++)
        if (outDegree[i] == 0)
          dangling += rank[i];

      if (hasView) {
        final int[] inNbrs = inView.neighbors();
        for (int i = 0; i < n; i++) {
          double incoming = 0.0;
          for (int k = inView.offset(i), end = inView.offsetEnd(i); k < end; k++) {
            final int j = inNbrs[k];
            if (outDegree[j] > 0)
              incoming += rank[j] / outDegree[j];
          }
          final double personal = (i == sourceIdx) ? 1.0 : 0.0;
          newRank[i] = (1.0 - dampingFactor) * personal + dampingFactor * incoming + dampingFactor * dangling * personal;
        }
      } else {
        for (int i = 0; i < n; i++) {
          double incoming = 0.0;
          for (final int j : inAdjFallback[i])
            if (outDegree[j] > 0)
              incoming += rank[j] / outDegree[j];
          final double personal = (i == sourceIdx) ? 1.0 : 0.0;
          newRank[i] = (1.0 - dampingFactor) * personal + dampingFactor * incoming + dampingFactor * dangling * personal;
        }
      }

      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        maxChange = Math.max(maxChange, Math.abs(newRank[i] - rank[i]));
        rank[i] = newRank[i];
      }

      if (maxChange < tolerance)
        break;
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", provider.getRID(i));
      r.setProperty("score", rank[i]);
      results.add(r);
    }
    return results.stream();
  }

  private Stream<Result> executeWithOLTP(final Database db, final Vertex sourceVertex, final String[] relTypes,
      final double dampingFactor, final int maxIterations, final double tolerance) {

    final GraphData graph = loadGraph(db, null, relTypes);


    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final int sourceIdx = graph.indexOf(sourceVertex.getIdentity());
    if (sourceIdx < 0)
      return Stream.empty();

    final int[][] outAdj = graph.adjacency(Vertex.DIRECTION.OUT, relTypes);
    final int[] outDegree = new int[n];
    for (int i = 0; i < n; i++)
      outDegree[i] = outAdj[i].length;

    final int[][] inAdj = graph.adjacency(Vertex.DIRECTION.IN, relTypes);

    final double[] rank = new double[n];
    rank[sourceIdx] = 1.0;

    for (int iter2 = 0; iter2 < maxIterations; iter2++) {
      final double[] newRank = new double[n];
      double dangling = 0.0;
      for (int i = 0; i < n; i++)
        if (outDegree[i] == 0)
          dangling += rank[i];

      for (int i = 0; i < n; i++) {
        double incoming = 0.0;
        for (final int j : inAdj[i])
          if (outDegree[j] > 0)
            incoming += rank[j] / outDegree[j];
        final double personal = (i == sourceIdx) ? 1.0 : 0.0;
        newRank[i] = (1.0 - dampingFactor) * personal + dampingFactor * incoming + dampingFactor * dangling * personal;
      }

      double maxChange = 0.0;
      for (int i = 0; i < n; i++) {
        maxChange = Math.max(maxChange, Math.abs(newRank[i] - rank[i]));
        rank[i] = newRank[i];
      }

      if (maxChange < tolerance)
        break;
    }

    final List<Result> results = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final ResultInternal r = new ResultInternal();
      r.setProperty("nodeId", graph.getRID(i));
      r.setProperty("score", rank[i]);
      results.add(r);
    }
    return results.stream();
  }
}
