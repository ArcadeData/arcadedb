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

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Procedure: algo.degree(relTypes?, direction?)
 * <p>
 * Computes degree centrality for every vertex: the normalized fraction of nodes connected to it.
 * Routes through {@link #loadGraph} like the rest of the {@code algo.*} package (issue #6316), so a Graph
 * Analytical View covering the graph is used when one is ready. CSR-backed, {@link GraphData#degrees} reads
 * the count straight off the view's offset arrays in O(1) per node rather than materialising and counting a
 * neighbour list, which is the same {@code vertex.countEdges()}-style efficiency the OLTP path already had.
 * </p>
 * <p>
 * {@code direction} (default {@code BOTH}) selects which edges count towards {@code degree}/{@code score}: with
 * {@code IN} or {@code OUT}, only that direction is counted (and the other of {@code inDegree}/{@code outDegree}
 * is 0, since it was never read), rather than always summing both regardless of what was requested.
 * </p>
 * <p>
 * Example:
 * <pre>
 * CALL algo.degree()
 * YIELD node, inDegree, outDegree, degree, score
 * RETURN node.name, score ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoDegreeCentrality extends AbstractAlgoProcedure {
  public static final String NAME = "algo.degree";

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
    return "Compute degree centrality (in, out, total) for all vertices";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "inDegree", "outDegree", "degree", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String[] relTypes = args.length > 0 ? extractRelTypes(args[0]) : null;
    final Vertex.DIRECTION dir = args.length > 1 ? parseDirection(extractString(args[1], "direction")) : Vertex.DIRECTION.BOTH;

    final Database db = context.getDatabase();
    final GraphData graph = loadGraph(db, null, relTypes, context);

    final int n = graph.nodeCount;
    if (n == 0)
      return Stream.empty();

    final double norm = n > 1 ? (double) (n - 1) : 1.0;

    // Only the requested direction(s) are computed: a caller that filters to IN or OUT is asking to skip the
    // cost of the other one too, not only to have it excluded from the total (issue #6716). Still routed
    // through GraphData.degrees() rather than a raw countEdges() walk, so a Graph Analytical View accelerates
    // whichever direction(s) are actually requested (issue #6316).
    final int[] inDegrees = dir != Vertex.DIRECTION.OUT ? graph.degrees(Vertex.DIRECTION.IN, relTypes) : null;
    final int[] outDegrees = dir != Vertex.DIRECTION.IN ? graph.degrees(Vertex.DIRECTION.OUT, relTypes) : null;

    return IntStream.range(0, n).mapToObj(i -> {
      final long in = inDegrees != null ? inDegrees[i] : 0;
      final long out = outDegrees != null ? outDegrees[i] : 0;
      final long total = in + out;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", graph.getRID(i));
      r.setProperty("inDegree", in);
      r.setProperty("outDegree", out);
      r.setProperty("degree", total);
      r.setProperty("score", total / norm);
      return (Result) r;
    });
  }
}
