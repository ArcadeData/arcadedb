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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.degree(relTypes?, direction?)
 * <p>
 * Computes degree centrality for every vertex: the normalized fraction of nodes connected to it.
 * Uses {@code vertex.countEdges()} for maximum efficiency — no edge objects are materialised.
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
    final String dirArg = args.length > 1 ? extractString(args[1], "direction") : "BOTH";

    final Database db = context.getDatabase();
    final List<Vertex> vertices = new ArrayList<>();
    final Iterator<Vertex> iter = getAllVertices(db, null);
    while (iter.hasNext())
      vertices.add(iter.next());

    final int n = vertices.size();
    if (n == 0)
      return Stream.empty();

    final double norm = n > 1 ? (double) (n - 1) : 1.0;

    return vertices.stream().map(v -> {
      final long in = relTypes != null && relTypes.length > 0 ?
          v.countEdges(Vertex.DIRECTION.IN, relTypes) :
          v.countEdges(Vertex.DIRECTION.IN);
      final long out = relTypes != null && relTypes.length > 0 ?
          v.countEdges(Vertex.DIRECTION.OUT, relTypes) :
          v.countEdges(Vertex.DIRECTION.OUT);
      final long total = in + out;
      final ResultInternal r = new ResultInternal();
      r.setProperty("node", v);
      r.setProperty("inDegree", in);
      r.setProperty("outDegree", out);
      r.setProperty("degree", total);
      r.setProperty("score", total / norm);
      return (Result) r;
    });
  }
}
