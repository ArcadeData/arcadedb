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

import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.ShortestPathFinder;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: algo.cch.shortestPath(startNode, endNode, relTypes, weightProperty, direction?)
 * <p>
 * Weighted point-to-point shortest path (issue #9437). When a Graph Analytical View keeps a Customizable Contraction
 * Hierarchy for the weight and relationship types asked for, the path is answered by the hierarchy in a time that does
 * not grow with the distance between the two nodes; otherwise by bidirectional Dijkstra, on the view's columns when it
 * materializes the weight and on the records when not. Either way the answer is exact and reflects the calling
 * transaction's own uncommitted changes.
 * <p>
 * The weight is the relationship property's numeric value; a relationship without one weighs 1, and one whose value is
 * negative is not walked. {@code relTypes} is a type name, a comma-separated list, a list, or null for every type;
 * {@code direction} is OUT, IN or BOTH (default).
 * <pre>
 * MATCH (a:City {name: 'A'}), (b:City {name: 'B'})
 * CALL algo.cch.shortestPath(a, b, 'ROAD', 'distance', 'OUT')
 * YIELD path, weight
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoCCHShortestPath extends AbstractAlgoProcedure {
  public static final String NAME = "algo.cch.shortestPath";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Find the shortest weighted path between two nodes, using a Customizable Contraction Hierarchy when a Graph "
        + "Analytical View keeps one";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path", "weight");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Vertex endNode = extractVertex(args[1], "endNode");
    final String[] relTypes = extractRelTypes(args[2]);
    final String weightProperty = extractString(args[3], "weightProperty");
    final Vertex.DIRECTION direction = args.length > 4 && args[4] != null ?
        parseDirection(extractString(args[4], "direction")) : Vertex.DIRECTION.BOTH;

    final ShortestPathFinder.Result found = ShortestPathFinder.find(context.getDatabase(), startNode.getIdentity(),
        endNode.getIdentity(), weightProperty, direction, relTypes, newWorkGuard(context));
    if (found == null)
      return Stream.empty();
    if (found.engine() == ShortestPathFinder.Engine.CONTRACTION_HIERARCHY)
      context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);

    final WeightedPath weighted = attachEdges(found.vertices(), relTypes, direction, weightProperty);
    final ResultInternal result = new ResultInternal();
    result.setProperty("path", buildPath(weighted.ridsWithEdges(), context.getDatabase()));
    result.setProperty("weight", found.weight());
    return Stream.of(result);
  }
}
