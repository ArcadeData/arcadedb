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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: path.subgraphNodes(startNode, config)
 * <p>
 * Returns all nodes reachable from a starting node within the configured constraints.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.subgraphNodes(a, {
 *   relationshipFilter: 'KNOWS',
 *   maxLevel: 3
 * })
 * YIELD node
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathSubgraphNodes extends AbstractPathProcedure {
  public static final String NAME = "path.subgraphnodes";

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Returns all nodes reachable from a starting node within the configured constraints";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Map<String, Object> config = optionalMapArg(args, 1, "config");

    final int minLevel = extractComponentMinLevel(config);
    final int maxLevel = extractMaxLevel(config);
    final int limit = extractLimit(config);
    final boolean optional = extractBoolean(config, "optional", false);
    // Read and checked, and then left to APOC's rule: this walk is node-global whatever the option says
    extractUniqueness(config, PathUniqueness.NODE_GLOBAL);
    final Filters filters = extractFilters(config, startNode.getDatabase(), minLevel);

    // Only the nodes are ever yielded here, so the walk never materialises an edge (issue #7976)
    final List<Vertex> reachableNodes = new ArrayList<>();
    collectReachableComponent(startNode, filters, minLevel, maxLevel, limit, reachableNodes, null, context);

    if (optional && reachableNodes.isEmpty())
      return Stream.of(nullRow("node"));

    return reachableNodes.stream().map(node -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", node);
      return (Result) result;
    });
  }
}
