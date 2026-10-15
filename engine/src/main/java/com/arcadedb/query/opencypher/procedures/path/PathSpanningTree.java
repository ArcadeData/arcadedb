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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: path.spanningTree(startNode, config)
 * <p>
 * Returns a spanning tree (paths from the start node to all reachable nodes)
 * using BFS traversal.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.spanningTree(a, {
 *   relationshipFilter: 'KNOWS',
 *   maxLevel: 3
 * })
 * YIELD path
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathSpanningTree extends AbstractPathProcedure {
  public static final String NAME = "path.spanningtree";

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
    return "Returns a spanning tree from the start node to all reachable nodes";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path");
  }

  /**
   * The walk is {@link PathUniqueness#NODE_GLOBAL} whatever {@code uniqueness} says - APOC overwrites the option here - which
   * is what makes the paths a tree: each node is reached once, along the first route to it.
   */
  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Map<String, Object> config = optionalMapArg(args, 1, "config");

    final int minLevel = extractComponentMinLevel(config);
    final int maxLevel = extractMaxLevel(config);
    final int limit = extractLimit(config);
    final boolean bfs = extractBoolean(config, "bfs", true);
    final boolean optional = extractBoolean(config, "optional", false);
    extractUniqueness(config, PathUniqueness.NODE_GLOBAL);
    final Filters filters = extractFilters(config, startNode.getDatabase(), minLevel);

    return pathRows(new PathExpansion(startNode, filters, PathUniqueness.NODE_GLOBAL, bfs, minLevel, maxLevel, limit, context),
        optional);
  }
}
