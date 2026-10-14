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
import com.arcadedb.utility.NumberUtils;

import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: path.expand(startNode, relationshipFilter, labelFilter, minDepth, maxDepth)
 * <p>
 * Expands paths from a starting node following relationship types and node labels. Both filters take APOC's syntax:
 * see {@link RelationshipFilter} and {@link LabelFilter}. The walk is APOC's for this form: breadth-first, one path per trail
 * ({@link PathUniqueness#RELATIONSHIP_PATH}).
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.expand(a, 'KNOWS>|&lt;WORKS_WITH', '+Person', 1, 3)
 * YIELD path
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathExpand extends AbstractPathProcedure {
  public static final String NAME = "path.expand";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 5;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Expand paths from a starting node following relationship types and node labels";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path");
  }

  /**
   * APOC's walk for this form is fixed: breadth-first, over the trails of the graph ({@link PathUniqueness#RELATIONSHIP_PATH}),
   * with no limit. A negative bound, APOC's {@code -1}, is no bound: {@code minDepth} then is 0 and {@code maxDepth} is the
   * whole graph.
   */
  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final int minDepth = Math.max(0, NumberUtils.saturateToInt((Number) args[3]));
    final int requestedMaxDepth = NumberUtils.saturateToInt((Number) args[4]);
    final int maxDepth = requestedMaxDepth < 0 ? Integer.MAX_VALUE : requestedMaxDepth;

    if (maxDepth < minDepth) {
      throw new IllegalArgumentException(getName() + "(): maxDepth must be >= minDepth");
    }

    final Filters filters = extractPositionalFilters(args[1], args[2], startNode.getDatabase(), minDepth);

    return pathRows(new PathExpansion(startNode, filters, PathUniqueness.RELATIONSHIP_PATH, true, minDepth, maxDepth,
        Integer.MAX_VALUE, context), false);
  }
}
