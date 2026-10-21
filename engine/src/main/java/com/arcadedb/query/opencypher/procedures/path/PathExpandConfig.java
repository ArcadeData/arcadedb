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
 * Procedure: path.expandConfig(startNode, config)
 * <p>
 * Expands paths from a starting node using a configuration map.
 * Config options: relationshipFilter, labelFilter, sequence, filterStartNode, beginSequenceAtStart, endNodes,
 * terminatorNodes, allowlistNodes (whitelistNodes), denylistNodes (blacklistNodes), uniqueness, minLevel, maxLevel, bfs, limit,
 * optional. The filters take APOC's syntax: see {@link RelationshipFilter}, {@link LabelFilter} and {@link NodeFilter}. The
 * default {@code uniqueness} is APOC's, {@code RELATIONSHIP_PATH}: every trail, not every node, is a path. See
 * {@link PathUniqueness} for the nine values, and {@link PathExpansion} for the walk.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.expandConfig(a, {
 *   relationshipFilter: 'KNOWS>|&lt;WORKS_WITH',
 *   labelFilter: '+Person',
 *   minLevel: 1,
 *   maxLevel: 3,
 *   bfs: true
 * })
 * YIELD path
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathExpandConfig extends AbstractPathProcedure {
  public static final String NAME = "path.expandconfig";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Expand paths from a starting node using a configuration map";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Map<String, Object> config = optionalMapArg(args, 1, "config");

    final int minLevel = extractInt(config, "minLevel", 0);
    final int maxLevel = extractMaxLevel(config);
    final int limit = extractLimit(config);
    final boolean bfs = extractBoolean(config, "bfs", true);
    final boolean optional = extractBoolean(config, "optional", false);
    final PathUniqueness uniqueness = extractUniqueness(config, PathUniqueness.RELATIONSHIP_PATH);
    final Filters filters = extractFilters(config, startNode.getDatabase(), minLevel);

    return pathRows(new PathExpansion(startNode, filters, uniqueness, bfs, minLevel, maxLevel, limit, context), optional);
  }
}
