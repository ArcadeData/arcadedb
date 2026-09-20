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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import com.arcadedb.utility.NumberUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: path.subgraphAll(startNode, config)
 * <p>
 * Returns all nodes and relationships reachable from a starting node within the configured constraints.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.subgraphAll(a, {
 *   relationshipFilter: 'KNOWS',
 *   maxLevel: 3
 * })
 * YIELD nodes, relationships
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathSubgraphAll extends AbstractPathProcedure {
  public static final  String       NAME             = "path.subgraphall";
  private static final List<String> YIELD_FIELDS     = List.of("nodes", "relationships");
  private static final Set<String>  ALL_YIELD_FIELDS = Set.copyOf(YIELD_FIELDS);

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
    return "Returns all nodes and relationships reachable from a starting node";
  }

  @Override
  public List<String> getYieldFields() {
    return YIELD_FIELDS;
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    return execute(args, inputRow, context, ALL_YIELD_FIELDS);
  }

  /**
   * Under a plain {@code YIELD nodes} - which is how a reachable-component query is written - {@code relationships}
   * is not built at all, so not one edge record of the component is materialised (issue #7976).
   */
  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context,
      final Set<String> requestedYieldFields) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Map<String, Object> config = extractConfig(args[1]);

    final String[] relTypes = extractRelTypes(config.get("relationshipFilter"));
    final String[] labelFilter = extractLabels(config.get("labelFilter"));
    final int maxLevel = config.containsKey("maxLevel") ? NumberUtils.saturateToInt((Number) config.get("maxLevel")) : Integer.MAX_VALUE;

    final boolean withRelationships = requestedYieldFields == null || requestedYieldFields.contains("relationships");

    final List<Vertex> reachableNodes = new ArrayList<>();
    final List<Edge> reachableEdges = withRelationships ? new ArrayList<>() : null;

    collectReachableComponent(startNode, relTypes, labelFilter, maxLevel, reachableNodes, reachableEdges);

    final ResultInternal result = new ResultInternal();
    result.setProperty("nodes", reachableNodes);
    // An empty list rather than nothing when the field was not asked for: the YIELD projection in CallStep drops it
    // before anyone can read it, and a declared field that is absent altogether reads as null to any path that skips
    // that projection, which is a worse answer than "none collected"
    result.setProperty("relationships", withRelationships ? reachableEdges : List.of());

    return Stream.of(result);
  }
}
