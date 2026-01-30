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
package com.arcadedb.query.opencypher.ast;

import java.util.List;

/**
 * Represents a shortestPath or allShortestPaths pattern in a Cypher query.
 * <p>
 * Examples:
 * - shortestPath((a)-[:KNOWS*]-(b))
 * - allShortestPaths((a)-[:KNOWS*]-(b))
 * - p = shortestPath((a)-[:KNOWS*]-(b))
 * <p>
 * This extends PathPattern to add the "all paths" flag which distinguishes
 * between shortestPath (single result) and allShortestPaths (all equal-length shortest paths).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ShortestPathPattern extends PathPattern {
  private final boolean allPaths;

  /**
   * Creates a shortest path pattern.
   *
   * @param nodes         list of node patterns (typically 2: start and end)
   * @param relationships list of relationship patterns (typically 1: the path to traverse)
   * @param pathVariable  optional variable name for the path result
   * @param allPaths      true for allShortestPaths, false for shortestPath
   */
  public ShortestPathPattern(final List<NodePattern> nodes, final List<RelationshipPattern> relationships,
      final String pathVariable, final boolean allPaths) {
    super(nodes, relationships, pathVariable);
    this.allPaths = allPaths;
  }

  /**
   * Returns true if this is an allShortestPaths pattern.
   *
   * @return true for allShortestPaths, false for shortestPath
   */
  public boolean isAllPaths() {
    return allPaths;
  }

  /**
   * Returns true - this is always a shortest path pattern.
   *
   * @return true
   */
  public boolean isShortestPath() {
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    if (getPathVariable() != null) {
      sb.append(getPathVariable()).append(" = ");
    }
    sb.append(allPaths ? "allShortestPaths(" : "shortestPath(");
    sb.append(super.toString());
    sb.append(")");
    return sb.toString();
  }
}
