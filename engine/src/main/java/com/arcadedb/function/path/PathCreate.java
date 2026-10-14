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
package com.arcadedb.function.path;

import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * path.create(start, relationships) - Create a path from a start node and a list of relationships (APOC's
 * {@code apoc.path.create}).
 * <p>
 * Each relationship continues the path from the node the previous one reached, in whichever direction it is stored,
 * so the nodes of the path are derived from the relationships; a relationship that does not touch that node is an
 * error, and a {@code null} one is skipped. Graph elements build a real path, which {@code nodes()}, {@code length()}
 * and the drivers read as one (issue #10017).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathCreate extends AbstractPathFunction {
  @Override
  protected String getSimpleName() {
    return "create";
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
    return "Create a path from a start node and a list of relationships";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final Object startNode = args[0];
    final List<Object> relationships = toList(args[1]);

    if (startNode instanceof Vertex start && allEdges(relationships))
      return buildPath(start, relationships);

    // Create path representation
    final Map<String, Object> path = new LinkedHashMap<>();
    path.put("_type", "path");

    final List<Object> nodes = new ArrayList<>();
    final List<Object> rels = new ArrayList<>();

    nodes.add(startNode);
    if (relationships != null) {
      rels.addAll(relationships);
    }

    path.put("nodes", nodes);
    path.put("relationships", rels);
    path.put("length", rels.size());

    return path;
  }

  private static boolean allEdges(final List<Object> relationships) {
    for (final Object relationship : relationships)
      if (relationship != null && !(relationship instanceof Edge))
        return false;
    return true;
  }

  private static TraversalPath buildPath(final Vertex start, final List<Object> relationships) {
    final TraversalPath path = new TraversalPath(start);
    Vertex current = start;
    for (final Object relationship : relationships) {
      if (relationship == null)
        continue;
      final Edge edge = (Edge) relationship;
      final RID at = current.getIdentity();
      final Vertex next;
      if (at.equals(edge.getOut()))
        next = edge.getInVertex();
      else if (at.equals(edge.getIn()))
        next = edge.getOutVertex();
      else
        throw new CommandExecutionException(
            "path.create(): relationship " + edge.getIdentity() + " (" + edge.getOut() + " -> " + edge.getIn()
                + ") does not continue the path, which has reached " + at);
      path.addStep(edge, next);
      current = next;
    }
    return path;
  }

  @SuppressWarnings("unchecked")
  private List<Object> toList(final Object input) {
    if (input == null)
      return new ArrayList<>();

    if (input instanceof List)
      return (List<Object>) input;

    if (input instanceof Collection)
      return new ArrayList<>((Collection<?>) input);

    final List<Object> result = new ArrayList<>();
    result.add(input);
    return result;
  }
}
