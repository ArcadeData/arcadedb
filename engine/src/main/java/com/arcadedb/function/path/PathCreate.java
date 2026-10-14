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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

  /**
   * Walks the relationships from the start node, each one continuing from the node the previous one reached in either
   * direction, as APOC's path builder does. The path used to hold the start node alone beside all the relationships, so
   * its nodes and its length disagreed (issue #9913); a {@code null} relationship is skipped, as in APOC.
   *
   * @throws CommandExecutionException when a relationship does not continue the path
   */
  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    if (!(args[0] instanceof Identifiable start) || !(start instanceof Vertex || start.getRecord() instanceof Vertex))
      throw CypherFunctionHelper.typeMismatch(getName(), "a NODE", args[0]);

    Vertex current = start instanceof Vertex vertex ? vertex : (Vertex) start.getRecord();
    final TraversalPath path = new TraversalPath(current);
    for (final Object element : toList(args[1])) {
      if (element == null)
        continue;
      if (!(element instanceof Identifiable identifiable) || !(identifiable instanceof Edge
          || identifiable.getRecord() instanceof Edge))
        throw CypherFunctionHelper.typeMismatch(getName(), "a LIST<RELATIONSHIP>", args[1]);

      final Edge relationship = identifiable instanceof Edge edge ? edge : (Edge) identifiable.getRecord();
      final RID at = current.getIdentity();
      if (at.equals(relationship.getOut()))
        current = relationship.getInVertex();
      else if (at.equals(relationship.getIn()))
        current = relationship.getOutVertex();
      else
        throw new CommandExecutionException(
            getName() + "(): relationship " + relationship.getIdentity() + " does not continue the path at node " + at);
      path.addStep(relationship, current);
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
