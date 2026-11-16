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
package com.arcadedb.function.node;

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.RelationshipPattern;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * node.relationship.types(node, [relTypes], [direction]) - Get the relationship types connected to a node.
 * <p>
 * {@code relTypes} is APOC's relationship pattern ({@code 'KNOWS>|<LIKES'}, {@code '>'}, a blank one for every relationship):
 * the answer is the types the pattern names that the node has a relationship of, in the pattern's order. It used to be a
 * direction, so {@code apoc.node.relationship.types(n, 'KNOWS')} was refused as an unknown direction and
 * {@code (n, 'OUT')} answered every outgoing type (issue #12103). The direction is now the third argument, which APOC does not
 * have, restricting the alternatives that carry no direction marker.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class NodeRelationshipTypes extends AbstractNodeFunction {
  @Override
  protected String getSimpleName() {
    return "relationship.types";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Get the relationship types connected to a node, optionally filtered by an APOC relationship pattern and a direction";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Vertex vertex = toVertex(args[0]);
    if (vertex == null)
      return Collections.emptyList();

    final RelationshipPattern pattern = patternArgument(args, 1);
    final Vertex.DIRECTION restriction = directionArgument(args, 2);
    final Set<String> types = new LinkedHashSet<>();
    if (pattern.isEmpty())
      addTypes(context, vertex, restriction, types);
    else
      for (final RelationshipPattern.Entry entry : pattern.entries()) {
        final Vertex.DIRECTION direction = effectiveDirection(entry, restriction);
        if (direction == null)
          continue;
        if (entry.type() == null)
          addTypes(context, vertex, direction, types);
        else if (IncomingEdgeLookup.countEdges(context, vertex, direction, entry.type()) > 0)
          types.add(entry.type());
      }
    return new ArrayList<>(types);
  }

  private static void addTypes(final CommandContext context, final Vertex vertex, final Vertex.DIRECTION direction,
      final Set<String> types) {
    // The incoming side of a unidirectional edge type is read from the edges that end in the node (issue #8629)
    for (final Iterator<Edge> edges = IncomingEdgeLookup.getEdges(context, vertex, direction); edges.hasNext(); )
      types.add(edges.next().getTypeName());
  }
}
