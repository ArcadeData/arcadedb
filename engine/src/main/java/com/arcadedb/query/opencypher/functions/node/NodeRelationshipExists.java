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
package com.arcadedb.query.opencypher.functions.node;

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Collection;

/**
 * node.relationship.exists(node, [relTypes], [direction]) - Check if a node has relationships.
 *
 * @author ArcadeDB Team
 */
public class NodeRelationshipExists extends AbstractNodeFunction {
  @Override
  protected String getSimpleName() {
    return "relationship.exists";
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
    return "Check if a node has any relationships, optionally filtered by type and direction";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Vertex vertex = toVertex(args[0]);
    if (vertex == null)
      return false;

    final String[] relTypes = extractRelTypes(args);
    final Vertex.DIRECTION direction = args.length > 2 && args[2] != null
        ? parseDirection(args[2].toString())
        : Vertex.DIRECTION.BOTH;

    if (relTypes != null && relTypes.length > 0) {
      for (final String relType : relTypes) {
        if (vertex.countEdges(direction, relType) > 0)
          return true;
      }
      return false;
    } else {
      return vertex.countEdges(direction, null) > 0;
    }
  }

  @SuppressWarnings("unchecked")
  private String[] extractRelTypes(final Object[] args) {
    if (args.length < 2 || args[1] == null)
      return null;

    if (args[1] instanceof String)
      return new String[] { (String) args[1] };

    if (args[1] instanceof Collection) {
      final Collection<?> coll = (Collection<?>) args[1];
      return coll.stream().map(Object::toString).toArray(String[]::new);
    }

    return new String[] { args[1].toString() };
  }
}
