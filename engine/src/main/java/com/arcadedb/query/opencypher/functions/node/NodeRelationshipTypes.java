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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * node.relationship.types(node, [direction]) - Get all relationship types connected to a node.
 *
 * @author ArcadeDB Team
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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Get all relationship types connected to a node";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Vertex vertex = toVertex(args[0]);
    if (vertex == null)
      return Collections.emptyList();

    final Vertex.DIRECTION direction = args.length > 1 && args[1] != null
        ? parseDirection(args[1].toString())
        : Vertex.DIRECTION.BOTH;

    final Set<String> types = new LinkedHashSet<>();

    for (final Edge edge : vertex.getEdges(direction)) {
      types.add(edge.getTypeName());
    }

    return new ArrayList<>(types);
  }
}
