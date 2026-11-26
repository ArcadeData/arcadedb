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
package com.arcadedb.function.create;

import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VirtualEdge;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Map;

/**
 * create.vRelationship(from, type, properties, to) - Create a virtual relationship: a relationship that is not persisted,
 * between two nodes (stored or virtual), as APOC's {@code apoc.create.vRelationship}. The argument order is APOC's, the
 * properties come before the end node. It is a real relationship value ({@code type()}, {@code startNode()},
 * {@code endNode()}, {@code properties()} work on it) with a negative id, not a map.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CreateVRelationship extends AbstractCreateFunction {
  @Override
  protected String getSimpleName() {
    return "vRelationship";
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Create a virtual relationship (not persisted) between two nodes: (from, type, properties, to)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    return create(getName(), args[0], args[1], args[2], args[3], context);
  }

  /**
   * Builds the virtual relationship; shared with the {@code CALL create.vRelationship(...) YIELD rel} form.
   */
  @SuppressWarnings("unchecked")
  public static VirtualEdge create(final String name, final Object from, final Object type, final Object properties,
      final Object to, final CommandContext context) {
    final Vertex start = node(name, from, "start");
    final Vertex end = node(name, to, "end");
    if (type == null)
      throw new IllegalArgumentException(name + "(): a relationship type cannot have a null name");
    if (!(type instanceof String typeName))
      throw CypherFunctionHelper.typeMismatch(name, "a STRING relationship type", type);
    if (properties != null && !(properties instanceof Map))
      throw new TypeMismatchException(name + "() expects the properties as a map, got " + properties.getClass().getSimpleName());
    return new VirtualEdge(context != null ? context.getDatabase() : null, start, typeName, (Map<String, Object>) properties, end);
  }

  private static Vertex node(final String name, final Object value, final String side) {
    if (value == null)
      throw new IllegalArgumentException(name + "(): the " + side + " node is null");
    if (value instanceof Vertex vertex)
      return vertex;
    if (value instanceof Result result && result.isVertex())
      return result.getVertex().get();
    throw CypherFunctionHelper.typeMismatch(name, "a NODE " + side + " node", value);
  }
}
