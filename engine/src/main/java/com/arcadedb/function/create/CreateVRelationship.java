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

import com.arcadedb.database.Document;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.HashMap;
import java.util.Map;

/**
 * create.vRelationship(from, type, to, properties) - Create a virtual relationship (map representation).
 * <p>
 * Virtual relationships are not persisted to the database. They are useful for
 * temporary data structures or for returning computed results.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class CreateVRelationship extends AbstractCreateFunction {
  private static long virtualIdCounter = 0;

  @Override
  protected String getSimpleName() {
    return "vRelationship";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Create a virtual relationship (not persisted) between two nodes";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    final Object fromNode = args[0];
    final String type = args[1] != null ? args[1].toString() : "RELATED";
    final Object toNode = args[2];
    final Map<String, Object> properties = args.length > 3 && args[3] != null
        ? new HashMap<>((Map<String, Object>) args[3])
        : new HashMap<>();

    // Create a virtual relationship representation
    final Map<String, Object> vRel = new HashMap<>(Map.of(
        "_type", "vRelationship",
        "_id", "vRel:" + (++virtualIdCounter),
        "_relType", type,
        "_start", getNodeId(fromNode),
        "_end", getNodeId(toNode)));
    vRel.putAll(properties);

    return vRel;
  }

  private Object getNodeId(final Object node) {
    if (node == null)
      return null;

    if (node instanceof Vertex vertex)
      return vertex.getIdentity().toString();

    if (node instanceof Document document)
      return document.getIdentity().toString();

    if (node instanceof Map<?, ?> map) {
      if (map.containsKey("_id"))
        return map.get("_id");
    }

    return node.toString();
  }
}
