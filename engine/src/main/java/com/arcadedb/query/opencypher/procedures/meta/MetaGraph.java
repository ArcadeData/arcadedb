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
package com.arcadedb.query.opencypher.procedures.meta;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: meta.graph()
 * <p>
 * Returns a virtual graph representing the schema structure with node types
 * and relationship types as virtual nodes and edges.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL meta.graph()
 * YIELD nodes, relationships
 * </pre>
 * </p>
 *
 * @author ArcadeDB Team
 */
public class MetaGraph extends AbstractMetaProcedure {
  public static final String NAME = "meta.graph";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 0;
  }

  @Override
  public String getDescription() {
    return "Returns a virtual graph representing the schema structure";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodes", "relationships");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Database database = context.getDatabase();
    final Schema schema = database.getSchema();

    final List<Map<String, Object>> nodes = new ArrayList<>();
    final List<Map<String, Object>> relationships = new ArrayList<>();

    // Create virtual nodes for each vertex type
    for (final DocumentType type : schema.getTypes()) {
      if (type instanceof VertexType) {
        final Map<String, Object> node = new HashMap<>();
        node.put("_type", "vNode");
        node.put("_id", "meta:" + type.getName());
        node.put("_labels", List.of(type.getName()));
        node.put("name", type.getName());
        node.put("count", database.countType(type.getName(), true));

        // Add property info
        final List<String> propertyNames = new ArrayList<>(type.getPropertyNames());
        node.put("properties", propertyNames);

        nodes.add(node);
      }
    }

    // Create virtual relationships for each edge type
    for (final DocumentType type : schema.getTypes()) {
      if (type instanceof EdgeType) {
        final Map<String, Object> rel = new HashMap<>();
        rel.put("_type", "vRelationship");
        rel.put("_id", "meta_rel:" + type.getName());
        rel.put("type", type.getName());
        rel.put("count", database.countType(type.getName(), true));

        // Add property info
        final List<String> propertyNames = new ArrayList<>(type.getPropertyNames());
        rel.put("properties", propertyNames);

        relationships.add(rel);
      }
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty("nodes", nodes);
    result.setProperty("relationships", relationships);

    return Stream.of(result);
  }
}
