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
package com.arcadedb.index.columnar;

import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;

/**
 * The hierarchy a COLUMNAR index facet rolls up along, as written in the index {@code METADATA}: the vertex type holding the tags,
 * the property of that type the indexed values join on, the edge type linking a tag to its parent and the direction in which a
 * child reaches its parents. With {@link Vertex.DIRECTION#IN} (the default) the edge runs {@code (parent)-[parentEdge]->(child)};
 * with {@link Vertex.DIRECTION#OUT} it runs {@code (child)-[parentEdge]->(parent)}.
 */
public record HierarchyDefinition(String vertexType, String key, String parentEdge, Vertex.DIRECTION parentDirection) {
  private static final String PARENT_DIRECTION = "parentDirection";

  /**
   * Reads the {@code hierarchy} object of the METADATA; null when {@code hierarchy} is null. A missing {@code parentDirection} is
   * {@code IN}. Nothing is checked against the schema (a restore must keep loading): see {@link #validate}. A direction that is
   * neither IN nor OUT is refused.
   */
  public static HierarchyDefinition fromJSON(final JSONObject hierarchy) {
    if (hierarchy == null)
      return null;
    return new HierarchyDefinition(hierarchy.getString("vertexType"), hierarchy.getString("key"), hierarchy.getString("parentEdge"),
        direction(hierarchy.getString(PARENT_DIRECTION, Vertex.DIRECTION.IN.name())));
  }

  /** The METADATA form, {@code parentDirection} written as "IN" or "OUT". */
  public JSONObject toJSON() {
    return new JSONObject().put("vertexType", vertexType).put("key", key).put("parentEdge", parentEdge)
        .put(PARENT_DIRECTION, parentDirection.name());
  }

  /**
   * Checks the definition against the schema for an index on {@code indexedType.property}.
   *
   * @throws IllegalArgumentException naming the offending METADATA field
   */
  public void validate(final Schema schema, final String indexedType, final String property) {
    if (!schema.existsType(vertexType) || !(schema.getType(vertexType) instanceof VertexType))
      throw new IllegalArgumentException("hierarchy.vertexType '" + vertexType + "' is not a vertex type");
    final DocumentType tags = schema.getType(vertexType);
    if (!tags.existsProperty(key))
      throw new IllegalArgumentException("hierarchy.key '" + key + "' is not a property of " + vertexType);
    if (!schema.existsType(parentEdge) || !(schema.getType(parentEdge) instanceof EdgeType edgeType))
      throw new IllegalArgumentException("hierarchy.parentEdge '" + parentEdge + "' is not an edge type");
    if (!edgeType.isBidirectional() && parentDirection == Vertex.DIRECTION.IN)
      throw new IllegalArgumentException("hierarchy.parentEdge '" + parentEdge
          + "' is unidirectional: use parentDirection OUT with child-to-parent edges");

    final Property declared = schema.getType(indexedType).getPolymorphicPropertyIfExists(property);
    if (declared != null) {
      final Type type = declared.getType();
      final boolean stringList =
          type == Type.LIST && (declared.getOfType() == null || Type.STRING.name().equalsIgnoreCase(declared.getOfType()));
      if (type != Type.STRING && !stringList)
        throw new IllegalArgumentException("hierarchy requires a STRING or LIST OF STRING property, but " + indexedType + "." + property
            + " is " + type + (type == Type.LIST ? " OF " + declared.getOfType() : ""));
    }
  }

  private static Vertex.DIRECTION direction(final String name) {
    if (Vertex.DIRECTION.IN.name().equals(name))
      return Vertex.DIRECTION.IN;
    if (Vertex.DIRECTION.OUT.name().equals(name))
      return Vertex.DIRECTION.OUT;
    throw new IllegalArgumentException("hierarchy.parentDirection must be IN or OUT, got: " + name);
  }
}
