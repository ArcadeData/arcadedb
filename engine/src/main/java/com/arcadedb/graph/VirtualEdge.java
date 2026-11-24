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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Map;

/**
 * A relationship that exists only in a query result ({@code create.vRelationship}, the relationships of
 * {@code meta.graph}). Its endpoints are the node objects it was built from, stored or virtual, so {@code startNode()} and
 * {@code endNode()} answer them without a lookup. It cannot be modified.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class VirtualEdge extends VirtualElement implements Edge {
  private final String type;
  private final Vertex out;
  private final Vertex in;

  public VirtualEdge(final Database database, final Vertex out, final String type, final Map<String, Object> properties,
      final Vertex in) {
    super(database, properties);
    if (out == null)
      throw new IllegalArgumentException("The start node of a virtual relationship cannot be null");
    if (in == null)
      throw new IllegalArgumentException("The end node of a virtual relationship cannot be null");
    if (type == null)
      throw new IllegalArgumentException("The type of a virtual relationship cannot be null");
    this.out = out;
    this.type = type;
    this.in = in;
  }

  @Override
  public String getTypeName() {
    return type;
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  @Override
  protected String kind() {
    return "relationship";
  }

  @Override
  public MutableEdge modify() {
    throw unsupported("modified");
  }

  @Override
  public RID getOut() {
    return out.getIdentity();
  }

  @Override
  public Vertex getOutVertex() {
    return out;
  }

  @Override
  public RID getIn() {
    return in.getIdentity();
  }

  @Override
  public Vertex getInVertex() {
    return in;
  }

  @Override
  public Vertex getVertex(final Vertex.DIRECTION direction) {
    return direction == Vertex.DIRECTION.OUT ? out : in;
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> map = super.toMap(includeMetadata);
    if (includeMetadata) {
      map.put(Property.OUT_PROPERTY, getOut());
      map.put(Property.IN_PROPERTY, getIn());
    }
    return map;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject json = super.toJSON(includeMetadata);
    if (includeMetadata)
      json.put(Property.OUT_PROPERTY, getOut().toString()).put(Property.IN_PROPERTY, getIn().toString());
    return json;
  }

  @Override
  public Vertex asVertex() {
    throw new ClassCastException("A virtual relationship is not a node");
  }

  @Override
  public Vertex asVertex(final boolean loadContent) {
    throw new ClassCastException("A virtual relationship is not a node");
  }

  @Override
  public String toString() {
    return "(" + getOut() + ")-[" + getIdentity() + ":" + type + " " + propertiesAsMap() + "]->(" + getIn() + ")";
  }
}
