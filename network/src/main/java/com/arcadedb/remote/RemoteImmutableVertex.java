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
package com.arcadedb.remote;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.JSONSerializer;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

public class RemoteImmutableVertex extends RemoteImmutableDocument implements Vertex {
  private final RemoteVertex internal;

  protected RemoteImmutableVertex(final RemoteDatabase database, final Map<String, Object> properties) {
    super(database, properties);
    this.internal = new RemoteVertex(this, database);
  }

  @Override
  public RemoteMutableVertex modify() {
    return new RemoteMutableVertex(this);
  }

  public RemoteDatabase getRemoteDatabase() {
    return internal.remoteDatabase;
  }

  @Override
  public void delete() {
    internal.delete();
  }

  @Override
  public long countEdges(final DIRECTION direction, final String edgeType) {
    return internal.countEdges(direction, edgeType);
  }

  @Override
  public Iterable<Edge> getEdges() {
    return internal.getEdges(DIRECTION.BOTH);
  }

  @Override
  public Iterable<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return internal.getEdges(direction, edgeTypes);
  }

  @Override
  public Iterable<Vertex> getVertices() {
    return internal.getVertices(DIRECTION.BOTH);
  }

  @Override
  public Iterable<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return internal.getVertices(direction, edgeTypes);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    return internal.isConnectedTo(toVertex);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return internal.isConnectedTo(toVertex, direction);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeType) {
    return internal.isConnectedTo(toVertex, direction, edgeType);
  }

  @Override
  public RID moveToType(final String targetType) {
    return internal.vertex.moveToType(targetType);
  }

  @Override
  public RID moveToBucket(final String targetBucket) {
    return internal.vertex.moveToBucket(targetBucket);
  }

  @Override
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final Object... properties) {
    return internal.newEdge(edgeType, toVertex, properties);
  }

  @Override
  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return internal.newEdge(edgeType, toVertex, properties);
  }

  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    return internal.newLightEdge(edgeType, toVertex);
  }

  @Override
  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return internal.newLightEdge(edgeType, toVertex);
  }

  @Override
  public Vertex asVertex() {
    return this;
  }

  @Override
  public Vertex asVertex(final boolean loadContent) {
    if (loadContent)
      checkForLazyLoading();
    return this;
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> result = new HashMap<>(map);
    if (includeMetadata) {
      result.put("@cat", "v");
      result.put("@type", getTypeName());
      if (getIdentity() != null)
        result.put("@rid", getIdentity().toString());
    }
    return result;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject result = new JSONSerializer(database).map2json(map, null);
    if (includeMetadata) {
      result.put("@cat", "v");
      result.put("@type", getTypeName());
      if (getIdentity() != null)
        result.put("@rid", getIdentity().toString());
    }
    return result;
  }

}
