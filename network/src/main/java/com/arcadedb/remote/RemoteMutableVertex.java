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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.JSONSerializer;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

public class RemoteMutableVertex extends MutableVertex {
  private final   RemoteVertex   internal;
  protected final RemoteDatabase remoteDatabase;

  protected RemoteMutableVertex(final RemoteDatabase database, final String typeName) {
    super(null, (VertexType) database.getSchema().getType(typeName), null);
    this.internal = new RemoteVertex(this, database);
    this.remoteDatabase = database;
  }

  protected RemoteMutableVertex(final RemoteImmutableVertex source) {
    super(null, (VertexType) source.getType(), source.getIdentity());
    this.internal = new RemoteVertex(this, source.getRemoteDatabase());
    this.remoteDatabase = source.remoteDatabase;
    this.map.putAll(source.map);
  }

  @Override
  public MutableVertex save() {
    rid = remoteDatabase.saveRecord(this);
    dirty = false;
    return this;
  }

  @Override
  public MutableVertex save(final String bucketName) {
    rid = remoteDatabase.saveRecord(this, bucketName);
    dirty = false;
    return this;
  }

  @Override
  public void reload() {
    final ResultSet resultSet = remoteDatabase.query("sql", "select from " + rid);
    if (resultSet.hasNext()) {
      final Document document = resultSet.next().toElement();

      map.clear();
      map.putAll(document.propertiesAsMap());
      dirty = false;
    } else
      throw new RecordNotFoundException("Record " + rid + " not found", rid);
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

  @Override
  public DocumentType getType() {
    throw new UnsupportedOperationException("Schema API are not supported in remote database");
  }

  @Override
  public Database getDatabase() {
    throw new UnsupportedOperationException("Embedded Database API not supported in remote database");
  }

  @Override
  public Binary getBuffer() {
    throw new UnsupportedOperationException("Raw buffer API not supported in remote database");
  }

  @Override
  public void setBuffer(final Binary buffer) {
    throw new UnsupportedOperationException("Raw buffer API not supported in remote database");
  }

  @Override
  protected void checkForLazyLoadingProperties() {
    // NO ACTIONS
  }

  @Override
  protected Object convertValueToSchemaType(final String name, final Object value, final DocumentType type) {
    return value;
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
    return internal.getEdges(DIRECTION.BOTH, edgeTypes);
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
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    return internal.newEdge(edgeType, toVertex, bidirectional, properties);
  }

  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    return internal.newLightEdge(edgeType, toVertex, bidirectional);
  }

  @Override
  public Vertex asVertex() {
    return this;
  }

  @Override
  public Vertex asVertex(final boolean loadContent) {
    return this;
  }
}
