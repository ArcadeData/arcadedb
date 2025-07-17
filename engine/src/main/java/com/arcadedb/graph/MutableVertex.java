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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Transaction;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

/**
 * Mutable vertex that supports updates. After any changes, call the method {@link #save()} to mark the record as dirty in the current transaction, so the
 * changes will be persistent at {@link Transaction#commit()} time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see ImmutableVertex
 */
public class MutableVertex extends MutableDocument implements VertexInternal {
  private RID outEdges;
  private RID inEdges;

  /**
   * Creation constructor.
   */
  public MutableVertex(final Database graph, final VertexType type, final RID rid) {
    super(graph, type, rid);
  }

  /**
   * Copy constructor from ImmutableVertex.modify().
   */
  public MutableVertex(final Database graph, final VertexType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
    init();
  }

  @Override
  public MutableVertex save() {
    return (MutableVertex) super.save();
  }

  @Override
  public MutableVertex save(final String bucketName) {
    return (MutableVertex) super.save(bucketName);
  }

  @Override
  public void reload() {
    super.reload();
    init();
  }

  @Override
  public MutableVertex fromMap(final Map<String, Object> map) {
    return (MutableVertex) super.fromMap(map);
  }

  @Override
  public MutableVertex fromJSON(final JSONObject json) {
    return (MutableVertex) super.fromJSON(json);
  }

  @Override
  public MutableVertex set(final String name, final Object value) {
    return (MutableVertex) super.set(name, value);
  }

  public MutableVertex set(final Object... properties) {
    return (MutableVertex) super.set(properties);
  }

  @Override
  public MutableVertex set(final Map<String, Object> properties) {
    super.set(properties);
    return this;
  }

  @Override
  public void setBuffer(final Binary buffer) {
    super.setBuffer(buffer);
    init();
  }

  @Override
  public MutableVertex modify() {
    return this;
  }

  public RID getOutEdgesHeadChunk() {
    return outEdges;
  }

  public RID getInEdgesHeadChunk() {
    return inEdges;
  }

  @Override
  public void setOutEdgesHeadChunk(final RID outEdges) {
    dirty = true;
    this.outEdges = outEdges;
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    dirty = true;
    this.inEdges = inEdges;
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  @Override
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final Object... properties) {
    return database.getGraphEngine().newEdge(this, edgeType, toVertex, properties);
  }

  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return database.getGraphEngine().newEdge(this, edgeType, toVertex, properties);
  }

  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    return database.getGraphEngine().newLightEdge(this, edgeType, toVertex);
  }

  @Deprecated
  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return database.getGraphEngine().newLightEdge(this, edgeType, toVertex);
  }

  @Override
  public long countEdges(final DIRECTION direction, final String edgeType) {
    return database.getGraphEngine().countEdges(this, direction, edgeType);
  }

  @Override
  public IterableGraph<Edge> getEdges() {
    return database.getGraphEngine().getEdges(this);
  }

  @Override
  public IterableGraph<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getEdges(this, direction, edgeTypes);
  }

  @Override
  public IterableGraph<Vertex> getVertices() {
    return database.getGraphEngine().getVertices(this);
  }

  @Override
  public IterableGraph<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getVertices(this, direction, edgeTypes);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    return database.getGraphEngine().isVertexConnectedTo(this, toVertex);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return database.getGraphEngine().isVertexConnectedTo(this, toVertex, direction);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeTypes) {
    return database.getGraphEngine().isVertexConnectedTo(this, toVertex, direction, edgeTypes);
  }

  @Override
  public RID moveToType(final String targetType) {
    return database.getGraphEngine().moveToType(this, targetType);
  }

  @Override
  public RID moveToBucket(String targetBucket) {
    return database.getGraphEngine().moveToBucket(this, targetBucket);
  }

  @Override
  public Vertex asVertex() {
    return this;
  }

  @Override
  public Vertex asVertex(final boolean loadContent) {
    return this;
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> map = super.toMap(includeMetadata);
    if (includeMetadata)
      map.put(Property.CAT_PROPERTY, "v");
    return map;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject json = super.toJSON(includeMetadata);
    if (includeMetadata)
      json.put(Property.CAT_PROPERTY, "v");
    return json;
  }

  private void init() {
    if (buffer != null) {
      buffer.position(1);
      this.outEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (this.outEdges.getBucketId() == -1)
        this.outEdges = null;
      this.inEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (this.inEdges.getBucketId() == -1)
        this.inEdges = null;

      this.propertiesStartingPosition = buffer.position();
    }
  }
}
