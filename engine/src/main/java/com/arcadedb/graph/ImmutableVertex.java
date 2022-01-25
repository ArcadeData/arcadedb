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
import com.arcadedb.database.EmbeddedModifierProperty;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Immutable read-only vertex. It is returned from database on read operations such as queries or lookups. To modify a vertex use {@link #modify()} to have the
 * MutableVertex instance created form the current record. Adding or removing edges does not change the state of the vertex as dirty because edges are managed
 * on different data structures. Any operation on edges is actually computed on the most updated version of the vertex in transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see MutableVertex
 */
public class ImmutableVertex extends ImmutableDocument implements VertexInternal {
  private RID outEdges;
  private RID inEdges;

  public ImmutableVertex(final Database database, final DocumentType type, final RID rid, final Binary buffer) {
    super(database, type, rid, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      outEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (outEdges.getBucketId() == -1)
        outEdges = null;
      inEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (inEdges.getBucketId() == -1)
        inEdges = null;
      propertiesStartingPosition = buffer.position();
    }
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  public synchronized MutableVertex modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null && recordInCache != this && recordInCache instanceof MutableVertex)
      return (MutableVertex) recordInCache;

    checkForLazyLoading();
    buffer.rewind();
    return new MutableVertex(database, type, rid, buffer.copy());
  }

  @Override
  public synchronized Object get(final String propertyName) {
    checkForLazyLoading();
    final Map<String, Object> map = database.getSerializer()
        .deserializeProperties(database, buffer, new EmbeddedModifierProperty(this, propertyName), propertyName);
    return map.get(propertyName);
  }

  @Override
  public synchronized RID getOutEdgesHeadChunk() {
    checkForLazyLoading();
    return outEdges;
  }

  @Override
  public synchronized RID getInEdgesHeadChunk() {
    checkForLazyLoading();
    return inEdges;
  }

  @Override
  public void setOutEdgesHeadChunk(final RID outEdges) {
    throw new UnsupportedOperationException("setOutEdgesHeadChunk");
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    throw new UnsupportedOperationException("setInEdgesHeadChunk");
  }

  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional, final Object... properties) {
    return database.getGraphEngine().newEdge(getMostUpdatedVertex(this), edgeType, toVertex, bidirectional, properties);
  }

  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    return database.getGraphEngine().newLightEdge(getMostUpdatedVertex(this), edgeType, toVertex, bidirectional);
  }

  @Override
  public long countEdges(DIRECTION direction, String edgeType) {
    return database.getGraphEngine().countEdges(getMostUpdatedVertex(this), direction, edgeType);
  }

  @Override
  public Iterable<Edge> getEdges() {
    return database.getGraphEngine().getEdges(getMostUpdatedVertex(this));
  }

  @Override
  public Iterable<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getEdges(getMostUpdatedVertex(this), direction, edgeTypes);
  }

  @Override
  public Iterable<Vertex> getVertices() {
    return database.getGraphEngine().getVertices(getMostUpdatedVertex(this));
  }

  @Override
  public Iterable<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getVertices(getMostUpdatedVertex(this), direction, edgeTypes);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    return database.getGraphEngine().isVertexConnectedTo(getMostUpdatedVertex(this), toVertex);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return database.getGraphEngine().isVertexConnectedTo(getMostUpdatedVertex(this), toVertex, direction);
  }

  @Override
  public Vertex asVertex() {
    return this;
  }

  @Override
  public Vertex asVertex(boolean loadContent) {
    return this;
  }

  @Override
  protected boolean checkForLazyLoading() {
    if (super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      outEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (outEdges.getBucketId() == -1)
        outEdges = null;
      inEdges = new RID(database, buffer.getInt(), buffer.getLong());
      if (inEdges.getBucketId() == -1)
        inEdges = null;
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }

  private VertexInternal getMostUpdatedVertex(final VertexInternal vertex) {
    if (!database.isTransactionActive())
      return vertex;

    VertexInternal mostUpdated = (VertexInternal) database.getTransaction().getRecordFromCache(vertex.getIdentity());
    if (mostUpdated == null)
      mostUpdated = vertex;
    return mostUpdated;
  }
}
