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

import com.arcadedb.database.*;
import com.arcadedb.schema.DocumentType;
import org.json.JSONObject;

import java.util.Map;

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
  public MutableVertex(final Database graph, final DocumentType type, final RID rid) {
    super(graph, type, rid);
  }

  /**
   * Copy constructor from ImmutableVertex.modify().
   */
  public MutableVertex(final Database graph, final DocumentType type, final RID rid, final Binary buffer) {
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
  public synchronized  void reload() {
    super.reload();
    init();
  }

  @Override
  public synchronized MutableVertex fromMap(Map<String, Object> map) {
    return (MutableVertex) super.fromMap(map);
  }

  @Override
  public synchronized MutableVertex fromJSON(JSONObject json) {
    return (MutableVertex) super.fromJSON(json);
  }

  @Override
  public MutableVertex set(final String name, final Object value) {
    return (MutableVertex) super.set(name, value);
  }

  public synchronized MutableVertex set(final Object... properties) {
    return (MutableVertex) super.set(properties);
  }

  @Override
  public  synchronized void setBuffer(final Binary buffer) {
    super.setBuffer(buffer);
    init();
  }

  @Override
  public synchronized  MutableVertex modify() {
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
    this.outEdges = outEdges;
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    this.inEdges = inEdges;
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional, final Object... properties) {
    return database.getGraphEngine().newEdge(this, edgeType, toVertex, bidirectional, properties);
  }

  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    return database.getGraphEngine().newLightEdge(this, edgeType, toVertex, bidirectional);
  }

  @Override
  public long countEdges(DIRECTION direction, String edgeType) {
    return database.getGraphEngine().countEdges(this, direction, edgeType);
  }

  @Override
  public Iterable<Edge> getEdges() {
    return database.getGraphEngine().getEdges(this);
  }

  @Override
  public Iterable<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getEdges(this, direction, edgeTypes);
  }

  @Override
  public Iterable<Vertex> getVertices() {
    return database.getGraphEngine().getVertices(this);
  }

  @Override
  public Iterable<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
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
  public Vertex asVertex() {
    return this;
  }

  @Override
  public Vertex asVertex(final boolean loadContent) {
    return this;
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
