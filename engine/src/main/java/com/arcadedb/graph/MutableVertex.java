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
import com.arcadedb.database.Record;
import com.arcadedb.database.Transaction;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Map;

/**
 * Mutable vertex that supports updates. After any changes, call the method {@link #save()} to mark the record as
 * dirty in the current transaction, so the
 * changes will be persistent at {@link Transaction#commit()} time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see ImmutableVertex
 */
public class MutableVertex extends MutableDocument implements VertexInternal {
  private RID     outEdges;
  private RID     inEdges;
  // #10929: whether a head was set on THIS instance since it was last queued for update, see adoptEdgeListHeadsOf()
  private boolean outEdgesAssigned;
  private boolean inEdgesAssigned;

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

  @Override
  public MutableVertex set(final String name1, final Object value1, final String name2, final Object value2) {
    return (MutableVertex) super.set(name1, value1, name2, value2);
  }

  @Override
  public MutableVertex set(final String name1, final Object value1, final String name2, final Object value2,
                           final String name3, final Object value3) {
    return (MutableVertex) super.set(name1, value1, name2, value2, name3, value3);
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

  /**
   * A reload installs a buffer the parsed prefix never described: the edge pointers and the offset of the properties are
   * read out of it, the way {@link ImmutableVertex} does, instead of seeking it to the offset of the previous one
   * (issue #9985). {@link #reload()} reaches this through the inherited one, so it needs no override of its own.
   */
  @Override
  protected void parseRecordPrefix(final Binary content) {
    parseEdgePointers(content);
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
    this.outEdgesAssigned = true;
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    dirty = true;
    this.inEdges = inEdges;
    this.inEdgesAssigned = true;
  }

  /**
   * Restores the heads a copy of this vertex was made with (Java deserialization): the same as the copy
   * {@link ImmutableVertex#modify()} makes, not an edge-list write, so a later save still takes the heads the transaction
   * holds (see {@link #adoptEdgeListHeadsOf}).
   */
  public void restoreEdgeListHeads(final RID outEdges, final RID inEdges) {
    dirty = true;
    this.outEdges = outEdges;
    this.inEdges = inEdges;
  }

  /**
   * Called when this instance is queued for update, with the instance of the same vertex the transaction already holds as
   * written ({@code null} if none). Issue #10929: a copy taken by {@code modify()} before an edge was appended through
   * another handle carries the heads it was copied with, and the append moved the head on the instance the transaction
   * holds, never on the copy. Saving the copy is a property write, so it must not write those older heads back: that
   * unlinked every edge appended since from this vertex, while the edge record and the far endpoint kept it. The heads are
   * therefore taken from the held instance, except for a direction whose head was set on THIS instance since it was last
   * queued: that one is a deliberate edge-list write (the graph engine, a batch load, a repair) and is kept.
   */
  public void adoptEdgeListHeadsOf(final Record held) {
    if (held != this && held instanceof VertexInternal holder) {
      if (!outEdgesAssigned)
        outEdges = holder.getOutEdgesHeadChunk();
      if (!inEdgesAssigned)
        inEdges = holder.getInEdgesHeadChunk();
    }
    outEdgesAssigned = false;
    inEdgesAssigned = false;
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  @Override
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final Object... properties) {
    return database.getGraphEngine().newEdge(getMostUpdatedVertex(this), edgeType, toVertex, properties);
  }

  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
                             final Object... properties) {
    if (!bidirectional && database.getSchema().getType(edgeType) instanceof EdgeType type && type.isBidirectional())
      throw new IllegalArgumentException(GraphEngine.unidirectionalEdgeOnBidirectionalTypeMessage(edgeType));

    return database.getGraphEngine().newEdge(getMostUpdatedVertex(this), edgeType, toVertex, properties);
  }

  @Override
  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    return database.getGraphEngine().newLightEdge(getMostUpdatedVertex(this), edgeType, toVertex);
  }

  @Deprecated
  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex,
                                         final boolean bidirectional) {
    if (!bidirectional && database.getSchema().getType(edgeType) instanceof EdgeType type && type.isBidirectional())
      throw new IllegalArgumentException(GraphEngine.unidirectionalEdgeOnBidirectionalTypeMessage(edgeType));

    return database.getGraphEngine().newLightEdge(getMostUpdatedVertex(this), edgeType, toVertex);
  }

  @Override
  public long countEdges(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().countEdges(getMostUpdatedVertex(this), direction, edgeTypes);
  }

  @Override
  public IterableGraph<Edge> getEdges() {
    return database.getGraphEngine().getEdges(getMostUpdatedVertex(this));
  }

  @Override
  public IterableGraph<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getEdges(getMostUpdatedVertex(this), direction, edgeTypes);
  }

  @Override
  public IterableGraph<Vertex> getVertices() {
    return database.getGraphEngine().getVertices(getMostUpdatedVertex(this));
  }

  @Override
  public IterableGraph<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getVertices(getMostUpdatedVertex(this), direction, edgeTypes);
  }

  @Override
  public Iterable<RID> getConnectedVertexRIDs(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().getConnectedVertexRIDs(getMostUpdatedVertex(this), direction, edgeTypes);
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
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeTypes) {
    return database.getGraphEngine().isVertexConnectedTo(getMostUpdatedVertex(this), toVertex, direction, edgeTypes);
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

  /** The heads are read from the instance the transaction holds, as {@link ImmutableVertex} does (issue #10929). */
  private VertexInternal getMostUpdatedVertex(final VertexInternal vertex) {
    return database.getGraphEngine().getMostUpdatedVertex(vertex);
  }

  private void init() {
    // READ THE FIELD ONCE, LIKE EVERY OTHER BUFFER DEREFERENCE
    final Binary content = buffer;
    if (content != null)
      parseEdgePointers(content);
  }

  private void parseEdgePointers(final Binary content) {
    ImmutableVertex.checkEdgePointersPrefix(content, rid);
    content.position(1);
    this.outEdges = new RID(content.getInt(), content.getLong());
    if (this.outEdges.getBucketId() == -1)
      this.outEdges = null;
    this.inEdges = new RID(content.getInt(), content.getLong());
    if (this.inEdges.getBucketId() == -1)
      this.inEdges = null;

    this.propertiesStartingPosition = content.position();
    // WHAT THE BUFFER SAYS REPLACES WHATEVER WAS SET BEFORE
    this.outEdgesAssigned = false;
    this.inEdgesAssigned = false;
  }
}
