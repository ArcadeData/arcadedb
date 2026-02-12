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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Mutable vertex that supports updates. After any changes, call the method {@link #save()} to mark the record as
 * dirty in the current transaction, so the
 * changes will be persistent at {@link Transaction#commit()} time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see ImmutableVertex
 */
public class MutableVertex extends MutableDocument implements VertexInternal {
  // v1 format fields - after migration, everything is v1 (Map key = edge bucket ID)
  private Map<Integer, RID> outEdgesMap;
  private Map<Integer, RID> inEdgesMap;

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
    // Serialization of v1 edge maps is now handled by BinarySerializer.serializeVertex()
    return (MutableVertex) super.save();
  }

  @Override
  public MutableVertex save(final String bucketName) {
    // Serialization of v1 edge maps is now handled by BinarySerializer.serializeVertex()
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

  @Override
  public MutableVertex modify() {
    return this;
  }

  public RID getOutEdgesHeadChunk() {
    // Return first edge list if map is not empty (for backward compatibility)
    return outEdgesMap != null && !outEdgesMap.isEmpty() ? outEdgesMap.values().iterator().next() : null;
  }

  public RID getInEdgesHeadChunk() {
    // Return first edge list if map is not empty (for backward compatibility)
    return inEdgesMap != null && !inEdgesMap.isEmpty() ? inEdgesMap.values().iterator().next() : null;
  }

  /**
   * Get outgoing edges head chunk for a specific edge bucket.
   */
  public RID getOutEdgesHeadChunk(final int bucketId) {
    return outEdgesMap != null ? outEdgesMap.get(bucketId) : null;
  }

  /**
   * Get incoming edges head chunk for a specific edge bucket.
   */
  public RID getInEdgesHeadChunk(final int bucketId) {
    return inEdgesMap != null ? inEdgesMap.get(bucketId) : null;
  }

  /**
   * Get all outgoing edge bucket IDs.
   */
  public Set<Integer> getOutEdgeBuckets() {
    return outEdgesMap != null ? outEdgesMap.keySet() : Collections.emptySet();
  }

  /**
   * Get all incoming edge bucket IDs.
   */
  public Set<Integer> getInEdgeBuckets() {
    return inEdgesMap != null ? inEdgesMap.keySet() : Collections.emptySet();
  }

  @Override
  public void setOutEdgesHeadChunk(final RID outEdges) {
    dirty = true;
    // For backward compatibility, store in map with bucket ID 0
    if (outEdgesMap == null)
      outEdgesMap = new HashMap<>();
    if (outEdges != null)
      outEdgesMap.put(0, outEdges);
  }

  /**
   * Set outgoing edges head chunk for a specific edge bucket.
   */
  public void setOutEdgesHeadChunk(final int bucketId, final RID headRID) {
    dirty = true;
    if (outEdgesMap == null)
      outEdgesMap = new HashMap<>();
    if (headRID != null)
      outEdgesMap.put(bucketId, headRID);
    else
      outEdgesMap.remove(bucketId);
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    dirty = true;
    // For backward compatibility, store in map with bucket ID 0
    if (inEdgesMap == null)
      inEdgesMap = new HashMap<>();
    if (inEdges != null)
      inEdgesMap.put(0, inEdges);
  }

  /**
   * Set incoming edges head chunk for a specific edge bucket.
   */
  public void setInEdgesHeadChunk(final int bucketId, final RID headRID) {
    dirty = true;
    if (inEdgesMap == null)
      inEdgesMap = new HashMap<>();
    if (headRID != null)
      inEdgesMap.put(bucketId, headRID);
    else
      inEdgesMap.remove(bucketId);
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
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex,
                                         final boolean bidirectional) {
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return database.getGraphEngine().newLightEdge(this, edgeType, toVertex);
  }

  @Override
  public long countEdges(final DIRECTION direction, final String... edgeTypes) {
    return database.getGraphEngine().countEdges(this, direction, edgeTypes);
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
      buffer.position(1); // SKIP RECORD TYPE

      // v1 format: edge maps with varlong encoding
      // [TYPE][OUT_MAP_SIZE: varlong][OUT_MAP_ENTRIES][IN_MAP_SIZE: varlong][IN_MAP_ENTRIES][PROPERTIES]

      // Read outgoing edges map
      final int outMapSize = (int) buffer.getNumber();
      if (outMapSize > 0) {
        outEdgesMap = new HashMap<>(outMapSize);
        for (int i = 0; i < outMapSize; i++) {
          final int bucketId = (int) buffer.getNumber();
          final int ridBucketId = (int) buffer.getNumber();
          final long ridPosition = buffer.getNumber();
          final RID headRID = new RID(database, ridBucketId, ridPosition);
          outEdgesMap.put(bucketId, headRID);
        }
      } else {
        outEdgesMap = new HashMap<>();
      }

      // Read incoming edges map
      final int inMapSize = (int) buffer.getNumber();
      if (inMapSize > 0) {
        inEdgesMap = new HashMap<>(inMapSize);
        for (int i = 0; i < inMapSize; i++) {
          final int bucketId = (int) buffer.getNumber();
          final int ridBucketId = (int) buffer.getNumber();
          final long ridPosition = buffer.getNumber();
          final RID headRID = new RID(database, ridBucketId, ridPosition);
          inEdgesMap.put(bucketId, headRID);
        }
      } else {
        inEdgesMap = new HashMap<>();
      }

      this.propertiesStartingPosition = buffer.position();
    }
  }
}
