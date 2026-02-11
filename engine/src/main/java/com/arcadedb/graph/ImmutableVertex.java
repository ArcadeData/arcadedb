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
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable read-only vertex. It is returned from database on read operations such as queries or lookups. To modify a vertex use {@link #modify()} to have the
 * MutableVertex instance created form the current record. Adding or removing edges does not change the state of the vertex as dirty because edges are managed
 * on different data structures. Any operation on edges is actually computed on the most updated version of the vertex in transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see MutableVertex
 */
public class ImmutableVertex extends ImmutableDocument implements VertexInternal {
  // v1 format fields - after migration, everything is v1
  private Map<Integer, RID> outEdgesMap;
  private Map<Integer, RID> inEdgesMap;

  public ImmutableVertex(final Database database, final DocumentType type, final RID rid, final Binary buffer) {
    super(database, type, rid, buffer);
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
      }

      propertiesStartingPosition = buffer.position();
    }
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  public MutableVertex modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null) {
      if (recordInCache instanceof MutableVertex fromCache)
        return fromCache;
    } else if (!database.getTransaction().hasPageForRecord(rid.getPageId())) {
      // THE RECORD IS NOT IN TX, SO IT MUST HAVE BEEN LOADED WITHOUT A TX OR PASSED FROM ANOTHER TX
      // IT MUST BE RELOADED TO GET THE LATEST CHANGES. FORCE RELOAD
      try {
        // RELOAD THE PAGE FIRST TO AVOID LOOP WITH TRIGGERS (ENCRYPTION)
        database.getTransaction()
            .getPageToModify(rid.getPageId(), ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).getPageSize(),
                false);
        reload();
      } catch (final IOException e) {
        throw new DatabaseOperationException("Error on reloading vertex " + rid, e);
      }
    }

    checkForLazyLoading();
    buffer.rewind();
    return new MutableVertex(database, (VertexType) type, rid, buffer.copyOfContent());
  }

  @Override
  public void reload() {
    // FORCE THE RELOAD
    buffer = null;
    super.reload();
  }

  @Override
  public RID getOutEdgesHeadChunk() {
    checkForLazyLoading();
    // v1: return first edge list (for backward compatibility)
    if (outEdgesMap != null && !outEdgesMap.isEmpty())
      return outEdgesMap.values().iterator().next();
    return null;
  }

  @Override
  public RID getInEdgesHeadChunk() {
    checkForLazyLoading();
    // v1: return first edge list (for backward compatibility)
    if (inEdgesMap != null && !inEdgesMap.isEmpty())
      return inEdgesMap.values().iterator().next();
    return null;
  }

  @Override
  public void setOutEdgesHeadChunk(final RID outEdges) {
    throw new UnsupportedOperationException("setOutEdgesHeadChunk");
  }

  @Override
  public void setInEdgesHeadChunk(final RID inEdges) {
    throw new UnsupportedOperationException("setInEdgesHeadChunk");
  }

  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final Object... properties) {
    return database.getGraphEngine().newEdge(getMostUpdatedVertex(this), edgeType, toVertex, properties);
  }

  @Override
  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional,
      final Object... properties) {
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

    return database.getGraphEngine().newEdge(getMostUpdatedVertex(this), edgeType, toVertex, properties);
  }

  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    return database.getGraphEngine().newLightEdge(getMostUpdatedVertex(this), edgeType, toVertex);
  }

  @Override
  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    if (!bidirectional && ((EdgeType) database.getSchema().getType(edgeType)).isBidirectional())
      throw new IllegalArgumentException("Edge type '" + edgeType + "' is not bidirectional");

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
  public boolean isConnectedTo(final Identifiable toVertex) {
    return database.getGraphEngine().isVertexConnectedTo(getMostUpdatedVertex(this), toVertex);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return database.getGraphEngine().isVertexConnectedTo(getMostUpdatedVertex(this), toVertex, direction);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeType) {
    return database.getGraphEngine().isVertexConnectedTo(getMostUpdatedVertex(this), toVertex, direction, edgeType);
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
    if (loadContent)
      checkForLazyLoading();
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

  @Override
  protected boolean checkForLazyLoading() {
    if (super.checkForLazyLoading() || buffer != null && buffer.position() == 1) {
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
      }

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

  @Override
  public java.util.Set<Integer> getOutEdgeBuckets() {
    checkForLazyLoading();
    if (outEdgesMap != null)
      return outEdgesMap.keySet();
    return java.util.Collections.emptySet();
  }

  @Override
  public java.util.Set<Integer> getInEdgeBuckets() {
    checkForLazyLoading();
    if (inEdgesMap != null)
      return inEdgesMap.keySet();
    return java.util.Collections.emptySet();
  }

  @Override
  public RID getOutEdgesHeadChunk(final int bucketId) {
    checkForLazyLoading();
    if (outEdgesMap != null)
      return outEdgesMap.get(bucketId);
    return null;
  }

  @Override
  public RID getInEdgesHeadChunk(final int bucketId) {
    checkForLazyLoading();
    if (inEdgesMap != null)
      return inEdgesMap.get(bucketId);
    return null;
  }
}
