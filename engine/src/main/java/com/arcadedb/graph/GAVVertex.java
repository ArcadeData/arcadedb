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
import com.arcadedb.database.DetachedDocument;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lazy-loading GAV-backed Vertex proxy. Implements the full {@link Vertex} interface
 * while deferring OLTP vertex loading as long as possible.
 * <p>
 * Property access reads from the GAV column store first (O(1) array access).
 * Adjacency queries (neighbors, edge counts) use CSR arrays.
 * Only falls back to OLTP ({@code lookupByRID}) when:
 * <ul>
 *   <li>A requested property is not in the column store</li>
 *   <li>A mutating operation is called (modify, newEdge, delete)</li>
 *   <li>Edge objects are needed (not just adjacency)</li>
 * </ul>
 * <p>
 * Memory: 3 object fields (RID, provider ref, lazy vertex ref) + 1 int.
 * No HashMap, no binary buffer, no property deserialization until needed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GAVVertex implements Vertex {
  private final RID                    rid;
  private final int                    nodeId;
  private final GraphTraversalProvider provider;
  private final Database               database;
  private volatile Vertex              resolved; // lazy OLTP load, cached

  public GAVVertex(final RID rid, final int nodeId, final GraphTraversalProvider provider, final Database database) {
    this.rid = rid;
    this.nodeId = nodeId;
    this.provider = provider;
    this.database = database;
  }

  public int getNodeId() {
    return nodeId;
  }

  public GraphTraversalProvider getProvider() {
    return provider;
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Identity (zero cost — stored in fields)
  // ═══════════════════════════════════════════════════════════════════

  @Override
  public RID getIdentity() {
    return rid;
  }

  @Override
  public String getTypeName() {
    return database.getSchema().getTypeByBucketId(rid.getBucketId()).getName();
  }

  @Override
  public DocumentType getType() {
    return database.getSchema().getTypeByBucketId(rid.getBucketId());
  }

  @Override
  public byte getRecordType() {
    return Vertex.RECORD_TYPE;
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Property access — column store first, OLTP fallback
  // ═══════════════════════════════════════════════════════════════════

  @Override
  public Object get(final String propertyName) {
    final Object colValue = provider.getProperty(nodeId, propertyName);
    if (colValue != null)
      return colValue;
    return resolve().get(propertyName);
  }

  @Override
  public boolean has(final String propertyName) {
    final Object colValue = provider.getProperty(nodeId, propertyName);
    if (colValue != null)
      return true;
    return resolve().has(propertyName);
  }

  @Override
  public Set<String> getPropertyNames() {
    return resolve().getPropertyNames();
  }

  @Override
  public String getString(final String p) {
    final Object v = get(p);
    return v instanceof String ? (String) v : v != null ? v.toString() : null;
  }

  @Override
  public Boolean getBoolean(final String p) {
    final Object v = get(p);
    return v instanceof Boolean ? (Boolean) v : null;
  }

  @Override
  public Byte getByte(final String p) {
    final Object v = get(p);
    return v instanceof Number ? ((Number) v).byteValue() : null;
  }

  @Override
  public Short getShort(final String p) {
    final Object v = get(p);
    return v instanceof Number ? ((Number) v).shortValue() : null;
  }

  @Override
  public Integer getInteger(final String p) {
    final Object v = get(p);
    return v instanceof Number ? ((Number) v).intValue() : null;
  }

  @Override
  public Long getLong(final String p) {
    final Object v = get(p);
    return v instanceof Number ? ((Number) v).longValue() : null;
  }

  @Override
  public Float getFloat(final String p) {
    final Object v = get(p);
    return v instanceof Number ? ((Number) v).floatValue() : null;
  }

  @Override
  public Double getDouble(final String p) {
    final Object v = get(p);
    return v instanceof Number ? ((Number) v).doubleValue() : null;
  }

  @Override
  public BigDecimal getDecimal(final String p) { return resolve().getDecimal(p); }

  @Override
  public byte[] getBinary(final String p) { return resolve().getBinary(p); }

  @Override
  public Date getDate(final String p) { return resolve().getDate(p); }

  @Override
  public Calendar getCalendar(final String p) { return resolve().getCalendar(p); }

  @Override
  public LocalDate getLocalDate(final String p) { return resolve().getLocalDate(p); }

  @Override
  public LocalDateTime getLocalDateTime(final String p) { return resolve().getLocalDateTime(p); }

  @Override
  public ZonedDateTime getZonedDateTime(final String p) { return resolve().getZonedDateTime(p); }

  @Override
  public Instant getInstant(final String p) { return resolve().getInstant(p); }

  @Override
  public Map<String, Object> getMap(final String p) { return resolve().getMap(p); }

  @Override
  public <T> List<T> getList(final String p) { return resolve().getList(p); }

  @Override
  public EmbeddedDocument getEmbedded(final String p) { return resolve().getEmbedded(p); }

  // ═══════════════════════════════════════════════════════════════════
  //  Adjacency — CSR arrays (zero OLTP)
  // ═══════════════════════════════════════════════════════════════════

  @Override
  public long countEdges(final DIRECTION direction, final String... edgeTypes) {
    return provider.countEdges(nodeId, direction, edgeTypes);
  }

  @Override
  public IterableGraph<Vertex> getVertices() {
    return resolve().getVertices();
  }

  @Override
  public IterableGraph<Vertex> getVertices(final DIRECTION direction, final String... edgeTypes) {
    return resolve().getVertices(direction, edgeTypes);
  }

  @Override
  public Iterable<RID> getConnectedVertexRIDs(final DIRECTION direction, final String... edgeTypes) {
    // Use CSR for RID-only traversal — no OLTP
    final int[] neighborIds = provider.getNeighborIds(nodeId, direction, edgeTypes);
    return () -> new java.util.Iterator<>() {
      private int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < neighborIds.length;
      }

      @Override
      public RID next() {
        return provider.getRID(neighborIds[idx++]);
      }
    };
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex) {
    return resolve().isConnectedTo(toVertex);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction) {
    return resolve().isConnectedTo(toVertex, direction);
  }

  @Override
  public boolean isConnectedTo(final Identifiable toVertex, final DIRECTION direction, final String edgeType) {
    final int targetNodeId = provider.getNodeId(toVertex.getIdentity());
    if (targetNodeId >= 0)
      return provider.isConnectedTo(nodeId, targetNodeId, direction, edgeType);
    return resolve().isConnectedTo(toVertex, direction, edgeType);
  }

  @Override
  public IterableGraph<Edge> getEdges() {
    return resolve().getEdges();
  }

  @Override
  public IterableGraph<Edge> getEdges(final DIRECTION direction, final String... edgeTypes) {
    return resolve().getEdges(direction, edgeTypes);
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Mutation — always resolves to OLTP vertex
  // ═══════════════════════════════════════════════════════════════════

  @Override
  public MutableVertex modify() {
    return resolve().modify();
  }

  @Override
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final Object... properties) {
    return resolve().newEdge(edgeType, toVertex, properties);
  }

  @Override
  @Deprecated
  public MutableEdge newEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional, final Object... properties) {
    return resolve().newEdge(edgeType, toVertex, bidirectional, properties);
  }

  @Override
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex) {
    return resolve().newLightEdge(edgeType, toVertex);
  }

  @Override
  @Deprecated
  public ImmutableLightEdge newLightEdge(final String edgeType, final Identifiable toVertex, final boolean bidirectional) {
    return resolve().newLightEdge(edgeType, toVertex, bidirectional);
  }

  @Override
  public RID moveToType(final String targetType) {
    return resolve().moveToType(targetType);
  }

  @Override
  public RID moveToBucket(final String targetBucket) {
    return resolve().moveToBucket(targetBucket);
  }

  @Override
  public void reload() {
    resolved = null;
  }

  @Override
  public void delete() {
    resolve().delete();
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Serialization — delegates to resolved vertex
  // ═══════════════════════════════════════════════════════════════════

  @Override
  public JSONObject toJSON(final boolean includeMetadata) { return resolve().toJSON(includeMetadata); }

  @Override
  public JSONObject toJSON(final String... includeProperties) { return resolve().toJSON(includeProperties); }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) { return resolve().toMap(includeMetadata); }

  @Override
  public Map<String, Object> propertiesAsMap() { return resolve().propertiesAsMap(); }

  @Override
  public DetachedDocument detach() { return resolve().detach(); }

  @Override
  public int size() { return -1; }

  // ═══════════════════════════════════════════════════════════════════
  //  Internals
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Lazily loads the OLTP vertex. Cached for subsequent calls.
   */
  private Vertex resolve() {
    Vertex v = resolved;
    if (v == null) {
      v = (Vertex) database.lookupByRID(rid, true);
      resolved = v;
    }
    return v;
  }

  @Override
  public Record getRecord() {
    return resolve();
  }

  @Override
  public Record getRecord(final boolean loadContent) {
    return loadContent ? resolve() : this;
  }

  @Override
  public Edge asEdge() {
    throw new UnsupportedOperationException("GAVVertex is a vertex, not an edge");
  }

  @Override
  public Edge asEdge(final boolean loadContent) {
    throw new UnsupportedOperationException("GAVVertex is a vertex, not an edge");
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj instanceof Identifiable other)
      return rid.getBucketId() == other.getIdentity().getBucketId()
          && rid.getPosition() == other.getIdentity().getPosition();
    return false;
  }

  @Override
  public String toString() {
    return rid.toString();
  }
}
