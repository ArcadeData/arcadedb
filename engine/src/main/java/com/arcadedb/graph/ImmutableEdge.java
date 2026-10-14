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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONObject;

import java.nio.BufferUnderflowException;
import java.util.Map;

/**
 * Immutable read-only edge. It is returned from database on read operations such as queries or lookups and graph traversal. To modify an edge use {@link #modify()}
 * to have the MutableEdge instance created form the current record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see MutableEdge
 */
public class ImmutableEdge extends ImmutableDocument implements Edge {
  // Record type (1 byte) + two compressed RIDs of one single-byte varint for the bucket and one for the position each:
  // the shortest prefix an edge can have, the only bound on it known before the RIDs are read
  private static final int MIN_PREFIX_SIZE = Binary.BYTE_SERIALIZED_SIZE + 2 * 2;

  private RID     out;
  private RID     in;
  // The endpoints were handed over by the edge list the edge was read from, not by whoever built the edge: the record
  // still has content, and an edge that cannot read it must not pass for one that never had any (see modify())
  private boolean endpointsFromEdgeList;

  public ImmutableEdge(final Database graph, final DocumentType type, final RID edgeRID, final RID out, final RID in) {
    super(graph, type, edgeRID, null);
    this.out = out;
    this.in = in;
  }

  public ImmutableEdge(final Database graph, final DocumentType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
    if (buffer != null)
      parseVertexPointers(buffer);
  }

  /**
   * Reads the edge prefix - the record-type byte followed by the out and in vertex RIDs, both compressed, so the
   * prefix has no fixed length - and leaves the buffer on the first byte of the properties.
   */
  private void parseVertexPointers(final Binary content) {
    checkVertexPointersPrefix(content, rid);
    content.position(1); // SKIP RECORD TYPE
    out = readVertexPointer(database, content, rid);
    in = readVertexPointer(database, content, rid);
    propertiesStartingPosition = content.position();
  }

  /**
   * The bounds check of the edge prefix that can be made before it is read, shared with {@link MutableEdge} like
   * {@link #readVertexPointer} (issue #9985). The prefix has no fixed length, so only its shortest form bounds it here:
   * the RIDs themselves are checked as they are read.
   */
  static void checkVertexPointersPrefix(final Binary content, final RID rid) {
    if (content.size() < MIN_PREFIX_SIZE)
      throw truncatedVertexPointers(content, rid, null);
  }

  /**
   * Reads one compressed vertex RID of the edge prefix, shared with {@link MutableEdge} so every shape that parses it
   * refuses a truncated buffer the same way (issue #9985). The prefix has no fixed length, so it cannot be validated
   * up front the way {@link ImmutableVertex} validates its own: each RID is checked as it is read instead. A buffer
   * that ends inside a RID underflows, and one whose backing array is longer than its declared size reads past that
   * size without failing at all - both name the record instead of escaping as a bare
   * {@link BufferUnderflowException}, or as a RID made of whatever followed. The guard on the normal path is one
   * comparison, and the try block costs nothing until it throws.
   */
  static RID readVertexPointer(final DatabaseInternal database, final Binary content, final RID rid) {
    final RID pointer;
    try {
      pointer = (RID) database.getSerializer().deserializeValue(database, content, BinaryTypes.TYPE_COMPRESSED_RID, null);
    } catch (final BufferUnderflowException | IndexOutOfBoundsException e) {
      throw truncatedVertexPointers(content, rid, e);
    }
    if (content.position() > content.size())
      throw truncatedVertexPointers(content, rid, null);
    return pointer;
  }

  private static SerializationException truncatedVertexPointers(final Binary content, final RID rid, final RuntimeException cause) {
    return new SerializationException(
        "Cannot read edge " + rid + " vertex pointers: record buffer is truncated or corrupted (size=" + content.size() + ")",
        cause);
  }

  /**
   * Hands a not-yet-loaded edge the endpoints the edge list it was read from already holds (issue #8537).
   * <p>
   * An entry of a vertex's edge list is the pair (edge RID, vertex at the other end), and the list belongs to one
   * endpoint in one direction, so an edge reached by walking it knows both of its endpoints without its record. With
   * them set, {@link #getOut()}, {@link #getIn()} and their vertex twins answer without loading the record, which is
   * what makes "follow the edge to its other end" cost what walking the adjacency costs: the record is read only
   * when something asks for the edge's own content. The endpoints of an edge never change after it is created
   * (moving a vertex rewrites both the edge record and the lists that point at it), so the pair is the same answer
   * the record would give. A no-op on an edge that already has them.
   */
  synchronized void setEndpointsFromEdgeList(final RID out, final RID in) {
    if (this.out == null && this.in == null) {
      this.out = out;
      this.in = in;
      this.endpointsFromEdgeList = true;
    }
  }

  public synchronized MutableEdge modify() {
    if (prepareForModify("edge") instanceof MutableEdge fromCache)
      return fromCache;

    checkForLazyLoading();
    final Binary content = buffer;
    if (content != null) {
      content.rewind();
      return markIfReadWentStale(new MutableEdge(database, (EdgeType) type, rid, content.copyOfContent()));
    }
    // AN EDGE BUILT OVER ITS TWO ENDPOINTS HAS NO RECORD CONTENT TO CARRY OVER, AND MODIFYING IT IS LEGITIMATE. BOTH
    // ENDPOINTS MISSING INSTEAD MEANS THE CONTENT SHOULD HAVE BEEN THERE AND IS NOT, AND THIS BRANCH WOULD HAND BACK A
    // MutableEdge WITH NO ENDPOINTS THAT SILENTLY DROPS THE EDGE'S PROPERTIES ON THE NEXT save()
    if (endpointsFromEdgeList || (out == null && in == null))
      requireBuffer("modify");
    return new MutableEdge(database, (EdgeType) type, rid, out, in);
  }

  @Override
  public synchronized Object get(final String propertyName) {
    if (Property.IN_PROPERTY.equals(propertyName))
      return in;
    else if (Property.OUT_PROPERTY.equals(propertyName))
      return out;
    return super.get(propertyName);
  }

  /**
   * Under the same monitor as {@link #get(String)}. {@code @in}/{@code @out} are answered by {@code get()} but not reported
   * by {@code has()}, so here they are absent, exactly as the {@code has()}-then-{@code get()} pair said.
   */
  @Override
  public synchronized Object getIfPresent(final String propertyName, final Object absentValue) {
    return super.getIfPresent(propertyName, absentValue);
  }

  @Override
  public synchronized RID getOut() {
    if (out == null)
      checkForLazyLoading();
    return out;
  }

  @Override
  public synchronized Vertex getOutVertex() {
    if (out == null)
      checkForLazyLoading();
    return (Vertex) database.lookupByRID(out, false);
  }

  @Override
  public synchronized RID getIn() {
    if (in == null)
      checkForLazyLoading();
    return in;
  }

  @Override
  public synchronized Vertex getInVertex() {
    if (in == null)
      checkForLazyLoading();
    return (Vertex) database.lookupByRID(in, false);
  }

  @Override
  public synchronized Vertex getVertex(final Vertex.DIRECTION iDirection) {
    if (out == null || in == null)
      checkForLazyLoading();
    if (iDirection == Vertex.DIRECTION.OUT)
      return (Vertex) database.lookupByRID(out, false);
    else
      return (Vertex) database.lookupByRID(in, false);
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  @Override
  public Edge asEdge() {
    return this;
  }

  @Override
  public Edge asEdge(final boolean loadContent) {
    if (loadContent)
      checkForLazyLoading();
    return this;
  }

  @Override
  public synchronized Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> map = super.toMap(includeMetadata);
    if (includeMetadata) {
      map.put(Property.CAT_PROPERTY, "e");
      map.put(Property.IN_PROPERTY, in);
      map.put(Property.OUT_PROPERTY, out);
    }
    return map;
  }

  @Override
  public synchronized JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject json = super.toJSON(includeMetadata);
    if (includeMetadata)
      json.put(Property.CAT_PROPERTY, "e").put(Property.IN_PROPERTY, in).put(Property.OUT_PROPERTY, out);
    return json;
  }

  @Override
  public synchronized String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("V(");
    buffer.append(out != null ? out.toString() : "?");
    buffer.append(")->[E");
    buffer.append(rid != null ? rid.toString() : "?");
    buffer.append("]->V(");
    buffer.append(in != null ? in.toString() : "?");
    buffer.append(")");
    return buffer.toString();
  }

  /**
   * Same shape as {@code ImmutableVertex.checkForLazyLoading()}: the buffer is read ONCE and the guard is explicit.
   * It used to survive {@code super} filtering the record away - which leaves the buffer {@code null} and returns
   * {@code false} - only by the short-circuit order of the {@code ||}, by accident rather than by design.
   */
  @Override
  protected boolean checkForLazyLoading() {
    if (rid == null)
      return false;
    final boolean materialised = super.checkForLazyLoading();
    final Binary content = buffer;
    if (content == null)
      return false;
    if (materialised || content.position() == 1) {
      parseVertexPointers(content);
      return true;
    }
    return false;
  }

  /**
   * Same reason as {@code ImmutableVertex.parseRecordPrefix()}: the out/in RIDs and the offset they push the
   * properties to belong to the buffer they were read from, and a reload installs another one. The prefix is not even
   * a fixed length here, the two RIDs being compressed, so a stale {@code propertiesStartingPosition} can point past
   * the properties rather than merely at the wrong one.
   */
  @Override
  protected void parseRecordPrefix(final Binary content) {
    parseVertexPointers(content);
  }
}
