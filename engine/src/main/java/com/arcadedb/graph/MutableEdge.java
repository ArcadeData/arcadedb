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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Transaction;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Map;

/**
 * Mutable edge that supports updates. After any changes, call the method {@link #save()} to mark the record as dirty
 * in the current transaction, so the
 * changes will be persistent at {@link Transaction#commit()} time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see ImmutableEdge
 */
public class MutableEdge extends MutableDocument implements Edge {
  protected RID out;
  protected RID in;

  public MutableEdge(final Database graph, final EdgeType type, final RID out, final RID in) {
    super(graph, type, null);
    this.out = out;
    this.in = in;
  }

  public MutableEdge(final Database graph, final EdgeType type, final RID edgeRID, final RID out, final RID in) {
    super(graph, type, edgeRID);
    this.out = out;
    this.in = in;
  }

  public MutableEdge(final Database graph, final EdgeType type, final RID rid) {
    super(graph, type, rid);
  }

  /**
   * Builds an edge without the eagerly-allocated property map. Only for {@link MutableLightEdge}, which can never
   * hold properties.
   */
  protected MutableEdge(final Database graph, final EdgeType type, final RID edgeRID, final RID out, final RID in,
                        final boolean withoutProperties) {
    super(graph, type, edgeRID, withoutProperties);
    this.out = out;
    this.in = in;
  }

  public MutableEdge(final Database graph, final EdgeType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
    init();
  }

  public MutableEdge modify() {
    return this;
  }

  @Override
  public Object get(final String propertyName) {
    if ("@in".equals(propertyName))
      return in;
    else if ("@out".equals(propertyName))
      return out;
    return super.get(propertyName);
  }

  @Override
  public void setBuffer(final Binary buffer) {
    super.setBuffer(buffer);
    init();
  }

  /**
   * Same reason as {@code MutableVertex.parseRecordPrefix()}, with the variable-length prefix of {@link ImmutableEdge}:
   * a stale {@code propertiesStartingPosition} can point past the properties of the reloaded buffer (issue #9985).
   */
  @Override
  protected void parseRecordPrefix(final Binary content) {
    parseVertexPointers(content);
  }

  @Override
  public RID getOut() {
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    return (Vertex) database.lookupByRID(out, false);
  }

  @Override
  public RID getIn() {
    return in;
  }

  @Override
  public Vertex getInVertex() {
    return (Vertex) database.lookupByRID(in, false);
  }

  @Override
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
    if (iDirection == Vertex.DIRECTION.OUT)
      return (Vertex) database.lookupByRID(out, false);
    else
      return (Vertex) database.lookupByRID(in, false);
  }

  @Override
  public MutableEdge set(final Object... properties) {
    super.set(properties);
    return this;
  }

  @Override
  public MutableEdge set(final Map<String, Object> properties) {
    super.set(properties);
    return this;
  }

  @Override
  public MutableEdge set(final String name, final Object value) {
    return (MutableEdge) super.set(name, value);
  }

  /**
   * {@code @in} and {@code @out} re-point the edge. Declared here, at the one funnel every setter of
   * {@link MutableDocument} goes through, so the positional overloads, {@code set(Map)}, {@code fromMap} and
   * {@code merge} all move the edge the same way (issue #9799).
   */
  @Override
  protected boolean isAssignableMetadata(final String name) {
    return Property.IN_PROPERTY.equals(name) || Property.OUT_PROPERTY.equals(name);
  }

  @Override
  protected void setMetadata(final String name, final Object value) {
    final Vertex.DIRECTION direction = Property.IN_PROPERTY.equals(name) ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT;
    final RID newEndpoint = toRID(value);
    if (newEndpoint == null)
      throw new IllegalArgumentException(
          "Cannot set " + name + " to " + (value == null ? "null" : "'" + abbreviate(value.toString()) + "'")
              + ": an edge endpoint must be a record, a RID or a RID string");

    // A move needs an endpoint to move FROM: an instance whose endpoint is still unknown is being initialized, as when
    // JavaBinarySerializer rebuilds an edge from its toMap() before the trailing endpoint block is read
    final RID current = direction == Vertex.DIRECTION.IN ? in : out;
    if (rid != null && current != null && !newEndpoint.equals(current) && database instanceof DatabaseInternal dbInt)
      dbInt.getGraphEngine().moveEdge(this, direction, newEndpoint);
    else if (direction == Vertex.DIRECTION.IN)
      this.in = newEndpoint;
    else
      this.out = newEndpoint;
  }

  @Override
  public MutableEdge set(final String name1, final Object value1, final String name2, final Object value2) {
    return (MutableEdge) super.set(name1, value1, name2, value2);
  }

  @Override
  public MutableEdge set(final String name1, final Object value1, final String name2, final Object value2,
                         final String name3, final Object value3) {
    return (MutableEdge) super.set(name1, value1, name2, value2, name3, value3);
  }

  @Override
  public MutableEdge fromMap(final Map<String, Object> map) {
    return (MutableEdge) super.fromMap(map);
  }

  @Override
  public MutableEdge fromJSON(final JSONObject json) {
    return (MutableEdge) super.fromJSON(json);
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  @Override
  public MutableEdge save() {
    if (isLightweight())
      return this;

    return (MutableEdge) super.save();
  }

  @Override
  public MutableEdge save(final String bucketName) {
    if (isLightweight())
      return this;

    return (MutableEdge) super.save(bucketName);
  }

  /** A lightweight edge has nothing to write, and so nothing a listener could have refused. */
  @Override
  public boolean saveReportingVeto() {
    return isLightweight() || super.saveReportingVeto();
  }

  @Override
  public boolean saveReportingVeto(final String bucketName) {
    return isLightweight() || super.saveReportingVeto(bucketName);
  }

  /** A lightweight edge owns no record: its identity is a negative position. */
  private boolean isLightweight() {
    return getIdentity() != null && getIdentity().getPosition() < 0;
  }

  @Override
  public Edge asEdge() {
    return this;
  }

  @Override
  public Edge asEdge(final boolean loadContent) {
    return this;
  }

  public void setOut(final RID out) {
    this.out = out;
  }

  public void setIn(final RID in) {
    this.in = in;
  }

  void updateIdentity(final RID newRid, final RID newOut, final RID newIn) {
    this.rid = newRid;
    this.out = newOut;
    this.in = newIn;
    this.buffer = null;
  }

  /** Keeps an arbitrary caller value from flooding the exception message and the logs that carry it. */
  private static String abbreviate(final String text) {
    return text.length() <= 64 ? text : text.substring(0, 64) + "...";
  }

  private RID toRID(final Object value) {
    if (value instanceof RID r) return r;
    if (value instanceof Identifiable i) return i.getIdentity();
    // The textual form a JSON payload (fromJSON, an HTTP document) carries the endpoint in
    if (value instanceof String s && RID.is(s)) return RID.create(database, s);
    return null;
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> map = super.toMap(includeMetadata);
    if (includeMetadata) {
      map.put(Property.CAT_PROPERTY, "e");
      map.put("@in", in);
      map.put("@out", out);
    }
    return map;
  }

  public JSONObject toJSON(final boolean includeMetadata) {
    JSONObject json = super.toJSON(includeMetadata);
    if (includeMetadata)
      json.put(Property.CAT_PROPERTY, "e").put("@in", in).put("@out", out);
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

  private void init() {
    // READ THE FIELD ONCE, LIKE EVERY OTHER BUFFER DEREFERENCE
    final Binary content = buffer;
    if (content != null)
      parseVertexPointers(content);
  }

  private void parseVertexPointers(final Binary content) {
    ImmutableEdge.checkVertexPointersPrefix(content, rid);
    content.position(1);
    out = ImmutableEdge.readVertexPointer(database, content, rid);
    in = ImmutableEdge.readVertexPointer(database, content, rid);
    this.propertiesStartingPosition = content.position();
  }
}
