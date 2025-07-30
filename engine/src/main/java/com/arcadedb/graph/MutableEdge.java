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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Transaction;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

/**
 * Mutable edge that supports updates. After any changes, call the method {@link #save()} to mark the record as dirty in the current transaction, so the
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

  public MutableEdge(final Database graph, final EdgeType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
    init();
  }

  public MutableEdge modify() {
    return this;
  }

  @Override
  public void reload() {
    super.reload();
    init();
  }

  @Override
  public Object get(final String propertyName) {
    if (propertyName.equals("@in"))
      return in;
    else if (propertyName.equals("@out"))
      return out;
    return super.get(propertyName);
  }

  @Override
  public void setBuffer(final Binary buffer) {
    super.setBuffer(buffer);
    init();
  }

  @Override
  public RID getOut() {
    return out;
  }

  @Override
  public Vertex getOutVertex() {
    return out.asVertex();
  }

  @Override
  public RID getIn() {
    return in;
  }

  @Override
  public Vertex getInVertex() {
    return in.asVertex();
  }

  @Override
  public Vertex getVertex(final Vertex.Direction iDirection) {
    if (iDirection == Vertex.Direction.OUT)
      return out.asVertex();
    else
      return in.asVertex();
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
    super.set(name, value);
    return this;
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
    if (getIdentity() != null && getIdentity().getPosition() < 0)
      // LIGHTWEIGHT
      return this;

    return (MutableEdge) super.save();
  }

  @Override
  public MutableEdge save(final String bucketName) {
    if (getIdentity() != null && getIdentity().getPosition() < 0)
      // LIGHTWEIGHT
      return this;

    return (MutableEdge) super.save(bucketName);
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
    if (buffer != null) {
      buffer.position(1);
      out = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      in = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      this.propertiesStartingPosition = buffer.position();
    }
  }
}
