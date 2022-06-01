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
import com.arcadedb.serializer.BinaryTypes;
import org.json.JSONObject;

import java.util.Map;

/**
 * Mutable edge that supports updates. After any changes, call the method {@link #save()} to mark the record as dirty in the current transaction, so the
 * changes will be persistent at {@link Transaction#commit()} time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see ImmutableEdge
 */
public class MutableEdge extends MutableDocument implements Edge {
  private RID out;
  private RID in;

  public MutableEdge(final Database graph, final DocumentType type, final RID out, RID in) {
    super(graph, type, null);
    this.out = out;
    this.in = in;
  }

  public MutableEdge(final Database graph, final DocumentType type, final RID edgeRID, final RID out, RID in) {
    super(graph, type, edgeRID);
    this.out = out;
    this.in = in;
  }

  public MutableEdge(final Database graph, final DocumentType type, final RID rid) {
    super(graph, type, rid);
  }

  public MutableEdge(final Database graph, final DocumentType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
    init();
  }

  public synchronized  MutableEdge modify() {
    return this;
  }

  @Override
  public synchronized  void reload() {
    super.reload();
    init();
  }

  @Override
  public  synchronized void setBuffer(final Binary buffer) {
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
  public Vertex getVertex(final Vertex.DIRECTION iDirection) {
    if (iDirection == Vertex.DIRECTION.OUT)
      return out.asVertex();
    else
      return in.asVertex();
  }

  @Override
  public  synchronized MutableEdge set(final Object... properties) {
    super.set(properties);
    checkForUpgradeLightWeight();
    return this;
  }

  @Override
  public synchronized  MutableEdge set(final String name, final Object value) {
    super.set(name, value);
    checkForUpgradeLightWeight();
    return this;
  }

  @Override
  public synchronized MutableEdge fromMap(final Map<String, Object> map) {
    return (MutableEdge) super.fromMap(map);
  }

  @Override
  public synchronized MutableEdge fromJSON(final JSONObject json) {
    return (MutableEdge) super.fromJSON(json);
  }

  @Override
  public synchronized MutableEdge set(final Map<String, Object> properties) {
    return (MutableEdge) super.set(properties);
  }

  @Override
  public byte getRecordType() {
    return Edge.RECORD_TYPE;
  }

  @Override
  public  synchronized MutableEdge save() {
    if (getIdentity() != null && getIdentity().getPosition() < 0)
      // LIGHTWEIGHT
      return this;

    return (MutableEdge) super.save();
  }

  @Override
  public synchronized  MutableEdge save(final String bucketName) {
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

  private void init() {
    if (buffer != null) {
      buffer.position(1);
      out = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      in = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      this.propertiesStartingPosition = buffer.position();
    }
  }

  private void checkForUpgradeLightWeight() {
    if (rid != null && rid.getPosition() < 0) {
      // REMOVE THE TEMPORARY RID SO IT WILL BE CREATED AT SAVE TIME
      rid = null;

      save();

      // UPDATE BOTH REFERENCES WITH THE NEW RID
      database.getGraphEngine().connectEdge((VertexInternal) out.asVertex(true), in, this, true);
    }
  }
}
