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
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.BinaryTypes;

/**
 * Immutable read-only edge. It is returned from database on read operations such as queries or lookups and graph traversal. To modify an edge use {@link #modify()}
 * to have the MutableEdge instance created form the current record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see MutableEdge
 */
public class ImmutableEdge extends ImmutableDocument implements Edge {
  private RID out;
  private RID in;

  public ImmutableEdge(final Database graph, final DocumentType type, final RID edgeRID, final RID out, final RID in) {
    super(graph, type, edgeRID, null);
    this.out = out;
    this.in = in;
  }

  public ImmutableEdge(final Database graph, final DocumentType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
    if (buffer != null) {
      buffer.position(1); // SKIP RECORD TYPE
      out = (RID) database.getSerializer().deserializeValue(graph, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      in = (RID) database.getSerializer().deserializeValue(graph, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      propertiesStartingPosition = buffer.position();
    }
  }

  @Override
  public Object get(final String propertyName) {
    return super.get(propertyName);
  }

  public synchronized MutableEdge modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != this && recordInCache instanceof MutableEdge)
      return (MutableEdge) recordInCache;

    checkForLazyLoading();
    if (buffer != null) {
      buffer.rewind();
      return new MutableEdge(database, type, rid, buffer.copy());
    }
    return new MutableEdge(database, type, rid, getOut(), getIn());
  }

  @Override
  public synchronized RID getOut() {
    checkForLazyLoading();
    return out;
  }

  @Override
  public synchronized Vertex getOutVertex() {
    checkForLazyLoading();
    return out.asVertex();
  }

  @Override
  public synchronized RID getIn() {
    checkForLazyLoading();
    return in;
  }

  @Override
  public synchronized Vertex getInVertex() {
    checkForLazyLoading();
    return in.asVertex();
  }

  @Override
  public synchronized Vertex getVertex(final Vertex.DIRECTION iDirection) {
    checkForLazyLoading();
    if (iDirection == Vertex.DIRECTION.OUT)
      return out.asVertex();
    else
      return in.asVertex();
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
    return this;
  }

  @Override
  protected boolean checkForLazyLoading() {
    if (rid != null && super.checkForLazyLoading()) {
      buffer.position(1); // SKIP RECORD TYPE
      out = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      in = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }

  @Override
  public synchronized String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(out.toString());
    buffer.append("<->");
    buffer.append(in.toString());
    return buffer.toString();
  }
}
