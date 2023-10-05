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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONObject;

import java.io.*;
import java.util.*;

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

  public synchronized MutableEdge modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null) {
      if (recordInCache instanceof MutableEdge)
        return (MutableEdge) recordInCache;
      else if (!database.getTransaction().hasPageForRecord(rid.getPageId())) {
        // THE RECORD IS NOT IN TX, SO IT MUST HAVE BEEN LOADED WITHOUT A TX OR PASSED FROM ANOTHER TX
        // IT MUST BE RELOADED TO GET THE LATEST CHANGES. FORCE RELOAD
        try {
          // RELOAD THE PAGE FIRST TO AVOID LOOP WITH TRIGGERS (ENCRYPTION)
          database.getTransaction()
              .getPageToModify(rid.getPageId(), database.getSchema().getBucketById(rid.getBucketId()).getPageSize(), false);
          reload();
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error on reloading edge " + rid, e);
        }
      }
    }

    checkForLazyLoading();
    if (buffer != null) {
      buffer.rewind();
      return new MutableEdge(database, (EdgeType) type, rid, buffer.copyOfContent());
    }
    return new MutableEdge(database, (EdgeType) type, rid, getOut(), getIn());
  }

  @Override
  public synchronized Object get(final String propertyName) {
    if ("@in".equals(propertyName))
      return in;
    else if ("@out".equals(propertyName))
      return out;
    return super.get(propertyName);
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
  public synchronized Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> map = super.toMap(includeMetadata);
    if (includeMetadata) {
      map.put("@cat", "e");
      map.put("@in", in);
      map.put("@out", out);
    }
    return map;
  }

  @Override
  public synchronized JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject json = super.toJSON(includeMetadata);
    if (includeMetadata)
      json.put("@cat", "e").put("@in", in).put("@out", out);
    return json;
  }

  @Override
  public synchronized String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(out != null ? out.toString() : "?");
    buffer.append("<->");
    buffer.append(in != null ? in.toString() : "?");
    return buffer.toString();
  }

  @Override
  protected boolean checkForLazyLoading() {
    if (rid != null && (super.checkForLazyLoading() || (buffer != null && buffer.position() == 1))) {
      buffer.position(1); // SKIP RECORD TYPE
      out = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      in = (RID) database.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      propertiesStartingPosition = buffer.position();
      return true;
    }
    return false;
  }
}
