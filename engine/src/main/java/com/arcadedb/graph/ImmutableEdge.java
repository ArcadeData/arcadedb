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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.Map;

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
    if (buffer != null)
      parseVertexPointers(buffer);
  }

  /**
   * Reads the fixed edge prefix - the record-type byte followed by the out and in vertex RIDs, both compressed, so the
   * prefix has no fixed length - and leaves the buffer on the first byte of the properties.
   */
  private void parseVertexPointers(final Binary content) {
    content.position(1); // SKIP RECORD TYPE
    out = (RID) database.getSerializer().deserializeValue(database, content, BinaryTypes.TYPE_COMPRESSED_RID, null);
    in = (RID) database.getSerializer().deserializeValue(database, content, BinaryTypes.TYPE_COMPRESSED_RID, null);
    propertiesStartingPosition = content.position();
  }

  public synchronized MutableEdge modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null) {
      if (recordInCache instanceof MutableEdge edge)
        return edge;
      else if (!database.getTransaction().hasPageForRecord(rid.getPageId(database))) {
        // THE RECORD IS NOT IN TX, SO IT MUST HAVE BEEN LOADED WITHOUT A TX OR PASSED FROM ANOTHER TX
        // IT MUST BE RELOADED TO GET THE LATEST CHANGES. FORCE RELOAD
        try {
          // RELOAD THE PAGE FIRST TO AVOID LOOP WITH TRIGGERS (ENCRYPTION)
          database.getTransaction().getPageToModify(rid.getPageId(database),
              ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).getPageSize(), false);
          reload();
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error on reloading edge " + rid, e);
        }
      }
    }

    checkForLazyLoading();
    final Binary content = buffer;
    if (content != null) {
      content.rewind();
      return new MutableEdge(database, (EdgeType) type, rid, content.copyOfContent());
    }
    // AN EDGE BUILT OVER ITS TWO ENDPOINTS HAS NO RECORD CONTENT TO CARRY OVER, AND MODIFYING IT IS LEGITIMATE. BOTH
    // ENDPOINTS MISSING INSTEAD MEANS THE CONTENT SHOULD HAVE BEEN THERE AND IS NOT, AND THIS BRANCH WOULD HAND BACK A
    // MutableEdge WITH NO ENDPOINTS THAT SILENTLY DROPS THE EDGE'S PROPERTIES ON THE NEXT save()
    if (out == null && in == null)
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

  @Override
  public synchronized RID getOut() {
    checkForLazyLoading();
    return out;
  }

  @Override
  public synchronized Vertex getOutVertex() {
    checkForLazyLoading();
    return (Vertex) database.lookupByRID(out, false);
  }

  @Override
  public synchronized RID getIn() {
    checkForLazyLoading();
    return in;
  }

  @Override
  public synchronized Vertex getInVertex() {
    checkForLazyLoading();
    return (Vertex) database.lookupByRID(in, false);
  }

  @Override
  public synchronized Vertex getVertex(final Vertex.DIRECTION iDirection) {
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
