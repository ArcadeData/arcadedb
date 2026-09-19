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
package com.arcadedb.serializer;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * Java binary serializer. Used to serialize/deserialize Java objects. Null properties are excluded from serialization.
 */
public class JavaBinarySerializer {
  public static void writeExternal(final Document document, final ObjectOutput out) throws IOException {
    // RID
    final RID rid = document.getIdentity();
    out.writeInt(rid != null ? rid.getBucketId() : -1);
    out.writeLong(rid != null ? rid.getPosition() : -1);

    final DatabaseInternal db = (DatabaseInternal) document.getDatabase();
    final DocumentType documentType = document.getType();

    final BinarySerializer serializer = db.getSerializer();

    final Binary buffer = db.getContext().getTemporaryBuffer1();

    // Count must be written before the payload, so stage the entries to know the final count up-front.
    final Map<String, Object> properties = document.toMap();
    final ByteArrayOutputStream staged = new ByteArrayOutputStream();
    int validCount = 0;
    try (final DataOutputStream stagedOut = new DataOutputStream(staged)) {
      for (final Map.Entry<String, Object> prop : properties.entrySet()) {
        final Object propValue = prop.getValue();
        if (propValue == null)
          continue;
        final String propName = prop.getKey();
        final Property schemaProp = documentType.getPropertyIfExists(propName);
        final byte type = BinaryTypes.getTypeFromValue(propValue, schemaProp);
        if (type == -1) {
          LogManager.instance()
              .log(BinaryTypes.class, Level.WARNING,
                  "Cannot serialize property '%s' of type %s, value %s. The property will be ignored",
                  propName, propValue.getClass(), propValue);
          continue;
        }
        buffer.clear();
        buffer.putByte(type);
        serializer.serializeValue(db, buffer, type, propValue);
        buffer.flip();

        stagedOut.writeUTF(propName);
        stagedOut.writeInt(buffer.size());
        stagedOut.write(buffer.getContent(), 0, buffer.size());
        validCount++;
      }
    }

    out.writeInt(validCount);
    out.write(staged.toByteArray());

    // SPECIAL OPERATION FOR VERTICES AND EDGES
    //
    // #7908: narrowed to the INTERFACES that declare the four accessors below, not to the MUTABLE classes. Both
    // arms used to cast to MutableVertex/MutableEdge, so every graph record the engine hands back from a scan, a
    // query or lookupByRID - an ImmutableVertex/ImmutableEdge/ImmutableLightEdge - died with a ClassCastException
    // inside writeObject, while the identical document path worked. The narrowing bought nothing: the head-chunk
    // pointers are declared on VertexInternal and the endpoints on Edge itself.
    if (document instanceof VertexInternal vertex) {
      final RID outRID = vertex.getOutEdgesHeadChunk();
      out.writeInt(outRID != null ? outRID.getBucketId() : -1);
      out.writeLong(outRID != null ? outRID.getPosition() : -1);

      final RID inRID = vertex.getInEdgesHeadChunk();
      out.writeInt(inRID != null ? inRID.getBucketId() : -1);
      out.writeLong(inRID != null ? inRID.getPosition() : -1);
    } else if (document instanceof Edge edge) {
      final RID outRID = edge.getOut();
      out.writeInt(outRID != null ? outRID.getBucketId() : -1);
      out.writeLong(outRID != null ? outRID.getPosition() : -1);

      final RID inRID = edge.getIn();
      out.writeInt(inRID != null ? inRID.getBucketId() : -1);
      out.writeLong(inRID != null ? inRID.getPosition() : -1);
    }
  }

  public static void readExternal(final Document document, final ObjectInput in) throws IOException {
    if (!(document instanceof MutableDocument))
      throw new IllegalStateException("Error on deserialization: the current object is immutable");

    final MutableDocument mutable = (MutableDocument) document;

    final DatabaseInternal db = (DatabaseInternal) document.getDatabase();

    // RID
    final RID rid = db.newRID(in.readInt(), in.readLong());
    mutable.setIdentity(rid.isValid() ? rid : null);

    // PROPERTIES
    final Map<String, Object> properties = new LinkedHashMap<>();
    final int propertyCount = in.readInt();
    for (int i = 0; i < propertyCount; i++) {
      final String propName = in.readUTF();
      final int propertySize = in.readInt();

      final byte[] array = new byte[propertySize];
      in.readFully(array);

      final Binary buffer = new Binary(array);
      final byte propType = buffer.getByte();
      final Object propValue = db.getSerializer().deserializeValue(db, buffer, propType, null);

      properties.put(propName, propValue);
    }
    mutable.fromMap(properties);

    // SPECIAL OPERATION FOR VERTICES AND EDGES - the same two arms writeExternal uses, so the two halves agree on
    // which records carry the trailing endpoint block. The target is a MutableDocument (rejected above otherwise),
    // so a Vertex here is always a MutableVertex and an Edge always a MutableEdge; pattern-matched rather than cast
    // so the pairing is checked by the compiler instead of by a cast that can only fail at runtime (#7908).
    if (document instanceof MutableVertex vertex) {
      // Edge-chain head chunks are internal pointers; bare RID is sufficient since they never leak to user code as identities.
      final RID outRID = new RID(in.readInt(), in.readLong());
      vertex.setOutEdgesHeadChunk(outRID.isValid() ? outRID : null);

      final RID inRID = new RID(in.readInt(), in.readLong());
      vertex.setInEdgesHeadChunk(inRID.isValid() ? inRID : null);

    } else if (document instanceof MutableEdge edge) {
      // Edge endpoints are user-facing via edge.getOut()/getIn(): bind to the database so asVertex() works across threads.
      final RID outRID = db.newRID(in.readInt(), in.readLong());
      edge.setOut(outRID.isValid() ? outRID : null);

      final RID inRID = db.newRID(in.readInt(), in.readLong());
      edge.setIn(inRID.isValid() ? inRID : null);
    }
  }
}
