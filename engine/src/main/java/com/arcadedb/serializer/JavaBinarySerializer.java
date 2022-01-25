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

import com.arcadedb.database.*;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Java binary serializer. Used to serialize/deserialize Java objects. Null properties are excluded from serialization.
 */
public class JavaBinarySerializer {
  public static void writeExternal(final Document document, final ObjectOutput out) throws IOException {
    // RID
    final RID rid = document.getIdentity();
    out.writeInt(rid != null ? rid.getBucketId() : -1);
    out.writeLong(rid != null ? rid.getPosition() : -1);

    final DatabaseInternal db = ((DatabaseInternal) document.getDatabase());

    final BinarySerializer serializer = db.getSerializer();

    final Binary buffer = db.getContext().getTemporaryBuffer1();

    // PROPERTY COUNT
    final Set<String> properties = document.getPropertyNames();
    out.writeInt(properties.size());
    for (String propName : properties) {
      // PROPERTY NAME
      out.writeUTF(propName);

      final Object propValue = document.get(propName);
      if (propValue != null) {
        // PROPERTY VALUE
        buffer.clear();

        final byte type = BinaryTypes.getTypeFromValue(propValue);
        buffer.putByte(type);
        serializer.serializeValue(db, buffer, type, propValue);
        buffer.flip();

        out.writeInt(buffer.size());
        out.write(buffer.getContent(), 0, buffer.size());
      }
    }

    // SPECIAL OPERATION FOR VERTICES AND EDGES
    if (document instanceof Vertex) {
      final RID outRID = ((MutableVertex) document).getOutEdgesHeadChunk();
      out.writeInt(outRID != null ? outRID.getBucketId() : -1);
      out.writeLong(outRID != null ? outRID.getPosition() : -1);

      final RID inRID = ((MutableVertex) document).getInEdgesHeadChunk();
      out.writeInt(inRID != null ? inRID.getBucketId() : -1);
      out.writeLong(inRID != null ? inRID.getPosition() : -1);
    } else if (document instanceof Edge) {
      final RID outRID = ((MutableEdge) document).getOut();
      out.writeInt(outRID != null ? outRID.getBucketId() : -1);
      out.writeLong(outRID != null ? outRID.getPosition() : -1);

      final RID inRID = ((MutableEdge) document).getIn();
      out.writeInt(inRID != null ? inRID.getBucketId() : -1);
      out.writeLong(inRID != null ? inRID.getPosition() : -1);
    }
  }

  public static void readExternal(final Document document, final ObjectInput in) throws IOException, ClassNotFoundException {
    if (!(document instanceof MutableDocument))
      throw new IllegalStateException("Error on deserialization: the current object is immutable");

    final MutableDocument mutable = (MutableDocument) document;

    final DatabaseInternal db = ((DatabaseInternal) document.getDatabase());

    // RID
    final RID rid = new RID(db, in.readInt(), in.readLong());
    mutable.setIdentity(rid.isValid() ? rid : null);

    // PROPERTIES
    final Map<String, Object> properties = new LinkedHashMap<>();
    final int propertyCount = in.readInt();
    for (int i = 0; i < propertyCount; i++) {
      final String propName = in.readUTF();
      final int propertySize = in.readInt();

      final byte[] array = new byte[propertySize];
      in.read(array);

      Binary buffer = new Binary(array);
      final byte propType = buffer.getByte();
      final Object propValue = db.getSerializer().deserializeValue(db, buffer, propType, null);

      properties.put(propName, propValue);
    }
    mutable.fromMap(properties);

    // SPECIAL OPERATION FOR VERTICES AND EDGES
    if (document instanceof Vertex) {
      final RID outRID = new RID(db, in.readInt(), in.readLong());
      ((MutableVertex) document).setOutEdgesHeadChunk(outRID.isValid() ? outRID : null);

      final RID inRID = new RID(db, in.readInt(), in.readLong());
      ((MutableVertex) document).setInEdgesHeadChunk(inRID.isValid() ? inRID : null);

    } else if (document instanceof Edge) {
      final RID outRID = new RID(db, in.readInt(), in.readLong());
      ((MutableEdge) document).setOut(outRID.isValid() ? outRID : null);

      final RID inRID = new RID(db, in.readInt(), in.readLong());
      ((MutableEdge) document).setIn(inRID.isValid() ? inRID : null);
    }
  }
}
