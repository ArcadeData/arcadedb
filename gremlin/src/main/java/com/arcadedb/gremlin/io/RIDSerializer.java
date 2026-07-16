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
package com.arcadedb.gremlin.io;

import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;

import java.io.IOException;

/**
 * GraphBinary custom-type serializer for ArcadeDB {@link RID} (and its {@code DatabaseRID} subclass). Without it, any
 * Gremlin Server traversal whose result carries a raw RID (e.g. the value of a LINK property, or an explicit RID
 * projection) fails with "Serializer for type com.arcadedb.database.DatabaseRID not found". The RID is encoded as the
 * pair (bucketId:int, position:long) so it round-trips symmetrically between an ArcadeDB server and an ArcadeDB client,
 * both of which register {@link ArcadeIoRegistry}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RIDSerializer implements CustomTypeSerializer<RID> {
  public static final String TYPE_NAME = "arcadedb.RID";

  @Override
  public String getTypeName() {
    return TYPE_NAME;
  }

  @Override
  public DataType getDataType() {
    return DataType.CUSTOM;
  }

  @Override
  public RID read(final Buffer buffer, final GraphBinaryReader context) throws IOException {
    // FULLY-QUALIFIED FORM: THE {type_code}{type_name} HAVE ALREADY BEEN CONSUMED BY THE READER
    return readValue(buffer, context, true);
  }

  @Override
  public RID readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws IOException {
    if (nullable) {
      final byte valueFlag = buffer.readByte();
      if ((valueFlag & 1) == 1)
        return null;
    }
    final int bucketId = buffer.readInt();
    final long position = buffer.readLong();
    return new RID(bucketId, position);
  }

  @Override
  public void write(final RID value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
    // FULLY-QUALIFIED FORM: THE {type_code}{type_name} HAVE ALREADY BEEN WRITTEN BY THE WRITER
    writeValue(value, buffer, context, true);
  }

  @Override
  public void writeValue(final RID value, final Buffer buffer, final GraphBinaryWriter context, final boolean nullable) throws IOException {
    if (value == null) {
      if (!nullable)
        throw new IOException("Unexpected null value when nullable is false");
      context.writeValueFlagNull(buffer);
      return;
    }

    if (nullable)
      context.writeValueFlagNone(buffer);

    buffer.writeInt(value.getBucketId());
    buffer.writeLong(value.getPosition());
  }
}
