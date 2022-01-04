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
 */
package org.apache.tinkerpop.gremlin.arcadedb.structure.io.binary;

import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;

import java.io.IOException;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RIDBinarySerializer implements CustomTypeSerializer {
  @Override
  public String getTypeName() {
    return RID.class.getTypeName();
  }

  @Override
  public DataType getDataType() {
    return DataType.CUSTOM;
  }

  @Override
  public Object read(final Buffer buffer, final GraphBinaryReader context) throws IOException {
    return new RID(null, buffer.readInt(), buffer.readLong());
  }

  @Override
  public Object readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws IOException {
    return new RID(null, buffer.readInt(), buffer.readLong());
  }

  @Override
  public void write(final Object value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
    final RID rid = (RID) value;
    buffer.writeInt(rid.getBucketId());
    buffer.writeLong(rid.getPosition());
  }

  @Override
  public void writeValue(final Object value, final Buffer buffer, final GraphBinaryWriter context, final boolean nullable) throws IOException {
    final RID rid = (RID) value;
    buffer.writeInt(rid.getBucketId());
    buffer.writeLong(rid.getPosition());
  }
}
